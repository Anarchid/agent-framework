/**
 * #145 end-to-end: a wake that arrives while a puppeted tool is executing
 * must not start a turn. Before the fix the scheduler saw an idle agent with
 * no turn alive, started streaming, and the puppet's tool_use/tool_result
 * pair then landed inside that turn.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { AgentFramework } from '../src/index.js';
import type { InferenceRequest } from '../src/index.js';
import { MockMembrane, createMockResponse } from './helpers/mock-membrane.js';

type Internals = {
  pendingRequests: InferenceRequest[];
  processInferenceRequests(): Promise<void>;
  activeTurnTokens: Map<string, number>;
  executeToolCall: (call: Record<string, unknown>) => Promise<unknown>;
};

describe('puppetToolCall vs a concurrent wake (#145)', () => {
  let tempDir: string;
  let membrane: MockMembrane;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'puppet-reservation-'));
    membrane = new MockMembrane();
  });
  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it('a wake during a held puppet execution is requeued; the pair lands before the turn it would have corrupted', async () => {
    membrane.pushResponse(createMockResponse([{ type: 'text', text: 'ok, I see it' }]));
    const framework = await AgentFramework.create({
      storePath: join(tempDir, 'test.chronicle'),
      membrane: membrane.asMembrane(),
      agents: [{ name: 'scout', model: 'test-model', systemPrompt: 'You are scout.' }],
      modules: [],
    });
    const i = framework as unknown as Internals;
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    i.executeToolCall = async () => {
      await gate;
      return { success: true, data: 'settings snapshot', isError: false };
    };
    const quiet = console.log;
    console.log = () => {};
    try {
      const puppet = framework.puppetToolCall('scout', 'agent_settings', { action: 'get' });
      await new Promise((r) => setImmediate(r));
      assert.equal(i.activeTurnTokens.has('scout'), true, 'reserved while the tool runs');

      // the race: a wake arrives mid-execution and the scheduler runs
      i.pendingRequests.push({ agentName: 'scout', reason: 'mcpl:channel-incoming', source: 'test', timestamp: Date.now() });
      await i.processInferenceRequests();
      assert.equal(membrane.calls.length, 0, 'no turn started under the puppet');
      assert.equal(i.pendingRequests.length, 1, 'the wake is requeued, not dropped');

      release();
      await puppet;
      assert.equal(i.activeTurnTokens.has('scout'), false, 'reservation released');

      await i.processInferenceRequests();
      await framework.runUntilIdle();
      assert.equal(membrane.calls.length, 1, 'the requeued wake ran once the puppet was done');

      const all = framework.getAgent('scout')!.getContextManager().getAllMessages() as Array<{ content: Array<{ type: string }> }>;
      const types = all.map((m) => m.content[0]?.type);
      const use = types.indexOf('tool_use');
      assert.ok(use >= 0, 'pair stored');
      assert.equal(types[use + 1], 'tool_result', 'pair adjacent');
      const turnText = types.lastIndexOf('text');
      assert.ok(turnText > use + 1, 'the turn the wake started comes AFTER the pair, and saw it');
    } finally {
      console.log = quiet;
      await framework.stop();
    }
  });
});
