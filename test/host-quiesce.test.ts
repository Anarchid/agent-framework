/**
 * Host quiesce/maintenance mode (issue #122) — deterministic state-machine
 * coverage. No child processes; the MCPL plane/barrier interplay is covered
 * by the awareness-barrier suite.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type {
  ContextStrategy,
  StrategyContext,
  ReadinessState,
  MessageStoreView,
  ContextLogView,
  TokenBudget,
  ContextEntry,
} from '@animalabs/context-manager';
import { AutobiographicalStrategy } from '@animalabs/context-manager';
import type { StreamEvent, YieldingStream, NormalizedRequest } from '@animalabs/membrane';

import { AgentFramework, ResumeBlockedError } from '../src/index.js';
import type { TraceEvent } from '../src/types/trace.js';
import { MockMembrane, createMockResponse } from './helpers/mock-membrane.js';

async function waitFor(
  description: string,
  predicate: () => boolean,
  timeoutMs = 3_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${description}`);
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

function autobiographical(): AutobiographicalStrategy {
  return new AutobiographicalStrategy({
    adaptiveResolution: true,
    foldingStrategy: 'kv-stable',
    recentWindowTokens: 30_000,
    kvStableReachTokens: 8_000,
  });
}

function stubPreview(
  framework: AgentFramework,
  agentName: string,
  impl: (budget: { maxTokens: number }) => Record<string, unknown>,
): void {
  const cm = framework.getAgent(agentName)!.getContextManager() as unknown as {
    previewContext?: (budget: { maxTokens: number }) => unknown;
  };
  cm.previewContext = (budget) => impl(budget);
}

async function withFramework(
  membrane: MockMembrane,
  fn: (framework: AgentFramework, dir: string) => Promise<void>,
  agentExtras: Record<string, unknown> = {},
): Promise<void> {
  const dir = mkdtempSync(join(tmpdir(), 'host-quiesce-'));
  const framework = await AgentFramework.create({
    storePath: join(dir, 'store'),
    membrane: membrane.asMembrane(),
    agents: [{
      name: 'agent',
      model: 'test-model',
      systemPrompt: 'test',
      ...agentExtras,
    }],
    modules: [],
  });
  try {
    await fn(framework, dir);
  } finally {
    await framework.stop();
    rmSync(dir, { recursive: true, force: true });
  }
}

test('quiesce parks wakes; resume releases them', async () => {
  const membrane = new MockMembrane();
  await withFramework(membrane, async (framework) => {
    framework.start();
    const before = await framework.quiesce({ reason: 'test window' });
    assert.equal(before.quiesced, true);
    assert.equal(before.drained, true);

    const nudge = framework.nudgeAgent('agent', 'operator');
    assert.equal(nudge.ok, true);
    // Give the running loop time to (not) start a turn.
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.equal(membrane.calls.length, 0, 'no inference while quiesced');
    assert.equal(framework.getAgent('agent')!.state.status, 'idle');
    assert.equal(framework.getHostModeStatus().gatedRequests, 1);

    const after = await framework.resume();
    assert.equal(after.quiesced, false);
    await waitFor('parked wake to fire', () => membrane.calls.length > 0);
  });
});

test('parked wakes coalesce per (agent, reason), keeping the set bounded', async () => {
  const membrane = new MockMembrane();
  await withFramework(membrane, async (framework) => {
    framework.start();
    await framework.quiesce();
    for (let i = 0; i < 5; i++) framework.nudgeAgent('agent', 'operator');
    // The loop's next pass compresses same-reason parked requests to one.
    await waitFor(
      'coalescing to a single parked request',
      () => framework.getHostModeStatus().gatedRequests === 1,
    );
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.equal(framework.getHostModeStatus().gatedRequests, 1, 'stays bounded');
    assert.equal(membrane.calls.length, 0);
  });
});

test('runUntilIdle returns with gated requests parked', async () => {
  const membrane = new MockMembrane();
  await withFramework(membrane, async (framework) => {
    await framework.quiesce();
    framework.nudgeAgent('agent', 'operator');
    // Must not hang: parked requests are not progress while quiesced.
    await framework.runUntilIdle();
    assert.equal(membrane.calls.length, 0);
  });
});

test('ephemeral admission is refused while quiesced', async () => {
  const membrane = new MockMembrane();
  await withFramework(membrane, async (framework) => {
    await framework.quiesce({ reason: 'surgery' });
    await assert.rejects(
      // Refused before the agent/CM are touched — a minimal stand-in suffices.
      framework.runEphemeralToCompletion(
        { name: 'ephemeral' } as never,
        {} as never,
      ),
      /quiesced \(surgery\).*resume\(\) first/,
    );
  });
});

test('drain waits for an in-flight turn; abandon cancels without failure accounting', async () => {
  // A stream that never completes until cancelled — models a hung provider.
  class HangingStream implements YieldingStream {
    private pendingResolve: (() => void) | null = null;
    private aborted = false;
    cancel(): void {
      this.aborted = true;
      this.pendingResolve?.();
    }
    provideToolResults(): void {}
    get isWaitingForTools() { return false; }
    get pendingToolCallIds(): string[] { return []; }
    get toolDepth() { return 0; }
    async *[Symbol.asyncIterator](): AsyncIterator<StreamEvent> {
      if (!this.aborted) {
        await new Promise<void>((resolve) => { this.pendingResolve = resolve; });
      }
      yield { type: 'aborted', reason: 'user' } as StreamEvent;
    }
  }
  class HangingMembrane extends MockMembrane {
    override streamYielding(request: NormalizedRequest): YieldingStream {
      this.calls.push(request);
      return new HangingStream();
    }
  }

  const membrane = new HangingMembrane();
  await withFramework(membrane, async (framework) => {
    const traces: TraceEvent[] = [];
    framework.onTrace((event) => traces.push(event));
    framework.start();
    framework.nudgeAgent('agent', 'operator');
    await waitFor('turn to start', () => membrane.calls.length === 1);

    const status = await framework.quiesce({ timeoutMs: 1_000, abandon: true });
    assert.equal(status.quiesced, true);
    assert.equal(status.drained, true, 'abandon settles the hung turn');
    await waitFor('agent back to idle', () =>
      framework.getAgent('agent')!.state.status === 'idle');

    assert.ok(
      traces.some((t) => t.type === 'inference:aborted'
        && (t as { reason?: string }).reason === 'quiesce_abandoned'),
      'abandon is traced as an operator abort',
    );
    assert.ok(
      !traces.some((t) => t.type === 'inference:exhausted'),
      'an operator cancel must not feed the failure streak / hard-down accounting',
    );
    const quiesceTrace = traces.find((t) => t.type === 'host:quiesce') as
      { abandoned?: boolean } | undefined;
    assert.equal(quiesceTrace?.abandoned, true);
  });
});

test('quiesce persists across restart; resume clears it durably', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'host-quiesce-persist-'));
  const storePath = join(dir, 'store');
  const config = () => ({
    storePath,
    membrane: new MockMembrane().asMembrane(),
    agents: [{ name: 'agent', model: 'test-model', systemPrompt: 'test' }],
    modules: [],
  });
  let framework = await AgentFramework.create(config());
  try {
    await framework.quiesce({ reason: 'refold in progress' });
    await framework.stop();

    framework = await AgentFramework.create(config());
    const restored = framework.getHostModeStatus();
    assert.equal(restored.quiesced, true, 'a restart mid-surgery boots quiesced');
    assert.equal(restored.reason, 'refold in progress');

    await framework.resume();
    await framework.stop();

    framework = await AgentFramework.create(config());
    assert.equal(framework.getHostModeStatus().quiesced, false);
  } finally {
    await framework.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('resume gates on a fresh feasibility verdict; force overrides', async () => {
  const membrane = new MockMembrane();
  await withFramework(membrane, async (framework) => {
    await framework.quiesce();
    stubPreview(framework, 'agent', (budget) => ({
      finalTokens: 450_000,
      budgetTokens: budget.maxTokens,
      fits: false,
      exhausted: true,
      headTokens: 1,
      tailTokens: 2,
      middleTokens: 3,
      middleChunkCount: 1,
      deepestLevel: 3,
      resolutions: {},
      moves: 0,
      producedCount: 0,
    }));
    await assert.rejects(
      framework.resume(),
      (error: unknown) => {
        assert.ok(error instanceof ResumeBlockedError);
        assert.equal(error.verdicts.length, 1);
        assert.equal(error.verdicts[0].agentName, 'agent');
        assert.match(error.message, /folded floor 450000/);
        return true;
      },
    );
    assert.equal(framework.getHostModeStatus().quiesced, true, 'still quiesced after refusal');

    const forced = await framework.resume({ force: true });
    assert.equal(forced.quiesced, false);
  }, {
    strategy: autobiographical(),
    contextBudgetTokens: 300_000,
    maxTokens: 10_000,
  });
});

test('resume proceeds with a warn when the strategy cannot preview', async () => {
  const membrane = new MockMembrane();
  await withFramework(membrane, async (framework) => {
    // Default PassthroughStrategy: no previewContext at all.
    await framework.quiesce();
    const status = await framework.resume();
    assert.equal(status.quiesced, false);
  });
});

test('maintenanceTick runs the real maintenance pass while quiesced', async () => {
  class QueuedStrategy implements ContextStrategy {
    readonly name = 'queued-test';
    ticks = 0;
    checkReadiness(): ReadinessState {
      return { ready: this.ticks >= 2, description: 'test maintenance queued' };
    }
    async tick(_ctx: StrategyContext): Promise<void> {
      this.ticks++;
    }
    select(
      _store: MessageStoreView,
      _log: ContextLogView,
      _budget: TokenBudget,
    ): ContextEntry[] {
      return [];
    }
  }
  const strategy = new QueuedStrategy();
  const membrane = new MockMembrane();
  await withFramework(membrane, async (framework) => {
    framework.start();
    await framework.quiesce({ reason: 'compression window' });
    const snapshot = await framework.maintenanceTick();
    assert.ok(strategy.ticks >= 2, `maintenance ticked while quiesced (${strategy.ticks})`);
    assert.ok(snapshot.agents.some((a) => a.agentName === 'agent'));
  }, { strategy });
});

// ---------------------------------------------------------------------------
// End-to-end MCPL leg: data plane held while quiesced, resume deliverable
// over host/command (control plane), funnel reopens and flushes the backlog.
// ---------------------------------------------------------------------------

const QUIESCE_STDIO_SERVER = `
const fs = require('node:fs');
const statusPath = process.env.STATUS_PATH;
const pushPath = process.env.PUSH_PATH;
const resumePath = process.env.RESUME_PATH;
const log = (event, extra) =>
  fs.appendFileSync(statusPath, JSON.stringify({ event, ...extra }) + '\\n');
let buf = '';
let pushSent = false;
let resumeSent = false;
process.stdin.on('data', (c) => {
  buf += c.toString('utf8');
  let i;
  while ((i = buf.indexOf('\\n')) >= 0) {
    const line = buf.slice(0, i); buf = buf.slice(i + 1);
    if (!line.trim()) continue;
    let m; try { m = JSON.parse(line); } catch { continue; }
    const reply = (result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: m.id, result }) + '\\n');
    if (m.method === 'initialize') reply({ capabilities: { experimental: { mcpl: { version: '0.4', pushEvents: true } } } });
    else if (m.method === 'tools/list') reply({ tools: [] });
    else if (m.id === 900) log('push-response', { error: m.error ?? null });
    else if (m.id === 901) log('resume-response', { result: m.result ?? null, error: m.error ?? null });
  }
});
setInterval(() => {
  if (!pushSent && fs.existsSync(pushPath)) {
    pushSent = true;
    process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: 900, method: 'push/event', params: {
      featureSet: 'chat', eventId: 'quiesce-push', timestamp: new Date().toISOString(),
      payload: { content: [{ type: 'text', text: 'held while quiesced' }] },
    } }) + '\\n');
    log('push-sent');
  }
  if (!resumeSent && fs.existsSync(resumePath)) {
    resumeSent = true;
    process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: 901, method: 'host/command', params: {
      command: 'resume', requesterName: 'test-operator',
    } }) + '\\n');
    log('resume-sent');
  }
}, 25);
`;

test('MCPL: pushes buffer while quiesced; host/command resume reopens and flushes', async () => {
  const { writeFileSync, readFileSync, existsSync } = await import('node:fs');
  const dir = mkdtempSync(join(tmpdir(), 'host-quiesce-mcpl-'));
  const statusPath = join(dir, 'status.jsonl');
  const pushPath = join(dir, 'push');
  const resumePath = join(dir, 'resume');
  const records = (): Array<{ event: string; [key: string]: unknown }> =>
    existsSync(statusPath)
      ? readFileSync(statusPath, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))
      : [];

  const membrane = new MockMembrane();
  const framework = await AgentFramework.create({
    storePath: join(dir, 'store'),
    membrane: membrane.asMembrane(),
    agents: [{ name: 'assistant', model: 'test-model', systemPrompt: 'test' }],
    modules: [],
    mcplServers: [{
      id: 'pusher',
      command: process.execPath,
      args: ['-e', QUIESCE_STDIO_SERVER],
      allowHostCommands: true,
      env: { STATUS_PATH: statusPath, PUSH_PATH: pushPath, RESUME_PATH: resumePath },
    }],
  });
  try {
    framework.start();
    await framework.quiesce({ reason: 'mcpl leg' });

    writeFileSync(pushPath, 'go');
    await waitFor('push sent by server', () => records().some((r) => r.event === 'push-sent'));
    await new Promise((resolve) => setTimeout(resolve, 400));
    assert.ok(
      !records().some((r) => r.event === 'push-response'),
      'push must buffer on the paused data plane — no response while quiesced',
    );
    assert.equal(membrane.calls.length, 0, 'no wake while quiesced');

    // Resume arrives OVER MCPL: host/command rides the control plane, so it
    // is deliverable through the very pause it lifts.
    writeFileSync(resumePath, 'go');
    await waitFor('resume handled via host/command', () =>
      records().some((r) => r.event === 'resume-response'
        && (r.result as { ok?: boolean } | null)?.ok === true));
    assert.equal(framework.getHostModeStatus().quiesced, false);
    await waitFor('buffered push flushed after resume', () =>
      records().some((r) => r.event === 'push-response'));
  } finally {
    await framework.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// Review-fix regressions (code review 08-21)
// ---------------------------------------------------------------------------

test('a non-finite timeoutMs falls back to the default drain window, never NaN-collapses', async () => {
  const membrane = new MockMembrane();
  await withFramework(membrane, async (framework) => {
    // NaN reached quiesce() via Number('12O000') on the HTTP ingress. With
    // the old Math.max(1_000, NaN) the deadline was NaN and the drain window
    // collapsed to zero. No turn is in flight here, so the observable is
    // simply that quiesce completes sanely (drained) instead of misbehaving.
    const status = await framework.quiesce({ timeoutMs: Number('12O000') });
    assert.equal(status.quiesced, true);
    assert.equal(status.drained, true);
  });
});

test('abandon can escalate an already-quiesced, undrained host', async () => {
  class HangingStream implements YieldingStream {
    private pendingResolve: (() => void) | null = null;
    private aborted = false;
    cancel(): void { this.aborted = true; this.pendingResolve?.(); }
    provideToolResults(): void {}
    get isWaitingForTools() { return false; }
    get pendingToolCallIds(): string[] { return []; }
    get toolDepth() { return 0; }
    async *[Symbol.asyncIterator](): AsyncIterator<StreamEvent> {
      if (!this.aborted) {
        await new Promise<void>((resolve) => { this.pendingResolve = resolve; });
      }
      yield { type: 'aborted', reason: 'user' } as StreamEvent;
    }
  }
  class HangingMembrane extends MockMembrane {
    override streamYielding(request: NormalizedRequest): YieldingStream {
      this.calls.push(request);
      return new HangingStream();
    }
  }
  const membrane = new HangingMembrane();
  await withFramework(membrane, async (framework) => {
    framework.start();
    framework.nudgeAgent('agent', 'operator');
    await waitFor('turn to start', () => membrane.calls.length === 1);

    const first = await framework.quiesce({ timeoutMs: 1_000 });
    assert.equal(first.drained, false, 'hung turn survives the first window');

    // Old behavior: `if (this.quiesced) return status` made this a no-op and
    // the only way out was resume()+re-quiesce, reopening planes mid-surgery.
    const second = await framework.quiesce({ abandon: true });
    assert.equal(second.drained, true, 'second quiesce escalates to abandon');
  });
});

test('context writes are deferred while quiesced and flushed by resume', async () => {
  const membrane = new MockMembrane();
  await withFramework(membrane, async (framework) => {
    await framework.quiesce({ reason: 'refold' });
    const cm = framework.getAgent('agent')!.getContextManager();
    const before = cm.getAllMessages().length;

    // Module events / api message.send route through framework.addMessage
    // (private — invoked here the way handleProcessEvent invokes it).
    (framework as unknown as {
      addMessage(participant: string, content: unknown[]): unknown;
    }).addMessage('user', [{ type: 'text', text: 'mid-surgery message' }]);
    assert.equal(
      cm.getAllMessages().length, before,
      'no context append while the window is open',
    );

    await framework.resume();
    assert.equal(
      cm.getAllMessages().length, before + 1,
      'the deferred write lands at resume',
    );
  });
});

test('a batch carrying a budget restart re-parks its sibling wakes instead of consuming them', async () => {
  const membrane = new MockMembrane();
  // The restart's continuation round needs a response to complete on — an
  // empty mock stream yields no `complete` and the agent never leaves
  // `streaming`, which reads as a scheduler hang rather than a re-park bug.
  membrane.pushResponse(createMockResponse([{ type: 'text', text: 'restart round' }]));
  await withFramework(membrane, async (framework) => {
    await framework.quiesce();
    // A parked wake exists...
    framework.nudgeAgent('agent', 'operator');
    // ...and a budget restart lands in the same pendingRequests batch.
    (framework as unknown as {
      pendingRequests: Array<Record<string, unknown>>;
    }).pendingRequests.push({
      agentName: 'agent',
      reason: 'context_budget_restart',
      source: 'framework',
      timestamp: Date.now(),
    });
    // Drive one scheduler pass: the restart may start a turn, but the nudge
    // must survive as a parked request rather than being consumed by it.
    await framework.runUntilIdle();
    await waitFor(
      'sibling wake re-parked',
      () => framework.getHostModeStatus().gatedRequests >= 1,
    );
  });
});
