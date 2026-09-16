/**
 * Live operator surgery: rollbackToMessage / suppressMessages on an OPEN
 * store, and the durable operator-actions.jsonl record every operator
 * mutation leaves behind.
 *
 * Asserted at the store seam (branch names, message counts on each branch,
 * log file contents, operator:action traces) — no inference runs.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { AgentFramework, OperatorActionError, OperatorLog } from '../src/index.js';
import type { OperatorLogEntry, TraceEvent } from '../src/index.js';
import { MockMembrane } from './helpers/mock-membrane.js';

function readLog(path: string): OperatorLogEntry[] {
  if (!existsSync(path)) return [];
  return readFileSync(path, 'utf8')
    .split('\n')
    .filter((l) => l.length > 0)
    .map((l) => JSON.parse(l) as OperatorLogEntry);
}

describe('live operator surgery', () => {
  let tempDir: string;
  let storePath: string;
  let framework: AgentFramework;
  let traces: TraceEvent[];
  let ids: string[];

  const cm = () => framework.getAgent('scout')!.getContextManager();
  const remainingTexts = () =>
    cm().getAllMessages().map((m) => (m.content[0] as { text: string }).text);

  beforeEach(async () => {
    tempDir = mkdtempSync(join(tmpdir(), 'operator-surgery-test-'));
    storePath = join(tempDir, 'test.chronicle');
    framework = await AgentFramework.create({
      storePath,
      membrane: new MockMembrane().asMembrane(),
      agents: [{ name: 'scout', model: 'test-model', systemPrompt: 'You are scout.' }],
      modules: [],
    });
    traces = [];
    framework.onTrace((e) => { traces.push(e); });
    ids = [];
    for (let i = 1; i <= 5; i++) {
      ids.push(cm().addMessage(i % 2 ? 'user' : 'scout', [{ type: 'text', text: `m${i}` }], {
        serverId: 'discord',
        channelId: 'discord:g1:c1',
        messageId: `d${i}`,
      }));
    }
  });

  afterEach(async () => {
    await framework.stop();
    rmSync(tempDir, { recursive: true, force: true });
  });

  it('defaults the operator log next to the store', () => {
    assert.equal(framework.getOperatorLogPath(), join(storePath, 'operator-actions.jsonl'));
  });

  it('rollbackToMessage forks at the message, switches to the fork, keeps the source intact, logs', async () => {
    const r = await framework.rollbackToMessage('scout', {
      messageId: ids[2],
      requester: { via: 'webui', name: 'antra' },
      note: 'bad turn',
    });
    assert.equal(r.sourceBranch, 'main');
    assert.match(r.targetBranch, /^rollback\/scout\/\d+$/);
    assert.equal(r.messagesRemoved, 2);
    assert.equal(r.removedRefs.length, 2, 'discord refs of the discarded messages are collected');
    assert.equal(cm().currentBranch().name, r.targetBranch);
    assert.deepEqual(remainingTexts(), ['m1', 'm2', 'm3']);

    // The source branch still has everything.
    await cm().switchBranch('main');
    assert.deepEqual(remainingTexts(), ['m1', 'm2', 'm3', 'm4', 'm5']);

    const log = readLog(framework.getOperatorLogPath()!);
    const entry = log.find((e) => e.kind === 'rollback');
    assert.ok(entry, 'rollback recorded in operator-actions.jsonl');
    assert.equal(entry.agent, 'scout');
    assert.deepEqual(entry.requester, { via: 'webui', name: 'antra' });
    assert.equal(entry.note, 'bad turn');
    assert.deepEqual(entry.params, { messageId: ids[2] });
    assert.equal(entry.result?.messagesRemoved, 2);
    assert.equal(entry.result?.targetBranch, r.targetBranch);
    assert.ok(typeof entry.at === 'string' && !Number.isNaN(Date.parse(entry.at)));

    const trace = traces.find((t) => t.type === 'operator:action' && t.kind === 'rollback');
    assert.ok(trace, 'operator:action trace broadcast');
    assert.equal((trace as { agentName?: string }).agentName, 'scout');
  });

  it('rollback refuses the tail, unknown ids, and busy agents — and logs the refusal', async () => {
    await assert.rejects(
      framework.rollbackToMessage('scout', { messageId: ids[4] }),
      (e: unknown) => e instanceof OperatorActionError && e.code === 'invalid',
    );
    await assert.rejects(
      framework.rollbackToMessage('scout', { messageId: 'nope' }),
      (e: unknown) => e instanceof OperatorActionError && e.code === 'unknown-message',
    );
    const agent = framework.getAgent('scout')! as unknown as { _state: { status: string } };
    agent._state = { status: 'inferring' };
    try {
      await assert.rejects(
        framework.rollbackToMessage('scout', { messageId: ids[1] }),
        (e: unknown) => e instanceof OperatorActionError && e.code === 'agent-busy',
      );
    } finally {
      agent._state = { status: 'idle' };
    }
    assert.equal(cm().currentBranch().name, 'main', 'nothing moved');
    assert.equal(cm().getMessageCount(), 5);
    const errors = readLog(framework.getOperatorLogPath()!).filter((e) => e.kind === 'rollback' && e.error);
    assert.equal(errors.length, 3);
    assert.match(errors[2].error!, /Cannot roll back while agent is inferring/);
  });

  it('suppressMessages forks at head, redacts only the chosen messages on the fork, logs', async () => {
    const r = await framework.suppressMessages('scout', {
      messageIds: [ids[1], ids[3], ids[3]],
      requester: { via: 'webui', name: 'antra' },
    });
    assert.equal(r.sourceBranch, 'main');
    assert.match(r.targetBranch, /^suppress\/scout\/\d+$/);
    assert.equal(r.messagesRemoved, 2);
    assert.deepEqual(new Set(r.removedIds), new Set([ids[1], ids[3]]));
    assert.equal(cm().currentBranch().name, r.targetBranch);
    assert.deepEqual(remainingTexts(), ['m1', 'm3', 'm5']);

    await cm().switchBranch('main');
    assert.deepEqual(remainingTexts(), ['m1', 'm2', 'm3', 'm4', 'm5'], 'source branch untouched');

    const entry = readLog(framework.getOperatorLogPath()!).find((e) => e.kind === 'suppress');
    assert.ok(entry);
    assert.deepEqual(entry.params, { messageIds: [ids[1], ids[3]] });
    assert.deepEqual(new Set(entry.result?.removedIds as string[]), new Set([ids[1], ids[3]]));
  });

  it('suppress validates every id before mutating anything', async () => {
    await assert.rejects(
      framework.suppressMessages('scout', { messageIds: [ids[0], 'ghost'] }),
      (e: unknown) => e instanceof OperatorActionError && e.code === 'unknown-message' && /ghost/.test(e.message),
    );
    await assert.rejects(
      framework.suppressMessages('scout', { messageIds: ids }),
      (e: unknown) => e instanceof OperatorActionError && e.code === 'invalid',
    );
    assert.equal(cm().currentBranch().name, 'main');
    assert.equal(cm().listBranches().length, 1, 'no fork was created');
    assert.equal(cm().getMessageCount(), 5);
  });

  it('host-command message-granular undo rides on rollbackToMessage and keeps its branch prefix', async () => {
    const internals = framework as unknown as {
      handleHostCommand(serverId: string, params: Record<string, unknown>): Promise<Record<string, unknown>>;
    };
    const r = await internals.handleHostCommand('discord', {
      command: 'undo',
      agentName: 'scout',
      messages: 2,
      requesterName: 'antra',
    });
    assert.equal(r.ok, true);
    assert.equal(r.messagesRemoved, 2);
    assert.match(cm().currentBranch().name, /^undo-msgs\/scout\/\d+$/);
    assert.deepEqual(remainingTexts(), ['m1', 'm2', 'm3']);
    const entry = readLog(framework.getOperatorLogPath()!).find((e) => e.kind === 'rollback');
    assert.ok(entry);
    assert.equal(entry.requester?.via, 'host-command:discord');
    assert.equal(entry.requester?.name, 'antra');
  });

  it('nudge and settings changes are recorded; getOperatorLog reads newest-last', async () => {
    framework.nudgeAgent('scout', 'host-console');
    framework.updateAgentRuntimeSettings('scout', { contextBudgetTokens: 150_000 }, {
      persist: false,
      requester: { via: 'webui', name: 'antra' },
    });
    framework.recordOperatorAction({ kind: 'quiesce', requester: { via: 'webui' }, params: { reason: 'maintenance' } });
    const log = framework.getOperatorLog({ limit: 10 });
    assert.deepEqual(log.map((e) => e.kind), ['nudge', 'settings-update', 'quiesce']);
    assert.equal(log[1].params?.persist, false);
    assert.deepEqual(framework.getOperatorLog({ limit: 1 }).map((e) => e.kind), ['quiesce']);
  });
});

describe('OperatorLog', () => {
  it('creates parent directories, tolerates corrupt lines, and is inert when disabled', () => {
    const dir = mkdtempSync(join(tmpdir(), 'operator-log-'));
    try {
      const path = join(dir, 'nested', 'deeper', 'ops.jsonl');
      const log = new OperatorLog(path);
      log.append({ kind: 'a' });
      writeFileSync(path, '{not json\n', { flag: 'a' });
      log.append({ kind: 'b', agent: 'x' });
      assert.deepEqual(log.readTail().map((e) => e.kind), ['a', 'b']);
      assert.deepEqual(log.readTail(1).map((e) => e.kind), ['b']);

      const off = new OperatorLog(undefined);
      assert.equal(off.enabled, false);
      const entry = off.append({ kind: 'noop' });
      assert.equal(entry.kind, 'noop');
      assert.deepEqual(off.readTail(), []);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
