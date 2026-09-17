/**
 * Child process for the deferred-write crash-recovery regressions
 * (host-quiesce.test.ts). It stages a host with deferred writes and then
 * HARD-EXITS (process.exit, no stop(), no sync) at the point named by
 * argv[3]. Exit code 0 means the intended exit point was hit; anything else
 * means the scenario could not be staged.
 *
 *   after-ack      quiesce, defer ACKED-BUT-NOT-SYNCED + STILL-PENDING, sync,
 *                  resume(); exit right after the first durable ack of the
 *                  resume flush completed.
 *   before-sync    same staging; exit inside the first chronicle sync() of
 *                  the resume flush (appended in memory, nothing on disk).
 *   turn-start-ack quiesce, defer ONCE-ONLY, sync; resume while the target
 *                  holds a turn token (message kept for its own boundary);
 *                  release the token and drive an ORDINARY wake; exit after
 *                  the turn-start flush's ack synced the chronicle but
 *                  before it rewrote the recovery queue.
 *   redefer        hanging turn; quiesce({abandon}); defer REDEFER-ONCE while
 *                  quiesced; the abandonment teardown drains and re-defers it;
 *                  exit at that re-deferral's recovery-file write.
 *
 * argv[2] = store path.
 */
import type { NormalizedRequest, StreamEvent, YieldingStream } from '@animalabs/membrane';
import { AgentFramework } from '../../src/index.js';
import { MockMembrane, createMockResponse } from './mock-membrane.js';

const MODES = ['after-ack', 'before-sync', 'turn-start-ack', 'redefer'] as const;
type Mode = typeof MODES[number];
const [storePath, modeArg] = process.argv.slice(2);
if (!storePath || !MODES.includes(modeArg as Mode)) {
  console.error(`usage: quiesce-crash-child <storePath> ${MODES.join('|')}`);
  process.exit(2);
}
const mode = modeArg as Mode;
// Never hang the parent: an un-staged scenario is a failure, not a timeout.
setTimeout(() => { console.error('quiesce-crash-child: exit point never reached'); process.exit(4); }, 30_000);

class HangingStream implements YieldingStream {
  private pendingResolve: (() => void) | null = null;
  private aborted = false;
  cancel(): void { this.aborted = true; this.pendingResolve?.(); }
  provideToolResults(): void {}
  get isWaitingForTools() { return false; }
  get pendingToolCallIds(): string[] { return []; }
  get toolDepth() { return 0; }
  async *[Symbol.asyncIterator](): AsyncIterator<StreamEvent> {
    if (!this.aborted) await new Promise<void>((resolve) => { this.pendingResolve = resolve; });
    yield { type: 'aborted', reason: 'user' } as StreamEvent;
  }
}
class HangingMembrane extends MockMembrane {
  override streamYielding(request: NormalizedRequest): YieldingStream {
    this.calls.push(request);
    return new HangingStream();
  }
}

const membrane = mode === 'redefer' ? new HangingMembrane() : new MockMembrane();
const framework = await AgentFramework.create({
  storePath,
  membrane: membrane.asMembrane(),
  agents: [{ name: 'agent', model: 'test-model', systemPrompt: 'test' }],
  modules: [],
});
const internals = framework as unknown as {
  addMessage(p: string, c: unknown[]): unknown;
  store: { sync(): void };
  persistDeferredWrites(): void;
  activeTurnTokens: Map<string, number>;
};
const say = (text: string) => internals.addMessage('user', [{ type: 'text', text }]);
const waitFor = async (pred: () => boolean): Promise<void> => {
  while (!pred()) await new Promise((r) => setTimeout(r, 20));
};
const realPersist = internals.persistDeferredWrites.bind(framework);
const exitAfterNextPersist = (): void => {
  let armed = true;
  internals.persistDeferredWrites = () => {
    realPersist();
    if (armed) { armed = false; process.exit(0); }
  };
};

if (mode === 'after-ack' || mode === 'before-sync') {
  await framework.quiesce({ reason: 'crash probe' });
  say('ACKED-BUT-NOT-SYNCED');
  say('STILL-PENDING');
  if (framework.getHostModeStatus().deferredWrites !== 2) process.exit(3);
  internals.store.sync();
  if (mode === 'after-ack') {
    exitAfterNextPersist();
  } else {
    const realSync = internals.store.sync.bind(internals.store);
    let armed = true;
    internals.store.sync = () => {
      if (armed) { armed = false; process.exit(0); }
      realSync();
    };
  }
  await framework.resume();
  process.exit(4);
}

if (mode === 'turn-start-ack') {
  await framework.quiesce({ reason: 'crash probe' });
  say('ONCE-ONLY');
  internals.store.sync();
  // The target "holds a turn" across resume: its message is kept for its
  // own boundary, the durable queue still lists it, the flag is cleared.
  internals.activeTurnTokens.set('agent', 1);
  await framework.resume();
  if (framework.getHostModeStatus().deferredWrites !== 1) process.exit(3);
  internals.activeTurnTokens.delete('agent');
  // An ORDINARY wake: the turn-start flush writes ONCE-ONLY, then acks
  // (sync, then queue rewrite). Exit between the two.
  exitAfterNextPersist();
  membrane.pushResponse(createMockResponse([{ type: 'text', text: 'ok' }]));
  framework.start();
  framework.nudgeAgent('agent', 'operator');
  await new Promise(() => {});
}

if (mode === 'redefer') {
  framework.start();
  framework.nudgeAgent('agent', 'operator');
  await waitFor(() => membrane.calls.length === 1);
  const quiescing = framework.quiesce({ timeoutMs: 1_000, abandon: true });
  await waitFor(() => framework.getHostModeStatus().quiesced);
  say('REDEFER-ONCE'); // deferred (quiesced) and persisted
  if (framework.getHostModeStatus().deferredWrites !== 1) process.exit(3);
  // The abandonment teardown drains it (→ un-acked), tries to write it,
  // and — still quiesced — re-defers it. Exit at that recovery-file write.
  exitAfterNextPersist();
  await quiescing;
  await new Promise(() => {});
}
