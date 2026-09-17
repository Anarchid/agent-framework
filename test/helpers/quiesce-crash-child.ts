/**
 * Child process for the deferred-write crash-recovery regressions
 * (host-quiesce.test.ts). It builds a quiesced host with two deferred
 * writes, syncs the store, starts resume(), and then HARD-EXITS
 * (process.exit, no stop(), no sync) at the point named by argv[3]:
 *
 *   after-ack    — right after the first durable acknowledgement of the
 *                  resume flush completed (the reviewer's probe).
 *   before-sync  — inside the first chronicle sync() of the resume flush,
 *                  i.e. after the messages were appended in memory but before
 *                  anything reached disk.
 *
 * argv[2] = store path. Exit code 0 in both modes on the intended path;
 * anything else means the scenario could not be staged.
 */
import { AgentFramework } from '../../src/index.js';
import { MockMembrane } from './mock-membrane.js';

const [storePath, mode] = process.argv.slice(2);
if (!storePath || (mode !== 'after-ack' && mode !== 'before-sync')) {
  console.error('usage: quiesce-crash-child <storePath> after-ack|before-sync');
  process.exit(2);
}

const framework = await AgentFramework.create({
  storePath,
  membrane: new MockMembrane().asMembrane(),
  agents: [{ name: 'agent', model: 'test-model', systemPrompt: 'test' }],
  modules: [],
});
const internals = framework as unknown as {
  addMessage(p: string, c: unknown[]): unknown;
  store: { sync(): void };
  persistDeferredWrites(): void;
};

await framework.quiesce({ reason: 'crash probe' });
internals.addMessage('user', [{ type: 'text', text: 'ACKED-BUT-NOT-SYNCED' }]);
internals.addMessage('user', [{ type: 'text', text: 'STILL-PENDING' }]);
if (framework.getHostModeStatus().deferredWrites !== 2) process.exit(3);
// Starting point is fully durable: flag, queue, and store all on disk.
internals.store.sync();

let armed = true;
if (mode === 'after-ack') {
  const realPersist = internals.persistDeferredWrites.bind(framework);
  internals.persistDeferredWrites = () => {
    realPersist();
    if (armed) { armed = false; process.exit(0); }
  };
} else {
  const realSync = internals.store.sync.bind(internals.store);
  internals.store.sync = () => {
    if (armed) { armed = false; process.exit(0); }
    realSync();
  };
}

await framework.resume();
// Reaching here means the exit point was never hit — scenario not staged.
process.exit(4);
