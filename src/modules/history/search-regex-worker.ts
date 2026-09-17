/**
 * search-regex-worker — runs a caller-supplied regex against a batch of
 * message text on a separate thread, so HistoryModule's `search` tool can
 * forcibly terminate a catastrophically-backtracking pattern (ReDoS)
 * instead of blocking the framework's single-threaded event loop.
 *
 * Why a worker and not a Promise.race/setTimeout "timeout": JS is
 * single-threaded, so nothing on the SAME thread can interrupt a
 * synchronous RegExp.exec() call already in flight — a timeout callback
 * racing it can't even fire until the blocking call returns, by which point
 * it's too late. A worker thread is real OS-level concurrency: the parent's
 * event loop keeps running, and `worker.terminate()` actually kills the
 * stuck thread rather than just giving up on waiting for it.
 *
 * Only regex-mode search routes through this worker. Plain substring search
 * (String.prototype.indexOf) is inherently linear in input length and has
 * no equivalent risk, so it stays in-process — the common case pays no
 * worker-spawn overhead.
 *
 * One-shot: the whole job (already-flattened text, already syntax-validated
 * pattern/flags, match limit) arrives via `workerData` at construction; this
 * script runs it to completion and posts exactly one message, then the
 * parent always terminates the worker (whether it finished normally or had
 * to be killed for running long) — no persistent pool, so a wedged worker
 * from one bad pattern can never contaminate a later call.
 */
import { parentPort, workerData } from 'node:worker_threads';

export interface SearchWorkerJob {
  /** Flattened candidate text, aligned by index with the parent's candidate array. */
  texts: string[];
  pattern: string;
  flags: string;
  /** Stop scanning once this many matches are found. */
  limit: number;
}

export interface SearchWorkerMatch {
  /** Index into `texts` / the parent's candidate array. */
  candidateIndex: number;
  matchIndex: number;
  matchLength: number;
}

export type SearchWorkerMessage =
  | { type: 'done'; matches: SearchWorkerMatch[]; scanned: number }
  | { type: 'error'; error: string };

const { texts, pattern, flags, limit } = workerData as SearchWorkerJob;

try {
  // Re-construct (not re-validate for safety — RegExp syntax is what it is;
  // the parent already rejected an uncompilable pattern before ever getting
  // here) the matcher on this thread. Compilation succeeding doesn't bound
  // MATCHING time — that's exactly the risk this worker exists to contain.
  const matcher = new RegExp(pattern, flags);
  const matches: SearchWorkerMatch[] = [];
  let scanned = 0;
  for (let i = 0; i < texts.length; i++) {
    // Check the limit BEFORE doing any work for this candidate — not after
    // pushing a match — so limit:0 (a valid clampCount value: it's >= 0)
    // correctly yields zero matches instead of one. Checking post-push would
    // always let through the match that first reaches the limit.
    if (matches.length >= limit) break;
    scanned++;
    const m = matcher.exec(texts[i] ?? '');
    if (m) {
      matches.push({ candidateIndex: i, matchIndex: m.index, matchLength: m[0].length });
    }
  }
  const done: SearchWorkerMessage = { type: 'done', matches, scanned };
  parentPort?.postMessage(done);
} catch (e) {
  const errMsg: SearchWorkerMessage = { type: 'error', error: e instanceof Error ? e.message : String(e) };
  parentPort?.postMessage(errMsg);
}
