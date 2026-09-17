import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HistoryModule } from '../src/modules/history/index.js';
import type { ContextManager, ChannelTokenStats, TimeRangeSummaryEntry } from '@animalabs/context-manager';
import type { ToolCall } from '../src/types/events.js';

/**
 * `overview`'s own logic is: dispatch to getSummariesInRange (flat, when an
 * exact `level` is given) vs getSummariesInRange-then-fold-reduce (when it
 * isn't), gap-fill unsummarized spans via getChannelTokenStats, and apply an
 * optional channel filter. The underlying summary/token-stats correctness is
 * context-manager's own responsibility (see its own
 * test/summary-overview.test.ts) — so this stub implements simple in-memory
 * overlap filtering over small fixture arrays, mirroring the stub pattern in
 * history-module.test.ts.
 */

interface StubMessage {
  ms: number;
  channelId: string;
  tokens: number;
}

function buildStub(opts: {
  summaries?: TimeRangeSummaryEntry[];
  messages?: StubMessage[];
  maxSummaryLevel?: number;
}): { cm: ContextManager; calls: Array<{ method: string; args: unknown }> } {
  const summaries = opts.summaries ?? [];
  const messages = opts.messages ?? [];
  const calls: Array<{ method: string; args: unknown }> = [];

  const cm = {
    getSummariesInRange(args: { fromMs?: number; toMs?: number; level?: number }): TimeRangeSummaryEntry[] {
      calls.push({ method: 'getSummariesInRange', args });
      return summaries.filter((s) => {
        if (args.level !== undefined && s.level !== args.level) return false;
        // Overlap test: entry overlaps [fromMs, toMs] (either bound optional).
        if (args.toMs !== undefined && s.startMs > args.toMs) return false;
        if (args.fromMs !== undefined && s.endMs < args.fromMs) return false;
        return true;
      });
    },
    getMaxSummaryLevel(): number {
      calls.push({ method: 'getMaxSummaryLevel', args: undefined });
      return opts.maxSummaryLevel ?? Math.max(0, ...summaries.map((s) => s.level));
    },
    getChannelTokenStats(args?: { fromMs?: number; toMs?: number }): ChannelTokenStats {
      calls.push({ method: 'getChannelTokenStats', args });
      const inRange = messages.filter((m) => {
        if (args?.fromMs !== undefined && m.ms < args.fromMs) return false;
        if (args?.toMs !== undefined && m.ms > args.toMs) return false;
        return true;
      });
      const byChannel = new Map<string, { messages: number; tokensEstimate: number }>();
      let totalTokensEstimate = 0;
      for (const m of inRange) {
        totalTokensEstimate += m.tokens;
        const agg = byChannel.get(m.channelId) ?? { messages: 0, tokensEstimate: 0 };
        agg.messages++;
        agg.tokensEstimate += m.tokens;
        byChannel.set(m.channelId, agg);
      }
      return {
        totalMessages: inRange.length,
        totalTokensEstimate,
        byChannel: [...byChannel.entries()].map(([channelId, agg]) => ({ channelId, ...agg })),
      };
    },
    // Only ever called by HistoryModule.earliestMessageMs() with `{ toMs,
    // limit: 1 }` — oldest-first by default, so the first returned message
    // is the earliest one at or before `toMs`. Real matchedCount/paging
    // fidelity isn't exercised by that caller, so this stub keeps it simple.
    queryMessagesByTime(args: { fromMs?: number; toMs?: number; limit?: number; offset?: number; reverse?: boolean }) {
      calls.push({ method: 'queryMessagesByTime', args });
      let inRange = messages
        .filter((m) => {
          if (args.fromMs !== undefined && m.ms < args.fromMs) return false;
          if (args.toMs !== undefined && m.ms > args.toMs) return false;
          return true;
        })
        .slice()
        .sort((a, b) => a.ms - b.ms);
      if (args.reverse) inRange = inRange.reverse();
      const offset = args.offset ?? 0;
      const limit = args.limit ?? inRange.length;
      const page = inRange.slice(offset, offset + limit).map((m) => ({ timestamp: new Date(m.ms) }));
      return { messages: page, matchedCount: inRange.length };
    },
  } as unknown as ContextManager;

  return { cm, calls };
}

function call(name: string, input: Record<string, unknown>): ToolCall {
  return { id: `call-${name}`, name, input } as unknown as ToolCall;
}

function summary(
  id: string,
  level: number,
  startMs: number,
  endMs: number,
  content: string,
  opts: { parentId?: string } = {},
): TimeRangeSummaryEntry {
  return {
    id,
    level,
    content,
    tokens: content.length,
    startMs,
    endMs,
    createdMs: endMs,
    sourceIds: [`${id}-src`],
    ...opts,
  };
}

interface OverviewEntry {
  from: string;
  to: string;
  summarized: boolean;
  content?: string;
  level?: number;
  messageCount: number;
  tokensEstimate: number;
  byChannel: Array<{ channelId: string; messages: number; tokensEstimate: number }>;
}

describe('HistoryModule.overview', () => {
  it('a summary-backed span appears with its content and level', async () => {
    const { cm } = buildStub({
      summaries: [summary('s1', 1, 1000, 2000, 'Chapter one')],
      messages: [{ ms: 1500, channelId: 'c1', tokens: 5 }],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', { from: new Date(1000).toISOString(), to: new Date(2000).toISOString() }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    assert.equal(data.entries.length, 1);
    const [entry] = data.entries;
    assert.equal(entry!.summarized, true);
    assert.equal(entry!.content, 'Chapter one');
    assert.equal(entry!.level, 1);
    assert.equal(entry!.messageCount, 1);
    assert.deepEqual(entry!.byChannel.map((c) => c.channelId), ['c1']);
    assert.equal(entry!.from, new Date(1000).toISOString());
    assert.equal(entry!.to, new Date(2000).toISOString());
  });

  it('a gap with real messages but no summary appears as summarized:false with correct counts', async () => {
    const { cm } = buildStub({
      summaries: [],
      messages: [{ ms: 10_000, channelId: 'c1', tokens: 7 }],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', { from: new Date(0).toISOString(), to: new Date(20_000).toISOString() }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    assert.equal(data.entries.length, 1);
    const [entry] = data.entries;
    assert.equal(entry!.summarized, false);
    assert.equal(entry!.content, undefined);
    assert.equal(entry!.messageCount, 1);
    assert.deepEqual(entry!.byChannel.map((c) => c.channelId), ['c1']);
  });

  it('a gap with zero messages is not included', async () => {
    const { cm } = buildStub({ summaries: [], messages: [] });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', { from: new Date(0).toISOString(), to: new Date(20_000).toISOString() }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    assert.equal(data.entries.length, 0);
  });

  it('fold-reduction drops a folded child when its parent is also in range', async () => {
    const child = summary('child', 1, 1000, 2000, 'child summary', { parentId: 'parent' });
    const parent = summary('parent', 2, 1000, 3000, 'parent summary');
    const { cm } = buildStub({
      summaries: [child, parent],
      messages: [{ ms: 1500, channelId: 'c1', tokens: 5 }],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', { from: new Date(1000).toISOString(), to: new Date(3000).toISOString() }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    // Only the coarser parent survives — the child's span is already
    // covered by it, so showing both would duplicate the same source
    // messages at two granularities.
    assert.equal(data.entries.length, 1);
    assert.equal(data.entries[0]!.content, 'parent summary');
    assert.equal(data.entries[0]!.level, 2);
  });

  it('an explicit level bypasses fold-reduction, returning flat single-level results', async () => {
    const child = summary('child', 1, 1000, 2000, 'child summary', { parentId: 'parent' });
    const parent = summary('parent', 2, 1000, 3000, 'parent summary');
    const { cm } = buildStub({
      summaries: [child, parent],
      messages: [{ ms: 1500, channelId: 'c1', tokens: 5 }],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', {
        from: new Date(1000).toISOString(),
        to: new Date(3000).toISOString(),
        level: 1,
      }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    // Level explicitly pinned to 1: the child entry comes back even though
    // it has a parentId — no reduction is applied when level is given.
    assert.equal(data.entries.length, 1);
    assert.equal(data.entries[0]!.content, 'child summary');
    assert.equal(data.entries[0]!.level, 1);
  });

  it('channelId filtering drops empty-channel spans but keeps populated ones with unfiltered summary text', async () => {
    const sA = summary('sA', 1, 1000, 2000, 'Channel A stuff');
    const sB = summary('sB', 1, 3000, 4000, 'Channel B stuff mentions A too');
    const { cm } = buildStub({
      summaries: [sA, sB],
      messages: [
        { ms: 1500, channelId: 'c1', tokens: 5 },
        { ms: 3500, channelId: 'c2', tokens: 5 },
      ],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', {
        from: new Date(1000).toISOString(),
        to: new Date(4000).toISOString(),
        channelId: 'c1',
      }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    assert.equal(data.entries.length, 1);
    // sB is dropped (zero c1 messages in its span); sA's summary content is
    // kept whole/unfiltered — compression doesn't chunk per-channel.
    assert.equal(data.entries[0]!.content, 'Channel A stuff');
    assert.deepEqual(data.entries[0]!.byChannel.map((c) => c.channelId), ['c1']);
  });

  it('reports maxLevelAvailable (not a misleading single `level`) when level is omitted (finding #10)', async () => {
    const { cm } = buildStub({
      summaries: [summary('s1', 3, 1000, 2000, 'top level summary')],
      messages: [],
      maxSummaryLevel: 3,
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(call('overview', { from: new Date(1000).toISOString(), to: new Date(2000).toISOString() }));
    assert.equal(result.success, true, result.error);
    const data = result.data as { level?: number; maxLevelAvailable?: number };
    assert.equal(data.maxLevelAvailable, 3);
    assert.equal(data.level, undefined, 'a single `level` number would be misleading for a mixed-level default result');
  });

  it('echoes back the exact `level` requested when one was given (finding #10)', async () => {
    const { cm } = buildStub({
      summaries: [summary('s1', 1, 1000, 2000, 'a level-1 summary')],
      messages: [],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', { from: new Date(1000).toISOString(), to: new Date(2000).toISOString(), level: 1 }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { level?: number; maxLevelAvailable?: number };
    assert.equal(data.level, 1);
    assert.equal(data.maxLevelAvailable, undefined);
  });

  it('channelId scopes messageCount/tokensEstimate to that channel, keeping whole-span totals under span* keys (finding #7)', async () => {
    const s1 = summary('s1', 1, 1000, 2000, 'mixed-channel chapter');
    const { cm } = buildStub({
      summaries: [s1],
      messages: [
        { ms: 1100, channelId: 'c1', tokens: 10 },
        { ms: 1200, channelId: 'c1', tokens: 10 },
        { ms: 1300, channelId: 'c2', tokens: 100 },
      ],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', { from: new Date(1000).toISOString(), to: new Date(2000).toISOString(), channelId: 'c1' }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: Array<OverviewEntry & { spanMessageCount?: number; spanTokensEstimate?: number }> };
    assert.equal(data.entries.length, 1);
    const [entry] = data.entries;
    // Scoped to c1 only: 2 messages, 20 tokens — NOT the whole span's 3
    // messages / 120 tokens (which the numeric fields used to report even
    // when the caller had narrowed to one channel).
    assert.equal(entry!.messageCount, 2);
    assert.equal(entry!.tokensEstimate, 20);
    assert.equal(entry!.spanMessageCount, 3);
    assert.equal(entry!.spanTokensEstimate, 120);
  });

  it('two adjacent summaries with a wide seam but zero real unsummarized traffic produce NO fabricated gap entry (finding #1)', async () => {
    const s1 = summary('s1', 1, 1000, 1005, 'first chapter');
    const s2 = summary('s2', 1, 20_000, 20_005, 'second chapter');
    const { cm } = buildStub({
      summaries: [s1, s2],
      // Only the boundary messages anchoring each summary's own span exist
      // — nothing real happened between ms 1006 and ms 19999.
      // getChannelTokenStats's range is BOTH-ENDS INCLUSIVE, so a naive
      // probe of [1005, 20000] would re-count these two boundary messages
      // (already inside their own summaries) and fabricate a phantom gap.
      messages: [
        { ms: 1000, channelId: 'c1', tokens: 5 },
        { ms: 1005, channelId: 'c1', tokens: 5 },
        { ms: 20_000, channelId: 'c1', tokens: 5 },
        { ms: 20_005, channelId: 'c1', tokens: 5 },
      ],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', { from: new Date(1000).toISOString(), to: new Date(20_005).toISOString() }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    assert.equal(data.entries.length, 2, `expected exactly the two summaries, got ${JSON.stringify(data.entries)}`);
    assert.ok(data.entries.every((e) => e.summarized), 'no fabricated summarized:false gap entry should appear between adjacent summaries');
  });

  it('overview({}) with real messages but zero summaries minted returns a summarized:false entry, not an empty array (finding #4)', async () => {
    const { cm } = buildStub({
      summaries: [],
      messages: [
        { ms: 1_000_000, channelId: 'c1', tokens: 5 },
        { ms: 1_000_100, channelId: 'c1', tokens: 5 },
      ],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(call('overview', {}));
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    assert.equal(data.entries.length, 1);
    assert.equal(data.entries[0]!.summarized, false);
    assert.equal(data.entries[0]!.messageCount, 2);
    assert.equal(data.entries[0]!.from, new Date(1_000_000).toISOString());
  });

  it('overview({}) against a genuinely empty store returns entries:[] without crashing (finding #4, empty-store edge)', async () => {
    const { cm } = buildStub({ summaries: [], messages: [] });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(call('overview', {}));
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    assert.equal(data.entries.length, 0);
  });

  it('unsummarized messages older than the earliest summary still appear when `from` is omitted (finding #6)', async () => {
    const s1 = summary('s1', 1, 50_000, 51_000, 'a later chapter');
    const { cm } = buildStub({
      summaries: [s1],
      messages: [
        { ms: 1000, channelId: 'c1', tokens: 5 }, // predates every summary
        { ms: 50_500, channelId: 'c1', tokens: 5 },
      ],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(call('overview', { to: new Date(51_000).toISOString() }));
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    // Must see BOTH the old unsummarized message (as a summarized:false gap
    // before the summary) AND the summary itself — starting the walk at the
    // first summary's own startMs would silently make pre-compression
    // history invisible.
    assert.equal(data.entries.length, 2, `expected the old gap AND the summary, got ${JSON.stringify(data.entries)}`);
    assert.equal(data.entries[0]!.summarized, false);
    assert.equal(data.entries[0]!.messageCount, 1);
    assert.equal(data.entries[1]!.summarized, true);
  });

  it('a broken ancestor chain (unresolvable intermediate level) still reduces via span-containment (finding #5)', async () => {
    // L2 is deliberately absent from the fetched set — simulating
    // context-manager's listSummariesInRange silently skipping an
    // intermediate level whose source range no longer resolves (a
    // pruned/redacted source message, or a viewFilter exclusion). L1's
    // parentId still points at 'L2', but 'L2' never comes back from
    // getSummariesInRange, so the immediate-parent check alone can't see
    // past the gap in the chain.
    const l1 = summary('L1', 1, 1000, 1500, 'l1 detail', { parentId: 'L2' });
    const l3 = summary('L3', 3, 1000, 3000, 'l3 grandparent, covers L1 fully');
    const { cm } = buildStub({
      summaries: [l1, l3],
      messages: [{ ms: 1200, channelId: 'c1', tokens: 5 }],
    });
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(
      call('overview', { from: new Date(1000).toISOString(), to: new Date(3000).toISOString() }),
    );
    assert.equal(result.success, true, result.error);
    const data = result.data as { entries: OverviewEntry[] };
    // Without the containment-sweep backstop, L1 would incorrectly survive
    // alongside L3 — its immediate parentId 'L2' isn't in the fetched set,
    // so the parentId-only check can't tell it's superseded.
    assert.equal(
      data.entries.length,
      1,
      `expected only L3 to survive, got ${JSON.stringify(data.entries.map((e) => e.content))}`,
    );
    assert.equal(data.entries[0]!.content, 'l3 grandparent, covers L1 fully');
    assert.equal(data.entries[0]!.level, 3);
  });

  it('surfaces the capability-absent error as a clean tool error, not a crash', async () => {
    const cm = {
      getSummariesInRange() {
        throw new Error('Chronicle history index unsupported: native field-index capability absent on this chronicle build.');
      },
    } as unknown as ContextManager;
    const h = new HistoryModule();
    h.bind(cm);

    const result = await h.handleToolCall(call('overview', {}));
    assert.equal(result.success, false);
    assert.equal(result.isError, true);
    assert.match(result.error ?? '', /Chronicle history index unsupported/);
  });
});
