import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HistoryModule } from '../src/modules/history/index.js';
import type { ContextManager, StoredMessage, IndexedMessageQueryResult, ChannelCount, ChannelTokenStats } from '@animalabs/context-manager';
import type { ContentBlock } from '@animalabs/membrane';
import type { ToolCall } from '../src/types/events.js';

/**
 * HistoryModule's own logic is dispatch (always via
 * queryMessagesByTimeAndChannel, per context-manager's own test evidence
 * that it already handles "only one filter" and "neither filter"), input
 * validation (ISO dates, regex, limit capping), and presentation (content
 * flattening, snippet extraction, truncation detection). The underlying
 * index-query *correctness* is context-manager's own responsibility and is
 * covered by its test suite (test/message-store-history-index.test.ts) — so
 * this stub implements realistic-but-simple in-memory filtering over a small
 * fixture rather than standing up a real chronicle store, and a `calls` log
 * lets tests assert on exactly what HistoryModule asked for (e.g. the capped
 * limit).
 */

function textBlock(text: string): ContentBlock {
  return { type: 'text', text };
}

function metaFor(channelId?: string): Record<string, unknown> | undefined {
  return channelId ? { external: { channelId } } : undefined;
}

function msg(id: string, ms: number, participant: string, content: ContentBlock[], channelId?: string): StoredMessage {
  return {
    id,
    sequence: Number(id.replace(/\D/g, '')),
    participant,
    content,
    metadata: metaFor(channelId),
    timestamp: new Date(ms),
  } as StoredMessage;
}

function channelIdOf(m: StoredMessage): string | undefined {
  return (m.metadata as { external?: { channelId?: string } } | undefined)?.external?.channelId;
}

/** Trivial, deterministic stand-in for real token estimation — tests only
 *  assert on shape/filtering, never on the exact number. */
function estimate(m: StoredMessage): number {
  return m.content.reduce((n, b) => n + (b.type === 'text' ? b.text.length : 4), 0);
}

interface StubOptions {
  /** When set, every query/stats method throws this — simulates a chronicle
   *  build that predates the native index-query capability. */
  throwUnsupported?: boolean;
}

function buildStub(messages: StoredMessage[], opts: StubOptions = {}): { cm: ContextManager; calls: Array<{ method: string; args: unknown }> } {
  const calls: Array<{ method: string; args: unknown }> = [];
  const unsupported = () => {
    throw new Error('Chronicle history index unsupported: native field-index capability absent on this chronicle build.');
  };

  const cm = {
    queryMessagesByTimeAndChannel(args: { fromMs?: number; toMs?: number; channelId?: string; limit?: number; offset?: number }): IndexedMessageQueryResult {
      calls.push({ method: 'queryMessagesByTimeAndChannel', args });
      if (opts.throwUnsupported) return unsupported();
      const filtered = messages.filter((m) => {
        const ts = m.timestamp.getTime();
        if (args.fromMs !== undefined && ts < args.fromMs) return false;
        if (args.toMs !== undefined && ts > args.toMs) return false;
        if (args.channelId !== undefined && channelIdOf(m) !== args.channelId) return false;
        return true;
      });
      const matchedCount = filtered.length;
      const offset = args.offset ?? 0;
      const limit = args.limit ?? filtered.length;
      return { messages: filtered.slice(offset, offset + limit), matchedCount };
    },
    getChannelMessageCounts(): ChannelCount[] {
      calls.push({ method: 'getChannelMessageCounts', args: undefined });
      if (opts.throwUnsupported) return unsupported();
      const counts = new Map<string, number>();
      for (const m of messages) {
        const c = channelIdOf(m);
        if (c) counts.set(c, (counts.get(c) ?? 0) + 1);
      }
      return [...counts.entries()].map(([channelId, count]) => ({ channelId, messages: count }));
    },
    getChannelTokenStats(args?: { fromMs?: number; toMs?: number }): ChannelTokenStats {
      calls.push({ method: 'getChannelTokenStats', args });
      if (opts.throwUnsupported) return unsupported();
      const inRange = messages.filter((m) => {
        const ts = m.timestamp.getTime();
        if (args?.fromMs !== undefined && ts < args.fromMs) return false;
        if (args?.toMs !== undefined && ts > args.toMs) return false;
        return true;
      });
      const byChannel = new Map<string, { messages: number; tokensEstimate: number }>();
      let totalTokensEstimate = 0;
      for (const m of inRange) {
        const est = estimate(m);
        totalTokensEstimate += est;
        const c = channelIdOf(m);
        if (c) {
          const agg = byChannel.get(c) ?? { messages: 0, tokensEstimate: 0 };
          agg.messages++;
          agg.tokensEstimate += est;
          byChannel.set(c, agg);
        }
      }
      return {
        totalMessages: inRange.length,
        totalTokensEstimate,
        byChannel: [...byChannel.entries()].map(([channelId, agg]) => ({ channelId, ...agg })),
      };
    },
  } as unknown as ContextManager;

  return { cm, calls };
}

function call(name: string, input: Record<string, unknown>): ToolCall {
  return { id: `call-${name}`, name, input } as unknown as ToolCall;
}

// Fixture: two channels (c1, c2), one channel-less message, spread across
// distinct timestamps so time-range filtering is exercisable.
const FIXTURE: StoredMessage[] = [
  msg('m1', 1000, 'User', [textBlock('hello world')], 'c1'),
  msg('m2', 2000, 'Claude', [{ type: 'tool_use', id: 't1', name: 'search', input: {} }], 'c1'),
  msg('m3', 3000, 'User', [textBlock('foo BAR baz')], 'c2'),
  msg('m4', 4000, 'Claude', [{ type: 'thinking', thinking: 'hmm' }, textBlock('final answer')], 'c2'),
  msg('m5', 5000, 'User', [textBlock('unchanneled message')]),
  msg('m6', 6000, 'User', [textBlock('another c1 message with searchterm inside')], 'c1'),
];

describe('HistoryModule', () => {
  describe('stats', () => {
    it('reports all-time message counts and range-scoped token stats separately', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      // Range covers only m3..m5 (3000-5000), but messageCountsAllTime must
      // still reflect the whole store.
      const result = await h.handleToolCall(call('stats', { from: new Date(3000).toISOString(), to: new Date(5000).toISOString() }));
      assert.equal(result.success, true, result.error);
      const data = result.data as {
        messageCountsAllTime: ChannelCount[];
        tokenStatsForRange: ChannelTokenStats;
      };

      const allTimeByChannel = new Map(data.messageCountsAllTime.map((c) => [c.channelId, c.messages]));
      assert.equal(allTimeByChannel.get('c1'), 3); // m1, m2, m6 — unaffected by the range
      assert.equal(allTimeByChannel.get('c2'), 2); // m3, m4

      assert.equal(data.tokenStatsForRange.totalMessages, 3); // m3, m4, m5
      const rangeByChannel = new Map(data.tokenStatsForRange.byChannel.map((c) => [c.channelId, c.messages]));
      assert.equal(rangeByChannel.get('c2'), 2); // m3 and m4 both fall in [3000,5000]
      assert.equal(rangeByChannel.has('c1'), false); // no c1 message falls in [3000,5000]
    });

    it('filters both parts of the response to one channelId when given', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('stats', { channelId: 'c1' }));
      assert.equal(result.success, true, result.error);
      const data = result.data as { messageCountsAllTime: ChannelCount[]; tokenStatsForRange: ChannelTokenStats };
      assert.deepEqual(data.messageCountsAllTime.map((c) => c.channelId), ['c1']);
      assert.deepEqual(data.tokenStatsForRange.byChannel.map((c) => c.channelId), ['c1']);
    });

    it('rejects an invalid ISO date cleanly instead of throwing', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('stats', { from: 'not-a-date' }));
      assert.equal(result.success, false);
      assert.equal(result.isError, true);
      assert.match(result.error ?? '', /Invalid ISO 8601 date/);
    });

    it('surfaces the capability-absent error as a clean tool error, not a crash', async () => {
      const { cm } = buildStub(FIXTURE, { throwUnsupported: true });
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('stats', {}));
      assert.equal(result.success, false);
      assert.equal(result.isError, true);
      assert.match(result.error ?? '', /Chronicle history index unsupported/);
    });
  });

  describe('extract', () => {
    it('filters by channel and date range', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(
        call('extract', { channelId: 'c1', from: new Date(1500).toISOString() }),
      );
      assert.equal(result.success, true, result.error);
      const data = result.data as { matchedCount: number; returned: number; messages: Array<{ id: string }> };
      // c1 messages at ts >= 1500: m2 (2000), m6 (6000) — m1 (1000) excluded.
      assert.deepEqual(data.messages.map((m) => m.id), ['m2', 'm6']);
      assert.equal(data.returned, 2);
      assert.equal(data.matchedCount, 2);
    });

    it('format:"text" flattens content; format:"raw" returns blocks as-is', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const textResult = await h.handleToolCall(call('extract', { channelId: 'c2', format: 'text' }));
      const textData = textResult.data as { messages: Array<{ id: string; content: unknown }> };
      const m4 = textData.messages.find((m) => m.id === 'm4')!;
      assert.equal(m4.content, '[thinking] final answer');

      const rawResult = await h.handleToolCall(call('extract', { channelId: 'c2', format: 'raw' }));
      const rawData = rawResult.data as { messages: Array<{ id: string; content: unknown }> };
      const m4Raw = rawData.messages.find((m) => m.id === 'm4')!;
      assert.ok(Array.isArray(m4Raw.content));
      assert.equal((m4Raw.content as ContentBlock[]).length, 2);
      assert.equal((m4Raw.content as ContentBlock[])[0]?.type, 'thinking');
    });

    it('caps limit at the hard maximum before it reaches context-manager', async () => {
      const { cm, calls } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      await h.handleToolCall(call('extract', { limit: 999999 }));
      const queryCall = calls.find((c) => c.method === 'queryMessagesByTimeAndChannel');
      assert.equal((queryCall?.args as { limit?: number }).limit, 200);
    });

    it('surfaces the capability-absent error cleanly', async () => {
      const { cm } = buildStub(FIXTURE, { throwUnsupported: true });
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('extract', {}));
      assert.equal(result.success, false);
      assert.equal(result.isError, true);
      assert.match(result.error ?? '', /Chronicle history index unsupported/);
    });
  });

  describe('search', () => {
    it('matches a case-insensitive substring by default', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('search', { query: 'bar' }));
      assert.equal(result.success, true, result.error);
      const data = result.data as { matches: Array<{ id: string; snippet: string }> };
      assert.deepEqual(data.matches.map((m) => m.id), ['m3']);
      assert.match(data.matches[0]!.snippet, /BAR/);
    });

    it('caseSensitive:true respects case', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('search', { query: 'bar', caseSensitive: true }));
      assert.equal(result.success, true, result.error);
      const data = result.data as { matches: unknown[] };
      assert.equal(data.matches.length, 0); // fixture only has "BAR", not "bar"
    });

    it('matches a regex', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('search', { query: '\\bworld\\b', regex: true }));
      assert.equal(result.success, true, result.error);
      const data = result.data as { matches: Array<{ id: string }> };
      assert.deepEqual(data.matches.map((m) => m.id), ['m1']);
    });

    it('returns a clean error for an invalid regex instead of throwing', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('search', { query: '(unclosed', regex: true }));
      assert.equal(result.success, false);
      assert.equal(result.isError, true);
      assert.match(result.error ?? '', /Invalid regex/);
    });

    it('reports truncated:true up front when the candidate window exceeds maxScan', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('search', { query: 'm', maxScan: 3 }));
      assert.equal(result.success, true, result.error);
      const data = result.data as { truncated: boolean; candidatePoolSize: number; scanned: number };
      assert.equal(data.truncated, true);
      assert.equal(data.candidatePoolSize, 3);
      assert.ok(data.scanned <= 3);
    });

    it('does not report truncated when the candidate window fits within maxScan', async () => {
      const { cm } = buildStub(FIXTURE);
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('search', { query: 'searchterm', maxScan: 5000 }));
      assert.equal(result.success, true, result.error);
      const data = result.data as { truncated: boolean; matches: Array<{ id: string }> };
      assert.equal(data.truncated, false);
      assert.deepEqual(data.matches.map((m) => m.id), ['m6']);
    });

    it('surfaces the capability-absent error cleanly', async () => {
      const { cm } = buildStub(FIXTURE, { throwUnsupported: true });
      const h = new HistoryModule();
      h.bind(cm);

      const result = await h.handleToolCall(call('search', { query: 'x' }));
      assert.equal(result.success, false);
      assert.equal(result.isError, true);
      assert.match(result.error ?? '', /Chronicle history index unsupported/);
    });
  });

  it('rejects tool calls before bind()', async () => {
    const h = new HistoryModule();
    const result = await h.handleToolCall(call('stats', {}));
    assert.equal(result.success, false);
    assert.equal(result.isError, true);
    assert.match(result.error ?? '', /not bound/);
  });

  it('rejects an unknown tool name', async () => {
    const { cm } = buildStub(FIXTURE);
    const h = new HistoryModule();
    h.bind(cm);
    const result = await h.handleToolCall(call('bogus', {}));
    assert.equal(result.success, false);
    assert.equal(result.isError, true);
    assert.match(result.error ?? '', /Unknown tool/);
  });
});
