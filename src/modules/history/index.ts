/**
 * HistoryModule — read-only access to an agent's full uncompressed message
 * history, backed by context-manager's native chronicle secondary index
 * (`queryMessagesByTime` / `queryMessagesByChannel` /
 * `queryMessagesByTimeAndChannel` / `getChannelMessageCounts` /
 * `getChannelTokenStats`, @animalabs/context-manager >= 0.6.0). Those calls
 * are O(log n + k) against chronicle's `/timestamp` and
 * `/metadata/external/channelId` field indexes, not a full-store scan, so
 * this module can answer "what happened in #foo last Tuesday" against a
 * multi-million-message store without walking the whole log.
 *
 * Three tools:
 *  - `stats`   — per-channel message counts (all-time) + token totals
 *                (range-scoped), for orienting before pulling raw content.
 *  - `extract` — paginated raw messages for a time range and/or channel.
 *  - `search`  — substring/regex match over a narrowed candidate window,
 *                with an explicit truncation signal when the caller's
 *                filter was too broad for `maxScan`.
 *
 * Kept deliberately read-only, same posture as HealthModule: no side
 * effects, no message mutation. `onProcess` is a no-op.
 *
 * Every query dispatch collapses onto `queryMessagesByTimeAndChannel` — its
 * own implementation already handles "only a time range", "only a channel",
 * or "neither" by delegating to the single-filter native queries (see
 * context-manager's `test/message-store-history-index.test.ts`), so this
 * module doesn't need to re-decide which of the three query methods to call.
 */

import { Worker } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import type { ContextManager, StoredMessage, ChannelCount, ChannelTokenStats } from '@animalabs/context-manager';
import type { ContentBlock } from '@animalabs/membrane';
import type { Module, ModuleContext } from '../../types/module.js';
import type { ToolDefinition, ToolCall, ToolResult, ProcessEvent } from '../../types/events.js';
import type { EventResponse, ProcessState } from '../../types/module.js';
import type { SearchWorkerMessage, SearchWorkerMatch } from './search-regex-worker.js';

// ============================================================================
// Tool input shapes
// ============================================================================

interface StatsInput {
  from?: string;
  to?: string;
  channelId?: string;
}

interface ExtractInput {
  from?: string;
  to?: string;
  channelId?: string;
  limit?: number;
  offset?: number;
  format?: 'text' | 'raw';
}

interface SearchInput {
  query: string;
  regex?: boolean;
  caseSensitive?: boolean;
  from?: string;
  to?: string;
  channelId?: string;
  limit?: number;
  maxScan?: number;
}

// ============================================================================
// Limits
// ============================================================================

const EXTRACT_DEFAULT_LIMIT = 50;
const EXTRACT_MAX_LIMIT = 200;

const SEARCH_DEFAULT_LIMIT = 20;
const SEARCH_MAX_LIMIT = 100;
const SEARCH_DEFAULT_MAX_SCAN = 5000;
const SEARCH_MAX_MAX_SCAN = 50000;

/** Characters of surrounding context kept on each side of a search snippet. */
const SNIPPET_CONTEXT_CHARS = 80;
/** Fallback snippet length when a match position isn't meaningful to center on. */
const SNIPPET_FALLBACK_CHARS = 160;

/**
 * Wall-clock deadline for a single regex-mode search call, enforced by
 * forcibly terminating the worker thread doing the matching (see
 * search-regex-worker.ts's header for why a worker — not a
 * Promise.race/setTimeout — is required to actually interrupt a stuck
 * synchronous RegExp.exec()). Generous enough for any legitimate pattern
 * against a few thousand short strings; short enough that a catastrophic
 * pattern doesn't tie up a worker (or an agent's turn) for long.
 */
const SEARCH_REGEX_TIMEOUT_MS = 2000;

/** Compiled sibling of search-regex-worker.ts — resolved at runtime the same
 *  way gate-script.ts locates gate-script-worker.js, so it tracks whatever
 *  directory this module's own compiled output lives in. */
const SEARCH_WORKER_PATH = join(dirname(fileURLToPath(import.meta.url)), 'search-regex-worker.js');

export class HistoryModule implements Module {
  readonly name = 'history';

  private ctx: ModuleContext | null = null;
  private cm: ContextManager | null = null;

  /**
   * Wire the context-manager instance. Host calls this after
   * ContextManager.open() so the module can issue the native index-backed
   * queries — ModuleContext itself exposes no store/context-manager
   * reference, only the narrower message CRUD surface (addMessage/getMessage/
   * queryMessages), which can't do range or channel queries.
   */
  bind(contextManager: ContextManager): void {
    this.cm = contextManager;
  }

  async start(ctx: ModuleContext): Promise<void> {
    this.ctx = ctx;
  }

  async stop(): Promise<void> {
    this.ctx = null;
  }

  getTools(): ToolDefinition[] {
    return [
      {
        name: 'stats',
        description:
          'Orient before pulling raw history: per-channel message counts and token totals. ' +
          'messageCountsAllTime is always all-time and all-channel-scope (the underlying index has no ' +
          'range dimension) — it is NOT limited by from/to, only optionally filtered to one channelId. ' +
          'tokenStatsForRange IS scoped to from/to when given. The two are reported separately, never ' +
          'merged, because their range semantics differ.',
        inputSchema: {
          type: 'object' as const,
          properties: {
            from: { type: 'string', description: 'ISO 8601 inclusive lower bound for tokenStatsForRange. Omit for open-ended.' },
            to: { type: 'string', description: 'ISO 8601 inclusive upper bound for tokenStatsForRange. Omit for open-ended.' },
            channelId: { type: 'string', description: 'Restrict both parts of the response to one channel.' },
          },
        },
      },
      {
        name: 'extract',
        description:
          'Fetch raw, uncompressed messages for a time range and/or channel, paginated oldest-first. ' +
          'Use after `stats` to pull the actual content. format:"text" flattens each message to a short ' +
          'readable string (tool_use/tool_result/thinking/images rendered as bracketed labels); ' +
          'format:"raw" returns the content blocks unmodified.',
        inputSchema: {
          type: 'object' as const,
          properties: {
            from: { type: 'string', description: 'ISO 8601 inclusive lower bound. Omit for open-ended.' },
            to: { type: 'string', description: 'ISO 8601 inclusive upper bound. Omit for open-ended.' },
            channelId: { type: 'string', description: 'Restrict to one channel.' },
            limit: { type: 'number', description: `Max messages to return (default ${EXTRACT_DEFAULT_LIMIT}, hard cap ${EXTRACT_MAX_LIMIT}). Must be a non-negative integer.` },
            offset: { type: 'number', description: 'Number of matching messages to skip (default 0). Must be a non-negative integer.' },
            format: { type: 'string', enum: ['text', 'raw'], description: 'Content rendering (default "text").' },
          },
        },
      },
      {
        name: 'search',
        description:
          'Search message history for a substring (default) or regex match, within an optional time ' +
          'range/channel window. Narrows candidates via the same query as `extract` before matching, up ' +
          'to maxScan candidates — if the narrowed window is larger than maxScan, the response reports ' +
          'truncated:true up front rather than silently missing later matches; narrow the filter or raise ' +
          'maxScan and retry. regex:true matching runs under a wall-clock deadline and is cleanly failed ' +
          `(not silently empty) if a pattern is too slow — avoid nested-quantifier patterns like (a+)+.`,
        inputSchema: {
          type: 'object' as const,
          properties: {
            query: { type: 'string', description: 'Substring (or regex source, when regex:true) to search for.' },
            regex: { type: 'boolean', description: 'Treat query as a regular expression (default false).' },
            caseSensitive: { type: 'boolean', description: 'Case-sensitive match (default false).' },
            from: { type: 'string', description: 'ISO 8601 inclusive lower bound. Omit for open-ended.' },
            to: { type: 'string', description: 'ISO 8601 inclusive upper bound. Omit for open-ended.' },
            channelId: { type: 'string', description: 'Restrict to one channel.' },
            limit: { type: 'number', description: `Max matches to return (default ${SEARCH_DEFAULT_LIMIT}, hard cap ${SEARCH_MAX_LIMIT}). Must be a non-negative integer.` },
            maxScan: { type: 'number', description: `Max candidate messages to scan (default ${SEARCH_DEFAULT_MAX_SCAN}, hard cap ${SEARCH_MAX_MAX_SCAN}). Must be a non-negative integer.` },
          },
          required: ['query'],
        },
      },
    ];
  }

  async handleToolCall(call: ToolCall): Promise<ToolResult> {
    try {
      if (!this.cm) {
        throw new Error('HistoryModule not bound — host must call bind(contextManager) before tool dispatch.');
      }
      switch (call.name) {
        case 'stats':
          return this.handleStats((call.input ?? {}) as StatsInput);
        case 'extract':
          return this.handleExtract((call.input ?? {}) as ExtractInput);
        case 'search':
          return await this.handleSearch((call.input ?? {}) as SearchInput);
        default:
          return { success: false, isError: true, error: `Unknown tool: ${call.name}` };
      }
    } catch (error) {
      // Catches our own validation errors (bad ISO date, invalid regex,
      // out-of-range limit/offset/maxScan, unbound module), a regex-search
      // worker timeout or worker-side error (see handleSearch/
      // searchWithRegexWorker — a ReDoS-shaped pattern surfaces here as a
      // clean timeout error, never a hang), and context-manager's
      // capability-absent error ("Chronicle history index unsupported...",
      // thrown by queryMessagesByTime/queryMessagesByChannel/
      // queryMessagesByTimeAndChannel/getChannelMessageCounts/
      // getChannelTokenStats on a chronicle build that predates the native
      // index-query capability) — all surfaced as a normal tool error
      // rather than crashing the module.
      return {
        success: false,
        isError: true,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  async onProcess(_event: ProcessEvent, _state: ProcessState): Promise<EventResponse> {
    return {};
  }

  // ==========================================================================
  // stats
  // ==========================================================================

  private handleStats(input: StatsInput): ToolResult {
    const fromMs = parseIsoDate(input.from, 'from');
    const toMs = parseIsoDate(input.to, 'to');
    const cm = this.cm as ContextManager;

    let messageCountsAllTime: ChannelCount[] = cm.getChannelMessageCounts();
    let tokenStatsForRange: ChannelTokenStats = cm.getChannelTokenStats({ fromMs, toMs });

    if (input.channelId) {
      messageCountsAllTime = messageCountsAllTime.filter((c) => c.channelId === input.channelId);
      tokenStatsForRange = {
        // Totals stay whole-range (they're documented as store-wide-for-the-range,
        // not per-channel); only the byChannel breakdown is narrowed. Labeling the
        // field `byChannel` (rather than folding a channel-filtered total into
        // totalMessages/totalTokensEstimate) keeps this from reading as a
        // misleadingly-merged single-channel total.
        totalMessages: tokenStatsForRange.totalMessages,
        totalTokensEstimate: tokenStatsForRange.totalTokensEstimate,
        byChannel: tokenStatsForRange.byChannel.filter((c) => c.channelId === input.channelId),
      };
    }

    return {
      success: true,
      data: {
        query: { from: input.from ?? null, to: input.to ?? null, channelId: input.channelId ?? null },
        messageCountsAllTime,
        tokenStatsForRange,
      },
    };
  }

  // ==========================================================================
  // extract
  // ==========================================================================

  private handleExtract(input: ExtractInput): ToolResult {
    const fromMs = parseIsoDate(input.from, 'from');
    const toMs = parseIsoDate(input.to, 'to');
    const limit = clampCount(input.limit, EXTRACT_DEFAULT_LIMIT, EXTRACT_MAX_LIMIT, 'limit');
    const offset = clampCount(input.offset, 0, Number.MAX_SAFE_INTEGER, 'offset');
    const format = input.format ?? 'text';

    const cm = this.cm as ContextManager;
    const result = cm.queryMessagesByTimeAndChannel({
      fromMs,
      toMs,
      channelId: input.channelId,
      limit,
      offset,
    });

    return {
      success: true,
      data: {
        matchedCount: result.matchedCount,
        returned: result.messages.length,
        messages: result.messages.map((msg) => projectMessage(msg, format)),
      },
    };
  }

  // ==========================================================================
  // search
  // ==========================================================================

  private async handleSearch(input: SearchInput): Promise<ToolResult> {
    if (!input.query) {
      throw new Error('search requires a non-empty "query".');
    }
    const fromMs = parseIsoDate(input.from, 'from');
    const toMs = parseIsoDate(input.to, 'to');
    const limit = clampCount(input.limit, SEARCH_DEFAULT_LIMIT, SEARCH_MAX_LIMIT, 'limit');
    const maxScan = clampCount(input.maxScan, SEARCH_DEFAULT_MAX_SCAN, SEARCH_MAX_MAX_SCAN, 'maxScan');
    const caseSensitive = input.caseSensitive ?? false;
    const flags = caseSensitive ? '' : 'i';

    // Validate regex SYNTAX up front — an invalid pattern is a clean tool
    // error, not a crash mid-scan. This does NOT bound match TIME (a
    // syntactically valid pattern can still backtrack catastrophically),
    // which is why regex-mode matching itself runs on a worker below rather
    // than here.
    if (input.regex) {
      try {
        new RegExp(input.query, flags);
      } catch (error) {
        throw new Error(`Invalid regex "${input.query}": ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    const needle = caseSensitive ? input.query : input.query.toLowerCase();

    const cm = this.cm as ContextManager;
    // Fetch one more than maxScan so an oversized candidate window is
    // detected up front (before any matching is attempted), rather than
    // silently scanning maxScan candidates and returning as if that were the
    // whole window. See the tool description's `truncated` contract.
    const probe = cm.queryMessagesByTimeAndChannel({
      fromMs,
      toMs,
      channelId: input.channelId,
      limit: maxScan + 1,
      offset: 0,
    });
    const truncated = probe.messages.length > maxScan;
    const candidates = truncated ? probe.messages.slice(0, maxScan) : probe.messages;

    if (input.regex) {
      // Regex matching against caller-supplied patterns is ReDoS-shaped: a
      // pathological pattern (e.g. `(a+)+$`) can take catastrophically long
      // against one candidate string, and a synchronous RegExp.exec() on
      // this thread would block the WHOLE framework's event loop — every
      // agent's turns, health checks, timers — for as long as it runs, with
      // no way to interrupt it from this same thread. Route matching
      // through a worker thread instead, which can be forcibly terminated
      // on a deadline. See search-regex-worker.ts's header for the full
      // rationale.
      const { matches, scanned } = await this.searchWithRegexWorker(candidates, input.query, flags, limit);
      return { success: true, data: { scanned, candidatePoolSize: candidates.length, truncated, matches } };
    }

    // Plain substring search (String.prototype.indexOf) is inherently
    // linear in input length — no ReDoS-equivalent risk — so it stays
    // in-process. This is also the common case, so it pays no worker-spawn
    // overhead.
    const matches: SearchMatch[] = [];
    let scanned = 0;
    for (const msg of candidates) {
      scanned++;
      const text = flattenContent(msg.content);
      const hit = matchSubstring(text, needle, caseSensitive);
      if (!hit) continue;
      matches.push({
        id: String(msg.id),
        timestamp: msg.timestamp.toISOString(),
        participant: msg.participant,
        channelId: getChannelId(msg) ?? null,
        snippet: snippetAround(text, hit.index, hit.length),
      });
      if (matches.length >= limit) break;
    }

    return {
      success: true,
      data: { scanned, candidatePoolSize: candidates.length, truncated, matches },
    };
  }

  /**
   * Run regex matching for `search` on a worker thread with a hard
   * wall-clock deadline, so a catastrophically-backtracking pattern can be
   * forcibly killed instead of hanging the framework. One worker per call
   * (not per candidate — spawn overhead would dominate at scale; not a
   * persistent pool — a fresh worker per call means one bad pattern can
   * never contaminate a later search). Always terminated on the way out,
   * success or failure, so nothing lingers.
   *
   * On timeout or a worker-side error this THROWS (caught by
   * handleToolCall's try/catch, same as every other error path in this
   * module) rather than returning an empty match list — a timed-out search
   * must never be indistinguishable from a clean "no matches" result.
   */
  private async searchWithRegexWorker(
    candidates: StoredMessage[],
    pattern: string,
    flags: string,
    limit: number,
  ): Promise<{ matches: SearchMatch[]; scanned: number }> {
    const texts = candidates.map((msg) => flattenContent(msg.content));
    let worker: Worker | undefined;
    try {
      const { matches: rawMatches, scanned } = await new Promise<{ matches: SearchWorkerMatch[]; scanned: number }>(
        (resolve, reject) => {
          worker = new Worker(SEARCH_WORKER_PATH, { workerData: { texts, pattern, flags, limit } });
          const timer = setTimeout(() => {
            reject(
              new Error(
                `search timed out after ${SEARCH_REGEX_TIMEOUT_MS}ms while matching regex "${pattern}" against ` +
                  `up to ${texts.length} candidate(s) — the pattern may be catastrophically slow (exponential ` +
                  'backtracking) against this data; try a simpler pattern, a literal substring search ' +
                  '(regex:false), or a smaller maxScan.',
              ),
            );
          }, SEARCH_REGEX_TIMEOUT_MS);
          timer.unref?.();
          worker.once('message', (msg: SearchWorkerMessage) => {
            clearTimeout(timer);
            if (msg.type === 'error') reject(new Error(msg.error));
            else resolve({ matches: msg.matches, scanned: msg.scanned });
          });
          worker.once('error', (err) => {
            clearTimeout(timer);
            reject(err);
          });
        },
      );

      const matches: SearchMatch[] = rawMatches.map(({ candidateIndex, matchIndex, matchLength }) => {
        const msg = candidates[candidateIndex]!;
        const text = texts[candidateIndex]!;
        return {
          id: String(msg.id),
          timestamp: msg.timestamp.toISOString(),
          participant: msg.participant,
          channelId: getChannelId(msg) ?? null,
          snippet: snippetAround(text, matchIndex, matchLength),
        };
      });
      return { matches, scanned };
    } finally {
      // Always kill the worker — whether it finished, errored, or is still
      // stuck mid-backtrack when the deadline hit. terminate() on an
      // already-exited worker is a harmless no-op.
      if (worker) void worker.terminate().catch(() => {});
    }
  }
}

// ============================================================================
// Helpers
// ============================================================================

/** Parse an ISO 8601 date string to Unix ms. Throws a clear error (not a
 *  crash) on an unparseable string; returns undefined for an omitted field
 *  (open-ended bound). */
function parseIsoDate(value: string | undefined, field: string): number | undefined {
  if (value === undefined) return undefined;
  const ms = Date.parse(value);
  if (Number.isNaN(ms)) {
    throw new Error(`Invalid ISO 8601 date for "${field}": ${JSON.stringify(value)}`);
  }
  return ms;
}

/**
 * Validate a pagination-ish numeric input (limit/offset/maxScan) and clamp
 * it to an upper bound. `Math.min(value, max)` alone is NOT sufficient here:
 * it only enforces an upper bound, so `Math.min(-1, 200)` is `-1`, not a
 * sane value — a negative limit/offset/maxScan would sail straight past the
 * "hard cap" and reach a native chronicle call expecting an unsigned
 * pagination argument (observed: `extract({limit:-1})` returning every
 * message in the store instead of being capped). Rejects (rather than
 * silently clamping) anything that isn't a finite non-negative integer, so
 * a caller mistake is surfaced as a clean tool error instead of silently
 * doing something other than what was asked.
 */
function clampCount(value: number | undefined, def: number, max: number, field: string): number {
  if (value === undefined) return def;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`"${field}" must be a finite number, got ${JSON.stringify(value)}.`);
  }
  if (!Number.isInteger(value)) {
    throw new Error(`"${field}" must be an integer, got ${value}.`);
  }
  if (value < 0) {
    throw new Error(`"${field}" must be >= 0, got ${value}.`);
  }
  return Math.min(value, max);
}

/** channelId lives at metadata.external.channelId — same path context-manager's
 *  own native index reads from (message-store.ts's CHANNEL_FIELD). Metadata's
 *  index-signature type means this needs an explicit narrow, same as
 *  MessageStore.getChannelTokenStats does internally. */
function getChannelId(msg: StoredMessage): string | undefined {
  const external = msg.metadata?.external as { channelId?: string } | undefined;
  return external?.channelId;
}

function projectMessage(msg: StoredMessage, format: 'text' | 'raw'): Record<string, unknown> {
  return {
    id: String(msg.id),
    timestamp: msg.timestamp.toISOString(),
    participant: msg.participant,
    channelId: getChannelId(msg) ?? null,
    content: format === 'raw' ? msg.content : flattenContent(msg.content),
  };
}

/** Flatten a message's content blocks to one short, readable string. Text
 *  blocks verbatim; everything else (tool_use/tool_result/thinking/media) as
 *  a short bracketed label — this is for agent readability and search
 *  matching, not byte-faithful reconstruction. */
function flattenContent(content: ContentBlock[]): string {
  return content.map(blockLabel).join(' ').trim();
}

function blockLabel(block: ContentBlock): string {
  switch (block.type) {
    case 'text':
      return block.text;
    case 'tool_use':
      return `[tool_use: ${block.name}]`;
    case 'tool_result':
      return block.toolName ? `[tool_result: ${block.toolName}]` : '[tool_result]';
    case 'thinking':
    case 'redacted_thinking':
      return '[thinking]';
    case 'image':
      return '[image]';
    case 'generated_image':
      return '[image]';
    case 'document':
      return '[document]';
    case 'audio':
      return '[audio]';
    case 'video':
      return '[video]';
    default:
      // Exhaustiveness guard: a future ContentBlock variant falls back to a
      // generic label instead of a compile error at a call site far from here.
      return `[${(block as { type: string }).type}]`;
  }
}

interface MatchHit {
  index: number;
  length: number;
}

/** One `search` result entry — shared shape between the in-process substring
 *  path and the worker-backed regex path. */
interface SearchMatch {
  id: string;
  timestamp: string;
  participant: string;
  channelId: string | null;
  snippet: string;
}

function matchSubstring(text: string, needle: string, caseSensitive: boolean): MatchHit | null {
  const haystack = caseSensitive ? text : text.toLowerCase();
  const index = haystack.indexOf(needle);
  if (index === -1) return null;
  return { index, length: needle.length };
}

/** ~SNIPPET_CONTEXT_CHARS of surrounding context on each side of a match, or
 *  the first ~SNIPPET_FALLBACK_CHARS of content when no meaningful match
 *  position is given (kept simple on purpose — this is a readability aid,
 *  not a highlighting engine). */
function snippetAround(text: string, index: number, length: number): string {
  if (index < 0) return text.slice(0, SNIPPET_FALLBACK_CHARS);
  const start = Math.max(0, index - SNIPPET_CONTEXT_CHARS);
  const end = Math.min(text.length, index + length + SNIPPET_CONTEXT_CHARS);
  const prefix = start > 0 ? '…' : '';
  const suffix = end < text.length ? '…' : '';
  return prefix + text.slice(start, end) + suffix;
}
