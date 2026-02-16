# Yielding Stream Architecture

This document describes a proposed architectural change to how tool calls flow between AF and Membrane, enabling parallel agents and consistent Chronicle recording.

## Problem: Current Architecture

Currently, Membrane owns the entire inference cycle:

```
AF ──► Membrane.infer() ──► [black box: infer → parse → tools → loop] ──► final result
              │
              └── AF is blocked, nothing else can happen
```

Tool calls happen **inside** Membrane, invisible to AF:
- Tool calls are TraceEvents (observability only), not ProcessEvents (work queue)
- AF only sees results after all tools complete
- Chronicle write happens atomically after the cycle ends
- Consistency is "accidentally" maintained because the system is frozen

This falls apart with:
- **Parallel agents**: Multiple inferences can't run simultaneously
- **Streaming inference**: Can't handle external events mid-inference
- **External coupled systems**: Tool effects may not match chronicled state at fork points

## Solution: Membrane as Yielding Stream

Invert control—Membrane yields tool calls to AF instead of handling them internally:

```
AF                           Membrane                      LLM
 │                              │                           │
 ├─── stream(messages) ────────►│                           │
 │                              ├──── stream request ──────►│
 │                              │◄─── tokens... ────────────┤
 │                              │     (tool_use detected)   │
 │◄── yield ToolCallEvent ──────┤     [stream paused]       │
 │                              │                           │
 ├─── (dispatch to module) ─────│                           │
 │                              │                           │
 ├─── provideResult(id, res) ──►│                           │
 │                              ├──── continue stream ─────►│
 │                              │◄─── more tokens... ───────┤
 │                              │     (end_turn)            │
 │◄── yield CompleteEvent ──────┤                           │
```

Membrane becomes an async iterator that yields events and receives results.

## What This Enables

| Concern | Resolution |
|---------|------------|
| Tool calls as ProcessEvents | AF sees and chronicles every tool call |
| Parallel agents | Each stream is independent, AF interleaves events |
| Sync barriers | Natural pause points when all streams await tool results |
| Observable cycle | No more Membrane black box |
| Consistent forking | Tool calls are recorded when they happen, not when they complete |

## Generalized Tool Call Sources

Once tool calls are ProcessEvents, the source becomes irrelevant:

```
                    ┌──────────────┐
                    │ ProcessQueue │
                    └──────▲───────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ToolCallEvent      ToolCallEvent      ToolCallEvent
        │                  │                  │
   ┌────┴────┐       ┌─────┴─────┐      ┌─────┴─────┐
   │ Membrane │       │ Lua Session│      │  External │
   │  (LLM)   │       │  (script)  │      │   Source  │
   └──────────┘       └───────────┘      └───────────┘
```

Any source that can emit tool calls and block waiting for results uses the same interface:

```typescript
interface ToolCallSource {
  readonly sourceId: string;
  provideToolResult(callId: string, result: ToolResult): void;
  cancel?(): void;
}
```

This covers:
- Membrane (LLM inference)
- Lua/scripting sessions (persistent, long-lived)
- External clients (WebSocket, API)
- Future sources (WASM plugins, external agents)

## Interface Sketch

### Membrane Stream

```typescript
type StreamEvent =
  | { type: 'tokens'; content: string }
  | { type: 'tool-calls'; calls: ToolCall[] }
  | { type: 'complete'; response: InferenceResponse }
  | { type: 'error'; error: Error };

interface MembraneStream {
  [Symbol.asyncIterator](): AsyncIterator<StreamEvent>;
  provideToolResult(callId: string, result: ToolResult): void;
  cancel(): void;
}

// Usage
const stream = membrane.stream(messages, options);
for await (const event of stream) {
  if (event.type === 'tool-calls') {
    // Dispatch to modules, collect results
    for (const call of event.calls) {
      const result = await dispatchToolCall(call);
      stream.provideToolResult(call.id, result);
    }
  }
}
```

### AF Tool Call Event

```typescript
interface ToolCallEvent extends ProcessEvent {
  type: 'tool-call';
  sourceId: string;      // which stream/session
  sourceType: string;    // 'membrane' | 'lua' | 'external'
  agentName?: string;    // if from an agent's inference
  calls: ToolCall[];     // batch from one chunk
  correlationId: string; // to route results back
}
```

## Design Decisions

### Batching

If LLM returns multiple tool calls in one response chunk, Membrane yields once with the batch. AF decides whether to execute them in parallel or sequentially.

### Correlation

Each `ToolCallEvent` carries a `correlationId`. Results are routed back to the correct waiting source via this ID. Sources track "waiting for results for call IDs [a, b, c]" internally.

### Cancellation

`cancel()` cleanly terminates a stream. Use case: reflexer agent preempts deliberator mid-inference.

### Backpressure

If AF is slow to process tool calls, the stream pauses (natural async iterator backpressure). Memory-safe by default.

## Sync Barrier Implications

With this architecture, sync-safe points become detectable:

```
Sync-safe when:
  - All MembraneStreams are paused (waiting for tool results or idle)
  - All pending ToolCallEvents have been chronicled
  - All tool results have been chronicled
```

For parallel agents, this is still a rendezvous problem, but now it's **observable**—AF can see exactly who is waiting for what.

## Chronicle Ordering

With tool requests/results as ProcessEvents, they get chronicled as they hit the queue. But parallel tool execution creates ordering questions:

```
Wall clock:     t0────t1────t2────t3────t4────t5
Tool A:         [dispatch]────────────────[complete]
Tool B:              [dispatch]────[complete]

Chronicle:      dispatch-A → dispatch-B → complete-B → complete-A
                     │            │            │            │
                 linear append order ≠ causal/temporal order
```

Chronicle records **arrival order**, not execution order. A fork point mid-flight (after dispatch, before complete) has ambiguous semantics.

**Pragmatic constraint**: Fork points are only valid when all `tool-dispatched` events have matching `tool-completed` events. Chronicle can validate this. Forking mid-flight is an error or requires explicit "abandon pending calls" semantics.

## Backwards Compatibility

Membrane is used by other projects. The streaming API must be additive, not breaking:

```typescript
// Existing API (unchanged)
interface Membrane {
  infer(messages, options): Promise<InferenceResult>;  // blocking, owns cycle
}

// Extended API
interface Membrane {
  infer(messages, options): Promise<InferenceResult>;  // unchanged
  stream(messages, options): MembraneStream;           // new, yielding
}
```

Existing consumers keep calling `infer()`. New consumers (AF v2) use `stream()`.

### Implementation Options

**Option A: `infer()` wraps `stream()`**

Implement `stream()` as the primitive, rewrite `infer()` as a wrapper:

```typescript
async infer(messages, options): Promise<InferenceResult> {
  const stream = this.stream(messages, options);
  for await (const event of stream) {
    if (event.type === 'tool-calls') {
      // Handle internally, old behavior
      for (const call of event.calls) {
        const result = await this.executeToolInternally(call);
        stream.provideToolResult(call.id, result);
      }
    }
    if (event.type === 'complete') {
      return event.response;
    }
  }
}
```

Single implementation, `infer()` becomes a convenience wrapper. Risk: subtle behavior changes if internal tool handling differs.

**Option B: Parallel implementations (recommended)**

Keep `infer()` exactly as-is, implement `stream()` separately. More code, but zero risk to existing consumers. Can deprecate `infer()` later once all consumers migrate.

### Recommendation

Option B for safety, then migrate incrementally:

1. Add `stream()` without touching `infer()`
2. AF adopts `stream()`
3. Other projects migrate at their own pace
4. Eventually deprecate `infer()` or keep as convenience wrapper

The key is `stream()` is purely additive—no existing code paths change.

## Migration Path

1. **Add `stream()` to Membrane** — New method alongside existing `infer()`, no changes to `infer()`
2. **Add ToolCallEvent to AF** — New ProcessEvent type
3. **Wire up dispatch** — AF handles tool-call events, routes results back
4. **Migrate AF to use `stream()`** — AF v2 adoption
5. **Other consumers migrate** — At their own pace
6. **Generalize to other sources** — Lua sessions, external clients
7. **Optional: deprecate `infer()`** — Or keep as convenience wrapper over `stream()`

## Related

- [Multi-Agent Execution Model](./multi-agent-execution.md) — Current sequential design and its limitations
- Chronicle consistency concerns — Tool calls recorded when dispatched, not when completed
