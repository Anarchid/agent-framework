# Yielding Stream Architecture + MCPL Integration

This document describes how the [Yielding Stream Architecture](./yielding-stream-architecture.md) enables [MCPL](../../mcpl/SPEC.md) implementation.

## Summary

The yielding stream architecture is a **prerequisite** for MCPL. It creates the observable lifecycle and pause points that MCPL hooks require. Without it, MCPL would need a fundamentally different approach.

## Alignment Points

### 1. ToolCallSource ≈ MCPL Server

The yielding architecture generalizes tool call sources:

```typescript
interface ToolCallSource {
  readonly sourceId: string;
  provideToolResult(callId: string, result: ToolResult): void;
}
```

MCPL servers become first-class tool call sources. When an MCPL server sends `push/event` or `inference/request`, AF receives it as a `ToolCallEvent` with `sourceType: 'mcpl'`.

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
   │ Membrane │       │    Lua    │      │   MCPL    │
   │  (LLM)   │       │  Session  │      │  Server   │
   └──────────┘       └───────────┘      └───────────┘
```

### 2. Yield Points = Hook Interception Points

Yielding creates observable pause points where MCPL context hooks execute:

```
AF                    Membrane                    MCPL Servers
 │                       │                             │
 ├─── stream(msgs) ─────►│                             │
 │                       ├──── stream request ────────►│ LLM
 │                       │◄─── tokens... ──────────────┤
 │◄─ yield tool-calls ───┤     [stream paused]         │
 │                       │                             │
 ├─── context/beforeInference ────────────────────────►│
 │◄─────────── contextInjections ──────────────────────┤
 │                       │                             │
 ├─── dispatch tools ────┼─────────────────────────────┤
 │                       │                             │
 ├─── provideResult ────►│                             │
 │                       ├──── continue ──────────────►│ LLM
```

Without yielding, the inference cycle is a black box — no place to hook `context/beforeInference` or `context/afterInference`.

### 3. State/Rollback ↔ Chronicle Forking

MCPL state management aligns with Chronicle's branchable event store:

| MCPL Concept | Chronicle Equivalent |
|--------------|---------------------|
| `checkpoint` | Event ID / commit hash |
| `parent` | Parent event pointer |
| `state/rollback` | Fork + restore to event |
| `hostState: true` | Chronicle stores state |
| `hostState: false` | Server-managed (opaque refs) |

The yielding architecture's "sync-safe points" (all streams paused, all tool calls chronicled) become valid fork points for both Chronicle and MCPL servers.

When AF forks Chronicle, it can issue `state/rollback` to MCPL servers that declared `rollback: true`, keeping external state consistent with narrative state.

### 4. Parallel Agents + Push Events

MCPL's `push/event` requires AF to handle external events while agents are mid-inference. The yielding architecture enables this:

```
Agent A stream: [paused at tool-call yield]
Agent B stream: [inferring]
                                    │
                              push/event arrives
                                    │
                              AF handles it
                              (not blocked!)
```

With the old `Membrane.infer()` blocking model, push events would queue until inference completes — defeating their purpose.

### 5. Feature Sets → Module Permissions

MCPL's feature sets (scoped access, whitelist/blacklist patterns) map to AF's module capability system:

| MCPL | AF |
|------|-----|
| `featureSets/update` | Module permission configuration |
| `scope/elevate` | Runtime permission escalation |
| `featureSet.uses` | Module capability declaration |

## Implementation Path

1. **Implement `stream()` in Membrane** — Yielding tool calls instead of handling internally
2. **AF adopts `stream()`** — Tool calls become ProcessEvents
3. **Add MCPL transport** — Parse/emit MCPL JSON-RPC messages
4. **MCPL servers as ToolCallSource** — Register with `sourceType: 'mcpl'`
5. **Wire context hooks** — `beforeInference`/`afterInference` at yield points
6. **Chronicle stores MCPL state** — For `hostState: true` servers
7. **Coordinate rollback** — Fork operations issue `state/rollback` to capable servers

## Related

- [Yielding Stream Architecture](./yielding-stream-architecture.md) — Full proposal
- [MCPL Specification](../../mcpl/SPEC.md) — Protocol details
