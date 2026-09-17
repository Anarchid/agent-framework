- Live operator surgery on the open store, no restart: `rollbackToMessage()`
  forks the chronicle at a message and switches to the fork (the source
  branch keeps everything after it); `suppressMessages()` forks at head,
  redacts the chosen messages on the fork (body-group shards always
  together), and switches. Both are the offline-recovery idiom made live and
  reuse the Discord awareness outbox (markers for messages that left the
  context; suppression batches activate only after the last redaction and
  finish at next boot if interrupted). Both refuse — never queue — while the
  agent is not idle (`OperatorActionError`, code `agent-busy`). The
  message-granular `host/command undo` now rides on `rollbackToMessage`
  (reads windowed, blob-free — no longer re-inflates every attachment on the
  branch). Its stderr line is now `[operator] rollback …` rather than
  `[host-command] undo-messages …`, and the reply carries the refusal `code`.
  Body groups are never bisected: a rollback target inside a sharded message
  snaps to the group's last shard (`tailMessageId` in the result), and a
  suppression removes the whole group as a range. The idle gate is the
  scheduler's own (`idle+turn-alive` counts as busy).
- `DiscordAwarenessOutbox.discard(batchId)` retires a prepared-but-never-
  activated batch. Live suppression uses it on every failure path so an
  orphaned explicit batch can no longer re-arm at boot and abort
  `AgentFramework.create()`.
- Durable operator log: `<storePath>/operator-actions.jsonl` (config
  `operatorLogPath`, `false` to disable) records who asked, from where, and
  why for every operator mutation — rollback, suppress, hide, undo/redo turn,
  unstick, nudge, runtime-settings update/reset/cancel — plus anything a host
  records through `recordOperatorAction()` (e.g. quiesce/resume). Each record
  is also broadcast as an `operator:action` trace; `getOperatorLog()` reads
  the tail. `undoLastTurn`/`redo`/settings methods accept an optional
  requester.
