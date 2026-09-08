- `puppetToolCall` reserves the agent against turn start for its whole
  duration (#145). The idle check was a point in time before two awaits
  (tool execution, result build); a wake arriving in either gap started a
  real turn, and the synthetic `tool_use`/`tool_result` pair then wrote
  straight into the window under that turn — the wire-order corruption the
  puppet exists to avoid. The puppet now holds the turn-alive marker the
  scheduler and the `addMessage` deferral guard already respect, refuses
  when a turn is alive even if status reads idle, and — should a turn parked
  on provider admission re-enter and replace the reservation — queues the
  pair as a unit for that turn's end flush rather than splitting it.
