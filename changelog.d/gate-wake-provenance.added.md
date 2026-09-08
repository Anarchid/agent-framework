- Gate-batched wakes (`gate:debounce`) carry telemetry provenance: the
  EventGate hands the framework the newest addressed channel event (else the
  newest event naming a channel or author) as `counterparty`
  (`<serverId>:user:<id>`), `wakeChannelId` (composite channel id; push-event
  raw ids are not reported) and `wakeAt`, all from the same event, so the
  turn's `InferenceRequest` — and a host stamping gateway telemetry from it —
  can say who and where woke the agent. Telemetry only: the turn's speech
  locus (`channelId` / `addressed`) is not set by gate wakes, so routing is
  unchanged. `WakeProvenance` is exported.
