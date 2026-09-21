- `FrameworkConfig.providerHold(error, agentName)`: a host hook consulted on
  a failed inference, before the error policy. Returning `{ holdMs, reason }`
  parks the agent's provider admission the same way the built-in
  organization-acceleration cooldown does — no immediate retry, no
  `[inference-failed]` marker, no hard-down streak, arrivals held and merged
  into one later compile. Holds are served in slices of at most 10 minutes;
  when a slice expires the hook is asked again before any inference is
  attempted, so a long wait (a spent subscription quota window) costs no
  provider calls and an early reset is noticed within one slice.
