# Multi-Agent Execution Model

This document describes how the Agent Framework handles multiple agents—specifically, the sequential (not parallel) execution model and its implications.

## Summary

**Agents run sequentially, not in parallel.** The framework uses a single-threaded event loop. If one agent takes extra time (slow inference, many tool calls), all other agents wait.

## Execution Architecture

### Event Loop

The framework processes events one at a time in a FIFO queue:

```typescript
// framework.ts:641-665
private async runLoop(): Promise<void> {
  while (this.running) {
    await this.processNextEvent();
  }
}

private async processNextEvent(): Promise<void> {
  const event = this.queue.tryPop();
  if (event) {
    await this.handleProcessEvent(event);  // blocks until done
  }
  await this.processInferenceRequests();   // agents processed one at a time
}
```

### Inference Processing

When checking which agents need to run inference, the framework iterates sequentially:

```typescript
// framework.ts:789-810
for (const [agentName, requests] of requestsByAgent) {
  const agent = this.agents.get(agentName);

  // Skip if agent is busy
  if (agent.state.status === 'inferring' || agent.state.status === 'waiting_for_tools') {
    this.pendingRequests.push(...requests);  // re-queue for later
    continue;
  }

  await this.runAgentInference(agent, 0, trigger);  // awaited, blocking
}
```

## What IS Parallel

| Aspect | Parallel? | Notes |
|--------|-----------|-------|
| Tool execution | Yes | Tool calls fire asynchronously; results queued |
| Multiple tools from one inference | Yes | Can run concurrently |
| Agent state machines | Independent | Each agent tracks own state |

## What's NOT Parallel

| Aspect | Parallel? | Notes |
|--------|-----------|-------|
| Inference | No | One agent runs inference at a time |
| Event processing | No | FIFO, one event at a time |
| Module dispatch | No | Modules called sequentially per event |

## Agent State Machine

Each agent maintains independent state, but this doesn't enable parallel inference:

```
idle → inferring → waiting_for_tools → ready → idle
       ↑__________________________________|
```

States:
- **idle**: Ready to accept inference request
- **inferring**: Currently running inference (blocks other agents from starting)
- **waiting_for_tools**: Waiting for tool results
- **ready**: Has tool results, will re-run inference next cycle

## Configuring Multiple Agents

```typescript
const framework = await AgentFramework.create({
  agents: [
    {
      name: 'assistant',
      model: 'claude-opus-4-20250514',
      systemPrompt: 'You are a helpful assistant.',
      triggerSources: 'all',
      allowedTools: 'all',
    },
    {
      name: 'reviewer',
      model: 'claude-sonnet-4-20250514',
      systemPrompt: 'You review and validate responses.',
      triggerSources: ['assistant'],  // only triggered by assistant module
      allowedTools: ['files:read'],   // limited tool access
    },
  ],
  modules: [new FilesModule(), new DiscordModule()],
});
```

### Per-Agent Configuration

- `triggerSources`: Which modules can wake this agent (`'all'` or specific module names)
- `allowedTools`: Which tools this agent can use (`'all'` or specific tool names with module prefix)
- `model`: Each agent can use a different model
- `strategy`: Each agent can have its own context management strategy

## Execution Flow

```
External Event (e.g., Discord message)
    ↓
[Dequeue Single Event]
    ↓
[Dispatch to ALL Modules Sequentially]
    ├→ Module 1: onProcess() → awaited
    ├→ Module 2: onProcess() → awaited
    └→ Module N: onProcess() → awaited
    ↓
[Apply Module Responses]
    └→ requestInference() calls queued
    ↓
[Process Pending Inference Requests]
    ↓
[Iterate Through Agents]
    ├→ Agent 1: idle? → await runAgentInference()
    │   └→ Tool calls dispatched async
    │   └→ Agent enters waiting_for_tools
    ├→ Agent 2: idle? → await runAgentInference()
    └→ Agent N: ...
    ↓
[Tool Results Arrive as Events]
    └→ Agent transitions: ready → re-inference
    ↓
[Next Event Loop Cycle]
```

## Design Rationale

This sequential design is a deliberate trade-off:

**Advantages:**
- Simpler to reason about (no race conditions)
- Predictable, deterministic ordering
- Shared state (message store, modules) doesn't need synchronization
- Easier debugging and testing

**Disadvantages:**
- Slow agents block others
- Can't utilize multiple CPU cores for inference
- Not suitable for high-concurrency scenarios

## Future: Parallel Execution

True parallel agent execution would require:

1. **Separate event loops per agent** — Each agent runs its own async loop
2. **Worker threads/processes** — Offload inference to separate threads
3. **Synchronized shared state** — Message store, module registry need locking or message-passing

None of these are currently implemented. The test suite has no multi-agent parallelism tests, confirming sequential execution is the intended design.

## Related

- Agent state machine: `agent-framework/src/agent.ts`
- Event loop: `agent-framework/src/framework.ts:641-665`
- Inference processing: `agent-framework/src/framework.ts:772-811`
- Agent configuration types: `agent-framework/src/types/agent.ts`
