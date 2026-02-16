# Agent Framework

Multi-agent framework with pluggable modules, persistent state, and concurrent tool execution. The orchestration layer of the Connectome stack.

## Overview

Agent Framework provides:

- **Event-driven architecture** — Async event loop with prioritized queue
- **Agent state machine** — Lifecycle management for LLM interactions
- **Module system** — Pluggable capabilities (Discord, Files, API, custom)
- **Persistent state** — All state stored in Chronicle
- **Concurrent tools** — Parallel tool execution with result aggregation
- **API/MCP servers** — HTTP and Model Context Protocol interfaces

## Installation

```bash
npm install @connectome/agent-framework
```

Requires Chronicle, Membrane, and Context Manager as dependencies.

## Usage

### Basic Setup

```typescript
import { AgentFramework } from '@connectome/agent-framework';
import { Membrane, AnthropicAdapter } from 'membrane';
import { FilesModule, DiscordModule } from '@connectome/agent-framework/modules';

const framework = await AgentFramework.create({
  storePath: './data',
  membrane: new Membrane(new AnthropicAdapter({ apiKey: '...' })),
  agents: [{
    name: 'assistant',
    model: 'claude-opus-4-20250514',
    systemPrompt: 'You are a helpful assistant.',
  }],
  modules: [
    new FilesModule({ workspaceRoot: './workspace' }),
    new DiscordModule({ token: process.env.DISCORD_TOKEN }),
  ],
});

// Start the event loop
framework.start();
```

### Sending Messages

```typescript
// Queue an external message
await framework.queueEvent({
  type: 'external_message',
  agentName: 'assistant',
  message: {
    role: 'user',
    content: 'What files are in the workspace?',
  },
});
```

### Custom Modules

```typescript
import { Module, ModuleContext, ToolDefinition, ToolCall, ToolResult } from '@connectome/agent-framework';

class MyModule implements Module {
  name = 'my-module';

  async start(ctx: ModuleContext): Promise<void> {
    // Initialize resources
  }

  async stop(): Promise<void> {
    // Cleanup
  }

  getTools(): ToolDefinition[] {
    return [{
      name: 'my_tool',
      description: 'Does something useful',
      input_schema: {
        type: 'object',
        properties: {
          input: { type: 'string' }
        },
        required: ['input']
      }
    }];
  }

  async handleToolCall(call: ToolCall): Promise<ToolResult> {
    if (call.name === 'my_tool') {
      return { success: true, output: `Processed: ${call.input.input}` };
    }
    return { success: false, error: 'Unknown tool' };
  }

  async onEvent(event: QueueEvent): Promise<EventResponse> {
    return { handled: false };
  }
}
```

### Built-in Modules

#### FilesModule

File operations with branching support:

```typescript
import { FilesModule } from '@connectome/agent-framework/modules';

const files = new FilesModule({
  workspaceRoot: './workspace',
  allowedExtensions: ['.ts', '.js', '.json', '.md'],
});
```

Tools: `files:read`, `files:write`, `files:list`, `files:search`, `files:edit`

#### DiscordModule

Discord bot integration:

```typescript
import { DiscordModule } from '@connectome/agent-framework/modules';

const discord = new DiscordModule({
  token: process.env.DISCORD_TOKEN,
  allowedChannels: ['general', 'bot-commands'],
});
```

Tools: `discord:send_message`, `discord:read_history`, `discord:react`

#### ApiModule

HTTP webhook handling:

```typescript
import { ApiModule } from '@connectome/agent-framework/modules';

const api = new ApiModule({
  port: 3000,
  webhookPath: '/webhook',
});
```

### API Server

```typescript
// HTTP API
framework.startApiServer({ port: 3000 });

// Endpoints:
// POST /message - Send message to agent
// GET /agents - List agents
// GET /agents/:name/state - Get agent state
```

### MCP Server

```typescript
// Model Context Protocol server
framework.startMcpServer({ port: 3001 });
```

Or via CLI:

```bash
npx agent-framework-mcp --store ./data --port 3001
```

## Architecture

```
src/
├── framework.ts       Main AgentFramework class, event loop
├── agent.ts           Agent implementation with state machine
├── module-registry.ts Module loading and tool routing
├── queue.ts           Priority event queue
├── index.ts           Public exports
├── api/
│   ├── server.ts      HTTP API server
│   └── mcp-server.ts  MCP server
├── modules/
│   ├── discord/       Discord integration
│   ├── files/         File operations
│   └── api/           Webhook handling
└── types/
    └── index.ts       Type definitions
```

## Agent State Machine

```
     ┌─────────────────────────────────────┐
     │                                     │
     ▼                                     │
   idle ──► inferring ──► waiting_for_tools ──► ready
     ▲                           │              │
     │                           │              │
     └───────────────────────────┴──────────────┘
```

| State | Description |
|-------|-------------|
| `idle` | Waiting for events |
| `inferring` | LLM request in progress |
| `waiting_for_tools` | Tool calls pending |
| `ready` | Response ready, returning to idle |

## Event Types

| Event | Description |
|-------|-------------|
| `external_message` | User/external input |
| `tool_result` | Tool execution result |
| `timer_fired` | Scheduled timer triggered |
| `inference_request` | Internal LLM request |
| `module_event` | Custom module event |

## Tool Namespacing

Tools are namespaced by module: `module:toolname`

```typescript
// Tool call from LLM
{ name: 'files:read', input: { path: './README.md' } }

// Routed to FilesModule.handleToolCall()
```

## Testing

```bash
npm run build
npm run test
```

## License

MIT
