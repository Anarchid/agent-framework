import type { ContentBlock } from 'membrane';
import type {
  Module,
  ModuleContext,
  ProcessState,
  ProcessEvent,
  EventResponse,
  ToolDefinition,
  ToolCall,
  ToolResult,
  SpeechContext,
} from '../../types/index.js';
import { McplClient, type McplClientConfig, type ServerInfo } from './mcpl-client.js';

export interface MCPLModuleConfig extends McplClientConfig {
  /** Module name for tool namespacing (e.g., "zk") */
  name: string;
  /** Which feature sets to enable (optional — enables all by default) */
  featureSets?: Record<string, boolean>;
}

interface MCPLModuleState {
  serverInfo: ServerInfo | null;
  tools: ToolDefinition[];
  channels: Record<string, ChannelInfo>;
  featureSets: string[];
}

interface ChannelInfo {
  id: string;
  type: string;
  label: string;
  direction: string;
  metadata?: unknown;
}

interface ToolSchema {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
}

/**
 * Generic MCPL client module for the Agent Framework.
 *
 * Connects to any MCPL server over TCP, proxies tools, forwards push events
 * as ExternalMessageEvents, and exposes channel operations as synthesized tools.
 */
export class MCPLModule implements Module {
  readonly name: string;
  private client: McplClient;
  private ctx: ModuleContext | null = null;
  private state: MCPLModuleState = {
    serverInfo: null,
    tools: [],
    channels: {},
    featureSets: [],
  };
  /** Most recent incoming channel ID — used as default publish target for agent speech. */
  private defaultPublishChannel: string | null = null;

  constructor(private config: MCPLModuleConfig) {
    this.name = config.name;
    this.client = new McplClient(config);
  }

  async start(ctx: ModuleContext): Promise<void> {
    this.ctx = ctx;

    // Restore state if restarting
    const saved = ctx.getState<MCPLModuleState>();
    if (saved) {
      this.state = saved;
    }

    // Set up event handlers for incoming MCPL messages
    this.client.on('request', (msg: { id: number | string; method: string; params: unknown }) => {
      this.handleIncomingRequest(msg.id, msg.method, msg.params).catch((err) => {
        console.error(`[${this.name}] Error handling MCPL request:`, err);
      });
    });

    this.client.on('notification', (msg: { method: string; params: unknown }) => {
      this.handleIncomingNotification(msg.method, msg.params);
    });

    this.client.on('disconnected', () => {
      console.log(`[${this.name}] MCPL server disconnected`);
    });

    this.client.on('error', (err: Error) => {
      console.error(`[${this.name}] MCPL connection error:`, err.message);
    });

    // Connect to MCPL server
    try {
      const serverInfo = await this.client.connect();
      this.state.serverInfo = serverInfo;
      console.log(
        `[${this.name}] Connected to MCPL server: ${serverInfo.name} v${serverInfo.version}`,
      );

      // Fetch available tools
      await this.refreshTools();

      // Store feature sets from server capabilities
      if (serverInfo.capabilities.featureSets) {
        this.state.featureSets = serverInfo.capabilities.featureSets.map((fs) => fs.name);

        // Send featureSets/update to enable requested sets
        if (this.config.featureSets) {
          const enabled = Object.entries(this.config.featureSets)
            .filter(([, v]) => v)
            .map(([k]) => k);
          const disabled = Object.entries(this.config.featureSets)
            .filter(([, v]) => !v)
            .map(([k]) => k);

          this.client.sendNotification('featureSets/update', {
            enabled: enabled.length > 0 ? enabled : undefined,
            disabled: disabled.length > 0 ? disabled : undefined,
          });
        }
      }

      ctx.setState(this.state);
    } catch (err) {
      console.error(`[${this.name}] Failed to connect to MCPL server:`, err);
    }
  }

  async stop(): Promise<void> {
    await this.client.disconnect();
  }

  getTools(): ToolDefinition[] {
    // Server-provided tools + synthesized channel tools
    const tools = [...this.state.tools];

    // Add channel management tools if server supports channels
    if (this.state.serverInfo?.capabilities.channels) {
      tools.push(
        {
          name: 'channel_open',
          description: 'Open a new channel on the MCPL server',
          inputSchema: {
            type: 'object' as const,
            properties: {
              type: { type: 'string', description: 'Channel type' },
              address: { type: 'object', description: 'Channel address/config' },
              metadata: { type: 'object', description: 'Optional metadata' },
            },
            required: ['type', 'address'],
          },
        },
        {
          name: 'channel_close',
          description: 'Close an open channel',
          inputSchema: {
            type: 'object' as const,
            properties: {
              channelId: { type: 'string', description: 'Channel ID to close' },
            },
            required: ['channelId'],
          },
        },
        {
          name: 'channel_publish',
          description: 'Send a message to a channel',
          inputSchema: {
            type: 'object' as const,
            properties: {
              channelId: { type: 'string', description: 'Target channel ID' },
              text: { type: 'string', description: 'Message text' },
            },
            required: ['channelId', 'text'],
          },
        },
        {
          name: 'channel_list',
          description: 'List available channels',
          inputSchema: {
            type: 'object' as const,
            properties: {},
          },
        },
      );
    }

    // Add rollback tool if server supports it
    if (this.state.serverInfo?.capabilities.rollback) {
      tools.push({
        name: 'rollback',
        description: 'Roll back server state to a checkpoint',
        inputSchema: {
          type: 'object' as const,
          properties: {
            featureSet: { type: 'string', description: 'Feature set to roll back' },
            checkpoint: { type: 'string', description: 'Checkpoint ID to restore' },
          },
          required: ['featureSet', 'checkpoint'],
        },
      });
    }

    return tools;
  }

  async handleToolCall(call: ToolCall): Promise<ToolResult> {
    const toolName = call.name.includes(':')
      ? call.name.split(':').slice(1).join(':')
      : call.name;
    const input = call.input as Record<string, unknown>;

    // Handle synthesized channel/rollback tools
    switch (toolName) {
      case 'channel_open':
        return this.handleChannelOpen(input);
      case 'channel_close':
        return this.handleChannelClose(input);
      case 'channel_publish':
        return this.handleChannelPublish(input);
      case 'channel_list':
        return this.handleChannelList();
      case 'rollback':
        return this.handleRollback(input);
      default:
        // Forward to MCPL server as tools/call
        return this.forwardToolCall(toolName, input);
    }
  }

  async onProcess(event: ProcessEvent, _state: ProcessState): Promise<EventResponse> {
    // We don't process events directly — we push ExternalMessageEvents
    // from the MCPL request handler. But we can handle custom events if needed.
    if (event.type === 'external-message' && event.source === this.name) {
      // Our own push events — request inference
      return { requestInference: true };
    }
    return {};
  }

  async onAgentSpeech?(
    agentName: string,
    content: ContentBlock[],
    context: SpeechContext,
  ): Promise<void> {
    // If server supports stream observation, forward agent speech
    if (this.state.serverInfo?.capabilities.streamObserver) {
      const streamId = `speech_${agentName}_${Date.now()}`;
      this.client.sendNotification('stream/complete', {
        streamId,
        content,
        context: { agentName },
      });
    }

    // If there's a default publish channel, also publish speech there
    if (this.defaultPublishChannel && this.state.channels[this.defaultPublishChannel]) {
      const text = content
        .filter((b) => b.type === 'text')
        .map((b) => (b as { type: 'text'; text: string }).text)
        .join('\n');
      if (text) {
        try {
          await this.client.sendRequest('channels/publish', {
            conversationId: context.trigger?.source ?? 'default',
            channelId: this.defaultPublishChannel,
            content: [{ type: 'text', text }],
          });
        } catch (err) {
          console.error(
            `[${this.name}] Failed to publish speech to channel:`,
            (err as Error).message,
          );
        }
      }
    }
  }

  // ── Private: MCPL incoming message handlers ──

  private async handleIncomingRequest(
    id: number | string,
    method: string,
    params: unknown,
  ): Promise<void> {
    const p = params as Record<string, unknown>;

    switch (method) {
      case 'push/event': {
        // Convert push event to ExternalMessageEvent
        const payload = p.payload as { content: Array<{ type: string; text?: string }> };
        const text = payload?.content
          ?.map((c) => (c.type === 'text' ? c.text : `[${c.type}]`))
          .join('\n') ?? '';

        this.ctx?.pushEvent({
          type: 'external-message',
          source: this.name,
          content: text,
          metadata: {
            mcplMethod: 'push/event',
            featureSet: p.featureSet,
            eventId: p.eventId,
            origin: p.origin,
          },
          triggerInference: true,
        });

        // Respond accepting the event
        this.client.sendResponse(id, { accepted: true });
        break;
      }

      case 'channels/register': {
        const channels = (p.channels as ChannelInfo[]) ?? [];
        for (const ch of channels) {
          this.state.channels[ch.id] = ch;
        }
        this.ctx?.setState(this.state);
        this.client.sendResponse(id, {});
        break;
      }

      case 'channels/incoming': {
        const messages = (p.messages as Array<{
          channelId: string;
          messageId: string;
          author: { id: string; name: string };
          content: Array<{ type: string; text?: string }>;
          timestamp: string;
          metadata?: unknown;
        }>) ?? [];

        const results = [];
        for (const msg of messages) {
          const text = msg.content
            .map((c) => (c.type === 'text' ? c.text : `[${c.type}]`))
            .join('\n');

          this.ctx?.pushEvent({
            type: 'external-message',
            source: this.name,
            content: `[channel:${msg.channelId}] ${msg.author.name}: ${text}`,
            metadata: {
              mcplMethod: 'channels/incoming',
              channelId: msg.channelId,
              messageId: msg.messageId,
              author: msg.author,
              timestamp: msg.timestamp,
              channelMetadata: msg.metadata,
            },
            triggerInference: true,
          });

          results.push({ messageId: msg.messageId, accepted: true });
        }

        // Track the most recent incoming channel for speech routing
        if (messages.length > 0) {
          this.defaultPublishChannel = messages[messages.length - 1].channelId;
        }

        this.client.sendResponse(id, { results });
        break;
      }

      default:
        this.client.sendErrorResponse(id, -32601, `Method not found: ${method}`);
    }
  }

  private handleIncomingNotification(method: string, params: unknown): void {
    const p = params as Record<string, unknown>;

    switch (method) {
      case 'channels/changed': {
        const added = (p.added as ChannelInfo[]) ?? [];
        const removed = (p.removed as string[]) ?? [];
        const updated = (p.updated as ChannelInfo[]) ?? [];

        for (const ch of added) this.state.channels[ch.id] = ch;
        for (const id of removed) delete this.state.channels[id];
        for (const ch of updated) this.state.channels[ch.id] = ch;

        this.ctx?.setState(this.state);

        // Push a notification event
        if (added.length > 0 || removed.length > 0) {
          this.ctx?.pushEvent({
            type: 'external-message',
            source: this.name,
            content: `Channels changed: +${added.length} -${removed.length} ~${updated.length}`,
            metadata: { mcplMethod: 'channels/changed', added, removed, updated },
            triggerInference: false,
          });
        }
        break;
      }

      case 'featureSets/changed': {
        // Update tracked feature sets
        const addedFs = p.added as Record<string, unknown> | undefined;
        const removedFs = p.removed as string[] | undefined;
        if (addedFs) {
          for (const name of Object.keys(addedFs)) {
            if (!this.state.featureSets.includes(name)) {
              this.state.featureSets.push(name);
            }
          }
        }
        if (removedFs) {
          this.state.featureSets = this.state.featureSets.filter(
            (fs) => !removedFs.includes(fs),
          );
        }
        this.ctx?.setState(this.state);
        break;
      }

      case 'inference/chunk': {
        // Streaming inference chunk — for server-initiated inference
        // Not needed in Phase 0
        break;
      }
    }
  }

  // ── Private: Tool call handlers ──

  private async forwardToolCall(
    name: string,
    input: Record<string, unknown>,
  ): Promise<ToolResult> {
    try {
      const result = await this.client.sendRequest('tools/call', {
        name,
        arguments: input,
      });

      const r = result as {
        content?: Array<{ type: string; text?: string }>;
        isError?: boolean;
      };

      if (r.isError) {
        const text = r.content?.map((c) => c.text ?? '').join('\n') ?? 'Unknown error';
        return { success: false, error: text, isError: true };
      }

      const text = r.content?.map((c) => c.text ?? '').join('\n') ?? '';
      return { success: true, data: text };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : String(err),
        isError: true,
      };
    }
  }

  private async handleChannelOpen(input: Record<string, unknown>): Promise<ToolResult> {
    try {
      const result = (await this.client.sendRequest('channels/open', {
        type: input.type,
        address: input.address,
        metadata: input.metadata,
      })) as { channel: ChannelInfo };

      this.state.channels[result.channel.id] = result.channel;
      this.ctx?.setState(this.state);

      return {
        success: true,
        data: `Channel opened: ${result.channel.id} (${result.channel.label})`,
      };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  private async handleChannelClose(input: Record<string, unknown>): Promise<ToolResult> {
    try {
      const result = (await this.client.sendRequest('channels/close', {
        channelId: input.channelId,
      })) as { closed: boolean };

      if (result.closed) {
        delete this.state.channels[input.channelId as string];
        this.ctx?.setState(this.state);
      }

      return { success: true, data: result.closed ? 'Channel closed' : 'Close failed' };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  private async handleChannelPublish(input: Record<string, unknown>): Promise<ToolResult> {
    try {
      const result = (await this.client.sendRequest('channels/publish', {
        conversationId: 'default',
        channelId: input.channelId,
        content: [{ type: 'text', text: input.text }],
      })) as { delivered: boolean; messageId?: string };

      return {
        success: true,
        data: result.delivered
          ? `Delivered (${result.messageId ?? 'ok'})`
          : 'Delivery failed',
      };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  private async handleChannelList(): Promise<ToolResult> {
    try {
      const result = (await this.client.sendRequest('channels/list', {})) as {
        channels: ChannelInfo[];
      };

      // Update local state
      for (const ch of result.channels) {
        this.state.channels[ch.id] = ch;
      }
      this.ctx?.setState(this.state);

      return { success: true, data: result.channels };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  private async handleRollback(input: Record<string, unknown>): Promise<ToolResult> {
    try {
      const result = (await this.client.sendRequest('state/rollback', {
        featureSet: input.featureSet,
        checkpoint: input.checkpoint,
      })) as { success: boolean; checkpoint: string; reason?: string };

      return {
        success: result.success,
        data: result.success
          ? `Rolled back to ${result.checkpoint}`
          : `Rollback failed: ${result.reason}`,
        error: result.success ? undefined : result.reason,
      };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  private async refreshTools(): Promise<void> {
    try {
      const result = (await this.client.sendRequest('tools/list', {})) as {
        tools: ToolSchema[];
      };

      this.state.tools = (result.tools ?? []).map((t) => ({
        name: t.name,
        description: t.description,
        inputSchema: t.inputSchema as ToolDefinition['inputSchema'],
      }));
    } catch (err) {
      console.error(`[${this.name}] Failed to fetch tools:`, err);
    }
  }
}

export type { McplClientConfig, ServerInfo };
