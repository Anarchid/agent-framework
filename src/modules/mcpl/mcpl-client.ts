import * as net from 'node:net';
import { EventEmitter } from 'node:events';

export interface McplClientConfig {
  host: string;
  port: number;
  reconnect?: boolean;
  reconnectInterval?: number;
}

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  method: string;
}

export type JsonRpcId = number | string;

export interface JsonRpcRequest {
  jsonrpc: '2.0';
  id: JsonRpcId;
  method: string;
  params?: unknown;
}

export interface JsonRpcResponse {
  jsonrpc: '2.0';
  id: JsonRpcId;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

export interface JsonRpcNotification {
  jsonrpc: '2.0';
  method: string;
  params?: unknown;
}

export type IncomingMessage =
  | { type: 'request'; id: JsonRpcId; method: string; params: unknown }
  | { type: 'notification'; method: string; params: unknown };

export interface McplCapabilities {
  version: string;
  pushEvents?: boolean;
  contextHooks?: {
    beforeInference?: boolean;
    afterInference?: { blocking: boolean };
  };
  inferenceRequest?: boolean;
  streamObserver?: boolean;
  rollback?: boolean;
  channels?: boolean;
  featureSets?: Array<{
    name: string;
    description?: string;
    uses: string[];
    rollback: boolean;
    hostState: boolean;
  }>;
  scopedAccess?: boolean;
}

export interface ServerInfo {
  name: string;
  version: string;
  capabilities: McplCapabilities;
}

/**
 * MCPL client — JSON-RPC 2.0 over TCP with newline-delimited framing.
 */
export class McplClient extends EventEmitter {
  private socket: net.Socket | null = null;
  private buffer = '';
  private nextId = 1;
  private pending = new Map<string, PendingRequest>();
  private connected = false;
  private serverInfo: ServerInfo | null = null;

  constructor(private config: McplClientConfig) {
    super();
  }

  get isConnected(): boolean {
    return this.connected;
  }

  get server(): ServerInfo | null {
    return this.serverInfo;
  }

  async connect(): Promise<ServerInfo> {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection(
        { host: this.config.host, port: this.config.port },
        async () => {
          this.socket = socket;
          this.connected = true;

          try {
            const info = await this.initialize();
            this.serverInfo = info;
            resolve(info);
          } catch (err) {
            reject(err);
          }
        },
      );

      socket.setEncoding('utf-8');

      socket.on('data', (data: string) => {
        this.buffer += data;
        this.processBuffer();
      });

      socket.on('error', (err) => {
        this.emit('error', err);
        if (!this.connected) {
          reject(err);
        }
      });

      socket.on('close', () => {
        this.connected = false;
        this.emit('disconnected');
        // Reject all pending requests
        for (const [key, pending] of this.pending) {
          pending.reject(new Error('Connection closed'));
          this.pending.delete(key);
        }
        if (this.config.reconnect) {
          setTimeout(() => {
            this.connect().catch((err) => this.emit('error', err));
          }, this.config.reconnectInterval ?? 5000);
        }
      });
    });
  }

  async disconnect(): Promise<void> {
    this.config.reconnect = false;
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
  }

  /** Send a JSON-RPC request and wait for the response. */
  async sendRequest(method: string, params?: unknown): Promise<unknown> {
    if (!this.socket || !this.connected) {
      throw new Error('Not connected');
    }

    const id = this.nextId++;
    const msg: JsonRpcRequest = { jsonrpc: '2.0', id, method, params };

    return new Promise((resolve, reject) => {
      this.pending.set(String(id), { resolve, reject, method });
      this.writeLine(JSON.stringify(msg));
    });
  }

  /** Send a JSON-RPC notification (no response expected). */
  sendNotification(method: string, params?: unknown): void {
    if (!this.socket || !this.connected) return;
    const msg: JsonRpcNotification = { jsonrpc: '2.0', method, params };
    this.writeLine(JSON.stringify(msg));
  }

  /** Send a JSON-RPC response (answering an incoming request). */
  sendResponse(id: JsonRpcId, result: unknown): void {
    if (!this.socket || !this.connected) return;
    const msg: JsonRpcResponse = { jsonrpc: '2.0', id, result };
    this.writeLine(JSON.stringify(msg));
  }

  /** Send a JSON-RPC error response. */
  sendErrorResponse(id: JsonRpcId, code: number, message: string): void {
    if (!this.socket || !this.connected) return;
    const msg: JsonRpcResponse = {
      jsonrpc: '2.0',
      id,
      error: { code, message },
    };
    this.writeLine(JSON.stringify(msg));
  }

  private async initialize(): Promise<ServerInfo> {
    const result = (await this.sendRequest('initialize', {
      protocolVersion: '2024-11-05',
      capabilities: {
        experimental: {
          mcpl: {
            version: '0.4',
            pushEvents: true,
            channels: true,
            rollback: true,
          },
        },
      },
      clientInfo: {
        name: 'afcomech-mcpl-module',
        version: '0.1.0',
      },
    })) as {
      protocolVersion: string;
      capabilities: { experimental?: { mcpl?: McplCapabilities } };
      serverInfo: { name: string; version: string };
    };

    // Send initialized notification
    this.sendNotification('notifications/initialized');

    const mcpl = result.capabilities?.experimental?.mcpl;
    return {
      name: result.serverInfo.name,
      version: result.serverInfo.version,
      capabilities: mcpl ?? { version: '0.4' },
    };
  }

  private writeLine(json: string): void {
    this.socket?.write(json + '\n');
  }

  private processBuffer(): void {
    const lines = this.buffer.split('\n');
    // Keep the last (possibly incomplete) line in buffer
    this.buffer = lines.pop() ?? '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      try {
        const msg = JSON.parse(trimmed);
        this.handleMessage(msg);
      } catch {
        // Ignore unparseable lines
      }
    }
  }

  private handleMessage(msg: Record<string, unknown>): void {
    const hasId = 'id' in msg && msg.id != null;
    const hasMethod = 'method' in msg;
    const hasResult = 'result' in msg;
    const hasError = 'error' in msg;

    if (hasId && (hasResult || hasError)) {
      // Response to our request
      const id = String(msg.id);
      const pending = this.pending.get(id);
      if (pending) {
        this.pending.delete(id);
        if (hasError) {
          const err = msg.error as { code: number; message: string };
          pending.reject(new Error(`RPC error ${err.code}: ${err.message}`));
        } else {
          pending.resolve(msg.result);
        }
      }
    } else if (hasId && hasMethod) {
      // Incoming request — emit for module to handle
      this.emit('request', {
        type: 'request',
        id: msg.id as JsonRpcId,
        method: msg.method as string,
        params: msg.params ?? {},
      });
    } else if (hasMethod && !hasId) {
      // Notification — emit for module to handle
      this.emit('notification', {
        type: 'notification',
        method: msg.method as string,
        params: msg.params ?? {},
      });
    }
  }
}
