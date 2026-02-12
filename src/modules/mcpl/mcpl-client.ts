import * as net from 'node:net';
import { spawn, type ChildProcess } from 'node:child_process';
import type { Readable, Writable } from 'node:stream';
import { EventEmitter } from 'node:events';

/** TCP connection config. */
export interface McplTcpConfig {
  host: string;
  port: number;
}

/** Spawn a child process and communicate over stdio. */
export interface McplSpawnConfig {
  command: string;
  args?: string[];
  env?: Record<string, string>;
  cwd?: string;
}

export type McplClientConfig = (McplTcpConfig | McplSpawnConfig) & {
  reconnect?: boolean;
  reconnectInterval?: number;
};

function isTcpConfig(config: McplClientConfig): config is McplTcpConfig & McplClientConfig {
  return 'host' in config && 'port' in config;
}

function isSpawnConfig(config: McplClientConfig): config is McplSpawnConfig & McplClientConfig {
  return 'command' in config;
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
 * MCPL client — JSON-RPC 2.0 with newline-delimited framing.
 *
 * Supports two transports:
 *   - TCP: connect to an existing server at host:port
 *   - Spawn: start a child process, communicate over stdin/stdout
 */
export class McplClient extends EventEmitter {
  private readable: Readable | null = null;
  private writable: Writable | null = null;
  private child: ChildProcess | null = null;
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
    if (isSpawnConfig(this.config)) {
      return this.connectSpawn();
    } else if (isTcpConfig(this.config)) {
      return this.connectTcp();
    }
    throw new Error('McplClientConfig must specify either host+port (TCP) or command (spawn)');
  }

  private async connectSpawn(): Promise<ServerInfo> {
    const cfg = this.config as McplSpawnConfig & McplClientConfig;

    const child = spawn(cfg.command, cfg.args ?? [], {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, ...cfg.env },
      cwd: cfg.cwd,
    });

    this.child = child;

    // Forward stderr for diagnostics
    child.stderr?.setEncoding('utf-8');
    child.stderr?.on('data', (data: string) => {
      for (const line of data.split('\n')) {
        if (line.trim()) {
          console.error(`[mcpl:child] ${line}`);
        }
      }
    });

    child.on('error', (err) => {
      this.emit('error', err);
    });

    child.on('exit', (code, signal) => {
      this.connected = false;
      this.child = null;
      this.readable = null;
      this.writable = null;
      this.emit('disconnected');
      this.rejectAllPending('Child process exited');

      if (this.config.reconnect) {
        console.error(`[mcpl] Child exited (code=${code}, signal=${signal}), respawning...`);
        this.scheduleReconnect();
      }
    });

    this.readable = child.stdout!;
    this.writable = child.stdin!;

    this.readable.setEncoding('utf-8');
    this.readable.on('data', (data: string) => {
      this.buffer += data;
      this.processBuffer();
    });

    this.connected = true;

    try {
      const info = await this.initialize();
      this.serverInfo = info;
      this.emit('connected', info);
      return info;
    } catch (err) {
      this.connected = false;
      child.kill();
      this.child = null;
      throw err;
    }
  }

  private async connectTcp(): Promise<ServerInfo> {
    const cfg = this.config as McplTcpConfig & McplClientConfig;

    return new Promise((resolve, reject) => {
      let resolved = false;

      const socket = net.createConnection(
        { host: cfg.host, port: cfg.port },
        async () => {
          this.socket = socket;
          this.readable = socket;
          this.writable = socket;
          this.connected = true;

          try {
            const info = await this.initialize();
            this.serverInfo = info;
            this.emit('connected', info);
            if (!resolved) {
              resolved = true;
              resolve(info);
            }
          } catch (err) {
            socket.destroy();
            if (!resolved && !this.config.reconnect) {
              resolved = true;
              reject(err as Error);
            }
          }
        },
      );

      socket.setEncoding('utf-8');

      socket.on('data', (data: string) => {
        this.buffer += data;
        this.processBuffer();
      });

      socket.on('error', (err) => {
        if (!this.connected && this.config.reconnect && !resolved) {
          // Initial connection failed — resolve start() so the module isn't blocked,
          // then retry in the background.
          resolved = true;
          resolve(null as unknown as ServerInfo);
          this.scheduleReconnect();
          return;
        }
        this.emit('error', err);
        if (!this.connected && !resolved) {
          resolved = true;
          reject(err);
        }
      });

      socket.on('close', () => {
        this.connected = false;
        this.socket = null;
        this.readable = null;
        this.writable = null;
        this.emit('disconnected');
        this.rejectAllPending('Connection closed');
        if (this.config.reconnect) {
          this.scheduleReconnect();
        }
      });
    });
  }

  private scheduleReconnect(): void {
    const interval = this.config.reconnectInterval ?? 5000;
    setTimeout(() => {
      this.connect().catch((err) => this.emit('error', err));
    }, interval);
  }

  async disconnect(): Promise<void> {
    this.config.reconnect = false;
    if (this.child) {
      this.child.kill();
      this.child = null;
    }
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.readable = null;
    this.writable = null;
    this.connected = false;
  }

  /** Send a JSON-RPC request and wait for the response. */
  async sendRequest(method: string, params?: unknown): Promise<unknown> {
    if (!this.writable || !this.connected) {
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
    if (!this.writable || !this.connected) return;
    const msg: JsonRpcNotification = { jsonrpc: '2.0', method, params };
    this.writeLine(JSON.stringify(msg));
  }

  /** Send a JSON-RPC response (answering an incoming request). */
  sendResponse(id: JsonRpcId, result: unknown): void {
    if (!this.writable || !this.connected) return;
    const msg: JsonRpcResponse = { jsonrpc: '2.0', id, result };
    this.writeLine(JSON.stringify(msg));
  }

  /** Send a JSON-RPC error response. */
  sendErrorResponse(id: JsonRpcId, code: number, message: string): void {
    if (!this.writable || !this.connected) return;
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
    this.writable?.write(json + '\n');
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

  private rejectAllPending(reason: string): void {
    for (const [key, pending] of this.pending) {
      pending.reject(new Error(reason));
      this.pending.delete(key);
    }
  }
}
