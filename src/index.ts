import { EventEmitter } from "events";
import { WebSocketServer, WebSocket } from "ws";
import { v4 as uuidv4 } from "uuid";
import * as http from "http";
import {
  StreamConfig,
  Streamer,
  StreamMessage,
  SessionInfo,
  StreamerEvents,
  LogStreamOptions
} from "./types";

interface ExtendedWebSocket extends WebSocket {
  sessionId: string;
  isAlive: boolean;
}

export class StreamerImpl extends EventEmitter implements Streamer {
  private config: Required<StreamConfig>;
  private wss: WebSocketServer | null = null;
  private server: http.Server | null = null;
  private sessions: Map<string, { ws: ExtendedWebSocket; info: SessionInfo }> = new Map();
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private started = false;

  constructor(config: StreamConfig) {
    super();
    this.config = {
      port: config.port,
      host: config.host || "0.0.0.0",
      path: config.path || "/stream",
      heartbeatInterval: config.heartbeatInterval || 30000,
      maxPayloadSize: config.maxPayloadSize || 16 * 1024 * 1024 // 16MB
    };
  }

  async start(): Promise<void> {
    if (this.started) return;

    this.server = http.createServer();
    this.wss = new WebSocketServer({
      server: this.server,
      path: this.config.path,
      maxPayload: this.config.maxPayloadSize
    });

    this.wss.on("connection", (ws: ExtendedWebSocket, req) => {
      this.handleConnection(ws, req);
    });

    this.wss.on("error", (error) => {
      this.emit("error", "server" as any, error);
    });

    return new Promise((resolve, reject) => {
      this.server!.listen(this.config.port, this.config.host, () => {
        this.started = true;
        this.startHeartbeat();
        resolve();
      });
      this.server!.on("error", reject);
    });
  }

  async stop(): Promise<void> {
    if (!this.started) return;

    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }

    // Close all connections
    for (const [sessionId, { ws }] of this.sessions) {
      ws.close(1001, "Server shutting down");
    }
    this.sessions.clear();

    if (this.wss) {
      await new Promise<void>((resolve) => {
        this.wss!.close(() => resolve());
      });
      this.wss = null;
    }

    if (this.server) {
      await new Promise<void>((resolve) => {
        this.server!.close(() => resolve());
      });
      this.server = null;
    }

    this.started = false;
  }

  private handleConnection(ws: ExtendedWebSocket, req: http.IncomingMessage): void {
    const sessionId = uuidv4();
    ws.sessionId = sessionId;
    ws.isAlive = true;

    const sessionInfo: SessionInfo = {
      id: sessionId,
      connectedAt: new Date(),
      lastActivity: new Date(),
      metadata: {
        ip: req.socket.remoteAddress,
        userAgent: req.headers["user-agent"]
      }
    };

    this.sessions.set(sessionId, { ws, info: sessionInfo });
    this.emit("connection", sessionInfo);

    ws.on("pong", () => {
      ws.isAlive = true;
    });

    ws.on("message", (data: Buffer) => {
      this.handleMessage(sessionId, data);
    });

    ws.on("close", (code, reason) => {
      this.handleDisconnect(sessionId, code, reason.toString());
    });

    ws.on("error", (error) => {
      this.emit("error", sessionId, error);
    });

    // Send welcome message
    this.send(sessionId, "welcome", { sessionId, timestamp: new Date() });
  }

  private handleMessage(sessionId: string, data: Buffer): void {
    const session = this.sessions.get(sessionId);
    if (!session) return;

    session.info.lastActivity = new Date();

    try {
      const message = JSON.parse(data.toString()) as StreamMessage;
      message.timestamp = new Date(message.timestamp);
      message.sessionId = sessionId;
      
      this.emit("message", sessionId, message);
    } catch (error) {
      this.emit("error", sessionId, error as Error);
    }
  }

  private handleDisconnect(sessionId: string, code: number, reason: string): void {
    const session = this.sessions.get(sessionId);
    if (session) {
      this.sessions.delete(sessionId);
      this.emit("disconnection", sessionId);
    }
  }

  private startHeartbeat(): void {
    this.heartbeatInterval = setInterval(() => {
      for (const [sessionId, { ws }] of this.sessions) {
        if (ws.isAlive === false) {
          ws.terminate();
          this.sessions.delete(sessionId);
          this.emit("disconnection", sessionId);
          continue;
        }
        ws.isAlive = false;
        ws.ping();
      }
    }, this.config.heartbeatInterval);
  }

  broadcast(type: string, payload: unknown, excludeSessionId?: string): void {
    const message: StreamMessage = {
      id: uuidv4(),
      type,
      payload,
      timestamp: new Date()
    };

    const data = JSON.stringify(message);

    for (const [sessionId, { ws }] of this.sessions) {
      if (sessionId !== excludeSessionId && ws.readyState === WebSocket.OPEN) {
        ws.send(data);
      }
    }
  }

  send(sessionId: string, type: string, payload: unknown): boolean {
    const session = this.sessions.get(sessionId);
    if (!session || session.ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    const message: StreamMessage = {
      id: uuidv4(),
      type,
      payload,
      timestamp: new Date(),
      sessionId
    };

    try {
      session.ws.send(JSON.stringify(message));
      return true;
    } catch {
      return false;
    }
  }

  getSessions(): SessionInfo[] {
    return Array.from(this.sessions.values()).map(s => s.info);
  }

  getSession(sessionId: string): SessionInfo | undefined {
    return this.sessions.get(sessionId)?.info;
  }

  // Log streaming helper
  async *streamLogs(options: LogStreamOptions): AsyncGenerator<StreamMessage> {
    // This is a placeholder - actual implementation would connect to container logs
    const session = this.sessions.get(options.sessionId);
    if (!session) return;

    // Send initial message
    yield {
      id: uuidv4(),
      type: "log_start",
      payload: { containerId: options.containerId },
      timestamp: new Date(),
      sessionId: options.sessionId
    };
  }
}

export function createStreamer(config: StreamConfig): Streamer {
  return new StreamerImpl(config);
}
