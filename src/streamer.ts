/**
 * Event Streamer Service
 * WebSocket-based real-time event streaming for sandbox logs and events
 */

import { WebSocketServer, WebSocket } from "ws";
import { createServer, IncomingMessage } from "http";
import { v4 as uuidv4 } from "uuid";
import { EventEmitter } from "events";
import {
  StreamMessage,
  LogStreamMessage,
  ContainerEventMessage,
  ProcessEventMessage,
  PortMappingMessage,
  SessionEventMessage,
  SubscriptionMessage,
  HeartbeatMessage,
  AnyStreamMessage,
  StreamClient,
  StreamerConfig,
  LogPayload,
  ContainerEventPayload,
  ProcessEventPayload,
  PortMappingPayload,
  SessionEventPayload,
  SubscriptionPayload,
  HeartbeatPayload,
} from "./types.js";

const DEFAULT_CONFIG: StreamerConfig = {
  port: 8080,
  heartbeatInterval: 30000,
  maxMessageSize: 1024 * 1024, // 1MB
  allowedOrigins: ["*"],
};

export class EventStreamer extends EventEmitter<{
  clientConnected: (client: StreamClient) => void;
  clientDisconnected: (clientId: string) => void;
  messageReceived: (clientId: string, message: AnyStreamMessage) => void;
  subscriptionChanged: (clientId: string, sessionId: string, events: string[]) => void;
  error: (error: Error, clientId?: string) => void;
}> {
  private wss: WebSocketServer | null = null;
  private httpServer: ReturnType<typeof createServer> | null = null;
  private clients: Map<string, StreamClient> = new Map();
  private config: StreamerConfig;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  constructor(config?: Partial<StreamerConfig>) {
    super();
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Start the WebSocket server
   */
  async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.httpServer = createServer();
      
      this.wss = new WebSocketServer({
        server: this.httpServer,
        maxPayload: this.config.maxMessageSize,
      });

      this.wss.on("listening", () => {
        this.startHeartbeat();
        resolve();
      });

      this.wss.on("error", (error) => {
        reject(error);
      });

      this.wss.on("connection", (ws: WebSocket, req: IncomingMessage) => {
        this.handleConnection(ws, req);
      });

      this.httpServer.listen(this.config.port, () => {
        // Server started
      });

      this.httpServer.on("error", (error) => {
        reject(error);
      });
    });
  }

  /**
   * Stop the WebSocket server
   */
  async stop(): Promise<void> {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    // Close all client connections
    for (const client of this.clients.values()) {
      client.ws.close(1001, "Server shutting down");
    }
    this.clients.clear();

    if (this.wss) {
      await new Promise<void>((resolve) => {
        this.wss!.close(() => resolve());
      });
      this.wss = null;
    }

    if (this.httpServer) {
      await new Promise<void>((resolve) => {
        this.httpServer!.close(() => resolve());
      });
      this.httpServer = null;
    }
  }

  /**
   * Broadcast a log message to subscribed clients
   */
  broadcastLog(payload: LogPayload): void {
    const message: LogStreamMessage = {
      type: `log:${payload.stream}`,
      payload,
      timestamp: new Date(),
      id: uuidv4(),
      sessionId: payload.containerId,
    };
    this.broadcast(message, `log:${payload.stream}`);
  }

  /**
   * Broadcast a container event
   */
  broadcastContainerEvent(payload: ContainerEventPayload): void {
    const message: ContainerEventMessage = {
      type: `container:${payload.status}` as ContainerEventMessage["type"],
      payload,
      timestamp: new Date(),
      id: uuidv4(),
      sessionId: payload.sessionId,
    };
    this.broadcast(message, `container:${payload.status}`);
  }

  /**
   * Broadcast a process event
   */
  broadcastProcessEvent(payload: ProcessEventPayload): void {
    const message: ProcessEventMessage = {
      type: `process:${payload.status}` as ProcessEventMessage["type"],
      payload,
      timestamp: new Date(),
      id: uuidv4(),
      sessionId: payload.sessionId,
    };
    this.broadcast(message, `process:${payload.status}`);
  }

  /**
   * Broadcast a port mapping event
   */
  broadcastPortMapping(payload: PortMappingPayload): void {
    const message: PortMappingMessage = {
      type: "port:mapped",
      payload,
      timestamp: new Date(),
      id: uuidv4(),
      sessionId: payload.containerId,
    };
    this.broadcast(message, "port:mapped");
  }

  /**
   * Broadcast a session event
   */
  broadcastSessionEvent(payload: SessionEventPayload): void {
    const message: SessionEventMessage = {
      type: `session:${payload.status}` as SessionEventMessage["type"],
      payload,
      timestamp: new Date(),
      id: uuidv4(),
      sessionId: payload.sessionId,
    };
    this.broadcast(message, `session:${payload.status}`);
  }

  /**
   * Send a message to a specific client
   */
  sendToClient(clientId: string, message: AnyStreamMessage): boolean {
    const client = this.clients.get(clientId);
    if (!client || client.ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    try {
      client.ws.send(JSON.stringify(message));
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get connected client count
   */
  getClientCount(): number {
    return this.clients.size;
  }

  /**
   * Get clients for a session
   */
  getClientsForSession(sessionId: string): StreamClient[] {
    return Array.from(this.clients.values()).filter(
      (c) => c.sessionId === sessionId
    );
  }

  /**
   * Handle new WebSocket connection
   */
  private handleConnection(ws: WebSocket, req: IncomingMessage): void {
    const clientId = uuidv4();
    const origin = req.headers.origin || "";
    
    // Check origin if not wildcard
    if (this.config.allowedOrigins[0] !== "*" && 
        !this.config.allowedOrigins.includes(origin)) {
      ws.close(4003, "Origin not allowed");
      return;
    }

    const client: StreamClient = {
      id: clientId,
      ws,
      subscriptions: new Set(),
      connectedAt: new Date(),
      lastHeartbeat: new Date(),
    };

    this.clients.set(clientId, client);
    this.emit("clientConnected", client);

    // Send welcome message
    this.sendToClient(clientId, {
      type: "welcome",
      payload: { clientId, serverTime: new Date() },
      timestamp: new Date(),
      id: uuidv4(),
    });

    ws.on("message", (data: Buffer) => {
      try {
        const message = JSON.parse(data.toString()) as AnyStreamMessage;
        this.handleMessage(clientId, message);
      } catch (error) {
        this.emit("error", error as Error, clientId);
      }
    });

    ws.on("close", () => {
      this.clients.delete(clientId);
      this.emit("clientDisconnected", clientId);
    });

    ws.on("error", (error) => {
      this.emit("error", error, clientId);
    });

    ws.on("pong", () => {
      client.lastHeartbeat = new Date();
    });
  }

  /**
   * Handle incoming message from client
   */
  private handleMessage(clientId: string, message: AnyStreamMessage): void {
    const client = this.clients.get(clientId);
    if (!client) return;

    this.emit("messageReceived", clientId, message);

    switch (message.type) {
      case "subscribe": {
        const payload = message.payload as SubscriptionPayload;
        client.sessionId = payload.sessionId;
        for (const event of payload.events) {
          client.subscriptions.add(event);
        }
        this.emit("subscriptionChanged", clientId, payload.sessionId, payload.events);
        break;
      }
      case "unsubscribe": {
        const payload = message.payload as SubscriptionPayload;
        for (const event of payload.events) {
          client.subscriptions.delete(event);
        }
        this.emit("subscriptionChanged", clientId, payload.sessionId, payload.events);
        break;
      }
      case "heartbeat": {
        client.lastHeartbeat = new Date();
        // Respond with heartbeat ack
        this.sendToClient(clientId, {
          type: "heartbeat:ack",
          payload: { timestamp: new Date() },
          timestamp: new Date(),
          id: uuidv4(),
        });
        break;
      }
    }
  }

  /**
   * Broadcast message to all subscribed clients
   */
  private broadcast(message: AnyStreamMessage, eventType: string): void {
    const messageStr = JSON.stringify(message);
    
    for (const client of this.clients.values()) {
      if (client.ws.readyState === WebSocket.OPEN) {
        // Check if client is subscribed to this event type
        const hasEventSubscription = client.subscriptions.size === 0 || client.subscriptions.has(eventType);
        
        // Check session filtering - only filter if client has a sessionId subscription
        const sessionMatches = !client.sessionId || !message.sessionId || client.sessionId === message.sessionId;
        
        if (hasEventSubscription && sessionMatches) {
          try {
            client.ws.send(messageStr);
          } catch {
            // Client might be disconnected, will be cleaned up on close event
          }
        }
      }
    }
  }

  /**
   * Start heartbeat interval to detect stale connections
   */
  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(() => {
      const now = new Date();
      const staleThreshold = this.config.heartbeatInterval * 2;

      for (const [clientId, client] of this.clients.entries()) {
        const timeSinceHeartbeat = now.getTime() - client.lastHeartbeat.getTime();
        
        if (timeSinceHeartbeat > staleThreshold) {
          // Client is stale, close connection
          client.ws.close(1001, "Heartbeat timeout");
          this.clients.delete(clientId);
          this.emit("clientDisconnected", clientId);
        } else if (client.ws.readyState === WebSocket.OPEN) {
          // Send ping
          client.ws.ping();
        }
      }
    }, this.config.heartbeatInterval);
  }
}

export function createEventStreamer(config?: Partial<StreamerConfig>): EventStreamer {
  return new EventStreamer(config);
}
