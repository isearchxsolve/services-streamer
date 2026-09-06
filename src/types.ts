export interface StreamConfig {
  port: number;
  host?: string;
  path?: string;
  heartbeatInterval?: number;
  maxPayloadSize?: number;
}

export interface StreamMessage {
  id: string;
  type: string;
  payload: unknown;
  timestamp: Date;
  sessionId?: string;
}

export interface SessionInfo {
  id: string;
  connectedAt: Date;
  lastActivity: Date;
  metadata: Record<string, unknown>;
}

export interface StreamerEvents {
  connection: (session: SessionInfo) => void;
  disconnection: (sessionId: string) => void;
  message: (sessionId: string, message: StreamMessage) => void;
  error: (sessionId: string, error: Error) => void;
}

export interface Streamer {
  start(): Promise<void>;
  stop(): Promise<void>;
  broadcast(type: string, payload: unknown, sessionId?: string): void;
  send(sessionId: string, type: string, payload: unknown): boolean;
  getSessions(): SessionInfo[];
  getSession(sessionId: string): SessionInfo | undefined;
  
  on<K extends keyof StreamerEvents>(event: K, listener: StreamerEvents[K]): this;
  off<K extends keyof StreamerEvents>(event: K, listener: StreamerEvents[K]): this;
}

export interface LogStreamOptions {
  sessionId: string;
  containerId?: string;
  follow?: boolean;
  tail?: number;
  since?: Date;
}

export function createStreamer(config: StreamConfig): Streamer;
