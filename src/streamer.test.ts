/**
 * Vitest Tests for EventStreamer
 * Tests WebSocket event streaming functionality
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach, vi } from "vitest";
import { EventStreamer, createEventStreamer } from "./streamer.js";
import { WebSocket } from "ws";
import type { StreamClient, AnyStreamMessage, LogPayload, ContainerEventPayload, ProcessEventPayload, PortMappingPayload, SessionEventPayload, SubscriptionPayload } from "./types.js";

const TEST_PORT = 18080;
const TEST_TIMEOUT = 10000;

describe("EventStreamer", () => {
  let streamer: EventStreamer;
  let clientWs: WebSocket | null = null;
  let clientId: string = "";
  let receivedMessages: AnyStreamMessage[] = [];

  beforeAll(async () => {
    streamer = createEventStreamer({ port: TEST_PORT, heartbeatInterval: 5000 });
    await streamer.start();
  }, TEST_TIMEOUT);

  afterAll(async () => {
    if (clientWs && clientWs.readyState === WebSocket.OPEN) {
      clientWs.close();
    }
    await streamer.stop();
  }, TEST_TIMEOUT);

  beforeEach(() => {
    receivedMessages = [];
    clientId = "";
    clientWs = null;
  });

  afterEach(() => {
    if (clientWs && clientWs.readyState === WebSocket.OPEN) {
      clientWs.close();
    }
  });

  const connectClient = (): Promise<WebSocket> => {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(`ws://localhost:${TEST_PORT}`);
      ws.on("open", () => resolve(ws));
      ws.on("error", reject);
      ws.on("message", (data: Buffer) => {
        try {
          const message = JSON.parse(data.toString()) as AnyStreamMessage;
          receivedMessages.push(message);
          if (message.type === "welcome") {
            clientId = (message.payload as { clientId: string }).clientId;
          }
        } catch {
        }
      });
    });
  };

  describe("Connection Management", () => {
    it("should accept WebSocket connections", async () => {
      clientWs = await connectClient();
      expect(clientWs.readyState).toBe(WebSocket.OPEN);
      expect(streamer.getClientCount()).toBe(1);
    });

    it("should send welcome message on connect", async () => {
      clientWs = await connectClient();
      
      const welcomeMsg = receivedMessages.find(m => m.type === "welcome");
      expect(welcomeMsg).toBeDefined();
      expect(welcomeMsg?.payload).toHaveProperty("clientId");
      expect(welcomeMsg?.payload).toHaveProperty("serverTime");
    });

    it("should track connected clients", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50)); // Allow welcome message processing
      expect(streamer.getClientCount()).toBe(1);
      
      const clientWs2 = new WebSocket(`ws://localhost:${TEST_PORT}`);
      await new Promise<void>((resolve) => clientWs2.on("open", () => resolve()));
      await new Promise(r => setTimeout(r, 50));
      
      expect(streamer.getClientCount()).toBe(2);
      clientWs2.close();
    });

    it("should handle client disconnect", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      const initialCount = streamer.getClientCount();
      
      clientWs.close();
      await new Promise(r => setTimeout(r, 200));
      
      expect(streamer.getClientCount()).toBe(initialCount - 1);
    });

    it("should emit clientConnected and clientDisconnected events", async () => {
      const connectedPromise = new Promise<StreamClient>((resolve) => {
        streamer.once("clientConnected", (client) => resolve(client));
      });
      
      clientWs = await connectClient();
      const client = await connectedPromise;
      expect(client.id).toBeDefined();
      expect(client.ws).toBeDefined();
      
      const disconnectedPromise = new Promise<string>((resolve) => {
        streamer.once("clientDisconnected", (id) => resolve(id));
      });
      
      clientWs.close();
      const disconnectedId = await disconnectedPromise;
      expect(disconnectedId).toBe(client.id);
    });
  });

  describe("Subscription Management", () => {
    it("should handle subscribe messages", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      
      const subPromise = new Promise<{ clientId: string; sessionId: string; events: string[] }>((resolve) => {
        streamer.once("subscriptionChanged", (clientId, sessionId, events) => {
          resolve({ clientId, sessionId, events });
        });
      });
      
      clientWs!.send(JSON.stringify({
        type: "subscribe",
        payload: {
          sessionId: "session-123",
          events: ["log:stdout", "container:started"],
        },
        timestamp: new Date().toISOString(),
        id: "test-sub-1",
      }));
      
      const result = await subPromise;
      expect(result.sessionId).toBe("session-123");
      expect(result.events).toEqual(["log:stdout", "container:started"]);
    });

    it("should handle unsubscribe messages", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      
      // First subscribe
      clientWs!.send(JSON.stringify({
        type: "subscribe",
        payload: {
          sessionId: "session-123",
          events: ["log:stdout", "container:started"],
        },
        timestamp: new Date().toISOString(),
        id: "test-sub-1",
      }));
      await new Promise(r => setTimeout(r, 50));
      
      const unsubPromise = new Promise<{ clientId: string; sessionId: string; events: string[] }>((resolve) => {
        streamer.once("subscriptionChanged", (clientId, sessionId, events) => {
          resolve({ clientId, sessionId, events });
        });
      });
      
      clientWs!.send(JSON.stringify({
        type: "unsubscribe",
        payload: {
          sessionId: "session-123",
          events: ["log:stdout"],
        },
        timestamp: new Date().toISOString(),
        id: "test-unsub-1",
      }));
      
      const result = await unsubPromise;
      expect(result.events).toEqual(["log:stdout"]);
    });

    it("should filter broadcasts by subscription", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      
      // Subscribe only to log:stdout
      clientWs!.send(JSON.stringify({
        type: "subscribe",
        payload: {
          sessionId: "session-123",
          events: ["log:stdout"],
        },
        timestamp: new Date().toISOString(),
        id: "test-sub-1",
      }));
      await new Promise(r => setTimeout(r, 50));
      
      // Broadcast log:stdout - should receive
      streamer.broadcastLog({
        stream: "stdout",
        data: "test output",
        containerId: "session-123",
      });
      await new Promise(r => setTimeout(r, 100));
      
      const stdoutMsgs = receivedMessages.filter(m => m.type === "log:stdout");
      expect(stdoutMsgs.length).toBe(1);
      
      // Broadcast container:started - should NOT receive (not subscribed)
      streamer.broadcastContainerEvent({
        containerId: "container-1",
        sessionId: "session-123",
        status: "started",
      });
      await new Promise(r => setTimeout(r, 100));
      
      const containerMsgs = receivedMessages.filter(m => m.type === "container:started");
      expect(containerMsgs.length).toBe(0);
    });

    it("should filter broadcasts by sessionId", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      
      // Subscribe to all events for session-123
      clientWs!.send(JSON.stringify({
        type: "subscribe",
        payload: {
          sessionId: "session-123",
          events: ["log:stdout", "container:started"],
        },
        timestamp: new Date().toISOString(),
        id: "test-sub-1",
      }));
      await new Promise(r => setTimeout(r, 50));
      
      // Broadcast for session-123 - should receive
      streamer.broadcastLog({
        stream: "stdout",
        data: "test output",
        containerId: "session-123",
      });
      await new Promise(r => setTimeout(r, 100));
      
      let stdoutMsgs = receivedMessages.filter(m => m.type === "log:stdout");
      expect(stdoutMsgs.length).toBe(1);
      
      // Broadcast for different session - should NOT receive
      streamer.broadcastLog({
        stream: "stdout",
        data: "other session output",
        containerId: "session-456",
      });
      await new Promise(r => setTimeout(r, 100));
      
      stdoutMsgs = receivedMessages.filter(m => m.type === "log:stdout");
      expect(stdoutMsgs.length).toBe(1); // Still 1, no new message
    });

    it("should receive all events when no subscription filter", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      // Don't subscribe - should receive all
      
      streamer.broadcastLog({
        stream: "stdout",
        data: "test output",
        containerId: "any-session",
      });
      await new Promise(r => setTimeout(r, 100));
      
      const stdoutMsgs = receivedMessages.filter(m => m.type === "log:stdout");
      expect(stdoutMsgs.length).toBe(1);
    });
  });

  describe("Broadcast Methods", () => {
    beforeEach(async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      // Subscribe to all events
      clientWs!.send(JSON.stringify({
        type: "subscribe",
        payload: {
          sessionId: "test-session",
          events: ["log:stdout", "log:stderr", "container:started", "container:stopped", "process:started", "process:stopped", "port:mapped", "session:created"],
        },
        timestamp: new Date().toISOString(),
        id: "test-sub-all",
      }));
      await new Promise(r => setTimeout(r, 50));
    });

    it("should broadcast log:stdout", async () => {
      const payload: LogPayload = {
        stream: "stdout",
        data: "hello world",
        containerId: "test-session",
      };
      
      streamer.broadcastLog(payload);
      await new Promise(r => setTimeout(r, 100));
      
      const msg = receivedMessages.find(m => m.type === "log:stdout");
      expect(msg).toBeDefined();
      expect(msg?.payload.data).toBe("hello world");
      expect(msg?.payload.stream).toBe("stdout");
    });

    it("should broadcast log:stderr", async () => {
      const payload: LogPayload = {
        stream: "stderr",
        data: "error message",
        containerId: "test-session",
      };
      
      streamer.broadcastLog(payload);
      await new Promise(r => setTimeout(r, 100));
      
      const msg = receivedMessages.find(m => m.type === "log:stderr");
      expect(msg).toBeDefined();
      expect(msg?.payload.data).toBe("error message");
      expect(msg?.payload.stream).toBe("stderr");
    });

    it("should broadcast container events", async () => {
      const payload: ContainerEventPayload = {
        containerId: "container-1",
        sessionId: "test-session",
        status: "started",
      };
      
      streamer.broadcastContainerEvent(payload);
      await new Promise(r => setTimeout(r, 100));
      
      const msg = receivedMessages.find(m => m.type === "container:started");
      expect(msg).toBeDefined();
      expect(msg?.payload.status).toBe("started");
    });

    it("should broadcast process events", async () => {
      const payload: ProcessEventPayload = {
        processId: "process-1",
        sessionId: "test-session",
        command: ["node", "server.js"],
        status: "started",
        pid: 1234,
      };
      
      streamer.broadcastProcessEvent(payload);
      await new Promise(r => setTimeout(r, 100));
      
      const msg = receivedMessages.find(m => m.type === "process:started");
      expect(msg).toBeDefined();
      expect(msg?.payload.pid).toBe(1234);
    });

    it("should broadcast port mapping events", async () => {
      const payload: PortMappingPayload = {
        containerPort: 3000,
        hostPort: 30001,
        containerId: "test-session",
      };
      
      streamer.broadcastPortMapping(payload);
      await new Promise(r => setTimeout(r, 100));
      
      const msg = receivedMessages.find(m => m.type === "port:mapped");
      expect(msg).toBeDefined();
      expect(msg?.payload.containerPort).toBe(3000);
      expect(msg?.payload.hostPort).toBe(30001);
    });

    it("should broadcast session events", async () => {
      const payload: SessionEventPayload = {
        sessionId: "test-session",
        userId: "user-1",
        projectId: "project-1",
        status: "created",
      };
      
      streamer.broadcastSessionEvent(payload);
      await new Promise(r => setTimeout(r, 100));
      
      const msg = receivedMessages.find(m => m.type === "session:created");
      expect(msg).toBeDefined();
      expect(msg?.payload.sessionId).toBe("test-session");
    });
  });

  describe("Direct Client Messaging", () => {
    it("should send message to specific client", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      
      const success = streamer.sendToClient(clientId, {
        type: "custom:message",
        payload: { data: "direct message" },
        timestamp: new Date(),
        id: "direct-1",
      });
      
      expect(success).toBe(true);
      await new Promise(r => setTimeout(r, 100));
      
      const msg = receivedMessages.find(m => m.type === "custom:message");
      expect(msg).toBeDefined();
      expect(msg?.payload.data).toBe("direct message");
    });

    it("should return false for non-existent client", () => {
      const success = streamer.sendToClient("non-existent-client", {
        type: "test",
        payload: {},
        timestamp: new Date(),
        id: "test-1",
      });
      expect(success).toBe(false);
    });

    it("should return false for closed client", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      const id = clientId;
      clientWs.close();
      await new Promise(r => setTimeout(r, 200));
      
      const success = streamer.sendToClient(id, {
        type: "test",
        payload: {},
        timestamp: new Date(),
        id: "test-1",
      });
      expect(success).toBe(false);
    });
  });

  describe("Heartbeat", () => {
    it("should respond to heartbeat with ack", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      receivedMessages = []; // Clear welcome message
      
      clientWs!.send(JSON.stringify({
        type: "heartbeat",
        payload: { timestamp: new Date().toISOString(), clientId: "test" },
        timestamp: new Date().toISOString(),
        id: "hb-1",
      }));
      
      await new Promise(r => setTimeout(r, 100));
      
      const ack = receivedMessages.find(m => m.type === "heartbeat:ack");
      expect(ack).toBeDefined();
      expect(ack?.payload).toHaveProperty("timestamp");
    });
  });

  describe("getClientsForSession", () => {
    it("should return clients for a specific session", async () => {
      clientWs = await connectClient();
      await new Promise(r => setTimeout(r, 50));
      
      clientWs!.send(JSON.stringify({
        type: "subscribe",
        payload: { sessionId: "session-a", events: ["log:stdout"] },
        timestamp: new Date().toISOString(),
        id: "sub-1",
      }));
      await new Promise(r => setTimeout(r, 50));
      
      const clientWs2 = new WebSocket(`ws://localhost:${TEST_PORT}`);
      await new Promise<void>((resolve) => clientWs2.on("open", () => resolve()));
      await new Promise(r => setTimeout(r, 50));
      
      clientWs2.send(JSON.stringify({
        type: "subscribe",
        payload: { sessionId: "session-b", events: ["log:stdout"] },
        timestamp: new Date().toISOString(),
        id: "sub-2",
      }));
      await new Promise(r => setTimeout(r, 50));
      
      const sessionAClients = streamer.getClientsForSession("session-a");
      expect(sessionAClients.length).toBe(1);
      
      const sessionBClients = streamer.getClientsForSession("session-b");
      expect(sessionBClients.length).toBe(1);
      
      clientWs2.close();
    });
  });
});

describe("createEventStreamer factory", () => {
  it("should create an EventStreamer instance", () => {
    const streamer = createEventStreamer();
    expect(streamer).toBeInstanceOf(EventStreamer);
  });

  it("should accept custom config", () => {
    const streamer = createEventStreamer({ port: 9090, heartbeatInterval: 10000 });
    expect(streamer).toBeInstanceOf(EventStreamer);
  });
});