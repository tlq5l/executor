import { describe, expect, it } from "@effect/vitest";
import { StreamableHTTPError } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";

import { runMcpBridge } from "./mcp-bridge";

/** In-memory transport: `sent` records what the bridge sent; `deliver` plays a
 * message into the bridge; `onSend` decides how each send settles. */
class FakeTransport implements Transport {
  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: (message: JSONRPCMessage) => void;
  readonly sent: Array<JSONRPCMessage> = [];
  onSend: (message: JSONRPCMessage) => Promise<void> = async () => {};
  closed = false;

  async start() {}
  async send(message: JSONRPCMessage) {
    this.sent.push(message);
    return this.onSend(message);
  }
  async close() {
    this.closed = true;
    this.onclose?.();
  }
  deliver(message: JSONRPCMessage) {
    this.onmessage?.(message);
  }
}

const tick = (ms = 20) => new Promise((resolve) => setTimeout(resolve, ms));

const request = (
  id: number,
  method: string,
  params: Record<string, unknown> = {},
): JSONRPCMessage => ({
  jsonrpc: "2.0",
  id,
  method,
  params,
});

const INITIALIZE = request(1, "initialize", {
  protocolVersion: "2025-06-18",
  capabilities: {},
  clientInfo: { name: "test", version: "0" },
});

/** A daemon that answers initialize/ping and records everything else. */
const fakeDaemon = () => {
  const transport = new FakeTransport();
  transport.onSend = async (message) => {
    if ("method" in message && "id" in message) {
      if (message.method === "initialize" || message.method === "ping") {
        queueMicrotask(() =>
          transport.deliver({ jsonrpc: "2.0", id: message.id, result: {} } as JSONRPCMessage),
        );
      }
    }
  };
  return transport;
};

const startBridge = (daemons: Array<FakeTransport>, maxBackoffMs = 20) => {
  const client = new FakeTransport();
  const abort = new AbortController();
  let next = 0;
  const done = runMcpBridge({
    client,
    signal: abort.signal,
    log: () => {},
    maxBackoffMs,
    connect: async () => {
      const transport = daemons[Math.min(next++, daemons.length - 1)];
      return transport ? { transport, label: `daemon-${next}` } : null;
    },
  });
  return { client, stop: () => abort.abort(), done, connects: () => next };
};

const clientReplies = (client: FakeTransport) =>
  client.sent.filter((m): m is JSONRPCMessage & { id: number } => "id" in m);

describe("runMcpBridge", () => {
  it("replays initialize on a new session and resends a request whose POST failed", async () => {
    const first = fakeDaemon();
    const second = fakeDaemon();
    const { client, stop, done } = startBridge([first, second]);
    await tick();
    client.deliver(INITIALIZE);
    await tick();
    client.deliver({ jsonrpc: "2.0", method: "notifications/initialized" });
    await tick();

    // The first daemon dies: the next POST fails to connect.
    first.onSend = async () => {
      throw new Error("Unable to connect");
    };
    client.deliver(request(2, "tools/call", { name: "execute", arguments: { code: "1" } }));
    await tick(100);

    const replayed = second.sent.map((m) => ("method" in m ? m.method : "reply"));
    expect(replayed).toEqual(["initialize", "notifications/initialized", "tools/call"]);
    // The replayed initialize's answer is consumed, not forwarded to the client.
    expect(clientReplies(client).map((m) => m.id)).toEqual([1]);
    stop();
    await done;
  });

  it("errors a non-read-only call the daemon accepted before the session broke, and resends a read-only one", async () => {
    const first = fakeDaemon();
    const second = fakeDaemon();
    const { client, stop, done } = startBridge([first, second]);
    await tick();
    client.deliver(INITIALIZE);
    client.deliver({ jsonrpc: "2.0", method: "notifications/initialized" });
    await tick();
    client.deliver(request(2, "tools/call", { name: "execute", arguments: { code: "1" } }));
    client.deliver(request(3, "tools/list"));
    await tick();

    // Both POSTs were accepted; the event stream then dies for good.
    first.onerror?.(new Error("Maximum reconnection attempts (2) exceeded."));
    await tick(100);

    const lost = clientReplies(client).find((m) => m.id === 2) as { error?: { message: string } };
    expect(lost?.error?.message).toContain("restarted while this call was running");
    expect(second.sent.some((m) => "id" in m && m.id === 3)).toBe(true);
    expect(second.sent.some((m) => "id" in m && m.id === 2)).toBe(false);
    stop();
    await done;
  });

  it("queues calls while no daemon is reachable and sends them once one is", async () => {
    const daemon = fakeDaemon();
    const client = new FakeTransport();
    const abort = new AbortController();
    let available = false;
    const done = runMcpBridge({
      client,
      signal: abort.signal,
      log: () => {},
      maxBackoffMs: 20,
      connect: async () => (available ? { transport: daemon, label: "daemon" } : null),
    });
    await tick();
    client.deliver(INITIALIZE);
    await tick(60);
    expect(daemon.sent).toEqual([]);
    available = true;
    await tick(100);
    expect(daemon.sent.map((m) => ("method" in m ? m.method : ""))).toEqual(["initialize"]);
    expect(clientReplies(client).map((m) => m.id)).toEqual([1]);
    abort.abort();
    await done;
  });

  it("answers a message the live daemon refuses with an error instead of reconnecting", async () => {
    const daemon = fakeDaemon();
    const { client, stop, done, connects } = startBridge([daemon]);
    await tick();
    client.deliver(INITIALIZE);
    client.deliver({ jsonrpc: "2.0", method: "notifications/initialized" });
    await tick();
    daemon.onSend = async () => {
      throw new StreamableHTTPError(
        400,
        'Error POSTing to endpoint: {"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request: Server already initialized"},"id":null}',
      );
    };
    client.deliver(
      request(2, "initialize", (INITIALIZE as { params: Record<string, unknown> }).params),
    );
    client.deliver({ jsonrpc: "2.0", method: "notifications/roots/list_changed" });
    await tick(200);

    const refused = clientReplies(client).find((m) => m.id === 2) as {
      error?: { code: number; message: string };
    };
    expect(refused?.error).toEqual({
      code: -32600,
      message: "Invalid Request: Server already initialized",
    });
    expect(connects()).toBe(1);
    expect(daemon.closed).toBe(false);
    stop();
    await done;
  });

  it("backs off when a session keeps dropping right after it connects", async () => {
    const daemon = fakeDaemon();
    const { client, stop, done, connects } = startBridge([daemon], 200);
    await tick();
    client.deliver(INITIALIZE);
    client.deliver({ jsonrpc: "2.0", method: "notifications/initialized" });
    await tick();
    // The replayed handshake keeps working, but this one notification fails
    // as if the session were gone, so each reconnect's flush fails at once.
    const answer = daemon.onSend;
    daemon.onSend = async (message) => {
      if ("method" in message && message.method === "notifications/roots/list_changed") {
        throw new StreamableHTTPError(404, "Error POSTing to endpoint: Session not found");
      }
      return answer(message);
    };
    client.deliver({ jsonrpc: "2.0", method: "notifications/roots/list_changed" });
    await tick(1_000);
    // Without backoff this reconnects hundreds of times a second.
    expect(connects()).toBeLessThan(12);
    stop();
    await done;
  });

  it("gives daemon requests per-session ids so a reply to an old prompt never answers a new one", async () => {
    const first = fakeDaemon();
    const second = fakeDaemon();
    const { client, stop, done } = startBridge([first, second]);
    await tick();
    client.deliver(INITIALIZE);
    client.deliver({ jsonrpc: "2.0", method: "notifications/initialized" });
    await tick();

    first.deliver(request(0, "elicitation/create", { message: "approve OLD action" }));
    await tick();
    first.onerror?.(new Error("Maximum reconnection attempts (2) exceeded."));
    await tick(100);
    second.deliver(request(0, "elicitation/create", { message: "approve NEW action" }));
    await tick();
    const [oldPrompt, newPrompt] = client.sent.filter(
      (m) => "method" in m && m.method === "elicitation/create",
    ) as Array<JSONRPCMessage & { id: string }>;
    expect(oldPrompt!.id).not.toBe(newPrompt!.id);

    // The user accepts the OLD prompt: it must not reach the new session.
    client.deliver({ jsonrpc: "2.0", id: oldPrompt!.id, result: { action: "accept" } });
    await tick();
    const repliesToNew = () => second.sent.filter((m) => !("method" in m));
    expect(repliesToNew()).toEqual([]);

    // Answering the NEW prompt reaches the daemon under the daemon's own id.
    client.deliver({ jsonrpc: "2.0", id: newPrompt!.id, result: { action: "decline" } });
    await tick();
    expect(repliesToNew()).toEqual([{ jsonrpc: "2.0", id: 0, result: { action: "decline" } }]);
    stop();
    await done;
  });
});
