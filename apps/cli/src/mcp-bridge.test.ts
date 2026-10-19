import { describe, expect, it } from "@effect/vitest";
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

const startBridge = (daemons: Array<FakeTransport>) => {
  const client = new FakeTransport();
  const abort = new AbortController();
  let next = 0;
  const done = runMcpBridge({
    client,
    signal: abort.signal,
    log: () => {},
    maxBackoffMs: 20,
    connect: async () => {
      const transport = daemons[Math.min(next++, daemons.length - 1)];
      return transport ? { transport, label: `daemon-${next}` } : null;
    },
  });
  return { client, stop: () => abort.abort(), done };
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
});
