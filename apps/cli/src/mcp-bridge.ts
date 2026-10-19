import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import {
  isInitializedNotification,
  isInitializeRequest,
  isJSONRPCErrorResponse,
  isJSONRPCRequest,
  isJSONRPCResultResponse,
  type JSONRPCMessage,
  type JSONRPCRequest,
  type RequestId,
} from "@modelcontextprotocol/sdk/types.js";

// ---------------------------------------------------------------------------
// A stdio <-> daemon MCP bridge that survives the daemon going away.
//
// `executor mcp` is a long-lived child of the MCP client (Claude Code etc.).
// The daemon behind it is not: it restarts on upgrade, under launchd KeepAlive,
// or when an OS supervisor takes over from a session-spawned daemon. If the
// bridge exited whenever its HTTP session broke, the client would report
// "Connection closed" for the rest of its session. Instead the bridge opens a
// fresh daemon session, replays the client's cached `initialize` and
// `notifications/initialized` on it, and carries on:
//
// - messages that arrive while reconnecting are queued and sent once the new
//   session is up;
// - a request whose POST failed never reached the daemon, so it is re-sent;
// - a request the daemon had accepted but not answered when the session broke
//   is re-sent only when its method is read-only; otherwise the client gets a
//   JSON-RPC error saying the outcome is unknown (never silence);
// - queued requests that wait longer than `requestBudgetMs` get an error.
// ---------------------------------------------------------------------------

export interface McpBridgeLink {
  /** Transport to a daemon session. Not started yet. */
  readonly transport: Transport;
  /** Human label for logs (the daemon origin). */
  readonly label: string;
}

export interface McpBridgeOptions {
  /** The client side (stdio in production). Not started yet. */
  readonly client: Transport;
  /** Open a transport to the current daemon owner, or null if none is up yet
   * (the bridge retries with backoff). May wait or start a daemon. */
  readonly connect: () => Promise<McpBridgeLink | null>;
  readonly log: (line: string) => void;
  /** How long a queued client request may wait for a daemon before it is
   * answered with an error. Default 45 s (below typical 60 s client caps). */
  readonly requestBudgetMs?: number;
  /** Cap for the reconnect backoff. Default 2 s. */
  readonly maxBackoffMs?: number;
  /** Aborting it closes both sides and resolves the bridge. */
  readonly signal?: AbortSignal;
}

/** Methods that only read daemon state: safe to re-send after a drop even if
 * the daemon may already have run them. */
const READ_ONLY_METHODS = new Set([
  "ping",
  "tools/list",
  "resources/list",
  "resources/templates/list",
  "resources/read",
  "prompts/list",
  "prompts/get",
  "completion/complete",
]);

const LOST_CALL_ERROR_CODE = -32000;

/** Ids of requests the bridge itself sends (initialize replay, liveness
 * probe); their answers are consumed here, never forwarded to the client. */
const INTERNAL_ID_PREFIX = "executor-bridge-";

interface InFlight {
  readonly message: JSONRPCRequest;
  readonly queuedAt: number;
  /** The daemon answered the POST (headers received): it is handling it. */
  accepted: boolean;
}

export const runMcpBridge = async (options: McpBridgeOptions): Promise<void> => {
  const { client, log } = options;
  const requestBudgetMs = options.requestBudgetMs ?? 45_000;
  const maxBackoffMs = options.maxBackoffMs ?? 2_000;

  let link: McpBridgeLink | null = null;
  let generation = 0;
  let state: "connecting" | "connected" | "closed" = "connecting";
  // A function, not an inline comparison: `state` changes across awaits.
  const isClosed = () => state === "closed";
  let cachedInitialize: JSONRPCRequest | null = null;
  let clientInitialized = false;
  let initializeAnswered = false;
  let internalCounter = 0;
  const replayWaiters = new Map<RequestId, (ok: boolean) => void>();

  /** Client messages waiting for a connected daemon session. */
  const queue: Array<{ message: JSONRPCMessage; queuedAt: number }> = [];
  /** Client requests sent to the daemon and not yet answered. */
  const inFlight = new Map<RequestId, InFlight>();
  /** Daemon-to-client request ids, tagged with the session they came from. */
  const serverRequests = new Map<RequestId, number>();

  let resolveExit: () => void = () => {};
  const exited = new Promise<void>((resolve) => {
    resolveExit = resolve;
  });

  const answerWithError = (id: RequestId, message: string) => {
    void client
      .send({ jsonrpc: "2.0", id, error: { code: LOST_CALL_ERROR_CODE, message } })
      .catch(() => {});
  };

  const expireQueuedRequests = () => {
    const now = Date.now();
    for (let i = queue.length - 1; i >= 0; i--) {
      const entry = queue[i]!;
      if (isJSONRPCRequest(entry.message) && now - entry.queuedAt > requestBudgetMs) {
        queue.splice(i, 1);
        answerWithError(
          entry.message.id,
          `The Executor daemon was unreachable for ${Math.round(requestBudgetMs / 1000)}s; the call was not sent. Retry once it is back.`,
        );
      }
    }
  };

  /** A request the daemon accepted on a session that is now gone: re-send it
   * when that is safe, otherwise tell the client its result was lost. */
  const settleLost = (id: RequestId, entry: InFlight) => {
    inFlight.delete(id);
    if (READ_ONLY_METHODS.has(entry.message.method)) {
      queue.push({ message: entry.message, queuedAt: entry.queuedAt });
      if (state === "connected") flushQueue();
    } else {
      answerWithError(
        id,
        "The Executor daemon restarted while this call was running, so its result was lost. It may or may not have completed; check before retrying.",
      );
    }
  };

  const sendOnLink = (message: JSONRPCMessage, queuedAt: number) => {
    const current = link;
    const gen = generation;
    if (!current || state !== "connected") {
      queue.push({ message, queuedAt });
      return;
    }
    const entry: InFlight | null = isJSONRPCRequest(message)
      ? { message, queuedAt, accepted: false }
      : null;
    if (entry) inFlight.set(entry.message.id, entry);
    current.transport.send(message).then(
      () => {
        if (!entry || inFlight.get(entry.message.id) !== entry) return;
        entry.accepted = true;
        // Accepted by a session that was dropped while the POST was pending.
        if (gen !== generation) settleLost(entry.message.id, entry);
      },
      (cause: unknown) => {
        // The POST failed, so the daemon never handled it: always safe to resend.
        if (entry) inFlight.delete(entry.message.id);
        if (gen !== generation) {
          queue.push({ message, queuedAt });
          if (state === "connected") flushQueue();
          return;
        }
        log(`send to daemon failed: ${errorMessage(cause)}`);
        queue.unshift({ message, queuedAt });
        dropLink(`send failed: ${errorMessage(cause)}`);
      },
    );
  };

  /** A daemon stream broke: if calls are waiting on it, check the session is
   * still alive. A failed probe POST drops the session like any failed send. */
  const probeSession = () => {
    const current = link;
    const gen = generation;
    if (!current || state !== "connected" || inFlight.size === 0) return;
    const id = `${INTERNAL_ID_PREFIX}probe-${++internalCounter}`;
    replayWaiters.set(id, () => {});
    current.transport.send({ jsonrpc: "2.0", id, method: "ping" }).then(undefined, (cause) => {
      replayWaiters.delete(id);
      if (gen === generation) dropLink(`liveness probe failed: ${errorMessage(cause)}`);
    });
  };

  const forwardFromClient = (message: JSONRPCMessage) => {
    if (isInitializeRequest(message)) {
      cachedInitialize = message as JSONRPCRequest;
    } else if (isInitializedNotification(message)) {
      clientInitialized = true;
    } else if (
      (isJSONRPCResultResponse(message) || isJSONRPCErrorResponse(message)) &&
      message.id !== undefined
    ) {
      const from = serverRequests.get(message.id);
      serverRequests.delete(message.id);
      if (from !== undefined && from !== generation) {
        log(`dropping a reply to a request from an earlier daemon session (id ${message.id})`);
        return;
      }
    }
    sendOnLink(message, Date.now());
  };

  const forwardFromDaemon = (gen: number) => (message: JSONRPCMessage) => {
    if (gen !== generation) return;
    if (
      (isJSONRPCResultResponse(message) || isJSONRPCErrorResponse(message)) &&
      message.id !== undefined
    ) {
      const waiter = replayWaiters.get(message.id);
      if (waiter) {
        replayWaiters.delete(message.id);
        waiter(isJSONRPCResultResponse(message));
        return;
      }
      if (typeof message.id === "string" && message.id.startsWith(INTERNAL_ID_PREFIX)) return;
      if (cachedInitialize && message.id === cachedInitialize.id) initializeAnswered = true;
      inFlight.delete(message.id);
    } else if (isJSONRPCRequest(message)) {
      serverRequests.set(message.id, gen);
    }
    void client.send(message).catch((cause: unknown) => {
      log(`send to client failed: ${errorMessage(cause)}`);
    });
  };

  const replayInitialize = (transport: Transport): Promise<boolean> => {
    if (!cachedInitialize || !initializeAnswered) return Promise.resolve(true);
    const id = `${INTERNAL_ID_PREFIX}reinit-${++internalCounter}`;
    return new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => {
        replayWaiters.delete(id);
        resolve(false);
      }, 15_000);
      replayWaiters.set(id, (ok) => {
        clearTimeout(timer);
        resolve(ok);
      });
      transport.send({ ...cachedInitialize!, id }).then(undefined, () => {
        replayWaiters.delete(id);
        clearTimeout(timer);
        resolve(false);
      });
    }).then(async (ok) => {
      if (!ok) return false;
      if (clientInitialized) {
        await transport.send({ jsonrpc: "2.0", method: "notifications/initialized" });
      }
      return true;
    });
  };

  const flushQueue = () => {
    const pending = queue.splice(0, queue.length);
    for (const entry of pending) sendOnLink(entry.message, entry.queuedAt);
  };

  let reconnecting: Promise<void> | null = null;

  const establish = async (): Promise<void> => {
    let attempt = 0;
    while (!isClosed()) {
      expireQueuedRequests();
      let next: McpBridgeLink | null = null;
      try {
        next = await options.connect();
      } catch (cause) {
        log(`daemon connect failed: ${errorMessage(cause)}`);
      }
      if (next && !isClosed()) {
        const gen = ++generation;
        next.transport.onmessage = forwardFromDaemon(gen);
        next.transport.onerror = (error) => {
          if (gen !== generation || isClosed()) return;
          log(`daemon transport error: ${error.message}`);
          if (isSessionLostError(error)) dropLink(error.message);
          else probeSession();
        };
        next.transport.onclose = () => {
          if (gen === generation && !isClosed()) dropLink("daemon session closed");
        };
        try {
          await next.transport.start();
          if (await replayInitialize(next.transport)) {
            if (gen !== generation || isClosed()) return;
            link = next;
            state = "connected";
            if (attempt > 0 || generation > 1) log(`reconnected to the daemon at ${next.label}`);
            flushQueue();
            return;
          }
          log(`daemon at ${next.label} did not accept the replayed initialize`);
        } catch (cause) {
          log(`daemon session start failed: ${errorMessage(cause)}`);
        }
        closeQuietly(next.transport);
      }
      attempt += 1;
      await sleep(Math.min(100 * 2 ** Math.min(attempt, 5), maxBackoffMs));
    }
  };

  function dropLink(reason: string) {
    if (state === "closed" || state === "connecting") return;
    log(`lost the daemon session (${reason}); reconnecting`);
    state = "connecting";
    const old = link;
    link = null;
    generation += 1; // retire the old session's callbacks
    if (old) closeQuietly(old.transport);
    // Requests the daemon had accepted but not answered. Ones whose POST is
    // still pending are settled when it resolves (accepted) or fails (resent).
    for (const [id, entry] of inFlight) {
      if (entry.accepted) settleLost(id, entry);
    }
    reconnecting ??= establish().finally(() => {
      reconnecting = null;
    });
  }

  const shutdown = () => {
    if (state === "closed") return;
    state = "closed";
    const current = link;
    link = null;
    void Promise.allSettled([
      client.close(),
      current ? current.transport.close() : Promise.resolve(),
    ]).then(() => resolveExit());
  };

  client.onmessage = forwardFromClient;
  client.onclose = shutdown;
  client.onerror = (error) => log(`stdio transport error: ${error.message}`);

  const expiry = setInterval(expireQueuedRequests, 1_000);
  expiry.unref?.();

  if (options.signal?.aborted) return;
  options.signal?.addEventListener("abort", shutdown, { once: true });

  // Start reading the client right away: anything it sends before the first
  // daemon session is up waits in the queue.
  await client.start();
  reconnecting = establish().finally(() => {
    reconnecting = null;
  });
  try {
    await exited;
  } finally {
    clearInterval(expiry);
    options.signal?.removeEventListener("abort", shutdown);
  }
};

/** Stream errors that mean the daemon session is gone: the SDK gave up
 * reopening its event stream. A failed POST is not matched here; the send's
 * own rejection handles it, knowing which message failed. */
const isSessionLostError = (error: Error): boolean => {
  const text = error.message.toLowerCase();
  return text.includes("failed to reconnect") || text.includes("maximum reconnection attempts");
};

const closeQuietly = (transport: Transport) => {
  transport.onmessage = undefined;
  transport.onerror = undefined;
  transport.onclose = undefined;
  void transport.close().catch(() => {});
};

const errorMessage = (cause: unknown): string =>
  cause instanceof Error ? cause.message : String(cause);

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
