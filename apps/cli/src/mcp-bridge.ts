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
// - a request that provably never ran (connect refused, or an HTTP answer
//   such as 404 for a gone session) is re-sent;
// - a request the daemon may have started when the session broke (its POST
//   was accepted, or its connection broke mid-request) is re-sent only when
//   its method is safe to repeat (read-only, or `initialize`); otherwise the
//   client gets a JSON-RPC error saying the outcome is unknown (never silence);
// - a message the live daemon refuses (HTTP 4xx other than 401/404) is not a
//   lost session: a request gets that error back, anything else is dropped;
// - sessions that drop right after connecting back off like failed connects;
// - daemon-to-client request ids are rewritten per session, so a reply meant
//   for an old session's prompt can never answer a new session's prompt;
// - queued requests that wait longer than `requestBudgetMs` get an error;
//   queued notifications and replies that old are dropped, so one the daemon
//   keeps refusing cannot reopen sessions forever.
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

/** Methods safe to re-send after a drop even if the daemon may already have
 * run them: they only read daemon state, or (`initialize`) only set up the
 * session that is gone, so the new session needs it anyway. */
const RESENDABLE_METHODS = new Set([
  "initialize",
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
/** Prefix of the ids the client sees on daemon-to-client requests. */
const SERVER_REQUEST_ID_PREFIX = `${INTERNAL_ID_PREFIX}srv-`;
/** A session that drops sooner than this after connecting counts as a quick
 * drop: the next reconnect waits, so a refused replay cannot spin. */
const QUICK_DROP_MS = 2_000;

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

  /** Client messages waiting for a connected daemon session. `forGeneration`
   * pins a reply to the session whose prompt it answers. */
  const queue: Array<{ message: JSONRPCMessage; queuedAt: number; forGeneration?: number }> = [];
  /** Client requests sent to the daemon and not yet answered. */
  const inFlight = new Map<RequestId, InFlight>();
  /** Daemon-to-client requests by the id the client saw: the session they
   * came from and the daemon's own id. */
  const serverRequests = new Map<string, { generation: number; id: RequestId }>();
  let connectedAt = 0;
  let quickDrops = 0;
  const backoffMs = (attempt: number) => Math.min(100 * 2 ** Math.min(attempt, 5), maxBackoffMs);

  let resolveExit: () => void = () => {};
  const exited = new Promise<void>((resolve) => {
    resolveExit = resolve;
  });

  const answerWithError = (id: RequestId, message: string, code = LOST_CALL_ERROR_CODE) => {
    void client.send({ jsonrpc: "2.0", id, error: { code, message } }).catch(() => {});
  };

  const expireQueued = () => {
    const now = Date.now();
    for (let i = queue.length - 1; i >= 0; i--) {
      const entry = queue[i]!;
      if (now - entry.queuedAt <= requestBudgetMs) continue;
      queue.splice(i, 1);
      const seconds = Math.round(requestBudgetMs / 1000);
      if (isJSONRPCRequest(entry.message)) {
        answerWithError(
          entry.message.id,
          `The Executor daemon was unreachable for ${seconds}s; the call was not sent. Retry once it is back.`,
        );
      } else {
        const what = "method" in entry.message ? entry.message.method : "reply";
        log(`dropping a queued ${what} the daemon did not take within ${seconds}s`);
      }
    }
  };

  /** A request the daemon accepted on a session that is now gone: re-send it
   * when that is safe, otherwise tell the client its result was lost. */
  const settleLost = (id: RequestId, entry: InFlight) => {
    inFlight.delete(id);
    if (RESENDABLE_METHODS.has(entry.message.method)) {
      sendOnLink(entry.message, entry.queuedAt);
    } else {
      answerWithError(
        id,
        "The Executor daemon restarted while this call was running, so its result was lost. It may or may not have completed; check before retrying.",
      );
    }
  };

  /** `forGeneration`: the message is a reply to that session's prompt, so it
   * is dropped once that session is gone (a new session would read it as the
   * answer to its own prompt with the same daemon id). */
  const sendOnLink = (message: JSONRPCMessage, queuedAt: number, forGeneration?: number) => {
    if (forGeneration !== undefined && forGeneration !== generation) {
      log("dropping a reply to a prompt from an earlier daemon session");
      return;
    }
    const current = link;
    const gen = generation;
    if (!current || state !== "connected") {
      queue.push({ message, queuedAt, forGeneration });
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
        if (entry) inFlight.delete(entry.message.id);
        if (gen === generation) {
          const rejection = daemonRejection(cause);
          if (rejection) {
            // The session is alive; the daemon refused this one message.
            log(`daemon refused a message (HTTP ${rejection.status}): ${rejection.message}`);
            if (entry) answerWithError(entry.message.id, rejection.message, rejection.code);
            return;
          }
          log(`send to daemon failed: ${errorMessage(cause)}`);
          dropLink(`send failed: ${errorMessage(cause)}`);
        }
        // The daemon does not answer a POST until the call finishes, so a
        // connection that broke mid-request may have run it. Only an HTTP
        // answer or a refused connect proves it never ran.
        if (entry && !neverReachedDaemon(cause)) settleLost(entry.message.id, entry);
        else sendOnLink(message, queuedAt, forGeneration);
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
      // A refused ping still proves the session is alive.
      if (gen === generation && !daemonRejection(cause)) {
        dropLink(`liveness probe failed: ${errorMessage(cause)}`);
      }
    });
  };

  const forwardFromClient = (message: JSONRPCMessage) => {
    if (isInitializeRequest(message)) {
      // Replays use the handshake the daemon accepted, not a later repeat.
      if (!initializeAnswered) cachedInitialize = message as JSONRPCRequest;
    } else if (isInitializedNotification(message)) {
      clientInitialized = true;
    } else if (isJSONRPCResultResponse(message) || isJSONRPCErrorResponse(message)) {
      // Every daemon request reached the client under a bridge id, so a reply
      // with any other id, or for an earlier session, has nowhere to go.
      const origin = typeof message.id === "string" ? serverRequests.get(message.id) : undefined;
      if (typeof message.id === "string") serverRequests.delete(message.id);
      if (!origin || origin.generation !== generation) {
        log(`dropping a reply to no current daemon request (id ${String(message.id)})`);
        return;
      }
      sendOnLink({ ...message, id: origin.id }, Date.now(), origin.generation);
      return;
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
    }
    void client.send(withClientIds(gen, message)).catch((cause: unknown) => {
      log(`send to client failed: ${errorMessage(cause)}`);
    });
  };

  /** Daemon requests get a per-session id before the client sees them, so a
   * new session restarting its ids at 0 cannot collide with an old prompt. */
  const withClientIds = (gen: number, message: JSONRPCMessage): JSONRPCMessage => {
    if (isJSONRPCRequest(message)) {
      const clientId = serverRequestId(gen, message.id);
      serverRequests.set(clientId, { generation: gen, id: message.id });
      return { ...message, id: clientId };
    }
    if (isCancelledNotification(message)) {
      // The daemon withdrew one of its own requests: name it as the client saw it.
      const requestId = serverRequestId(gen, message.params.requestId);
      return { ...message, params: { ...message.params, requestId } };
    }
    return message;
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
    expireQueued();
    const pending = queue.splice(0, queue.length);
    for (const entry of pending) sendOnLink(entry.message, entry.queuedAt, entry.forGeneration);
  };

  let reconnecting: Promise<void> | null = null;
  const reconnect = () => {
    reconnecting ??= establish().finally(() => {
      reconnecting = null;
      // A send that failed during the queue flush can drop the new session
      // before this runs; its reconnect() found this one still busy.
      if (state === "connecting") reconnect();
    });
  };

  const establish = async (): Promise<void> => {
    // A session that dropped right after connecting waits like a failed
    // connect, so a daemon refusing the replay cannot cause a tight loop.
    let attempt = quickDrops;
    if (attempt > 0) await sleep(backoffMs(attempt));
    while (!isClosed()) {
      expireQueued();
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
          if (!isIdleStreamTimeout(error)) log(`daemon transport error: ${error.message}`);
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
            connectedAt = Date.now();
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
      await sleep(backoffMs(attempt));
    }
  };

  function dropLink(reason: string) {
    if (state === "closed" || state === "connecting") return;
    log(`lost the daemon session (${reason}); reconnecting`);
    state = "connecting";
    quickDrops = Date.now() - connectedAt < QUICK_DROP_MS ? quickDrops + 1 : 0;
    const old = link;
    link = null;
    generation += 1; // retire the old session's callbacks
    if (old) closeQuietly(old.transport);
    // Prompts from the old session can no longer be answered.
    for (const [clientId, origin] of serverRequests) {
      if (origin.generation !== generation) serverRequests.delete(clientId);
    }
    // Requests the daemon had accepted but not answered. Ones whose POST is
    // still pending are settled when it resolves or fails.
    for (const [id, entry] of inFlight) {
      if (entry.accepted) settleLost(id, entry);
    }
    reconnect();
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

  if (options.signal?.aborted) return;
  const expiry = setInterval(expireQueued, 1_000);
  expiry.unref?.();
  options.signal?.addEventListener("abort", shutdown, { once: true });

  // Start reading the client right away: anything it sends before the first
  // daemon session is up waits in the queue.
  await client.start();
  reconnect();
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

/** Bun's fetch (~300 s timeout) giving up on the standalone GET event stream
 * of an idle session; the SDK reports it twice and does not reopen it. The
 * daemon sends prompts on the POST of the call they belong to
 * (relatedRequestId), so losing this stream is harmless. The same timeout also
 * ends a POST whose call stays silent for 300 s; that call comes back as lost.
 * Not logged; calls in flight are still probed. */
const isIdleStreamTimeout = (error: Error): boolean =>
  error.name === "TimeoutError" || error.message === "The operation timed out.";

const serverRequestId = (generation: number, id: RequestId) =>
  `${SERVER_REQUEST_ID_PREFIX}${generation}-${typeof id === "number" ? "n" : "s"}${id}`;

const isCancelledNotification = (
  message: JSONRPCMessage,
): message is JSONRPCMessage & {
  method: "notifications/cancelled";
  params: { requestId: RequestId };
} =>
  "method" in message &&
  !("id" in message) &&
  message.method === "notifications/cancelled" &&
  typeof message.params === "object" &&
  message.params !== null &&
  "requestId" in message.params;

/** The HTTP status of the SDK's StreamableHTTPError (the daemon answered),
 * or null for anything else. Duck-typed: the SDK error is not tagged. */
const httpStatus = (cause: unknown): number | null => {
  if (!(cause instanceof Error) || !cause.message.startsWith("Streamable HTTP error")) return null;
  const { code } = cause as { code?: unknown };
  return typeof code === "number" && code > 0 ? code : null;
};

/** A POST the daemon answered with a 4xx while its session is still alive:
 * it refused that one message. 404 (session not found) and 401 (a restarted
 * daemon with a new token) mean the session is gone, so they are not this. */
const daemonRejection = (
  cause: unknown,
): { status: number; code: number; message: string } | null => {
  const status = httpStatus(cause);
  if (status === null || status < 400 || status >= 500) return null;
  if (status === 404 || status === 401) return null;
  // The SDK puts the response body after this prefix; the daemon sends a
  // JSON-RPC error there, whose code and text are passed on when present.
  const text = (cause as Error).message;
  const body = text.split("Error POSTing to endpoint: ")[1] ?? "";
  try {
    const parsed = JSON.parse(body) as { error?: { code?: unknown; message?: unknown } };
    if (typeof parsed.error?.message === "string") {
      return {
        status,
        code: typeof parsed.error.code === "number" ? parsed.error.code : -32600,
        message: parsed.error.message,
      };
    }
  } catch {}
  return { status, code: -32600, message: body || text };
};

/** The POST failed in a way that proves the daemon never ran it: it answered
 * with an HTTP status, or the connection was refused before any request.
 * Known gap, left on purpose: a 5xx counts too, though the daemon may have
 * started the call before failing, so a non-read-only call can be re-sent and
 * run twice. A daemon 5xx is an unhandled server error and rare; revisit if
 * the daemon starts answering 5xx for calls it ran. */
const neverReachedDaemon = (cause: unknown): boolean => {
  if (httpStatus(cause) !== null) return true;
  for (let current = cause, depth = 0; current && depth < 4; depth++) {
    const { code, message } = current as { code?: unknown; message?: unknown };
    if (code === "ConnectionRefused" || code === "ECONNREFUSED") return true;
    if (typeof message === "string" && message.includes("Unable to connect")) return true;
    current = (current as { cause?: unknown }).cause;
  }
  return false;
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
