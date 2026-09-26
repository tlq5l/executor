import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect } from "effect";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { ElicitationResponse, type Elicit } from "@executor-js/sdk";

import { createMcpConnector, type McpConnection } from "./connection";
import { createMcpConnectionPool } from "./connection-pool";
import { McpInvocationError } from "./errors";
import { invokeMcpTool } from "./invoke";
import { makeEchoMcpServer, serveMcpServer } from "../testing";

const acceptAll: Elicit = () =>
  Effect.succeed(ElicitationResponse.make({ action: "accept", content: { approved: true } }));

const invoke = (input: {
  readonly endpoint: string;
  readonly pool: ReturnType<typeof createMcpConnectionPool>;
  readonly toolName: string;
  readonly args: Record<string, unknown>;
  readonly elicit?: Elicit;
}) =>
  invokeMcpTool({
    toolId: input.toolName,
    toolName: input.toolName,
    args: input.args,
    transport: "streamable-http",
    connector: createMcpConnector({
      transport: "remote",
      endpoint: input.endpoint,
      remoteTransport: "streamable-http",
    }),
    connectionPool: input.pool,
    connectionPoolKey: input.endpoint,
    elicit: input.elicit ?? acceptAll,
  });

const stdioReconnectFixture = fileURLToPath(
  new URL("./stdio-reconnect-test-server.ts", import.meta.url),
);

const waitUntil = (predicate: () => boolean, timeoutMs: number) =>
  Effect.gen(function* () {
    const deadline = Date.now() + timeoutMs;
    while (!predicate()) {
      if (Date.now() > deadline) return false;
      yield* Effect.sleep(Duration.millis(25));
    }
    return true;
  });

const isAlive = (pid: number): boolean => {
  // oxlint-disable-next-line executor/no-try-catch-or-throw -- boundary: process.kill(pid, 0) reports an exited child via ESRCH
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

describe("MCP connection pool", () => {
  it.effect("reuses one session for sequential invokes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* serveMcpServer(() => makeEchoMcpServer());
        const pool = createMcpConnectionPool();

        const first = yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "first" },
        });
        const second = yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "second" },
        });

        expect(first).toMatchObject({ content: [{ type: "text", text: "first" }] });
        expect(second).toMatchObject({ content: [{ type: "text", text: "second" }] });
        expect(server.sessionCount()).toBe(1);
        yield* pool.close();
      }),
    ),
  );

  it.live("reconnects when a cached stdio server process exits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const pidFile = join(mkdtempSync(join(tmpdir(), "mcp-stdio-reconnect-")), "pids");
        const connector = createMcpConnector({
          transport: "stdio",
          command: "bun",
          args: ["run", stdioReconnectFixture, pidFile],
        });
        const pool = createMcpConnectionPool();
        yield* Effect.addFinalizer(() => pool.close());
        const invokeEcho = (value: string) =>
          invokeMcpTool({
            toolId: "echo",
            toolName: "echo",
            args: { value },
            transport: "stdio",
            connector,
            connectionPool: pool,
            connectionPoolKey: pidFile,
            elicit: acceptAll,
          });

        const first = yield* invokeEcho("before");
        expect(first).toMatchObject({ content: [{ type: "text", text: "before" }] });
        expect(yield* waitUntil(() => existsSync(pidFile), 10_000)).toBe(true);
        const firstPid = Number(readFileSync(pidFile, "utf8").trim().split("\n")[0]);

        process.kill(firstPid, "SIGKILL");
        expect(yield* waitUntil(() => !isAlive(firstPid), 5_000)).toBe(true);

        const second = yield* invokeEcho("after");

        expect(second).toMatchObject({ content: [{ type: "text", text: "after" }] });
        expect(readFileSync(pidFile, "utf8").trim().split("\n")).toHaveLength(2);
      }),
    ),
  );

  it.effect("leases separate sessions to concurrent invokes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* serveMcpServer(() => makeEchoMcpServer());
        const pool = createMcpConnectionPool();

        const results = yield* Effect.all(
          [
            invoke({
              endpoint: server.endpoint,
              pool,
              toolName: "echo",
              args: { value: "left" },
            }),
            invoke({
              endpoint: server.endpoint,
              pool,
              toolName: "echo",
              args: { value: "right" },
            }),
          ],
          { concurrency: "unbounded" },
        );

        expect(results).toEqual([
          expect.objectContaining({ content: [{ type: "text", text: "left" }] }),
          expect.objectContaining({ content: [{ type: "text", text: "right" }] }),
        ]);
        expect(server.sessionCount()).toBe(2);
        yield* pool.close();
      }),
    ),
  );

  it.effect("redials once when a reused session has expired", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* serveMcpServer(() => makeEchoMcpServer());
        const pool = createMcpConnectionPool();

        yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "before" },
        });
        yield* server.forgetSessions;
        const after = yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "after" },
        });

        expect(after).toMatchObject({ content: [{ type: "text", text: "after" }] });
        expect(server.sessionCount()).toBe(2);
        yield* pool.close();
      }),
    ),
  );

  it.effect("drops a connection after a transport-level invocation failure", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* serveMcpServer(() => makeEchoMcpServer());
        const pool = createMcpConnectionPool();

        yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "before" },
        });
        yield* server.rejectNextSessionRequest(500);
        yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "fails" },
        }).pipe(Effect.flip);
        const after = yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "after" },
        });

        expect(after).toMatchObject({ content: [{ type: "text", text: "after" }] });
        expect(server.sessionCount()).toBe(2);
        yield* pool.close();
      }),
    ),
  );

  it.effect("retries once only when the failed tool call was never sent", () =>
    Effect.gen(function* () {
      const pool = createMcpConnectionPool();
      let dials = 0;
      let calls = 0;
      const connector = Effect.sync(() => {
        dials += 1;
        return {
          // oxlint-disable-next-line executor/no-double-cast -- boundary: this pool test never calls methods on the client
          client: {} as unknown as McpConnection["client"],
          close: async () => undefined,
        };
      });

      const result = yield* pool.withConnection("retry-safe", connector, () => {
        calls += 1;
        return calls === 1
          ? Effect.fail(
              new McpInvocationError({
                toolName: "echo",
                message: "not sent",
                transportFailure: true,
                requestNotSent: true,
              }),
            )
          : Effect.succeed("retried");
      });

      expect(result).toBe("retried");
      expect(calls).toBe(2);
      expect(dials).toBe(2);
      yield* pool.close();
    }),
  );

  it.effect("does not retry a transport failure that may have reached the server", () =>
    Effect.gen(function* () {
      const pool = createMcpConnectionPool();
      let dials = 0;
      const connector = Effect.sync(() => {
        dials += 1;
        return {
          // oxlint-disable-next-line executor/no-double-cast -- boundary: this pool test never calls methods on the client
          client: {} as unknown as McpConnection["client"],
          close: async () => undefined,
        };
      });

      const failure = yield* pool
        .withConnection("retry-unsafe", connector, () =>
          Effect.fail(
            new McpInvocationError({
              toolName: "echo",
              message: "possibly sent",
              transportFailure: true,
            }),
          ),
        )
        .pipe(Effect.flip);

      expect(failure.message).toBe("possibly sent");
      expect(dials).toBe(1);
      yield* pool.close();
    }),
  );

  it.effect("drops a pooled session whose bearer the server has stopped accepting", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* serveMcpServer(() => makeEchoMcpServer());
        const pool = createMcpConnectionPool();

        yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "before" },
        });

        // A 401 means the bearer this session was dialled with is no longer
        // accepted. The session is bound to that token for its lifetime, so
        // retaining it would serve the same dead credential for the full idle
        // window — and would defeat core's post-401 refresh, whose new token can
        // only reach the server over a NEW session.
        yield* server.rejectNextSessionRequest(401);
        yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "unauthorized" },
        }).pipe(Effect.flip);

        const after = yield* invoke({
          endpoint: server.endpoint,
          pool,
          toolName: "echo",
          args: { value: "after" },
        });

        expect(after).toMatchObject({ content: [{ type: "text", text: "after" }] });
        // A second session was dialled rather than the dead one being reused.
        expect(server.sessionCount()).toBe(2);
        yield* pool.close();
      }),
    ),
  );
});
