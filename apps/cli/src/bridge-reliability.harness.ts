/**
 * Fault-injection harness for the `executor mcp` stdio bridge.
 *
 * Runs real bridges (`main.ts mcp`) against a real daemon on a random free port
 * with a throwaway data dir, injects daemon restarts / session kills / idle
 * gaps, and counts calls that never got a successful answer ("dropped").
 *
 *   bun run apps/cli/src/bridge-reliability.harness.ts [a b c d e]
 *
 * Env: BRIDGE_IDLE_MS (scenario c idle gap, default 5000), BRIDGE_RESTART_DELAY_MS
 * (supervisor restart delay, default 1000), BRIDGE_CALLS (calls per bridge,
 * default 30). Prints one `METRIC` line per scenario and a total.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const CLI_MAIN = resolve(import.meta.dirname, "main.ts");
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export type CallOutcome = "ok" | "error" | "timeout" | "exited";

export interface ScenarioResult {
  readonly name: string;
  readonly total: number;
  readonly dropped: number;
  readonly outcomes: Record<CallOutcome, number>;
  readonly notes: ReadonlyArray<string>;
}

export interface HarnessOptions {
  readonly idleMs: number;
  readonly restartDelayMs: number;
  readonly calls: number;
  readonly callTimeoutMs: number;
  readonly log?: (line: string) => void;
}

export const defaultHarnessOptions = (): HarnessOptions => ({
  idleMs: Number(process.env.BRIDGE_IDLE_MS) || 5_000,
  restartDelayMs: Number(process.env.BRIDGE_RESTART_DELAY_MS) || 1_000,
  calls: Number(process.env.BRIDGE_CALLS) || 30,
  callTimeoutMs: Number(process.env.BRIDGE_CALL_TIMEOUT_MS) || 45_000,
});

const freePort = (): Promise<number> =>
  new Promise((resolvePort, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close(() => resolvePort(port));
    });
  });

const isAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

const exited = (child: ChildProcess) => child.exitCode !== null || child.signalCode !== null;

const waitForExit = (child: ChildProcess, ms: number): Promise<boolean> =>
  exited(child)
    ? Promise.resolve(true)
    : new Promise((r) => {
        const timer = setTimeout(() => r(false), ms);
        child.once("exit", () => {
          clearTimeout(timer);
          r(true);
        });
      });

// ---------------------------------------------------------------------------
// Isolated environment: own data dir, own default port, no network side effects
// ---------------------------------------------------------------------------

class Sandbox {
  readonly dataDir = mkdtempSync(join(tmpdir(), "executor-bridge-rel-"));
  constructor(readonly port: number) {}

  env(extra: Record<string, string> = {}): NodeJS.ProcessEnv {
    const { EXECUTOR_SUPERVISED: _s, EXECUTOR_DAEMON_SUPERVISOR: _d, ...env } = process.env;
    return {
      ...env,
      EXECUTOR_DATA_DIR: this.dataDir,
      EXECUTOR_SCOPE_DIR: this.dataDir,
      EXECUTOR_DEFAULT_PORT: String(this.port),
      DO_NOT_TRACK: "1",
      EXECUTOR_DISABLE_ANALYTICS: "1",
      EXECUTOR_DISABLE_INTEGRATIONS_FETCH: "1",
      EXECUTOR_DISABLE_UPDATE_CHECK: "1",
      ...extra,
    };
  }

  manifestPid(): number | null {
    try {
      const raw = JSON.parse(
        readFileSync(join(this.dataDir, "server-control", "server.json"), "utf8"),
      ) as { pid?: number };
      return typeof raw.pid === "number" ? raw.pid : null;
    } catch {
      return null;
    }
  }

  async waitForOwner(pid: number | null, ms = 30_000): Promise<number | null> {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      const owner = this.manifestPid();
      if (owner !== null && (pid === null || owner === pid) && isAlive(owner)) return owner;
      await sleep(100);
    }
    return null;
  }

  /** Kill whatever daemon the manifest still points at (bridge-spawned ones). */
  killOwner() {
    const pid = this.manifestPid();
    if (pid !== null && pid !== process.pid && isAlive(pid)) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {}
    }
  }

  dispose() {
    this.killOwner();
    rmSync(this.dataDir, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// Supervisor: restarts the daemon after it exits, like a launchd KeepAlive job
// ---------------------------------------------------------------------------

class Supervisor {
  child: ChildProcess | null = null;
  starts = 0;
  private stopped = false;
  stderr = "";

  constructor(
    private readonly sandbox: Sandbox,
    private readonly restartDelayMs: number,
  ) {}

  start() {
    this.starts += 1;
    const child = spawn(
      process.execPath,
      [
        "run",
        CLI_MAIN,
        "daemon",
        "run",
        "--port",
        String(this.sandbox.port),
        "--hostname",
        "localhost",
        "--foreground",
      ],
      {
        env: this.sandbox.env({
          EXECUTOR_SUPERVISED: "1",
          EXECUTOR_DAEMON_SUPERVISOR: `harness:${process.pid}`,
        }),
        stdio: ["ignore", "ignore", "pipe"],
      },
    );
    child.stderr!.on("data", (chunk) => (this.stderr += String(chunk)));
    child.once("exit", () => {
      if (this.stopped) return;
      setTimeout(() => {
        if (!this.stopped) this.start();
      }, this.restartDelayMs);
    });
    this.child = child;
  }

  async waitReady(): Promise<void> {
    const pid = this.child?.pid ?? null;
    const owner = await this.sandbox.waitForOwner(pid);
    if (owner === null) throw new Error(`supervised daemon never became owner: ${this.stderr}`);
  }

  kill(signal: NodeJS.Signals) {
    if (this.child && !exited(this.child)) this.child.kill(signal);
  }

  async stop() {
    this.stopped = true;
    if (this.child && !exited(this.child)) {
      this.child.kill("SIGTERM");
      if (!(await waitForExit(this.child, 5_000))) this.child.kill("SIGKILL");
    }
  }
}

// ---------------------------------------------------------------------------
// Bridge: one `executor mcp` process driven over raw newline-delimited JSON-RPC
// ---------------------------------------------------------------------------

interface Pending {
  resolve: (outcome: CallOutcome, detail?: string) => void;
}

class Bridge {
  readonly child: ChildProcess;
  private nextId = 1;
  private readonly pending = new Map<number, Pending>();
  private buffer = "";
  stderr = "";

  constructor(
    sandbox: Sandbox,
    readonly label: string,
    private readonly callTimeoutMs: number,
  ) {
    this.child = spawn(process.execPath, ["run", CLI_MAIN, "mcp"], {
      env: sandbox.env(),
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.child.stderr!.on("data", (chunk) => (this.stderr += String(chunk)));
    this.child.stdout!.on("data", (chunk) => this.onData(String(chunk)));
    this.child.stdin!.on("error", () => {});
    this.child.once("exit", () => {
      for (const p of this.pending.values()) p.resolve("exited");
      this.pending.clear();
    });
  }

  private onData(chunk: string) {
    this.buffer += chunk;
    let newline = this.buffer.indexOf("\n");
    while (newline !== -1) {
      const line = this.buffer.slice(0, newline).trim();
      this.buffer = this.buffer.slice(newline + 1);
      newline = this.buffer.indexOf("\n");
      if (!line) continue;
      let message: { id?: unknown; result?: unknown; error?: { message?: string } };
      try {
        message = JSON.parse(line);
      } catch {
        continue;
      }
      if (typeof message.id !== "number") continue;
      const pending = this.pending.get(message.id);
      if (!pending) continue;
      this.pending.delete(message.id);
      if (message.error) pending.resolve("error", message.error.message);
      else pending.resolve("ok");
    }
  }

  private write(message: object) {
    if (exited(this.child) || !this.child.stdin!.writable) return false;
    this.child.stdin!.write(JSON.stringify({ jsonrpc: "2.0", ...message }) + "\n");
    return true;
  }

  request(method: string, params: object = {}): Promise<{ outcome: CallOutcome; detail?: string }> {
    const id = this.nextId++;
    return new Promise((resolveCall) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        resolveCall({ outcome: "timeout" });
      }, this.callTimeoutMs);
      this.pending.set(id, {
        resolve: (outcome, detail) => {
          clearTimeout(timer);
          resolveCall({ outcome, detail });
        },
      });
      if (!this.write({ id, method, params })) {
        this.pending.delete(id);
        clearTimeout(timer);
        resolveCall({ outcome: "exited" });
      }
    });
  }

  async initialize(): Promise<void> {
    const result = await this.request("initialize", {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: `bridge-reliability-${this.label}`, version: "0" },
    });
    if (result.outcome !== "ok") {
      throw new Error(`${this.label} initialize failed (${result.outcome}): ${this.stderr}`);
    }
    this.write({ method: "notifications/initialized" });
  }

  /** Alternate a cheap list with a cheap code-mode execute. */
  call(n: number) {
    return n % 2 === 0
      ? this.request("tools/list")
      : this.request("tools/call", { name: "execute", arguments: { code: "return 1" } });
  }

  async kill(signal: NodeJS.Signals) {
    if (!exited(this.child)) this.child.kill(signal);
  }

  async dispose() {
    if (exited(this.child)) return;
    this.child.stdin!.end();
    if (!(await waitForExit(this.child, 3_000))) this.child.kill("SIGKILL");
  }
}

// ---------------------------------------------------------------------------
// Tally
// ---------------------------------------------------------------------------

class Tally {
  readonly outcomes: Record<CallOutcome, number> = { ok: 0, error: 0, timeout: 0, exited: 0 };
  readonly notes: Array<string> = [];

  record(label: string, n: number, result: { outcome: CallOutcome; detail?: string }) {
    this.outcomes[result.outcome] += 1;
    if (result.outcome !== "ok" && this.notes.length < 12) {
      this.notes.push(`${label}#${n} ${result.outcome}${result.detail ? `: ${result.detail}` : ""}`);
    }
  }

  result(name: string): ScenarioResult {
    const total = Object.values(this.outcomes).reduce((a, b) => a + b, 0);
    return { name, total, dropped: total - this.outcomes.ok, outcomes: this.outcomes, notes: this.notes };
  }
}

const runCalls = async (input: {
  bridge: Bridge;
  tally: Tally;
  count: number;
  intervalMs: number;
  onCall?: (n: number) => Promise<void> | void;
}) => {
  for (let n = 0; n < input.count; n++) {
    await input.onCall?.(n);
    input.tally.record(input.bridge.label, n, await input.bridge.call(n));
    await sleep(input.intervalMs);
  }
};

const withSandbox = async (
  name: string,
  run: (sandbox: Sandbox, tally: Tally, cleanup: Array<() => Promise<void> | void>) => Promise<void>,
): Promise<ScenarioResult> => {
  const sandbox = new Sandbox(await freePort());
  const tally = new Tally();
  const cleanup: Array<() => Promise<void> | void> = [];
  try {
    await run(sandbox, tally, cleanup);
  } catch (cause) {
    tally.notes.push(`scenario error: ${cause instanceof Error ? cause.message : String(cause)}`);
  } finally {
    for (const fn of cleanup.reverse()) await Promise.resolve(fn()).catch(() => {});
    sandbox.dispose();
  }
  return tally.result(name);
};

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/** a. The supervised daemon restarts mid-session (SIGTERM, later SIGKILL). */
export const scenarioDaemonRestart = (opts: HarnessOptions) =>
  withSandbox("a-daemon-restart", async (sandbox, tally, cleanup) => {
    const supervisor = new Supervisor(sandbox, opts.restartDelayMs);
    cleanup.push(() => supervisor.stop());
    supervisor.start();
    await supervisor.waitReady();
    const bridge = new Bridge(sandbox, "a", opts.callTimeoutMs);
    cleanup.push(() => bridge.dispose());
    await bridge.initialize();
    const third = Math.floor(opts.calls / 3);
    await runCalls({
      bridge,
      tally,
      count: opts.calls,
      intervalMs: 150,
      onCall: (n) => {
        if (n === third) supervisor.kill("SIGTERM");
        if (n === third * 2) supervisor.kill("SIGKILL");
      },
    });
  });

/** b. The session whose bridge spawned the daemon is killed (SIGINT, then
 * SIGTERM, as Claude Code does) while a second bridge keeps calling. */
export const scenarioSpawnerKilled = (opts: HarnessOptions) =>
  withSandbox("b-spawner-killed", async (sandbox, tally, cleanup) => {
    const spawner = new Bridge(sandbox, "b-spawner", opts.callTimeoutMs);
    cleanup.push(() => spawner.dispose());
    await spawner.initialize();
    const other = new Bridge(sandbox, "b-other", opts.callTimeoutMs);
    cleanup.push(() => other.dispose());
    await other.initialize();
    const half = Math.floor(opts.calls / 2);
    await runCalls({
      bridge: other,
      tally,
      count: opts.calls,
      intervalMs: 150,
      onCall: async (n) => {
        if (n === half) {
          await spawner.kill("SIGINT");
          await sleep(300);
          await spawner.kill("SIGTERM");
        }
      },
    });
  });

/** c. A long idle gap between calls (Bun fetch / SSE idle timeouts). */
export const scenarioLongIdle = (opts: HarnessOptions) =>
  withSandbox(`c-idle-${Math.round(opts.idleMs / 1000)}s`, async (sandbox, tally, cleanup) => {
    const supervisor = new Supervisor(sandbox, opts.restartDelayMs);
    cleanup.push(() => supervisor.stop());
    supervisor.start();
    await supervisor.waitReady();
    const bridge = new Bridge(sandbox, "c", opts.callTimeoutMs);
    cleanup.push(() => bridge.dispose());
    await bridge.initialize();
    await runCalls({ bridge, tally, count: 4, intervalMs: 100 });
    await sleep(opts.idleMs);
    await runCalls({ bridge, tally, count: 6, intervalMs: 100 });
    if (exited(bridge.child)) tally.notes.push(`bridge exited: ${bridge.stderr.slice(-400)}`);
  });

/** d. Four concurrent bridges, calls interleaved with a daemon restart. */
export const scenarioConcurrentRestart = (opts: HarnessOptions) =>
  withSandbox("d-concurrent-restart", async (sandbox, tally, cleanup) => {
    const supervisor = new Supervisor(sandbox, opts.restartDelayMs);
    cleanup.push(() => supervisor.stop());
    supervisor.start();
    await supervisor.waitReady();
    const bridges = [0, 1, 2, 3].map((i) => new Bridge(sandbox, `d${i}`, opts.callTimeoutMs));
    for (const bridge of bridges) cleanup.push(() => bridge.dispose());
    await Promise.all(bridges.map((b) => b.initialize()));
    const restart = (async () => {
      await sleep(1_500);
      supervisor.kill("SIGTERM");
    })();
    await Promise.all(
      bridges.map((bridge, i) =>
        runCalls({ bridge, tally, count: opts.calls, intervalMs: 80 + i * 20 }),
      ),
    );
    await restart;
  });

/** e. A bridge-spawned daemon is replaced by the supervised owner starting up
 * (what happens when launchd comes back while a session daemon is running). */
export const scenarioSupervisorTakeover = (opts: HarnessOptions) =>
  withSandbox("e-supervisor-takeover", async (sandbox, tally, cleanup) => {
    const bridge = new Bridge(sandbox, "e", opts.callTimeoutMs);
    cleanup.push(() => bridge.dispose());
    await bridge.initialize();
    const supervisor = new Supervisor(sandbox, opts.restartDelayMs);
    cleanup.push(() => supervisor.stop());
    const third = Math.floor(opts.calls / 3);
    await runCalls({
      bridge,
      tally,
      count: opts.calls,
      intervalMs: 150,
      onCall: (n) => {
        if (n === third) supervisor.start();
      },
    });
  });

export const SCENARIOS = {
  a: scenarioDaemonRestart,
  b: scenarioSpawnerKilled,
  c: scenarioLongIdle,
  d: scenarioConcurrentRestart,
  e: scenarioSupervisorTakeover,
} as const;

export const formatMetric = (r: Pick<ScenarioResult, "dropped" | "total">, name?: string) =>
  `METRIC${name ? ` scenario=${name}` : ""} dropped=${r.dropped} total=${r.total}`;

if (import.meta.main) {
  const opts = defaultHarnessOptions();
  const picked = (process.argv.slice(2).length ? process.argv.slice(2) : Object.keys(SCENARIOS)) as Array<
    keyof typeof SCENARIOS
  >;
  let dropped = 0;
  let total = 0;
  for (const key of picked) {
    const started = Date.now();
    const result = await SCENARIOS[key](opts);
    dropped += result.dropped;
    total += result.total;
    console.log(
      `${formatMetric(result, result.name)} outcomes=${JSON.stringify(result.outcomes)} secs=${Math.round((Date.now() - started) / 1000)}`,
    );
    for (const note of result.notes) console.log(`  ${note}`);
  }
  console.log(formatMetric({ dropped, total }));
  process.exit(0);
}
