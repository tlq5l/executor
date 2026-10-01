import { describe, expect, it } from "@effect/vitest";
import { BunServices } from "@effect/platform-bun";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import * as Effect from "effect/Effect";

import {
  normalizeExecutorServerConnection,
  type ExecutorLocalServerManifest,
} from "@executor-js/sdk/shared";
import { isExecutorServerReachable } from "./daemon";
import { isPidAlive } from "./daemon-state";
import { readLocalServerManifest, writeLocalServerManifest } from "./local-server-manifest";
import {
  readLocalServerOwnerWaitingOutStall,
  type LocalServerOwnerStallEvent,
} from "./local-server-owner";

// Every process these tests start is their own child on an ephemeral port with
// its own temp EXECUTOR_DATA_DIR. Nothing here reads or probes a real daemon.

const CLI_MAIN = resolve(import.meta.dirname, "main.ts");

const manifestFor = (input: {
  readonly pid: number;
  readonly origin: string;
  readonly dataDir: string;
}): ExecutorLocalServerManifest => ({
  version: 1,
  kind: "cli-daemon",
  pid: input.pid,
  startedAt: new Date().toISOString(),
  dataDir: input.dataDir,
  scopeDir: input.dataDir,
  connection: normalizeExecutorServerConnection({ origin: input.origin }),
  owner: { client: "cli", version: "test", executablePath: null },
});

// ---------------------------------------------------------------------------
// Unit: decision logic with scripted probes
// ---------------------------------------------------------------------------

interface FakeOwner {
  manifest: ExecutorLocalServerManifest | null;
  alive: boolean;
  readonly reachable: Array<boolean>;
  probes: number;
  removed: Array<number>;
  events: Array<LocalServerOwnerStallEvent["kind"]>;
}

const fakeOwner = (
  input: Partial<Pick<FakeOwner, "manifest" | "alive" | "reachable">> = {},
): FakeOwner => ({
  manifest:
    input.manifest === undefined
      ? manifestFor({ pid: 4242, origin: "http://127.0.0.1:1", dataDir: "/tmp/x" })
      : input.manifest,
  alive: input.alive ?? true,
  reachable: input.reachable ?? [true],
  probes: 0,
  removed: [],
  events: [],
});

const readWithFake = (
  fake: FakeOwner,
  options: { readonly budgetMs?: number; readonly pollMs?: number; readonly onProbe?: () => void },
) =>
  readLocalServerOwnerWaitingOutStall({
    readManifest: Effect.sync(() => fake.manifest),
    isReachable: () =>
      Effect.sync(() => {
        fake.probes += 1;
        options.onProbe?.();
        return fake.reachable.length > 1 ? fake.reachable.shift()! : (fake.reachable[0] ?? false);
      }),
    isPidAlive: () => fake.alive,
    removeManifestIfOwnedBy: (pid) =>
      Effect.sync(() => {
        fake.removed.push(pid);
        fake.manifest = null;
      }),
    onStallEvent: (event) => fake.events.push(event.kind),
    budgetMs: options.budgetMs,
    pollMs: options.pollMs,
  });

describe("readLocalServerOwnerWaitingOutStall (decision logic)", () => {
  // it.effect runs on a TestClock that never advances by itself, so any sleep
  // in these first three cases would hang the test: passing proves no wait.
  it.effect("healthy owner: one probe, no wait", () =>
    Effect.gen(function* () {
      const fake = fakeOwner();
      const owner = yield* readWithFake(fake, { pollMs: 60_000 });
      expect(owner?.pid).toBe(4242);
      expect(fake.probes).toBe(1);
      expect(fake.events).toEqual([]);
    }),
  );

  it.effect("no manifest: returns null without probing", () =>
    Effect.gen(function* () {
      const fake = fakeOwner({ manifest: null });
      expect(yield* readWithFake(fake, { pollMs: 60_000 })).toBeNull();
      expect(fake.probes).toBe(0);
    }),
  );

  it.effect("dead owner pid: removes its manifest and returns null at once", () =>
    Effect.gen(function* () {
      const fake = fakeOwner({ alive: false, reachable: [false] });
      expect(yield* readWithFake(fake, { pollMs: 60_000 })).toBeNull();
      expect(fake.removed).toEqual([4242]);
      expect(fake.probes).toBe(1);
      expect(fake.events).toEqual([]);
    }),
  );

  it.live("live but silent owner: keeps probing until it answers", () =>
    Effect.gen(function* () {
      const fake = fakeOwner({ reachable: [false, false, false, true] });
      const owner = yield* readWithFake(fake, { pollMs: 5, budgetMs: 10_000 });
      expect(owner?.pid).toBe(4242);
      expect(fake.probes).toBe(4);
      expect(fake.removed).toEqual([]);
      expect(fake.events).toEqual(["stalled", "recovered"]);
    }),
  );

  it.live("owner dies while we wait: returns null and clears its manifest", () =>
    Effect.gen(function* () {
      const fake = fakeOwner({ reachable: [false] });
      const owner = yield* readWithFake(fake, {
        pollMs: 5,
        budgetMs: 10_000,
        onProbe: () => {
          if (fake.probes >= 3) fake.alive = false;
        },
      });
      expect(owner).toBeNull();
      expect(fake.removed).toEqual([4242]);
      expect(fake.events).toEqual(["stalled", "gone"]);
    }),
  );

  it.live("owner removes its manifest while we wait: returns null", () =>
    Effect.gen(function* () {
      const fake = fakeOwner({ reachable: [false] });
      const owner = yield* readWithFake(fake, {
        pollMs: 5,
        budgetMs: 10_000,
        onProbe: () => {
          if (fake.probes >= 2) fake.manifest = null;
        },
      });
      expect(owner).toBeNull();
      expect(fake.events).toEqual(["stalled", "gone"]);
    }),
  );

  it.live("owner silent past the budget: gives up so the caller can spawn", () =>
    Effect.gen(function* () {
      const fake = fakeOwner({ reachable: [false] });
      const owner = yield* readWithFake(fake, { pollMs: 5, budgetMs: 100 });
      expect(owner).toBeNull();
      expect(fake.removed).toEqual([]);
      expect(fake.probes).toBeGreaterThan(1);
      expect(fake.events).toEqual(["stalled", "gave-up"]);
    }),
  );
});

// ---------------------------------------------------------------------------
// Real processes: a stand-in owner that is frozen with SIGSTOP
// ---------------------------------------------------------------------------

// A minimal stand-in for a local server owner: answers /api/health with "ok",
// logs every other request, and writes its port once listening.
const FAKE_OWNER_SCRIPT = `
const { createServer } = require("node:http");
const { appendFileSync, writeFileSync } = require("node:fs");
let healthFailuresLeft = Number(process.env.FAIL_FIRST_HEALTH || 0);
const server = createServer((req, res) => {
  if (req.url === "/api/health") {
    if (healthFailuresLeft > 0) { healthFailuresLeft -= 1; res.writeHead(503); res.end(); return; }
    res.writeHead(200); res.end("ok"); return;
  }
  appendFileSync(process.env.REQUEST_LOG, req.method + " " + req.url.split("?")[0] + "\\n");
  res.writeHead(503); res.end();
});
server.listen(0, "127.0.0.1", () => writeFileSync(process.env.PORT_FILE, String(server.address().port)));
setInterval(() => {}, 1000);
`;

interface StartedFakeOwner {
  readonly child: ChildProcess;
  readonly pid: number;
  readonly origin: string;
  readonly requestLog: string;
}

const waitFor = async (check: () => boolean, timeoutMs: number, what: string) => {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
};

const startFakeOwner = async (
  dir: string,
  options: { readonly failFirstHealth?: number } = {},
): Promise<StartedFakeOwner> => {
  const portFile = join(dir, "fake-owner.port");
  const requestLog = join(dir, "fake-owner.requests");
  const child = spawn(process.execPath, ["-e", FAKE_OWNER_SCRIPT], {
    env: {
      ...process.env,
      PORT_FILE: portFile,
      REQUEST_LOG: requestLog,
      FAIL_FIRST_HEALTH: String(options.failFirstHealth ?? 0),
    },
    stdio: "ignore",
  });
  await waitFor(
    () => existsSync(portFile) && readFileSync(portFile, "utf8").length > 0,
    20_000,
    "fake owner port",
  );
  return {
    child,
    pid: child.pid!,
    origin: `http://127.0.0.1:${readFileSync(portFile, "utf8")}`,
    requestLog,
  };
};

const stopProcess = (child: ChildProcess) => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGCONT");
  child.kill("SIGKILL");
};

/** Freeze the owner for `ms`: its socket still accepts connections, but nothing
 * answers, which is exactly what a client sees when the owner's event loop or
 * the whole machine is stalled. */
const freezeFor = (child: ChildProcess, ms: number) => {
  child.kill("SIGSTOP");
  const timer = setTimeout(() => child.kill("SIGCONT"), ms);
  return () => clearTimeout(timer);
};

const withTempDataDir = <A>(run: (dataDir: string) => Promise<A>): Promise<A> => {
  const dataDir = mkdtempSync(join(tmpdir(), "executor-owner-stall-"));
  const previous = process.env.EXECUTOR_DATA_DIR;
  process.env.EXECUTOR_DATA_DIR = dataDir;
  return run(dataDir).finally(() => {
    if (previous === undefined) delete process.env.EXECUTOR_DATA_DIR;
    else process.env.EXECUTOR_DATA_DIR = previous;
    rmSync(dataDir, { recursive: true, force: true });
  });
};

const writeManifest = (manifest: ExecutorLocalServerManifest) =>
  Effect.runPromise(writeLocalServerManifest(manifest).pipe(Effect.provide(BunServices.layer)));

const readOwnerForReal = (events: Array<LocalServerOwnerStallEvent>) =>
  Effect.runPromise(
    readLocalServerOwnerWaitingOutStall({
      readManifest: readLocalServerManifest(),
      isReachable: (origin) => isExecutorServerReachable({ baseUrl: origin }),
      isPidAlive,
      removeManifestIfOwnedBy: () => Effect.void,
      onStallEvent: (event) => events.push(event),
    }).pipe(Effect.provide(BunServices.layer)),
  );

describe("readLocalServerOwnerWaitingOutStall (real stalled process)", () => {
  it("waits out an owner frozen for 5 s and returns it", async () => {
    await withTempDataDir(async (dataDir) => {
      const owner = await startFakeOwner(dataDir);
      try {
        await writeManifest(manifestFor({ pid: owner.pid, origin: owner.origin, dataDir }));
        const unfreeze = freezeFor(owner.child, 5_000);
        const events: Array<LocalServerOwnerStallEvent> = [];
        const startedAt = Date.now();
        const found = await readOwnerForReal(events);
        unfreeze();
        expect(found?.pid).toBe(owner.pid);
        expect(events.map((e) => e.kind)).toEqual(["stalled", "recovered"]);
        // It waited for the thaw rather than giving up at the first 2 s timeout.
        expect(Date.now() - startedAt).toBeGreaterThanOrEqual(4_000);
      } finally {
        stopProcess(owner.child);
      }
    });
  }, 60_000);
});

// ---------------------------------------------------------------------------
// The real CLI: `executor mcp` against a frozen owner must bridge, not spawn
// ---------------------------------------------------------------------------

const MCP_INITIALIZE =
  JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: "stall-test", version: "0" },
    },
  }) + "\n";

describe("executor mcp with a stalled owner", () => {
  it("waits for the frozen owner and bridges to it instead of starting a daemon", async () => {
    await withTempDataDir(async (dataDir) => {
      const owner = await startFakeOwner(dataDir);
      let cli: ChildProcess | null = null;
      try {
        await writeManifest(manifestFor({ pid: owner.pid, origin: owner.origin, dataDir }));
        const unfreeze = freezeFor(owner.child, 6_000);

        let stderr = "";
        cli = spawn(process.execPath, ["run", CLI_MAIN, "mcp"], {
          env: { ...process.env, EXECUTOR_DATA_DIR: dataDir, EXECUTOR_SCOPE_DIR: dataDir },
          stdio: ["pipe", "pipe", "pipe"],
        });
        cli.stderr!.on("data", (chunk) => (stderr += String(chunk)));
        cli.stdin!.write(MCP_INITIALIZE);

        // The bridge reaching the owner's /mcp endpoint is the client connecting.
        await waitFor(
          () =>
            existsSync(owner.requestLog) &&
            readFileSync(owner.requestLog, "utf8").includes("POST /mcp"),
          45_000,
          `the CLI to reach the owner's /mcp (stderr so far:\n${stderr})`,
        );
        unfreeze();

        expect(stderr).toContain(`Executor server pid ${owner.pid}`);
        expect(stderr).toContain("answered again");
        expect(stderr).not.toContain("Starting daemon");
        // The stand-in owner holds no data-dir lock, so a spawned daemon would
        // have become owner and created data.db. None exists: nothing spawned.
        expect(existsSync(join(dataDir, "data.db"))).toBe(false);
      } finally {
        if (cli) stopProcess(cli);
        stopProcess(owner.child);
      }
    });
  }, 90_000);
});

// ---------------------------------------------------------------------------
// The real CLI: auto-start still works when there is no live owner
// ---------------------------------------------------------------------------

const freePort = async (): Promise<number> => {
  const { createServer } = await import("node:net");
  return new Promise((resolvePort, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close(() => resolvePort(port));
    });
  });
};

const stopDaemonFromManifest = async (dataDir: string) => {
  const manifestPath = join(dataDir, "server-control", "server.json");
  if (!existsSync(manifestPath)) return;
  const pid = Number(JSON.parse(readFileSync(manifestPath, "utf8")).pid);
  if (!isPidAlive(pid) || pid === process.pid) return;
  process.kill(pid, "SIGTERM");
  const deadline = Date.now() + 15_000;
  while (isPidAlive(pid) && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 100));
  }
  if (isPidAlive(pid)) process.kill(pid, "SIGKILL");
};

/** Run `executor tools search` against an explicit free local port, which makes
 * the CLI auto-start a daemon there (never the default port). */
const runCliAutoStart = async (dataDir: string) => {
  const port = await freePort();
  const cli = spawn(
    process.execPath,
    ["run", CLI_MAIN, "tools", "search", "nothing", "--base-url", `http://localhost:${port}`],
    {
      env: { ...process.env, EXECUTOR_DATA_DIR: dataDir, EXECUTOR_SCOPE_DIR: dataDir },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let stderr = "";
  cli.stderr!.on("data", (chunk) => (stderr += String(chunk)));
  const code = await new Promise<number | null>((resolveCode) => cli.on("exit", resolveCode));
  return { code, stderr, port };
};

const expectAutoStartedDaemon = async (dataDir: string, deadPid?: number) => {
  const run = await runCliAutoStart(dataDir);
  expect(run.stderr, run.stderr).toContain(`Starting daemon on localhost:${run.port}`);
  expect(run.stderr).not.toContain("is running but not answering");
  expect(run.code, run.stderr).toBe(0);
  const manifest = JSON.parse(
    readFileSync(join(dataDir, "server-control", "server.json"), "utf8"),
  ) as { pid: number; connection: { origin: string } };
  expect(manifest.pid).not.toBe(deadPid);
  expect(isPidAlive(manifest.pid)).toBe(true);
  expect(manifest.connection.origin).toContain(String(run.port));
};

describe("executor auto-start without a live owner", () => {
  it("starts a daemon when the manifest's owner pid is dead", async () => {
    await withTempDataDir(async (dataDir) => {
      try {
        const exited = spawn(process.execPath, ["-e", "0"], { stdio: "ignore" });
        await new Promise((r) => exited.on("exit", r));
        const deadPid = exited.pid!;
        await writeManifest(
          manifestFor({ pid: deadPid, origin: `http://127.0.0.1:${await freePort()}`, dataDir }),
        );
        await expectAutoStartedDaemon(dataDir, deadPid);
      } finally {
        await stopDaemonFromManifest(dataDir);
      }
    });
  }, 120_000);

  it("starts a daemon when there is no manifest", async () => {
    await withTempDataDir(async (dataDir) => {
      try {
        await expectAutoStartedDaemon(dataDir);
      } finally {
        await stopDaemonFromManifest(dataDir);
      }
    });
  }, 120_000);
});

// ---------------------------------------------------------------------------
// The real CLI: a daemon that loses the data-dir lock must exit, not linger
// ---------------------------------------------------------------------------

/** Holds the data-dir ownership lock the same way a running server does: an
 * open BEGIN EXCLUSIVE transaction on `data.db.owner-lock`. */
const LOCK_HOLDER_SCRIPT = `
const { Database } = require("bun:sqlite");
const { writeFileSync } = require("node:fs");
const db = new Database(process.env.LOCK_PATH);
db.exec("PRAGMA journal_mode = DELETE");
db.exec("BEGIN EXCLUSIVE");
writeFileSync(process.env.READY_FILE, "1");
setInterval(() => {}, 1 << 30);
`;

/** Hold the data-dir lock, run `executor daemon run` on a free port against
 * the same data dir, and report whether it exited within 45 s. */
const runDaemonAgainstOwnedDataDir = async (
  dataDir: string,
  beforeDaemon: () => Promise<void> = async () => {},
) => {
  const readyFile = join(dataDir, "lock-holder.ready");
  const holder = spawn(process.execPath, ["-e", LOCK_HOLDER_SCRIPT], {
    env: {
      ...process.env,
      LOCK_PATH: join(realpathSync(dataDir), "data.db.owner-lock"),
      READY_FILE: readyFile,
    },
    stdio: "ignore",
  });
  let daemon: ChildProcess | undefined;
  try {
    await waitFor(() => existsSync(readyFile), 20_000, "lock holder");
    await beforeDaemon();
    const port = await freePort();
    const { EXECUTOR_SUPERVISED: _supervised, ...env } = process.env;
    daemon = spawn(
      process.execPath,
      [
        "run",
        CLI_MAIN,
        "daemon",
        "run",
        "--foreground",
        "--port",
        String(port),
        "--hostname",
        "localhost",
      ],
      {
        env: { ...env, EXECUTOR_DATA_DIR: dataDir, EXECUTOR_SCOPE_DIR: dataDir },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let output = "";
    daemon.stdout!.on("data", (chunk) => (output += String(chunk)));
    daemon.stderr!.on("data", (chunk) => (output += String(chunk)));
    const child = daemon;
    const exited = await new Promise<boolean>((resolveExit) => {
      const timer = setTimeout(() => resolveExit(false), 45_000);
      child.on("exit", () => {
        clearTimeout(timer);
        resolveExit(true);
      });
    });
    return { exited, output };
  } finally {
    if (daemon) stopProcess(daemon);
    stopProcess(holder);
  }
};

describe("executor daemon run while another process owns the data dir", () => {
  it("attaches to the advertised owner and exits instead of lingering", async () => {
    await withTempDataDir(async (dataDir) => {
      // The owner misses the first health probe (the "is another server up?"
      // pre-check), so the daemon goes on to startServer, loses the data-dir
      // lock, and attaches to the owner, which now answers.
      const owner = await startFakeOwner(dataDir, { failFirstHealth: 1 });
      try {
        const run = await runDaemonAgainstOwnedDataDir(dataDir, () =>
          writeManifest(manifestFor({ pid: owner.pid, origin: owner.origin, dataDir })),
        );
        expect(run.exited, `daemon still running after 45 s:\n${run.output}`).toBe(true);
        expect(run.output).toContain(`already running at ${owner.origin}`);
      } finally {
        stopProcess(owner.child);
      }
    });
  }, 90_000);

  it("exits with an error when the owner advertises no server", async () => {
    await withTempDataDir(async (dataDir) => {
      const run = await runDaemonAgainstOwnedDataDir(dataDir);
      expect(run.exited, `daemon still running after 45 s:\n${run.output}`).toBe(true);
      expect(run.output).toContain("owned by another live process");
      expect(existsSync(join(dataDir, "server-control", "server.json"))).toBe(false);
    });
  }, 90_000);
});
