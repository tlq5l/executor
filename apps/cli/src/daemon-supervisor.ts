import { execFile } from "node:child_process";
import { FileSystem, Path } from "effect";
import * as Effect from "effect/Effect";

import { resolveExecutorDataDir } from "./local-server-manifest";

// ---------------------------------------------------------------------------
// Who owns the daemon's lifetime.
//
// When an OS supervisor (a launchd LaunchAgent, systemd unit, ...) runs the
// daemon, it is the only process that should ever start one. Bridges that
// spawned their own daemon whenever the supervised one was briefly down
// (a restart, launchd's throttle window) fought it: the supervised daemon then
// crash-looped on "already running", or took over and cut every bridge
// attached to the session-spawned one.
//
// A supervised daemon (EXECUTOR_SUPERVISED set) records its supervisor in
// server-control/supervisor.json. The file outlives the daemon on purpose: it
// describes the configuration, not liveness. Bridges that find no reachable
// owner read it and, while the supervisor is still registered, wait for it to
// bring the daemon back instead of spawning one. With no marker (no supervisor
// configured) spawn-on-demand is unchanged.
// ---------------------------------------------------------------------------

/**
 * `launchd:<label>`, `pid:<n>` (a parent process that restarts the daemon,
 * e.g. a test harness), or any other string (registration can't be checked).
 * Set EXECUTOR_DAEMON_SUPERVISOR explicitly; under launchd it defaults to the
 * job label launchd exports as XPC_SERVICE_NAME.
 */
export type DaemonSupervisor =
  | { readonly kind: "launchd"; readonly label: string }
  | { readonly kind: "pid"; readonly pid: number }
  | { readonly kind: "other"; readonly name: string };

export const parseDaemonSupervisor = (raw: string): DaemonSupervisor => {
  const [kind, ...rest] = raw.split(":");
  const value = rest.join(":");
  if (kind === "launchd" && value) return { kind: "launchd", label: value };
  if (kind === "pid" && /^\d+$/.test(value)) return { kind: "pid", pid: Number(value) };
  return { kind: "other", name: raw };
};

/** The supervisor of THIS process, or null when it is not OS-supervised. */
export const daemonSupervisorFromEnv = (env: NodeJS.ProcessEnv): string | null => {
  if (!env.EXECUTOR_SUPERVISED) return null;
  if (env.EXECUTOR_DAEMON_SUPERVISOR) return env.EXECUTOR_DAEMON_SUPERVISOR;
  // launchd exports the job label to every job it starts.
  const xpc = env.XPC_SERVICE_NAME;
  if (process.platform === "darwin" && xpc && xpc !== "0" && !xpc.startsWith("application.")) {
    return `launchd:${xpc}`;
  }
  return "os";
};

const markerPath = (path: Path.Path): string =>
  path.join(resolveExecutorDataDir(path), "server-control", "supervisor.json");

export const writeDaemonSupervisorMarker = (supervisor: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = markerPath(path);
    yield* fs.makeDirectory(path.dirname(file), { recursive: true });
    yield* fs.writeFileString(
      file,
      JSON.stringify({ supervisor, pid: process.pid, writtenAt: new Date().toISOString() }) + "\n",
    );
  });

export const readDaemonSupervisorMarker = (): Effect.Effect<
  DaemonSupervisor | null,
  never,
  FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const raw = yield* fs
      .readFileString(markerPath(path))
      .pipe(Effect.catchCause(() => Effect.succeed(null)));
    if (raw === null) return null;
    try {
      const parsed = JSON.parse(raw) as { supervisor?: unknown };
      return typeof parsed.supervisor === "string"
        ? parseDaemonSupervisor(parsed.supervisor)
        : null;
    } catch {
      return null;
    }
  });

/** `launchctl print gui/<uid>/<label>` output, or null when the job is not
 * loaded (launchctl exits non-zero). */
const launchdPrint = (label: string): Promise<string | null> =>
  new Promise((resolve) => {
    const uid = process.getuid?.() ?? 0;
    execFile("launchctl", ["print", `gui/${uid}/${label}`], { timeout: 5_000 }, (error, stdout) =>
      resolve(error ? null : String(stdout)),
    );
  });

/** Will launchd (re)start this loaded job on its own, judging by its
 * `launchctl print` dump? A job that is running, starting or throttled
 * ("spawn scheduled") will be; so is one that crashed or was killed while a
 * KeepAlive rule applies. One that exited cleanly is not: under KeepAlive
 * SuccessfulExit=false (our LaunchAgent), e.g. after `executor daemon stop`,
 * launchd leaves it down while the job stays loaded. */
export const launchdWillRestart = (printOutput: string): boolean => {
  const field = (name: string) =>
    new RegExp(`^\\t${name} = (.*)$`, "m").exec(printOutput)?.[1]?.trim() ?? null;
  if (field("state") !== "not running") return true;
  if (/\bkeepalive\b/.test(field("properties") ?? "")) return true;
  const exitCode = field("last exit code");
  const crashed =
    field("last terminating signal") !== null ||
    (exitCode !== null && exitCode !== "0" && !exitCode.startsWith("(never"));
  return crashed && /successful exit => 0/.test(printOutput);
};

const pidAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (cause) {
    return (cause as NodeJS.ErrnoException).code === "EPERM";
  }
};

/**
 * Is the recorded supervisor still there, and going to restart the daemon?
 * "unknown" when it can't be checked (callers wait a bounded grace period).
 */
export const daemonSupervisorStatus = async (
  supervisor: DaemonSupervisor,
  deps: {
    /** `launchctl print` output for the job, null when it is not loaded. */
    readonly launchdPrint?: (label: string) => Promise<string | null>;
    readonly pidAlive?: (pid: number) => boolean;
  } = {},
): Promise<"registered" | "gone" | "unknown"> => {
  switch (supervisor.kind) {
    case "launchd": {
      if (process.platform !== "darwin" && !deps.launchdPrint) return "unknown";
      // "gone" also covers a loaded job launchd will not restart.
      const printed = await (deps.launchdPrint ?? launchdPrint)(supervisor.label);
      return printed !== null && launchdWillRestart(printed) ? "registered" : "gone";
    }
    case "pid":
      return (deps.pidAlive ?? pidAlive)(supervisor.pid) ? "registered" : "gone";
    case "other":
      return "unknown";
  }
};

export const describeDaemonSupervisor = (supervisor: DaemonSupervisor): string => {
  switch (supervisor.kind) {
    case "launchd":
      return `launchd job ${supervisor.label}`;
    case "pid":
      return `supervisor pid ${supervisor.pid}`;
    case "other":
      return `supervisor ${supervisor.name}`;
  }
};
