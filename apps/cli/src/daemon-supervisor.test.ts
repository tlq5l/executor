import { describe, expect, it } from "@effect/vitest";

import {
  daemonSupervisorFromEnv,
  daemonSupervisorStatus,
  launchdWillRestart,
  parseDaemonSupervisor,
} from "./daemon-supervisor";

describe("daemon supervisor", () => {
  it("parses launchd, pid and other supervisors", () => {
    expect(parseDaemonSupervisor("launchd:local.executor.daemon")).toEqual({
      kind: "launchd",
      label: "local.executor.daemon",
    });
    expect(parseDaemonSupervisor("pid:42")).toEqual({ kind: "pid", pid: 42 });
    expect(parseDaemonSupervisor("systemd")).toEqual({ kind: "other", name: "systemd" });
  });

  it("only names a supervisor for a supervised daemon", () => {
    expect(daemonSupervisorFromEnv({ EXECUTOR_DAEMON_SUPERVISOR: "pid:1" })).toBeNull();
    expect(
      daemonSupervisorFromEnv({ EXECUTOR_SUPERVISED: "1", EXECUTOR_DAEMON_SUPERVISOR: "pid:1" }),
    ).toBe("pid:1");
  });

  it("checks whether the supervisor is still registered", async () => {
    const launchd = parseDaemonSupervisor("launchd:x");
    const printed = launchdPrintFixture({ state: "running", lastExit: "(never exited)" });
    expect(await daemonSupervisorStatus(launchd, { launchdPrint: async () => printed })).toBe(
      "registered",
    );
    expect(await daemonSupervisorStatus(launchd, { launchdPrint: async () => null })).toBe("gone");
    expect(await daemonSupervisorStatus({ kind: "pid", pid: process.pid })).toBe("registered");
    expect(await daemonSupervisorStatus({ kind: "pid", pid: 2 ** 22 + 7 })).toBe("gone");
    expect(await daemonSupervisorStatus({ kind: "other", name: "os" })).toBe("unknown");
  });

  it("treats a loaded launchd job that exited cleanly as gone (launchd will not restart it)", async () => {
    // After `executor daemon stop` (exit 0) under KeepAlive SuccessfulExit=false
    // the job stays loaded but launchd leaves it down: spawn right away.
    const stopped = launchdPrintFixture({ state: "not running", lastExit: "0" });
    expect(
      await daemonSupervisorStatus(parseDaemonSupervisor("launchd:x"), {
        launchdPrint: async () => stopped,
      }),
    ).toBe("gone");
  });

  it("knows when launchd will restart the job", () => {
    const will = (fields: Parameters<typeof launchdPrintFixture>[0]) =>
      launchdWillRestart(launchdPrintFixture(fields));
    expect(will({ state: "running", lastExit: "0" })).toBe(true);
    expect(will({ state: "spawn scheduled", lastExit: "1" })).toBe(true);
    // Crashed or killed: KeepAlive SuccessfulExit=false restarts it after the throttle.
    expect(will({ state: "not running", lastExit: "1" })).toBe(true);
    expect(will({ state: "not running", lastExit: "0", signal: "Killed: 9" })).toBe(true);
    // Clean exit, or never started: launchd leaves it down.
    expect(will({ state: "not running", lastExit: "0" })).toBe(false);
    expect(will({ state: "not running", lastExit: "(never exited)" })).toBe(false);
    // Without a KeepAlive rule launchd restarts nothing.
    expect(will({ state: "not running", lastExit: "1", keepAlive: false })).toBe(false);
  });
});

/** The shape of `launchctl print gui/<uid>/<label>` (macOS 26) for our job. */
const launchdPrintFixture = (input: {
  readonly state: string;
  readonly lastExit: string;
  readonly signal?: string;
  readonly keepAlive?: boolean;
}): string =>
  [
    "gui/501/local.executor.daemon = {",
    "\tactive count = 0",
    "\tpath = /Users/me/Library/LaunchAgents/local.executor.daemon.plist",
    "\ttype = LaunchAgent",
    `\tstate = ${input.state}`,
    "",
    "\tprogram = /Users/me/.bun/bin/bun",
    "\truns = 2",
    `\tlast exit code = ${input.lastExit}`,
    ...(input.signal ? [`\tlast terminating signal = ${input.signal}`] : []),
    ...(input.keepAlive === false ? [] : ["\tsemaphores = {", "\t\tsuccessful exit => 0", "\t}"]),
    "\tresource coalition = {",
    "\t\tID = 3537",
    "\t\tstate = active",
    "\t}",
    "\tproperties = runatload | inferred program",
    "}",
  ].join("\n");
