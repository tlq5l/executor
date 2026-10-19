import { describe, expect, it } from "@effect/vitest";

import {
  daemonSupervisorFromEnv,
  daemonSupervisorStatus,
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
    expect(await daemonSupervisorStatus(launchd, { launchdJobLoaded: async () => true })).toBe(
      "registered",
    );
    expect(await daemonSupervisorStatus(launchd, { launchdJobLoaded: async () => false })).toBe(
      "gone",
    );
    expect(await daemonSupervisorStatus({ kind: "pid", pid: process.pid })).toBe("registered");
    expect(await daemonSupervisorStatus({ kind: "pid", pid: 2 ** 22 + 7 })).toBe("gone");
    expect(await daemonSupervisorStatus({ kind: "other", name: "os" })).toBe("unknown");
  });
});
