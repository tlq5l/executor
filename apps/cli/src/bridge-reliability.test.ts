import { describe, expect, it } from "@effect/vitest";

import {
  defaultHarnessOptions,
  scenarioConcurrentRestart,
  scenarioDaemonRestart,
  scenarioIdleRestart,
  scenarioKillInFlight,
  scenarioSpawnerKilled,
  scenarioSupervisorNeverRestarts,
  scenarioSupervisorTakeover,
  type ScenarioResult,
} from "./bridge-reliability.harness";

// Real bridges against a real daemon on a throwaway port and data dir. The
// full matrix (including a 6+ minute idle run) is the harness script itself:
//   BRIDGE_IDLE_MS=390000 bun run apps/cli/src/bridge-reliability.harness.ts

const options = { ...defaultHarnessOptions(), calls: 16 };

const expectNoDrops = (result: ScenarioResult) => {
  expect({ dropped: result.dropped, notes: result.notes }).toEqual({ dropped: 0, notes: [] });
  expect(result.total).toBeGreaterThan(0);
};

describe("executor mcp bridge under daemon faults", () => {
  it("keeps every call across supervised daemon restarts without spawning its own daemon", async () => {
    const result = await scenarioDaemonRestart(options);
    expectNoDrops(result);
    expect(result.bridgeSpawns).toBe(0);
  }, 120_000);

  it("keeps a second bridge working when the session that spawned the daemon is killed", async () => {
    expectNoDrops(await scenarioSpawnerKilled(options));
  }, 120_000);

  it("keeps four concurrent bridges working through a daemon restart", async () => {
    expectNoDrops(await scenarioConcurrentRestart(options));
  }, 120_000);

  it("reports a call running when the daemon is killed as lost, and resends read-only ones", async () => {
    const result = await scenarioKillInFlight(options);
    expectNoDrops(result);
    expect(result.outcomes.lost).toBeGreaterThan(0);
  }, 120_000);

  it("recovers when the daemon restarted while the bridge was idle (stale session id)", async () => {
    expectNoDrops(await scenarioIdleRestart(options));
  }, 120_000);

  it("follows a supervisor taking over from a bridge-spawned daemon", async () => {
    const result = await scenarioSupervisorTakeover(options);
    expectNoDrops(result);
    expect(result.bridgeSpawns).toBe(1);
  }, 120_000);

  it("starts its own daemon when a registered supervisor never restarts it, on every drop", async () => {
    const result = await scenarioSupervisorNeverRestarts(options);
    expectNoDrops(result);
    expect(result.bridgeSpawns).toBe(2);
  }, 180_000);
});
