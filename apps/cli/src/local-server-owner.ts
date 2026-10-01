import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

import type { ExecutorLocalServerManifest } from "@executor-js/sdk/shared";

// ---------------------------------------------------------------------------
// Stall-tolerant read of the local server owner.
//
// The plain manifest read treats "pid alive but /api/health did not answer
// within 2 s" the same as "no server", so a caller deciding whether to spawn a
// daemon spawned a second one next to a live owner that was only briefly
// unresponsive (an overloaded machine, a long synchronous task on the owner's
// event loop). That second daemon then lost the data-dir lock and either
// lingered or exited, and the client paid a full startup wait for nothing.
//
// This read waits out such a stall: while the advertised owner's pid stays
// alive it keeps probing until the owner answers or a wall-clock budget runs
// out. The healthy case pays exactly one probe and no sleep.
// ---------------------------------------------------------------------------

/** How long a caller waits for a live-but-silent owner before giving up. A
 * failed probe already costs up to its own 2 s timeout, so the budget is a
 * wall-clock deadline rather than an attempt count. */
export const LOCAL_SERVER_OWNER_STALL_BUDGET_MS = 20_000;

/** Pause between probes of a live-but-silent owner. */
export const LOCAL_SERVER_OWNER_STALL_POLL_MS = 250;

export type LocalServerOwnerStallEvent =
  | {
      readonly kind: "stalled";
      readonly manifest: ExecutorLocalServerManifest;
      readonly budgetMs: number;
    }
  | {
      readonly kind: "recovered";
      readonly manifest: ExecutorLocalServerManifest;
      readonly waitedMs: number;
      readonly probes: number;
    }
  | {
      readonly kind: "gone";
      readonly pid: number;
      readonly waitedMs: number;
    }
  | {
      readonly kind: "gave-up";
      readonly manifest: ExecutorLocalServerManifest;
      readonly waitedMs: number;
      readonly probes: number;
    };

export interface LocalServerOwnerDeps<R> {
  readonly readManifest: Effect.Effect<ExecutorLocalServerManifest | null, never, R>;
  readonly isReachable: (origin: string) => Effect.Effect<boolean, never, R>;
  readonly isPidAlive: (pid: number) => boolean;
  readonly removeManifestIfOwnedBy: (pid: number) => Effect.Effect<void, never, R>;
  readonly onStallEvent?: (event: LocalServerOwnerStallEvent) => void;
  readonly budgetMs?: number;
  readonly pollMs?: number;
}

/**
 * Return the advertised local server owner once it answers /api/health, or
 * null when there is no owner to attach to: no manifest, the owner's pid is
 * dead (its stale manifest is removed), or the owner stayed silent past the
 * budget. On that last case the caller falls back to its usual spawn path; a
 * daemon spawned next to a still-live owner fails the data-dir ownership lock
 * and exits, and the spawner's startup wait attaches to the owner once it
 * answers again.
 */
export const readLocalServerOwnerWaitingOutStall = <R>(
  deps: LocalServerOwnerDeps<R>,
): Effect.Effect<ExecutorLocalServerManifest | null, never, R> =>
  Effect.gen(function* () {
    const budgetMs = deps.budgetMs ?? LOCAL_SERVER_OWNER_STALL_BUDGET_MS;
    const pollMs = deps.pollMs ?? LOCAL_SERVER_OWNER_STALL_POLL_MS;

    const first = yield* deps.readManifest;
    if (!first) return null;
    if (yield* deps.isReachable(first.connection.origin)) return first;
    if (!deps.isPidAlive(first.pid)) {
      yield* deps.removeManifestIfOwnedBy(first.pid);
      return null;
    }

    // Live pid, silent server: a stalled owner, not a missing one.
    const startedAt = yield* Clock.currentTimeMillis;
    const elapsed = Effect.map(Clock.currentTimeMillis, (now) => now - startedAt);
    deps.onStallEvent?.({ kind: "stalled", manifest: first, budgetMs });

    let probes = 1;
    let current: ExecutorLocalServerManifest = first;
    while ((yield* elapsed) < budgetMs) {
      yield* Effect.sleep(pollMs);

      const manifest = yield* deps.readManifest;
      if (!manifest) {
        // The owner shut down cleanly (it removes its own manifest).
        deps.onStallEvent?.({ kind: "gone", pid: current.pid, waitedMs: yield* elapsed });
        return null;
      }
      current = manifest;

      probes += 1;
      if (yield* deps.isReachable(manifest.connection.origin)) {
        deps.onStallEvent?.({
          kind: "recovered",
          manifest,
          waitedMs: yield* elapsed,
          probes,
        });
        return manifest;
      }

      if (!deps.isPidAlive(manifest.pid)) {
        yield* deps.removeManifestIfOwnedBy(manifest.pid);
        deps.onStallEvent?.({ kind: "gone", pid: manifest.pid, waitedMs: yield* elapsed });
        return null;
      }
    }

    deps.onStallEvent?.({ kind: "gave-up", manifest: current, waitedMs: yield* elapsed, probes });
    return null;
  });
