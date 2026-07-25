import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type ChildProcess, spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { healthzResponseSchema } from "@glassbox/schema";

const gatewayRoot = fileURLToPath(new URL("..", import.meta.url));

/**
 * Integration test against a real Kafka broker (docker/compose.yaml, localhost:9092).
 * Spawns the gateway as a subprocess (rather than importing server.ts, which has
 * top-level startup side effects) on a dedicated port to avoid clashing with a
 * developer's own `bun run dev` instance.
 */

const PORT = 4100;
const BASE_URL = `http://localhost:${PORT}`;

let gatewayProcess: ChildProcess;
let gatewayOutput = "";

async function waitForHealthz(timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${BASE_URL}/healthz`);
      if (response.ok) {
        return;
      }
    } catch {
      // gateway not up yet, retry
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error("gateway did not become healthy in time");
}

beforeAll(async () => {
  gatewayProcess = spawn("npx", ["tsx", "src/server.ts"], {
    cwd: gatewayRoot,
    env: { ...process.env, PORT: String(PORT), ADMIN_POLL_INTERVAL_MS: "300" },
    stdio: "pipe",
  });
  gatewayProcess.stdout?.on("data", (chunk) => {
    gatewayOutput += chunk.toString();
  });
  gatewayProcess.stderr?.on("data", (chunk) => {
    gatewayOutput += chunk.toString();
  });
  await waitForHealthz(15_000);
}, 20_000);

afterAll(() => {
  gatewayProcess.kill();
});

describe("healthz Kafka reachability", () => {
  test("eventually reports kafka: connected once the admin poller has ticked", async () => {
    // ADMIN_POLL_INTERVAL_MS is 300ms for this test process (see beforeAll); the
    // gateway is already up (waitForHealthz), so this just waits out the first tick.
    const deadline = Date.now() + 5_000;
    let kafkaStatus: string | undefined;

    while (Date.now() < deadline) {
      const response = await fetch(`${BASE_URL}/healthz`);
      const body = healthzResponseSchema.parse(await response.json());
      kafkaStatus = body.kafka;
      if (kafkaStatus === "connected") {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    expect(kafkaStatus).toBe("connected");
  }, 10_000);
});

describe("gateway SSE + produce-burst integration", () => {
  test("streams producer.send.end and admin.snapshot after a produce-burst run", async () => {
    const controller = new AbortController();
    const sseResponse = await fetch(`${BASE_URL}/api/events`, { signal: controller.signal });
    const reader = sseResponse.body?.getReader();
    if (!reader) {
      throw new Error("expected a readable SSE stream");
    }

    const seenTypes = new Set<string>();
    const decoder = new TextDecoder();

    const collect = (async () => {
      let buffer = "";
      while (!seenTypes.has("producer.send.end") || !seenTypes.has("admin.snapshot")) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        buffer += decoder.decode(value, { stream: true });
        const frames = buffer.split("\n\n");
        buffer = frames.pop() ?? "";
        for (const frame of frames) {
          const dataLine = frame.split("\n").find((line) => line.startsWith("data: "));
          if (!dataLine) {
            continue;
          }
          const event = JSON.parse(dataLine.slice("data: ".length));
          seenTypes.add(event.type);
        }
      }
    })();

    const postResponse = await fetch(`${BASE_URL}/api/themes/kafka/scenarios/produce-burst`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count: 5, rateMs: 0, keyStrategy: "round-robin" }),
    });
    expect(postResponse.status).toBe(202);

    try {
      await Promise.race([
        collect,
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("timed out waiting for SSE events")), 10_000),
        ),
      ]);
    } catch (error) {
      console.error("gateway output:\n", gatewayOutput);
      throw error;
    }

    controller.abort();

    expect(seenTypes.has("producer.send.end")).toBe(true);
    expect(seenTypes.has("admin.snapshot")).toBe(true);
  }, 20_000);

  test("a fresh connection with no Last-Event-ID still receives the existing backlog immediately", async () => {
    // ADMIN_POLL_INTERVAL_MS is 300ms for this test process (see beforeAll); give it
    // time to have already published at least one admin.snapshot into the ring buffer
    // before this test even connects.
    await new Promise((resolve) => setTimeout(resolve, 500));

    const controller = new AbortController();
    const startedAt = Date.now();
    const sseResponse = await fetch(`${BASE_URL}/api/events`, { signal: controller.signal });
    const reader = sseResponse.body?.getReader();
    if (!reader) {
      throw new Error("expected a readable SSE stream");
    }

    const decoder = new TextDecoder();
    let buffer = "";
    let sawAdminSnapshotAt: number | undefined;

    try {
      while (sawAdminSnapshotAt === undefined) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        buffer += decoder.decode(value, { stream: true });
        const frames = buffer.split("\n\n");
        buffer = frames.pop() ?? "";
        for (const frame of frames) {
          const dataLine = frame.split("\n").find((line) => line.startsWith("data: "));
          if (!dataLine) {
            continue;
          }
          const event = JSON.parse(dataLine.slice("data: ".length));
          if (event.type === "admin.snapshot") {
            sawAdminSnapshotAt = Date.now() - startedAt;
            break;
          }
        }
      }
    } finally {
      controller.abort();
    }

    // Well under the 300ms poll interval: proves this arrived from the ring buffer's
    // existing backlog (getSinceAndSubscribe(-1)), delivered as part of the same
    // initial batch of reads as every other backlogged event, rather than from
    // waiting for the next live admin-poller tick -- which is what a fresh connection
    // with no Last-Event-ID used to be stuck doing (see server.ts's SSE route doc).
    expect(sawAdminSnapshotAt).toBeLessThan(150);
  }, 10_000);

  test("rejects a concurrent produce-burst request with 409 while one is in flight, and correlates events via scenarioRunId", async () => {
    const controller = new AbortController();
    const sseResponse = await fetch(`${BASE_URL}/api/events`, { signal: controller.signal });
    const reader = sseResponse.body?.getReader();
    if (!reader) {
      throw new Error("expected a readable SSE stream");
    }

    const events: Array<{ type: string; payload: { scenarioRunId?: string } }> = [];
    const decoder = new TextDecoder();

    // This test shares the gateway subprocess with earlier tests, one of which already
    // ran its own produce-burst (leaving a "scenario.finished" in the backlog): without
    // this cutoff, the collect loop below and the final assertions could be satisfied
    // by that leftover event instead of the one this test's own requests should cause.
    const baselineLength = await waitForBacklogToSettle(events);

    const collect = (async () => {
      let buffer = "";
      while (!events.slice(baselineLength).some((e) => e.type === "scenario.finished")) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        buffer += decoder.decode(value, { stream: true });
        const frames = buffer.split("\n\n");
        buffer = frames.pop() ?? "";
        for (const frame of frames) {
          const dataLine = frame.split("\n").find((line) => line.startsWith("data: "));
          if (!dataLine) {
            continue;
          }
          events.push(JSON.parse(dataLine.slice("data: ".length)));
        }
      }
    })();

    const firstResponse = await fetch(`${BASE_URL}/api/themes/kafka/scenarios/produce-burst`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count: 20, rateMs: 50, keyStrategy: "round-robin" }),
    });
    expect(firstResponse.status).toBe(202);

    const secondResponse = await fetch(`${BASE_URL}/api/themes/kafka/scenarios/produce-burst`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count: 5, rateMs: 0, keyStrategy: "round-robin" }),
    });
    expect(secondResponse.status).toBe(409);

    try {
      await Promise.race([
        collect,
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("timed out waiting for scenario.finished")), 15_000),
        ),
      ]);
    } catch (error) {
      console.error("gateway output:\n", gatewayOutput);
      throw error;
    }

    controller.abort();

    const liveEvents = events.slice(baselineLength);
    const started = liveEvents.find((e) => e.type === "scenario.started");
    const sendEnds = liveEvents.filter((e) => e.type === "producer.send.end");
    expect(started?.payload.scenarioRunId).toBeDefined();
    expect(sendEnds.length).toBeGreaterThan(0);
    expect(sendEnds.every((e) => e.payload.scenarioRunId === started?.payload.scenarioRunId)).toBe(
      true,
    );
  }, 25_000);
});

interface CapturedEvent {
  type: string;
  payload: Record<string, unknown>;
  source: { kind: string; actorId?: string };
}

interface SseCollector {
  events: CapturedEvent[];
  /**
   * Index into `events` where this connection's own backlog (delivered immediately
   * on connect per server.ts's getSinceAndSubscribe(-1) fix) ends and genuinely live
   * activity begins. Every test in this file shares one gateway subprocess, so a
   * fresh collector now legitimately receives earlier tests' history too -- callers
   * that care about "what happened as a result of the action I'm about to trigger"
   * must slice(baselineLength) rather than scanning `events` from the start.
   */
  baselineLength: number;
  waitFor: (predicate: (events: CapturedEvent[]) => boolean, timeoutMs: number) => Promise<void>;
  close: () => void;
}

/**
 * Waits until `events` stops growing for a couple of ticks, i.e. until an SSE
 * connection's initial backlog burst (written synchronously by the route handler,
 * all before any new event could possibly arrive) has fully drained in. Safe to use
 * as a "no more backlog, only live activity from here on" checkpoint as long as it's
 * called before triggering whatever action the test actually wants to observe.
 */
async function waitForBacklogToSettle(events: unknown[]): Promise<number> {
  let previousLength = -1;
  while (events.length !== previousLength) {
    previousLength = events.length;
    await new Promise((resolve) => setTimeout(resolve, 40));
  }
  return events.length;
}

/**
 * Opens the SSE stream and starts appending every event to `events` in the
 * background. Callers should open the collector *before* triggering the scenario
 * whose events they want to observe (mirroring the existing produce-burst tests'
 * ordering above), then use waitFor() to block until a condition over the
 * accumulated events becomes true.
 */
async function openSseCollector(): Promise<SseCollector> {
  const controller = new AbortController();
  const sseResponse = await fetch(`${BASE_URL}/api/events`, { signal: controller.signal });
  const reader = sseResponse.body?.getReader();
  if (!reader) {
    throw new Error("expected a readable SSE stream");
  }

  const events: CapturedEvent[] = [];
  const decoder = new TextDecoder();
  let buffer = "";

  const pump = (async () => {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) {
        return;
      }
      buffer += decoder.decode(value, { stream: true });
      const frames = buffer.split("\n\n");
      buffer = frames.pop() ?? "";
      for (const frame of frames) {
        const dataLine = frame.split("\n").find((line) => line.startsWith("data: "));
        if (!dataLine) {
          continue;
        }
        events.push(JSON.parse(dataLine.slice("data: ".length)));
      }
    }
  })();
  pump.catch(() => {
    // Expected once close() aborts the underlying fetch.
  });

  const baselineLength = await waitForBacklogToSettle(events);

  return {
    events,
    baselineLength,
    async waitFor(predicate, timeoutMs) {
      const deadline = Date.now() + timeoutMs;
      while (!predicate(events)) {
        if (Date.now() > deadline) {
          console.error("gateway output:\n", gatewayOutput);
          console.error("captured events so far:\n", JSON.stringify(events, null, 2));
          throw new Error("timed out waiting for expected SSE events");
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    },
    close() {
      controller.abort();
    },
  };
}

function groupStateChanges(events: CapturedEvent[], groupId: string): string[] {
  return events
    .filter((e) => e.type === "group.state.changed" && e.payload.groupId === groupId)
    .map((e) => e.payload.to)
    .filter((to): to is string => typeof to === "string");
}

interface SnapshotGroupOffset {
  lag: number;
}

interface SnapshotGroup {
  groupId: string;
  offsets: SnapshotGroupOffset[];
}

function isSnapshotGroupOffset(value: unknown): value is SnapshotGroupOffset {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { lag?: unknown }).lag === "number"
  );
}

function isSnapshotGroup(value: unknown): value is SnapshotGroup {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const record = value as { groupId?: unknown; offsets?: unknown };
  return typeof record.groupId === "string" && Array.isArray(record.offsets);
}

/** Extracts admin.snapshot's payload.groups for a specific groupId, without an `as`
 * cast: payload is `Record<string, unknown>` at the CapturedEvent boundary. */
function findSnapshotGroup(event: CapturedEvent, groupId: string): SnapshotGroup | undefined {
  const rawGroups = event.payload.groups;
  if (!Array.isArray(rawGroups)) {
    return undefined;
  }
  const rawGroup = rawGroups.find((g) => isSnapshotGroup(g) && g.groupId === groupId);
  if (!isSnapshotGroup(rawGroup)) {
    return undefined;
  }
  const offsets = rawGroup.offsets.filter(isSnapshotGroupOffset);
  return { groupId: rawGroup.groupId, offsets };
}

describe("consumer groups and rebalance (Phase 2)", () => {
  const GROUP_ID = "phase2-test-group";
  // Short enough to keep the "kill" test's session-timeout wait bounded; still
  // respects the library's rebalanceTimeout >= sessionTimeout >= heartbeatInterval
  // validation.
  const SHORT_TIMEOUTS = {
    sessionTimeoutMs: 6000,
    rebalanceTimeoutMs: 8000,
    heartbeatIntervalMs: 1000,
  };

  test("add-consumer x2 drives group.state.changed through Preparing -> Completing -> Stable", async () => {
    const collector = await openSseCollector();
    try {
      await fetch(`${BASE_URL}/api/themes/kafka/scenarios/add-consumer`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ groupId: GROUP_ID, ...SHORT_TIMEOUTS }),
      });

      // Wait for the first member to reach Stable before adding the second, so the
      // second member's join deterministically triggers a Stable -> Preparing edge
      // rather than racing the first member's own initial join. Scoped to
      // baselineLength onward: GROUP_ID is fresh here, so this doesn't currently
      // change behavior, but keeps this test correct regardless of suite ordering.
      await collector.waitFor(
        (events) =>
          groupStateChanges(events.slice(collector.baselineLength), GROUP_ID).includes("Stable"),
        15_000,
      );

      await fetch(`${BASE_URL}/api/themes/kafka/scenarios/add-consumer`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ groupId: GROUP_ID, ...SHORT_TIMEOUTS }),
      });

      await collector.waitFor((collected) => {
        const changes = groupStateChanges(collected.slice(collector.baselineLength), GROUP_ID);
        return changes.filter((s) => s === "Stable").length >= 2;
      }, 20_000);

      const changes = groupStateChanges(collector.events.slice(collector.baselineLength), GROUP_ID);
      const secondJoinOnward = changes.slice(changes.indexOf("Stable") + 1);
      expect(secondJoinOnward).toEqual(["PreparingRebalance", "CompletingRebalance", "Stable"]);
    } finally {
      collector.close();
    }
  }, 40_000);

  test("remove-consumer kill leaves the member until session timeout, then rebalances back to Stable", async () => {
    const collector = await openSseCollector();
    try {
      // consumer-2 was the second member added by the previous test.
      await fetch(`${BASE_URL}/api/themes/kafka/scenarios/remove-consumer`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ consumerId: "consumer-2", mode: "kill" }),
      });

      await collector.waitFor((collected) => {
        const changes = groupStateChanges(collected.slice(collector.baselineLength), GROUP_ID);
        return changes.filter((s) => s === "Stable").length >= 1;
      }, 20_000);

      // Scoped to baselineLength onward: the previous test already left this shared
      // GROUP_ID at "Stable", so without this cutoff, both waitFor above and the
      // assertions below could be trivially (and wrongly) satisfied by that leftover
      // backlog before this test's own remove-consumer request had any effect.
      const liveEvents = collector.events.slice(collector.baselineLength);
      const changes = groupStateChanges(liveEvents, GROUP_ID);
      expect(changes.at(-1)).toBe("Stable");
      // consumer.connection.lost IS expected immediately on kill (published by
      // ConsumerActor#close itself, source.kind: "scenario"): the broker still
      // believes this member is alive until the session times out, so this is
      // deliberately distinct from consumer.group.left (see ADR-0003 / the Phase 2
      // review's "kill flicker" fix, and consumer-actor.ts's close() doc). No real
      // LeaveGroup ever reaches the broker for a killed member, so consumer.group.left
      // for consumer-2 must never appear at all.
      const connectionLost = liveEvents.find(
        (e) => e.type === "consumer.connection.lost" && e.payload.clientId === "consumer-2",
      );
      expect(connectionLost?.source).toEqual({ kind: "scenario", actorId: "consumer-2" });
      expect(connectionLost?.payload).toEqual({
        groupId: GROUP_ID,
        clientId: "consumer-2",
        reason: "kill",
      });
      expect(
        liveEvents.some(
          (e) => e.type === "consumer.group.left" && e.payload.clientId === "consumer-2",
        ),
      ).toBe(false);
    } finally {
      collector.close();
    }
  }, 30_000);

  test("slow-consumer causes lag to appear in the next admin.snapshot", async () => {
    const collector = await openSseCollector();
    try {
      await fetch(`${BASE_URL}/api/themes/kafka/scenarios/slow-consumer`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ consumerId: "consumer-1", processingDelayMs: 300 }),
      });

      await fetch(`${BASE_URL}/api/themes/kafka/scenarios/produce-burst`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ topic: "glassbox.demo", count: 30, rateMs: 0 }),
      });

      // Scoped to baselineLength onward: an earlier test already produced admin.snapshot
      // entries for this shared GROUP_ID, so without this cutoff, waitFor below could
      // resolve immediately off stale backlog lag readings instead of the lag this
      // test's own slow-consumer/produce-burst calls are meant to cause.
      function hasLagForGroup(events: CapturedEvent[]): boolean {
        return events
          .slice(collector.baselineLength)
          .some(
            (e) =>
              e.type === "admin.snapshot" &&
              (findSnapshotGroup(e, GROUP_ID)?.offsets.some((o) => o.lag > 0) ?? false),
          );
      }

      await collector.waitFor(hasLagForGroup, 20_000);

      const liveEvents = collector.events.slice(collector.baselineLength);
      const snapshotWithLag = liveEvents.findLast((e) => e.type === "admin.snapshot");
      const group = snapshotWithLag ? findSnapshotGroup(snapshotWithLag, GROUP_ID) : undefined;
      expect(group?.offsets.some((o) => o.lag > 0)).toBe(true);
    } finally {
      collector.close();
    }
  }, 30_000);
});
