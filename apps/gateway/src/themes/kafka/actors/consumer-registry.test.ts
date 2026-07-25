import { describe, expect, test } from "bun:test";
import { EventBus } from "../../../event-bus";
import {
  type ConsumerActorLike,
  ConsumerCapacityExceededError,
  createConsumerRegistry,
  MAX_CONSUMERS,
} from "./consumer-registry";

/**
 * A fake ConsumerActorLike that records calls instead of touching a real Consumer, so
 * the registry's kill-mode watchdog scheduling logic can be tested without a real
 * broker or real elapsed time.
 */
function createFakeActor(
  clientId: string,
  sessionTimeoutMs = 6000,
): ConsumerActorLike & {
  closeCalls: Array<"graceful" | "kill">;
  finalizeKillTeardownCallCount: number;
} {
  const closeCalls: Array<"graceful" | "kill"> = [];
  let finalizeKillTeardownCallCount = 0;

  return {
    actorId: clientId,
    groupId: "test-group",
    closeCalls,
    get finalizeKillTeardownCallCount() {
      return finalizeKillTeardownCallCount;
    },
    async startConsuming() {},
    async close(mode) {
      closeCalls.push(mode);
    },
    setProcessingDelay() {},
    setJoinDelay() {},
    getSessionTimeoutMs() {
      return sessionTimeoutMs;
    },
    async finalizeKillTeardown() {
      finalizeKillTeardownCallCount += 1;
    },
  };
}

function makeEventBus(): EventBus {
  return new EventBus({ capacity: 100 });
}

describe("createConsumerRegistry kill-mode watchdog", () => {
  test("schedules finalizeKillTeardown after sessionTimeoutMs + margin, not immediately", async () => {
    const scheduled: Array<{ delayMs: number; fn: () => void }> = [];
    const fakeActor = createFakeActor("consumer-1", 6000);
    const registry = createConsumerRegistry({
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      createActor: () => fakeActor,
      scheduleDelayed: (fn, delayMs) => {
        scheduled.push({ delayMs, fn });
      },
    });

    registry.add({ groupId: "test-group", topics: ["t"] });
    await registry.remove("consumer-1", "kill");

    expect(fakeActor.closeCalls).toEqual(["kill"]);
    expect(fakeActor.finalizeKillTeardownCallCount).toBe(0);
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0]?.delayMs).toBe(6000 + 2000);
  });

  test("invokes finalizeKillTeardown once the scheduled callback actually fires", async () => {
    const scheduled: Array<() => void> = [];
    const fakeActor = createFakeActor("consumer-1");
    const registry = createConsumerRegistry({
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      createActor: () => fakeActor,
      scheduleDelayed: (fn) => {
        scheduled.push(fn);
      },
    });

    registry.add({ groupId: "test-group", topics: ["t"] });
    await registry.remove("consumer-1", "kill");

    expect(fakeActor.finalizeKillTeardownCallCount).toBe(0);
    scheduled[0]?.();
    // finalizeKillTeardown is async; give its microtask a tick to run.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(fakeActor.finalizeKillTeardownCallCount).toBe(1);
  });

  test("does NOT schedule a teardown watchdog for graceful removal", async () => {
    const scheduled: unknown[] = [];
    const fakeActor = createFakeActor("consumer-1");
    const registry = createConsumerRegistry({
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      createActor: () => fakeActor,
      scheduleDelayed: (fn) => {
        scheduled.push(fn);
      },
    });

    registry.add({ groupId: "test-group", topics: ["t"] });
    await registry.remove("consumer-1", "graceful");

    expect(fakeActor.closeCalls).toEqual(["graceful"]);
    expect(scheduled).toHaveLength(0);
  });

  test("reports an unexpected finalizeKillTeardown rejection via onTeardownError", async () => {
    const teardownErrors: Array<{ clientId: string; error: unknown }> = [];
    const scheduled: Array<() => void> = [];
    const fakeActor: ConsumerActorLike = {
      ...createFakeActor("consumer-1"),
      async finalizeKillTeardown() {
        throw new Error("unexpected");
      },
    };
    const registry = createConsumerRegistry({
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      createActor: () => fakeActor,
      scheduleDelayed: (fn) => {
        scheduled.push(fn);
      },
      onTeardownError: (clientId, error) => {
        teardownErrors.push({ clientId, error });
      },
    });

    registry.add({ groupId: "test-group", topics: ["t"] });
    await registry.remove("consumer-1", "kill");
    scheduled[0]?.();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(teardownErrors).toHaveLength(1);
    expect(teardownErrors[0]?.clientId).toBe("consumer-1");
  });

  test("removing an unknown clientId is a no-op (no watchdog scheduled)", async () => {
    const scheduled: unknown[] = [];
    const registry = createConsumerRegistry({
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      scheduleDelayed: (fn) => {
        scheduled.push(fn);
      },
    });

    await registry.remove("does-not-exist", "kill");

    expect(scheduled).toHaveLength(0);
  });
});

describe("createConsumerRegistry MAX_CONSUMERS capacity guard", () => {
  test("isFull() is false below MAX_CONSUMERS and true once it is reached", () => {
    let nextId = 1;
    const registry = createConsumerRegistry({
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      createActor: () => createFakeActor(`consumer-${nextId++}`),
    });

    for (let i = 0; i < MAX_CONSUMERS; i++) {
      expect(registry.isFull()).toBe(false);
      registry.add({ groupId: "test-group", topics: ["t"] });
    }

    expect(registry.isFull()).toBe(true);
  });

  test("add() throws ConsumerCapacityExceededError once at MAX_CONSUMERS (defense in depth)", () => {
    let nextId = 1;
    const registry = createConsumerRegistry({
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      createActor: () => createFakeActor(`consumer-${nextId++}`),
    });

    for (let i = 0; i < MAX_CONSUMERS; i++) {
      registry.add({ groupId: "test-group", topics: ["t"] });
    }

    expect(() => registry.add({ groupId: "test-group", topics: ["t"] })).toThrow(
      ConsumerCapacityExceededError,
    );
  });

  test("removing a consumer frees up capacity for a new add()", async () => {
    let nextId = 1;
    const registry = createConsumerRegistry({
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      createActor: () => createFakeActor(`consumer-${nextId++}`),
      scheduleDelayed: () => {},
    });

    for (let i = 0; i < MAX_CONSUMERS; i++) {
      registry.add({ groupId: "test-group", topics: ["t"] });
    }
    expect(registry.isFull()).toBe(true);

    await registry.remove("consumer-1", "graceful");

    expect(registry.isFull()).toBe(false);
    expect(() => registry.add({ groupId: "test-group", topics: ["t"] })).not.toThrow();
  });
});
