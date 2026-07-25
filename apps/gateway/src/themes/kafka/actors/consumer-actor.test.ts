import { describe, expect, test } from "bun:test";
import type { GlassboxEvent } from "@glassbox/schema";
import { EventBus } from "../../../event-bus";
import { ConsumerActor } from "./consumer-actor";

function makeEventBus(): EventBus {
  return new EventBus({ capacity: 100 });
}

/**
 * These tests never call startConsuming(), so the underlying Consumer never actually
 * joins a group or opens a connection: close("kill")/finalizeKillTeardown() are pure
 * local bookkeeping (an EventEmitter listener + an already-empty connection pool), so
 * they can run without a real broker, unlike the full lifecycle covered by
 * server.integration.test.ts.
 */
describe("ConsumerActor kill-mode resource cleanup", () => {
  test("getSessionTimeoutMs returns the library default when not configured", () => {
    const actor = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
    });

    expect(actor.getSessionTimeoutMs()).toBe(60_000);
  });

  test("getSessionTimeoutMs returns the configured override", () => {
    const actor = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      sessionTimeoutMs: 6000,
    });

    expect(actor.getSessionTimeoutMs()).toBe(6000);
  });

  test("is not blocking reconnection before close() is called", () => {
    const actor = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
    });

    expect(actor.isBlockingReconnect()).toBe(false);
  });

  test("close('kill') registers the reconnect-blocking listener", async () => {
    const actor = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
    });

    await actor.close("kill");

    expect(actor.isBlockingReconnect()).toBe(true);
  });

  test("finalizeKillTeardown removes the reconnect-blocking listener", async () => {
    const actor = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
    });

    await actor.close("kill");
    expect(actor.isBlockingReconnect()).toBe(true);

    await actor.finalizeKillTeardown();

    expect(actor.isBlockingReconnect()).toBe(false);
  });

  test("finalizeKillTeardown resolves without throwing even though no LeaveGroup was ever sent", async () => {
    const actor = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
    });

    await actor.close("kill");

    await expect(actor.finalizeKillTeardown()).resolves.toBeUndefined();
  });

  test("close('kill') publishes consumer.connection.lost, not consumer.group.left (the broker still believes the member is alive)", async () => {
    const eventBus = makeEventBus();
    const events: GlassboxEvent[] = [];
    eventBus.subscribe((event) => events.push(event));

    const actor = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus,
    });

    await actor.close("kill");

    const lost = events.find((e) => e.type === "consumer.connection.lost");
    expect(lost?.payload).toEqual({
      groupId: "test-group",
      clientId: "consumer-1",
      reason: "kill",
    });
    expect(events.some((e) => e.type === "consumer.group.left")).toBe(false);
  });

  test("close('graceful') does not publish consumer.connection.lost (the real LeaveGroup flow owns consumer.group.left instead)", async () => {
    const eventBus = makeEventBus();
    const events: GlassboxEvent[] = [];
    eventBus.subscribe((event) => events.push(event));

    const actor = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus,
    });

    await actor.close("graceful");

    expect(events.some((e) => e.type === "consumer.connection.lost")).toBe(false);
  });
});

describe("ConsumerActor 'error' listener (process-crash regression)", () => {
  test("always registers an 'error' listener on construction, regardless of onClientError being provided", () => {
    const actorWithoutCallback = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
    });

    expect(actorWithoutCallback.hasErrorListener()).toBe(true);
  });

  test("forwards a client 'error' emission to onClientError instead of letting it crash the process", () => {
    // Regression test for a real incident: @platformatic/kafka clients extend Node's
    // EventEmitter, and Node throws (crashing the whole process) when 'error' is
    // emitted with zero listeners attached. A killed consumer's internal
    // heartbeat-error -> rejoinGroup retry loop eventually exhausts its retries and
    // emits a plain 'error' event; without a listener, this crashed the gateway
    // during repeated add-consumer/remove-consumer cycling (see onClientError's doc
    // on ConsumerActorOptions). This test simulates that emission directly, without
    // needing a real broker to actually exhaust a retry loop.
    const receivedErrors: unknown[] = [];
    const actor = new ConsumerActor({
      clientId: "consumer-1",
      groupId: "test-group",
      topics: ["t"],
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      onClientError: (error) => {
        receivedErrors.push(error);
      },
    });

    const simulatedError = new Error("rejoinGroup failed 4 times.");
    expect(() => {
      actor.emitClientErrorForTest(simulatedError);
    }).not.toThrow();

    expect(receivedErrors).toEqual([simulatedError]);
  });
});
