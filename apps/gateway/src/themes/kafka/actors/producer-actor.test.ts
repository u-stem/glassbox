import { describe, expect, test } from "bun:test";
import { EventBus } from "../../../event-bus";
import { ProducerActor } from "./producer-actor";

function makeEventBus(): EventBus {
  return new EventBus({ capacity: 100 });
}

/**
 * These tests never call send(), so the underlying Producer never actually connects
 * to a broker: constructing a Producer and attaching listeners is pure local
 * bookkeeping, so this can run without a real broker.
 */
describe("ProducerActor 'error' listener (process-crash regression)", () => {
  test("always registers an 'error' listener on construction, regardless of onClientError being provided", () => {
    const actorWithoutCallback = new ProducerActor({
      clientId: "producer-1",
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
    });

    expect(actorWithoutCallback.hasErrorListener()).toBe(true);
  });

  test("forwards a client 'error' emission to onClientError instead of letting it crash the process", () => {
    // Regression test mirroring the real incident found in ConsumerActor (see its
    // equivalent test's doc): @platformatic/kafka clients extend Node's EventEmitter,
    // and Node throws (crashing the whole process) when 'error' is emitted with zero
    // listeners attached.
    const receivedErrors: unknown[] = [];
    const actor = new ProducerActor({
      clientId: "producer-1",
      bootstrapBrokers: ["localhost:9092"],
      eventBus: makeEventBus(),
      onClientError: (error) => {
        receivedErrors.push(error);
      },
    });

    const simulatedError = new Error("simulated internal producer error");
    expect(() => {
      actor.emitClientErrorForTest(simulatedError);
    }).not.toThrow();

    expect(receivedErrors).toEqual([simulatedError]);
  });
});
