import { describe, expect, test } from "bun:test";
import {
  mapConsumerGroupJoining,
  mapConsumerGroupSyncing,
  mapProducerSendEnd,
  mapProducerSendStart,
} from "./collector";

describe("mapProducerSendStart", () => {
  test("extracts actorId, topic, message count and keys", () => {
    const context = {
      client: { clientId: "producer-1" },
      options: {
        messages: [
          { topic: "orders", key: Buffer.from("k1") },
          { topic: "orders", key: undefined },
        ],
      },
    };

    const mapped = mapProducerSendStart(context);

    expect(mapped).toEqual({
      actorId: "producer-1",
      event: {
        type: "producer.send.start",
        payload: { topic: "orders", messageCount: 2, keys: ["k1", null] },
      },
    });
  });

  test("returns undefined when the context is not an object", () => {
    expect(mapProducerSendStart("not an object")).toBeUndefined();
  });

  test("returns undefined when the client is missing a clientId", () => {
    const context = { client: {}, options: { messages: [{ topic: "orders" }] } };

    expect(mapProducerSendStart(context)).toBeUndefined();
  });

  test("returns undefined when there are no messages", () => {
    const context = { client: { clientId: "producer-1" }, options: { messages: [] } };

    expect(mapProducerSendStart(context)).toBeUndefined();
  });

  test("treats a string key as-is", () => {
    const context = {
      client: { clientId: "producer-1" },
      options: { messages: [{ topic: "orders", key: "plain-key" }] },
    };

    const mapped = mapProducerSendStart(context);

    expect(mapped?.event.payload.keys).toEqual(["plain-key"]);
  });

  test("extracts scenarioRunId from the first message's metadata when present", () => {
    const context = {
      client: { clientId: "producer-1" },
      options: {
        messages: [{ topic: "orders", metadata: { scenarioRunId: "01ARZ3NDEKTSV4RRFFQ69G5FAV" } }],
      },
    };

    const mapped = mapProducerSendStart(context);

    expect(mapped?.event.payload.scenarioRunId).toBe("01ARZ3NDEKTSV4RRFFQ69G5FAV");
  });

  test("omits scenarioRunId when no message metadata is present", () => {
    const context = {
      client: { clientId: "producer-1" },
      options: { messages: [{ topic: "orders" }] },
    };

    const mapped = mapProducerSendStart(context);

    expect(mapped?.event.payload.scenarioRunId).toBeUndefined();
  });
});

describe("mapProducerSendEnd", () => {
  test("extracts actorId, topic and converted offsets", () => {
    const context = {
      client: { clientId: "producer-1" },
      result: {
        offsets: [
          { topic: "orders", partition: 0, offset: 10n },
          { topic: "orders", partition: 1, offset: 4n },
        ],
      },
    };

    const mapped = mapProducerSendEnd(context);

    expect(mapped).toEqual({
      actorId: "producer-1",
      event: {
        type: "producer.send.end",
        payload: {
          topic: "orders",
          offsets: [
            { partition: 0, offset: 10 },
            { partition: 1, offset: 4 },
          ],
        },
      },
    });
  });

  test("returns undefined when result is missing", () => {
    const context = { client: { clientId: "producer-1" } };

    expect(mapProducerSendEnd(context)).toBeUndefined();
  });

  test("returns undefined when offsets is empty", () => {
    const context = { client: { clientId: "producer-1" }, result: { offsets: [] } };

    expect(mapProducerSendEnd(context)).toBeUndefined();
  });

  test("returns undefined when the client is missing a clientId", () => {
    const context = {
      client: {},
      result: { offsets: [{ topic: "orders", partition: 0, offset: 1n }] },
    };

    expect(mapProducerSendEnd(context)).toBeUndefined();
  });
});

describe("mapConsumerGroupJoining", () => {
  test("extracts actorId, groupId and memberId for a joinGroup start", () => {
    const context = {
      operation: "joinGroup",
      client: { clientId: "consumer-a", groupId: "spike-group", memberId: null },
    };

    const mapped = mapConsumerGroupJoining(context);

    expect(mapped).toEqual({
      actorId: "consumer-a",
      event: {
        type: "consumer.group.joining",
        payload: { groupId: "spike-group", clientId: "consumer-a", memberId: null },
      },
    });
  });

  test("carries a non-null memberId for a re-join", () => {
    const context = {
      operation: "joinGroup",
      client: { clientId: "consumer-a", groupId: "spike-group", memberId: "consumer-a-1" },
    };

    const mapped = mapConsumerGroupJoining(context);

    expect(mapped?.event.payload.memberId).toBe("consumer-a-1");
  });

  test("returns undefined for a non-joinGroup operation", () => {
    const context = {
      operation: "syncGroup",
      client: { clientId: "consumer-a", groupId: "spike-group" },
    };

    expect(mapConsumerGroupJoining(context)).toBeUndefined();
  });

  test("returns undefined when groupId is missing", () => {
    const context = { operation: "joinGroup", client: { clientId: "consumer-a" } };

    expect(mapConsumerGroupJoining(context)).toBeUndefined();
  });
});

describe("mapConsumerGroupSyncing", () => {
  test("extracts actorId and groupId for a syncGroup start", () => {
    const context = {
      operation: "syncGroup",
      client: { clientId: "consumer-a", groupId: "spike-group" },
    };

    const mapped = mapConsumerGroupSyncing(context);

    expect(mapped).toEqual({
      actorId: "consumer-a",
      event: {
        type: "consumer.group.syncing",
        payload: { groupId: "spike-group", clientId: "consumer-a" },
      },
    });
  });

  test("returns undefined for a non-syncGroup operation", () => {
    const context = {
      operation: "joinGroup",
      client: { clientId: "consumer-a", groupId: "spike-group" },
    };

    expect(mapConsumerGroupSyncing(context)).toBeUndefined();
  });
});
