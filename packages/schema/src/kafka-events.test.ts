import { describe, expect, test } from "bun:test";
import { bigintOffsetToNumber, kafkaEventSchema } from "./kafka-events";

describe("bigintOffsetToNumber", () => {
  test("converts a small bigint to a number", () => {
    expect(bigintOffsetToNumber(42n)).toBe(42);
  });

  test("converts zero", () => {
    expect(bigintOffsetToNumber(0n)).toBe(0);
  });

  test("throws when the bigint exceeds Number.MAX_SAFE_INTEGER", () => {
    const overflow = BigInt(Number.MAX_SAFE_INTEGER) + 1n;

    expect(() => bigintOffsetToNumber(overflow)).toThrow(RangeError);
  });
});

describe("kafkaEventSchema", () => {
  test("accepts a producer.send.start event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "producer.send.start",
      payload: { topic: "orders", messageCount: 3, keys: ["a", null, "b"] },
    });

    expect(result.success).toBe(true);
  });

  test("rejects producer.send.start with a negative messageCount", () => {
    const result = kafkaEventSchema.safeParse({
      type: "producer.send.start",
      payload: { topic: "orders", messageCount: -1, keys: [] },
    });

    expect(result.success).toBe(false);
  });

  test("accepts a producer.send.start event with a scenarioRunId", () => {
    const result = kafkaEventSchema.safeParse({
      type: "producer.send.start",
      payload: {
        topic: "orders",
        messageCount: 1,
        keys: [null],
        scenarioRunId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a producer.send.end event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "producer.send.end",
      payload: {
        topic: "orders",
        offsets: [{ partition: 0, offset: 10 }],
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a producer.send.end event with a scenarioRunId", () => {
    const result = kafkaEventSchema.safeParse({
      type: "producer.send.end",
      payload: {
        topic: "orders",
        offsets: [{ partition: 0, offset: 10 }],
        scenarioRunId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts an admin.snapshot event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "admin.snapshot",
      payload: {
        topics: [
          {
            name: "orders",
            partitions: [{ index: 0, earliestOffset: 0, endOffset: 12 }],
          },
        ],
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a scenario.started event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "scenario.started",
      payload: { scenarioId: "produce-burst", params: { count: 10 } },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a scenario.started event with a scenarioRunId", () => {
    const result = kafkaEventSchema.safeParse({
      type: "scenario.started",
      payload: {
        scenarioId: "produce-burst",
        params: { count: 10 },
        scenarioRunId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a scenario.finished event without an error", () => {
    const result = kafkaEventSchema.safeParse({
      type: "scenario.finished",
      payload: { scenarioId: "produce-burst", params: { count: 10 } },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a scenario.finished event with an error", () => {
    const result = kafkaEventSchema.safeParse({
      type: "scenario.finished",
      payload: { scenarioId: "produce-burst", params: { count: 10 }, error: "boom" },
    });

    expect(result.success).toBe(true);
  });

  test("accepts an actor.created event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "actor.created",
      payload: { actorKind: "producer", actorId: "producer-1" },
    });

    expect(result.success).toBe(true);
  });

  test("accepts an actor.destroyed event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "actor.destroyed",
      payload: { actorKind: "producer", actorId: "producer-1" },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a control.reset event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "control.reset",
      payload: { reason: "gap" },
    });

    expect(result.success).toBe(true);
  });

  test("rejects a control.reset event with an unknown reason", () => {
    const result = kafkaEventSchema.safeParse({
      type: "control.reset",
      payload: { reason: "unknown-reason" },
    });

    expect(result.success).toBe(false);
  });

  test("rejects an unknown event type", () => {
    const result = kafkaEventSchema.safeParse({
      type: "unknown.event",
      payload: {},
    });

    expect(result.success).toBe(false);
  });

  test("accepts a consumer.group.joining event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.group.joining",
      payload: { groupId: "spike-group", clientId: "consumer-a", memberId: null },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a consumer.group.joined event with assignments", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.group.joined",
      payload: {
        groupId: "spike-group",
        clientId: "consumer-a",
        memberId: "consumer-a-1",
        generationId: 5,
        isLeader: true,
        assignments: [{ topic: "orders", partitions: [0, 1] }],
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a consumer.group.joined event without generationId/isLeader (unknown, not defaulted)", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.group.joined",
      payload: {
        groupId: "spike-group",
        clientId: "consumer-a",
        memberId: "consumer-a-1",
        assignments: [],
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a consumer.group.left event with a null memberId", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.group.left",
      payload: { groupId: "spike-group", clientId: "consumer-a", memberId: null },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a consumer.connection.lost event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.connection.lost",
      payload: { groupId: "spike-group", clientId: "consumer-a", reason: "kill" },
    });

    expect(result.success).toBe(true);
  });

  test("rejects a consumer.connection.lost event with an unknown reason", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.connection.lost",
      payload: { groupId: "spike-group", clientId: "consumer-a", reason: "network-blip" },
    });

    expect(result.success).toBe(false);
  });

  test("accepts a consumer.group.rebalancing event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.group.rebalancing",
      payload: { groupId: "spike-group", clientId: "consumer-a" },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a consumer.group.syncing event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.group.syncing",
      payload: { groupId: "spike-group", clientId: "consumer-a" },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a consumer.heartbeat.ok event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.heartbeat.ok",
      payload: {
        groupId: "spike-group",
        clientId: "consumer-a",
        memberId: "consumer-a-1",
        generationId: 5,
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a consumer.heartbeat.failed event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.heartbeat.failed",
      payload: {
        groupId: "spike-group",
        clientId: "consumer-a",
        error: "REBALANCE_IN_PROGRESS",
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a consumer.message event with a null key", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.message",
      payload: {
        groupId: "spike-group",
        clientId: "consumer-a",
        topic: "orders",
        partition: 0,
        offset: 12,
        key: null,
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a consumer.commit event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "consumer.commit",
      payload: {
        groupId: "spike-group",
        clientId: "consumer-a",
        offsets: [{ topic: "orders", partition: 0, offset: 13 }],
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts a group.state.changed event", () => {
    const result = kafkaEventSchema.safeParse({
      type: "group.state.changed",
      payload: {
        groupId: "spike-group",
        from: "Empty",
        to: "PreparingRebalance",
        generationId: 5,
        edge: "member-joining",
      },
    });

    expect(result.success).toBe(true);
  });

  test("rejects a group.state.changed event with an invalid state name", () => {
    const result = kafkaEventSchema.safeParse({
      type: "group.state.changed",
      payload: {
        groupId: "spike-group",
        from: "Empty",
        to: "Bogus",
        edge: "member-joining",
      },
    });

    expect(result.success).toBe(false);
  });

  test("accepts an admin.snapshot event with consumer groups and clamped lag", () => {
    const result = kafkaEventSchema.safeParse({
      type: "admin.snapshot",
      payload: {
        topics: [],
        groups: [
          {
            groupId: "spike-group",
            state: "Stable",
            members: [
              {
                memberId: "consumer-a-1",
                clientId: "consumer-a",
                assignments: [{ topic: "orders", partitions: [0] }],
              },
            ],
            offsets: [{ topic: "orders", partition: 0, committed: 10, lag: 0 }],
          },
        ],
      },
    });

    expect(result.success).toBe(true);
  });

  test("accepts an admin.snapshot group offset with an omitted lag (unknown end offset)", () => {
    const result = kafkaEventSchema.safeParse({
      type: "admin.snapshot",
      payload: {
        topics: [],
        groups: [
          {
            groupId: "spike-group",
            state: "Stable",
            members: [],
            offsets: [{ topic: "orders", partition: 0, committed: 10 }],
          },
        ],
      },
    });

    expect(result.success).toBe(true);
  });

  test("rejects an admin.snapshot group offset with a negative lag", () => {
    const result = kafkaEventSchema.safeParse({
      type: "admin.snapshot",
      payload: {
        topics: [],
        groups: [
          {
            groupId: "spike-group",
            state: "Stable",
            members: [],
            offsets: [{ topic: "orders", partition: 0, committed: 10, lag: -1 }],
          },
        ],
      },
    });

    expect(result.success).toBe(false);
  });

  test("accepts an admin.snapshot event without groups (backward compatible)", () => {
    const result = kafkaEventSchema.safeParse({
      type: "admin.snapshot",
      payload: {
        topics: [{ name: "orders", partitions: [{ index: 0, earliestOffset: 0, endOffset: 12 }] }],
      },
    });

    expect(result.success).toBe(true);
  });
});
