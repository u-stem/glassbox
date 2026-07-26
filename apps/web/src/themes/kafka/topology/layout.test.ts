import { describe, expect, test } from "bun:test";
import type { WorldGroup } from "../world-reducer";
import { computeTopologyLayout, NODE_WIDTH, TOPOLOGY_CONTENT_WIDTH } from "./layout";

describe("computeTopologyLayout", () => {
  test("places producer and topic with no consumers when there are no groups", () => {
    const layout = computeTopologyLayout("orders", []);

    expect(layout.consumers).toEqual([]);
    expect(layout.edges).toEqual([]);
    expect(layout.producer.x).toBeLessThan(layout.topic.x);
  });

  test("creates one consumer node per member across all groups", () => {
    const groups: WorldGroup[] = [
      {
        groupId: "g1",
        state: "Stable",
        members: [
          { memberId: "m1", clientId: "consumer-a", assignments: [], liveness: "alive" },
          { memberId: "m2", clientId: "consumer-b", assignments: [], liveness: "lost" },
        ],
        offsets: [],
      },
    ];

    const layout = computeTopologyLayout("orders", groups);

    expect(layout.consumers).toHaveLength(2);
    expect(layout.consumers[1]?.liveness).toBe("lost");
    expect(layout.consumers[0]?.y).not.toBe(layout.consumers[1]?.y);
  });

  test("creates one edge per (consumer, partition) pair for assignments matching the topic", () => {
    const groups: WorldGroup[] = [
      {
        groupId: "g1",
        state: "Stable",
        members: [
          {
            memberId: "m1",
            clientId: "consumer-a",
            assignments: [{ topic: "orders", partitions: [0, 1] }],
            liveness: "alive",
          },
        ],
        offsets: [],
      },
    ];

    const layout = computeTopologyLayout("orders", groups);

    expect(layout.edges).toHaveLength(2);
    expect(layout.edges.map((e) => e.partition).sort()).toEqual([0, 1]);
    expect(layout.edges.every((e) => e.consumerId === "consumer-a")).toBe(true);
  });

  test("ignores assignments for a different topic", () => {
    const groups: WorldGroup[] = [
      {
        groupId: "g1",
        state: "Stable",
        members: [
          {
            memberId: "m1",
            clientId: "consumer-a",
            assignments: [{ topic: "other-topic", partitions: [0] }],
            liveness: "alive",
          },
        ],
        offsets: [],
      },
    ];

    const layout = computeTopologyLayout("orders", groups);

    expect(layout.edges).toEqual([]);
  });
});

describe("TOPOLOGY_CONTENT_WIDTH", () => {
  /** Derived from a real layout rather than compared against the same constants
   * that build it, so moving a column actually fails this instead of moving the
   * expectation with it. Without this the canvas would silently clip the rightmost
   * nodes once the side drawer narrows its column (see layout.ts's doc). */
  test("covers the rightmost node a layout can produce", () => {
    const groups: WorldGroup[] = [
      {
        groupId: "g1",
        state: "Stable",
        members: [
          {
            memberId: "m1",
            clientId: "consumer-a",
            assignments: [{ topic: "orders", partitions: [0] }],
            liveness: "alive",
          },
        ],
        offsets: [],
      },
    ];

    const layout = computeTopologyLayout("orders", groups);
    const rightmost = Math.max(
      layout.producer.x,
      layout.topic.x,
      ...layout.consumers.map((consumer) => consumer.x),
    );

    expect(TOPOLOGY_CONTENT_WIDTH).toBeGreaterThanOrEqual(rightmost + NODE_WIDTH);
  });
});
