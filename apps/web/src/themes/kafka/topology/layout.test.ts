import { describe, expect, test } from "bun:test";
import type { WorldGroup } from "../world-reducer";
import { computeTopologyLayout } from "./layout";

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
