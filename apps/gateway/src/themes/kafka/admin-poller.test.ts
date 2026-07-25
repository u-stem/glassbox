import { describe, expect, test } from "bun:test";
import {
  type AdminPollerDeps,
  type AdminSnapshot,
  buildAdminGroup,
  buildAdminSnapshot,
  createAdminPoller,
  mapBrokerGroupState,
  snapshotsEqual,
} from "./admin-poller";

function makeFailingAdmin(): AdminPollerDeps {
  return {
    listTopics: async () => {
      throw new Error("broker unreachable");
    },
    metadata: async () => ({
      id: "test-cluster",
      brokers: new Map(),
      controllerId: 0,
      topics: new Map(),
      lastUpdate: 0,
    }),
    listOffsets: async () => [],
    listGroups: async () => new Map(),
    describeGroups: async () => new Map(),
    listConsumerGroupOffsets: async () => [],
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("buildAdminSnapshot", () => {
  test("sorts topics by name", () => {
    const snapshot = buildAdminSnapshot([
      { name: "b-topic", partitions: [] },
      { name: "a-topic", partitions: [] },
    ]);

    expect(snapshot.topics.map((t) => t.name)).toEqual(["a-topic", "b-topic"]);
  });

  test("sorts partitions by index", () => {
    const snapshot = buildAdminSnapshot([
      {
        name: "orders",
        partitions: [
          { partitionIndex: 1, earliestOffset: 0n, endOffset: 5n },
          { partitionIndex: 0, earliestOffset: 0n, endOffset: 3n },
        ],
      },
    ]);

    expect(snapshot.topics[0]?.partitions.map((p) => p.index)).toEqual([0, 1]);
  });

  test("converts bigint offsets to numbers", () => {
    const snapshot = buildAdminSnapshot([
      {
        name: "orders",
        partitions: [{ partitionIndex: 0, earliestOffset: 2n, endOffset: 20n }],
      },
    ]);

    expect(snapshot.topics[0]?.partitions[0]).toEqual({
      index: 0,
      earliestOffset: 2,
      endOffset: 20,
    });
  });

  test("returns an empty topics list for an empty input", () => {
    const snapshot = buildAdminSnapshot([]);

    expect(snapshot.topics).toEqual([]);
  });
});

describe("snapshotsEqual", () => {
  test("returns false when there is no previous snapshot", () => {
    const current = buildAdminSnapshot([{ name: "orders", partitions: [] }]);

    expect(snapshotsEqual(undefined, current)).toBe(false);
  });

  test("returns true for two snapshots with identical content", () => {
    const a = buildAdminSnapshot([
      { name: "orders", partitions: [{ partitionIndex: 0, earliestOffset: 0n, endOffset: 5n }] },
    ]);
    const b = buildAdminSnapshot([
      { name: "orders", partitions: [{ partitionIndex: 0, earliestOffset: 0n, endOffset: 5n }] },
    ]);

    expect(snapshotsEqual(a, b)).toBe(true);
  });

  test("returns false when an end offset differs", () => {
    const a = buildAdminSnapshot([
      { name: "orders", partitions: [{ partitionIndex: 0, earliestOffset: 0n, endOffset: 5n }] },
    ]);
    const b = buildAdminSnapshot([
      { name: "orders", partitions: [{ partitionIndex: 0, earliestOffset: 0n, endOffset: 6n }] },
    ]);

    expect(snapshotsEqual(a, b)).toBe(false);
  });

  test("returns false when a topic is added", () => {
    const a = buildAdminSnapshot([{ name: "orders", partitions: [] }]);
    const b = buildAdminSnapshot([
      { name: "orders", partitions: [] },
      { name: "shipments", partitions: [] },
    ]);

    expect(snapshotsEqual(a, b)).toBe(false);
  });
});

describe("mapBrokerGroupState", () => {
  test.each([
    ["PREPARING_REBALANCE", "PreparingRebalance"],
    ["COMPLETING_REBALANCE", "CompletingRebalance"],
    ["STABLE", "Stable"],
    ["EMPTY", "Empty"],
  ] as const)("maps broker state %s to %s", (raw, expected) => {
    expect(mapBrokerGroupState(raw)).toBe(expected);
  });

  test("returns undefined for DEAD (the group no longer exists)", () => {
    expect(mapBrokerGroupState("DEAD")).toBeUndefined();
  });

  test("returns undefined for an unrecognized raw state", () => {
    expect(mapBrokerGroupState("SOMETHING_NEW")).toBeUndefined();
  });
});

describe("buildAdminGroup", () => {
  test("returns undefined for a DEAD group (filtered out of the snapshot)", () => {
    const result = buildAdminGroup(
      { groupId: "g1", state: "DEAD", members: [], offsetsByTopic: [] },
      new Map(),
    );

    expect(result).toBeUndefined();
  });

  test("maps members and sorts them by memberId", () => {
    const result = buildAdminGroup(
      {
        groupId: "g1",
        state: "STABLE",
        members: [
          {
            memberId: "m-2",
            clientId: "consumer-b",
            assignments: [{ topic: "orders", partitions: [1] }],
          },
          {
            memberId: "m-1",
            clientId: "consumer-a",
            assignments: [{ topic: "orders", partitions: [0] }],
          },
        ],
        offsetsByTopic: [],
      },
      new Map(),
    );

    expect(result?.members.map((m) => m.memberId)).toEqual(["m-1", "m-2"]);
  });

  test("computes lag as end offset minus committed offset", () => {
    const result = buildAdminGroup(
      {
        groupId: "g1",
        state: "STABLE",
        members: [],
        offsetsByTopic: [
          { name: "orders", partitions: [{ partitionIndex: 0, committedOffset: 7n }] },
        ],
      },
      new Map([["orders", new Map([[0, 10n]])]]),
    );

    expect(result?.offsets).toEqual([{ topic: "orders", partition: 0, committed: 7, lag: 3 }]);
  });

  test("clamps lag to 0 when committed exceeds the known end offset (non-atomic RPC race, ADR-0003)", () => {
    const result = buildAdminGroup(
      {
        groupId: "g1",
        state: "STABLE",
        members: [],
        offsetsByTopic: [
          { name: "orders", partitions: [{ partitionIndex: 0, committedOffset: 12n }] },
        ],
      },
      new Map([["orders", new Map([[0, 10n]])]]),
    );

    expect(result?.offsets[0]?.lag).toBe(0);
  });

  test("omits lag (rather than defaulting to 0) when the end offset is unknown this tick", () => {
    const result = buildAdminGroup(
      {
        groupId: "g1",
        state: "STABLE",
        members: [],
        offsetsByTopic: [
          { name: "orders", partitions: [{ partitionIndex: 0, committedOffset: 5n }] },
        ],
      },
      new Map(),
    );

    expect(result?.offsets[0]).toEqual({ topic: "orders", partition: 0, committed: 5 });
    expect(result?.offsets[0]).not.toHaveProperty("lag");
  });
});

describe("createAdminPoller resiliency", () => {
  test("keeps ticking even when onError itself throws", async () => {
    const admin = makeFailingAdmin();
    let errorCount = 0;
    const poller = createAdminPoller(admin, {
      intervalMs: 5,
      onSnapshot: () => {},
      onError: () => {
        errorCount += 1;
        throw new Error("onError handler is buggy");
      },
    });

    poller.start();
    await sleep(60);
    poller.stop();

    // With a 5ms interval and 60ms elapsed, several ticks must have run; if a throwing
    // onError silently killed the poller, this would be 1.
    expect(errorCount).toBeGreaterThan(1);
  });

  test("still publishes topic offsets when listConsumerGroupOffsets fails (partial failure isolation)", async () => {
    const admin: AdminPollerDeps = {
      listTopics: async () => ["orders"],
      metadata: async () => ({
        id: "test-cluster",
        brokers: new Map(),
        controllerId: 0,
        topics: new Map([
          ["orders", { id: "t1", partitions: [], partitionsCount: 1, lastUpdate: 0 }],
        ]),
        lastUpdate: 0,
      }),
      listOffsets: async (options) => [
        {
          name: "orders",
          partitions:
            options.topics[0]?.partitions.map((p) => ({
              partitionIndex: p.partitionIndex,
              timestamp: p.timestamp,
              offset: 5n,
              leaderEpoch: 0,
            })) ?? [],
        },
      ],
      listGroups: async () =>
        new Map([
          [
            "g1",
            { id: "g1", state: "STABLE" as const, groupType: "classic", protocolType: "consumer" },
          ],
        ]),
      describeGroups: async () =>
        new Map([
          [
            "g1",
            {
              id: "g1",
              state: "STABLE" as const,
              protocol: "consumer",
              protocolType: "consumer",
              members: new Map(),
              authorizedOperations: 0,
            },
          ],
        ]),
      listConsumerGroupOffsets: async () => {
        throw new Error("listConsumerGroupOffsets unavailable");
      },
    };

    const errors: unknown[] = [];
    let snapshot: AdminSnapshot | undefined;

    const poller = createAdminPoller(admin, {
      intervalMs: 1000,
      onSnapshot: (s) => {
        snapshot = s;
      },
      onError: (error) => {
        errors.push(error);
      },
    });

    poller.start();
    await sleep(50);
    poller.stop();

    expect(snapshot?.topics).toEqual([
      { name: "orders", partitions: [{ index: 0, earliestOffset: 5, endOffset: 5 }] },
    ]);
    expect(snapshot?.groups?.[0]?.groupId).toBe("g1");
    expect(errors.length).toBeGreaterThan(0);
  });

  test("carries over the previous tick's group membership when describeGroups fails, rather than making it vanish (Problem B)", async () => {
    let describeGroupsCallCount = 0;
    let endOffset = 5n;

    const admin: AdminPollerDeps = {
      listTopics: async () => ["orders"],
      metadata: async () => ({
        id: "test-cluster",
        brokers: new Map(),
        controllerId: 0,
        topics: new Map([
          ["orders", { id: "t1", partitions: [], partitionsCount: 1, lastUpdate: 0 }],
        ]),
        lastUpdate: 0,
      }),
      listOffsets: async (options) => [
        {
          name: "orders",
          partitions:
            options.topics[0]?.partitions.map((p) => ({
              partitionIndex: p.partitionIndex,
              timestamp: p.timestamp,
              offset: endOffset,
              leaderEpoch: 0,
            })) ?? [],
        },
      ],
      listGroups: async () =>
        new Map([
          [
            "g1",
            { id: "g1", state: "STABLE" as const, groupType: "classic", protocolType: "consumer" },
          ],
        ]),
      describeGroups: async () => {
        describeGroupsCallCount += 1;
        if (describeGroupsCallCount === 1) {
          return new Map([
            [
              "g1",
              {
                id: "g1",
                state: "STABLE" as const,
                protocol: "consumer",
                protocolType: "consumer",
                members: new Map([
                  [
                    "m1",
                    {
                      id: "m1",
                      groupInstanceId: null,
                      clientId: "consumer-1",
                      clientHost: "localhost",
                      assignments: new Map(),
                    },
                  ],
                ]),
                authorizedOperations: 0,
              },
            ],
          ]);
        }
        throw new Error("describeGroups transient failure");
      },
      listConsumerGroupOffsets: async () => [
        { groupId: "g1", topics: [{ name: "orders", partitions: [] }] },
      ],
    };

    const snapshots: AdminSnapshot[] = [];
    const errors: unknown[] = [];

    const poller = createAdminPoller(admin, {
      intervalMs: 20,
      onSnapshot: (s) => {
        snapshots.push(s);
      },
      onError: (error) => {
        errors.push(error);
      },
    });

    poller.start();
    await sleep(15);
    endOffset = 9n; // bumps the topic offset so the second tick still counts as "changed"
    await sleep(40);
    poller.stop();

    expect(errors.length).toBeGreaterThan(0);

    const firstSnapshot = snapshots[0];
    expect(firstSnapshot?.groups?.[0]?.members).toEqual([
      { memberId: "m1", clientId: "consumer-1", assignments: [] },
    ]);

    const laterSnapshot = snapshots.at(-1);
    // Topic offsets keep updating even while describeGroups is failing...
    expect(laterSnapshot?.topics[0]?.partitions[0]?.endOffset).toBe(9);
    // ...and the group's membership is carried over from the last successful tick,
    // not dropped.
    expect(laterSnapshot?.groups?.[0]?.members).toEqual([
      { memberId: "m1", clientId: "consumer-1", assignments: [] },
    ]);
  });
});

function makeSucceedingAdmin(): AdminPollerDeps {
  return {
    listTopics: async () => [],
    metadata: async () => ({
      id: "test-cluster",
      brokers: new Map(),
      controllerId: 0,
      topics: new Map(),
      lastUpdate: 0,
    }),
    listOffsets: async () => [],
    listGroups: async () => new Map(),
    describeGroups: async () => new Map(),
    listConsumerGroupOffsets: async () => [],
  };
}

describe("createAdminPoller kafkaStatus", () => {
  test("is unknown before the first tick runs", () => {
    const poller = createAdminPoller(makeSucceedingAdmin(), {
      intervalMs: 1000,
      onSnapshot: () => {},
    });

    expect(poller.kafkaStatus()).toBe("unknown");
  });

  test("becomes connected after a successful tick", async () => {
    const poller = createAdminPoller(makeSucceedingAdmin(), {
      intervalMs: 1000,
      onSnapshot: () => {},
    });

    poller.start();
    await sleep(30);
    poller.stop();

    expect(poller.kafkaStatus()).toBe("connected");
  });

  test("becomes unreachable after a failing tick, then recovers to connected on the next success", async () => {
    const admin = makeFailingAdmin();
    const poller = createAdminPoller(admin, {
      intervalMs: 20,
      onSnapshot: () => {},
      onError: () => {},
    });

    poller.start();
    await sleep(30);
    expect(poller.kafkaStatus()).toBe("unreachable");

    admin.listTopics = async () => [];
    await sleep(30);
    poller.stop();

    expect(poller.kafkaStatus()).toBe("connected");
  });

  test("stays connected on a tick where only group RPCs fail (topic RPCs still succeed)", async () => {
    const admin: AdminPollerDeps = {
      listTopics: async () => ["orders"],
      metadata: async () => ({
        id: "test-cluster",
        brokers: new Map(),
        controllerId: 0,
        topics: new Map([
          ["orders", { id: "t1", partitions: [], partitionsCount: 1, lastUpdate: 0 }],
        ]),
        lastUpdate: 0,
      }),
      listOffsets: async (options) => [
        {
          name: "orders",
          partitions:
            options.topics[0]?.partitions.map((p) => ({
              partitionIndex: p.partitionIndex,
              timestamp: p.timestamp,
              offset: 5n,
              leaderEpoch: 0,
            })) ?? [],
        },
      ],
      listGroups: async () => {
        throw new Error("listGroups unavailable");
      },
      describeGroups: async () => new Map(),
      listConsumerGroupOffsets: async () => [],
    };

    const poller = createAdminPoller(admin, {
      intervalMs: 20,
      onSnapshot: () => {},
      onError: () => {},
    });

    poller.start();
    await sleep(30);
    poller.stop();

    expect(poller.kafkaStatus()).toBe("connected");
  });
});
