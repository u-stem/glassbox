import { describe, expect, test } from "bun:test";
import { buildPartitionRows, computeLagBand, computeWindow } from "./layout";

describe("computeWindow", () => {
  test("defaults to [0, windowSize] when there are no end offsets yet", () => {
    expect(computeWindow([], 50)).toEqual({ start: 0, end: 50 });
  });

  test("stays at [0, windowSize] while the max end offset is below the window size", () => {
    expect(computeWindow([0, 12, 30], 50)).toEqual({ start: 0, end: 50 });
  });

  test("follows the max end offset once it exceeds the window size", () => {
    expect(computeWindow([10, 120, 40], 50)).toEqual({ start: 70, end: 120 });
  });
});

describe("computeLagBand", () => {
  test("returns undefined when committed has already caught up to end (no lag)", () => {
    expect(computeLagBand(100, 100, 0)).toBeUndefined();
  });

  test("spans from committed to end when committed is inside the window", () => {
    expect(computeLagBand(70, 100, 50)).toEqual({ from: 70, to: 100 });
  });

  test("clamps the band's start to the window start when committed is further back", () => {
    expect(computeLagBand(10, 100, 50)).toEqual({ from: 50, to: 100 });
  });
});

describe("buildPartitionRows", () => {
  const topic = {
    name: "orders",
    partitions: [
      { index: 0, earliestOffset: 0, endOffset: 10 },
      { index: 1, earliestOffset: 0, endOffset: 20 },
    ],
  };

  test("projects each partition's end offset with no committed markers when there are no groups", () => {
    expect(buildPartitionRows(topic, [])).toEqual([
      { partition: 0, end: 10, committedByGroup: [] },
      { partition: 1, end: 20, committedByGroup: [] },
    ]);
  });

  test("attaches a committed marker for a group that has an offset on that partition", () => {
    const group = {
      groupId: "g1",
      state: "Stable" as const,
      members: [],
      offsets: [{ topic: "orders", partition: 0, committed: 8, lag: 2 }],
    };

    const rows = buildPartitionRows(topic, [group]);

    expect(rows[0]?.committedByGroup).toEqual([{ groupId: "g1", committed: 8, lag: 2 }]);
  });

  test("omits a committed marker for a group with no offset recorded on that partition", () => {
    const group = {
      groupId: "g1",
      state: "Stable" as const,
      members: [],
      offsets: [{ topic: "orders", partition: 1, committed: 20, lag: 0 }],
    };

    const rows = buildPartitionRows(topic, [group]);

    expect(rows[0]?.committedByGroup).toEqual([]);
  });
});
