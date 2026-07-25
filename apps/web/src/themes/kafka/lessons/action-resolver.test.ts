import { describe, expect, test } from "bun:test";
import { createInitialWorld, type KafkaWorld } from "../world-reducer";
import { latestAliveMemberClientId, resolveLessonAction } from "./action-resolver";

function worldWithMembers(
  groupId: string,
  members: { clientId: string; liveness: "alive" | "lost" }[],
): KafkaWorld {
  const world = createInitialWorld();
  return {
    ...world,
    groups: {
      [groupId]: {
        groupId,
        state: "Stable",
        offsets: [],
        members: members.map((m) => ({ ...m, memberId: m.clientId, assignments: [] })),
      },
    },
  };
}

describe("resolveLessonAction", () => {
  test("resolves a slow-motion action verbatim", () => {
    const resolved = resolveLessonAction(
      { kind: "slow-motion", enabled: true, factorMs: 3000 },
      createInitialWorld(),
    );

    expect(resolved).toEqual({ kind: "slow-motion", enabled: true, factorMs: 3000 });
  });

  test("resolves a scenario action with static params", () => {
    const resolved = resolveLessonAction(
      { kind: "scenario", scenarioId: "produce-burst", params: { count: 10 } },
      createInitialWorld(),
    );

    expect(resolved).toEqual({
      kind: "scenario",
      scenarioId: "produce-burst",
      params: { count: 10 },
    });
  });

  test("resolves a scenario action with a params resolver function", () => {
    const world = worldWithMembers("g1", [{ clientId: "consumer-1", liveness: "alive" }]);

    const resolved = resolveLessonAction(
      {
        kind: "scenario",
        scenarioId: "remove-consumer",
        params: (w) => {
          const clientId = latestAliveMemberClientId(w, "g1");
          return clientId === undefined ? undefined : { consumerId: clientId, mode: "kill" };
        },
      },
      world,
    );

    expect(resolved).toEqual({
      kind: "scenario",
      scenarioId: "remove-consumer",
      params: { consumerId: "consumer-1", mode: "kill" },
    });
  });

  test("returns undefined when the resolver reports the action isn't runnable yet", () => {
    const resolved = resolveLessonAction(
      {
        kind: "scenario",
        scenarioId: "remove-consumer",
        params: (w) => {
          const clientId = latestAliveMemberClientId(w, "g1");
          return clientId === undefined ? undefined : { consumerId: clientId };
        },
      },
      createInitialWorld(),
    );

    expect(resolved).toBeUndefined();
  });
});

describe("latestAliveMemberClientId", () => {
  test("returns undefined when the group has no alive members", () => {
    expect(latestAliveMemberClientId(createInitialWorld(), "g1")).toBeUndefined();
  });

  test("ignores lost members", () => {
    const world = worldWithMembers("g1", [
      { clientId: "consumer-1", liveness: "alive" },
      { clientId: "consumer-2", liveness: "lost" },
    ]);

    expect(latestAliveMemberClientId(world, "g1")).toBe("consumer-1");
  });

  test("picks the highest-numbered alive member", () => {
    const world = worldWithMembers("g1", [
      { clientId: "consumer-1", liveness: "alive" },
      { clientId: "consumer-2", liveness: "alive" },
    ]);

    expect(latestAliveMemberClientId(world, "g1")).toBe("consumer-2");
  });

  test("compares clientIds numerically, not lexicographically (consumer-10 outranks consumer-9)", () => {
    const world = worldWithMembers("g1", [
      { clientId: "consumer-9", liveness: "alive" },
      { clientId: "consumer-10", liveness: "alive" },
    ]);

    expect(latestAliveMemberClientId(world, "g1")).toBe("consumer-10");
  });
});
