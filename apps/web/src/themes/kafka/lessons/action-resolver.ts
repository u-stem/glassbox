import type { KafkaWorld } from "../world-reducer";
import type { LessonAction } from "./types";

export interface ResolvedScenarioAction {
  kind: "scenario";
  scenarioId: string;
  params: Record<string, unknown>;
}

export interface ResolvedSlowMotionAction {
  kind: "slow-motion";
  enabled: boolean;
  factorMs: number | undefined;
}

export type ResolvedLessonAction = ResolvedScenarioAction | ResolvedSlowMotionAction;

/**
 * Resolves a LessonStep's action against the current world. A "scenario" action
 * whose params is a resolver function returns undefined when it isn't runnable yet
 * (e.g. "kill the most recently joined consumer" before any consumer has joined),
 * so the caller (LessonPanel) can disable the step's run button instead of
 * dispatching a request with missing/garbage params.
 */
export function resolveLessonAction(
  action: LessonAction,
  world: KafkaWorld,
): ResolvedLessonAction | undefined {
  if (action.kind === "slow-motion") {
    return { kind: "slow-motion", enabled: action.enabled, factorMs: action.factorMs };
  }
  const params = typeof action.params === "function" ? action.params(world) : action.params;
  if (params === undefined) {
    return undefined;
  }
  return { kind: "scenario", scenarioId: action.scenarioId, params };
}

/**
 * Extracts the trailing numeric index from a clientId (e.g. "consumer-10" -> 10),
 * for numeric (not lexicographic) comparison -- clientIds are auto-numbered by the
 * gateway's ConsumerRegistry ("consumer-1", "consumer-2", ..., "consumer-10", ...)
 * and a plain string comparison would put "consumer-10" before "consumer-9" (bug
 * caught in Phase 4 review). Returns undefined for a clientId with no trailing
 * digits, e.g. an id format this theme doesn't currently produce; callers fall back
 * to lexicographic comparison in that case.
 */
function trailingIndex(clientId: string): number | undefined {
  const match = /(\d+)$/.exec(clientId);
  const digits = match?.[1];
  if (digits === undefined) {
    return undefined;
  }
  return Number.parseInt(digits, 10);
}

/**
 * Picks the highest-numbered still-alive member's clientId in a group. Consumer ids
 * are auto-numbered "consumer-1", "consumer-2", ... by the gateway's
 * ConsumerRegistry across its whole process lifetime (not reset per lesson run), so
 * lesson B's "kill the 2nd consumer" step resolves its target dynamically instead of
 * hardcoding a specific id. Compares clientIds by their trailing numeric index (see
 * trailingIndex's doc), not lexicographically, so "consumer-10" correctly outranks
 * "consumer-9".
 */
export function latestAliveMemberClientId(world: KafkaWorld, groupId: string): string | undefined {
  const members = world.groups[groupId]?.members.filter((m) => m.liveness === "alive") ?? [];
  if (members.length === 0) {
    return undefined;
  }
  return members.reduce((latest, member) => {
    const memberIndex = trailingIndex(member.clientId);
    const latestIndex = trailingIndex(latest.clientId);
    // Numeric comparison when both ids parse; otherwise fall back to lexicographic
    // comparison (only relevant for a clientId format this theme doesn't currently
    // produce, since every real consumerId here does have a trailing index).
    const memberIsLater =
      memberIndex !== undefined && latestIndex !== undefined
        ? memberIndex > latestIndex
        : member.clientId > latest.clientId;
    return memberIsLater ? member : latest;
  }).clientId;
}
