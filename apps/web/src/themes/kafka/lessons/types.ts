import type { KafkaWorld } from "../world-reducer";

/**
 * A step's effect on the running gateway. `params` for a "scenario" action can be a
 * plain object (static, e.g. produce-burst's keyStrategy) or a resolver function
 * reading the live KafkaWorld (e.g. picking which consumer's clientId to kill --
 * see action-resolver.ts's latestAliveMemberClientId, since consumer ids are
 * auto-numbered by the gateway's ConsumerRegistry and can't be hardcoded per lesson
 * run).
 */
export type LessonAction =
  | {
      kind: "scenario";
      scenarioId: string;
      params:
        | Record<string, unknown>
        | ((world: KafkaWorld) => Record<string, unknown> | undefined);
    }
  | { kind: "slow-motion"; enabled: boolean; factorMs?: number };

export interface LessonStep {
  id: string;
  title: string;
  /** Japanese prose explaining this step. Lesson content is learning material, not
   * code, so it's written in Japanese (see the plan: "レッスンは学習コンテンツのため日本語"). */
  body: string;
  /** What to look at after running this step (which panel, which visual change).
   * Omitted for steps that are pure narration (intro/summary). */
  observe?: string;
  /** Omitted for steps that only narrate/observe without driving the gateway. */
  action?: LessonAction;
}

export interface Lesson {
  id: string;
  title: string;
  summary: string;
  steps: LessonStep[];
}
