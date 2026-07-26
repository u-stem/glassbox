"use client";

import { useState } from "react";
import { useKafkaStore } from "@/lib/store";
import { type ResolvedLessonAction, resolveLessonAction } from "./action-resolver";
import { lessons } from "./lessons";
import { clampStepIndex } from "./navigation";
import type { Lesson } from "./types";

async function runResolvedAction(
  gatewayUrl: string,
  resolved: ResolvedLessonAction,
): Promise<void> {
  const response =
    resolved.kind === "slow-motion"
      ? await fetch(`${gatewayUrl}/api/themes/kafka/slow-motion`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled: resolved.enabled, factorMs: resolved.factorMs ?? 3000 }),
        })
      : await fetch(`${gatewayUrl}/api/themes/kafka/scenarios/${resolved.scenarioId}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(resolved.params),
        });
  if (!response.ok) {
    throw new Error(`gateway responded with ${response.status}`);
  }
}

/**
 * Contents of the lesson pane of the theme page's side drawer. The drawer shell
 * (the `<aside>`, its heading and close button) belongs to the page, which shares
 * it with the glossary pane -- this component renders only the lesson body.
 *
 * Which lesson is open and how far through it the reader is are owned by the page,
 * not here: the page swaps this pane out for the glossary and back, and lifting
 * that state is what lets it do so by conditional rendering (unmounting this
 * component) without losing the reader's place. `isRunning` stays local because it
 * only describes an in-flight request this component itself issued.
 */
export function LessonPanel({
  gatewayUrl,
  onError,
  onSlowMotionChanged,
  activeLesson,
  stepIndex,
  onSelectLesson,
  onStepIndexChange,
  onExitLesson,
}: {
  gatewayUrl: string;
  onError: (message: string | undefined) => void;
  /** Notified after a "slow-motion" step successfully runs, so the page's own
   * Slow-motion ON/OFF button stays in sync with a toggle a lesson step made (the
   * gateway's own state is the source of truth either way; this just keeps the two
   * independent UI surfaces from disagreeing about it). */
  onSlowMotionChanged?: (enabled: boolean) => void;
  activeLesson: Lesson | undefined;
  stepIndex: number;
  onSelectLesson: (lesson: Lesson) => void;
  onStepIndexChange: (stepIndex: number) => void;
  onExitLesson: () => void;
}) {
  const world = useKafkaStore((state) => state.world);
  const [isRunning, setIsRunning] = useState(false);

  async function runStepAction(): Promise<void> {
    if (activeLesson === undefined) {
      return;
    }
    const step = activeLesson.steps[clampStepIndex(stepIndex, activeLesson.steps.length)];
    if (step?.action === undefined) {
      return;
    }
    const resolved = resolveLessonAction(step.action, world);
    if (resolved === undefined) {
      return;
    }
    setIsRunning(true);
    onError(undefined);
    try {
      await runResolvedAction(gatewayUrl, resolved);
      if (resolved.kind === "slow-motion") {
        onSlowMotionChanged?.(resolved.enabled);
      }
    } catch (error) {
      onError(error instanceof Error ? error.message : String(error));
    } finally {
      setIsRunning(false);
    }
  }

  if (activeLesson === undefined) {
    return (
      <ul className="flex flex-col gap-2">
        {lessons.map((lesson) => (
          <li key={lesson.id}>
            <button
              type="button"
              onClick={() => onSelectLesson(lesson)}
              className="w-full rounded border border-(--text-muted) p-2 text-left text-sm hover:border-(--text-primary)"
            >
              <div className="font-medium">{lesson.title}</div>
              <div className="text-xs text-(--text-secondary)">{lesson.summary}</div>
            </button>
          </li>
        ))}
      </ul>
    );
  }

  const totalSteps = activeLesson.steps.length;
  const clampedIndex = clampStepIndex(stepIndex, totalSteps);
  const step = activeLesson.steps[clampedIndex];
  const resolvedAction =
    step?.action !== undefined ? resolveLessonAction(step.action, world) : undefined;

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between text-xs text-(--text-secondary)">
        <span>{activeLesson.title}</span>
        <span>
          {clampedIndex + 1} / {totalSteps}
        </span>
      </div>

      {step && (
        <div className="flex flex-col gap-2">
          <h3 className="text-sm font-semibold">{step.title}</h3>
          <p className="whitespace-pre-wrap text-sm text-(--text-secondary)">{step.body}</p>
          {step.observe && (
            <p className="rounded border border-(--status-good)/60 bg-(--status-good)/12 p-2 text-xs">
              観察ポイント: {step.observe}
            </p>
          )}
          {step.action && (
            <button
              type="button"
              onClick={() => {
                void runStepAction();
              }}
              disabled={isRunning || resolvedAction === undefined}
              className="rounded bg-(--accent) px-3 py-1.5 text-sm text-(--on-accent) hover:opacity-90 disabled:opacity-50"
            >
              このステップを実行
            </button>
          )}
        </div>
      )}

      <div className="flex items-center justify-between">
        <button
          type="button"
          onClick={() => onStepIndexChange(clampStepIndex(clampedIndex - 1, totalSteps))}
          disabled={clampedIndex === 0}
          className="rounded border border-(--text-muted) px-2 py-1 text-sm enabled:hover:border-(--text-primary) disabled:opacity-50"
        >
          戻る
        </button>
        <button
          type="button"
          onClick={onExitLesson}
          className="text-xs text-(--text-secondary) hover:text-(--text-primary)"
        >
          レッスンを終了
        </button>
        <button
          type="button"
          onClick={() => onStepIndexChange(clampStepIndex(clampedIndex + 1, totalSteps))}
          disabled={clampedIndex === totalSteps - 1}
          className="rounded border border-(--text-muted) px-2 py-1 text-sm enabled:hover:border-(--text-primary) disabled:opacity-50"
        >
          次へ
        </button>
      </div>
    </div>
  );
}
