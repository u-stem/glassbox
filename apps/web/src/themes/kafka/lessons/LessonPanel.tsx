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
 * Guided-lesson side panel (Phase 4). Deliberately modeless: it's a fixed, narrow
 * aside layered over the right edge of the dashboard rather than a full-screen
 * overlay, so every scenario control / panel underneath stays clickable while a
 * lesson is open -- a lesson only ever *suggests* an action via its own "実行"
 * button, it never disables anything else on the page.
 */
export function LessonPanel({
  gatewayUrl,
  onError,
  onSlowMotionChanged,
  initialLesson,
}: {
  gatewayUrl: string;
  onError: (message: string | undefined) => void;
  /** Notified after a "slow-motion" step successfully runs, so the page's own
   * Slow-motion ON/OFF button stays in sync with a toggle a lesson step made (the
   * gateway's own state is the source of truth either way; this just keeps the two
   * independent UI surfaces from disagreeing about it). */
  onSlowMotionChanged?: (enabled: boolean) => void;
  /** Lesson to open on mount (e.g. resolved from a `?lesson=` deep link by the
   * theme page). Only consulted for the initial useState value -- this panel
   * doesn't react to `initialLesson` changing after mount, since the theme page
   * only resolves it once from the page's own initial searchParams. */
  initialLesson?: Lesson;
}) {
  const world = useKafkaStore((state) => state.world);
  const [isOpen, setIsOpen] = useState(initialLesson !== undefined);
  const [activeLesson, setActiveLesson] = useState<Lesson | undefined>(initialLesson);
  const [stepIndex, setStepIndex] = useState(0);
  const [isRunning, setIsRunning] = useState(false);

  function startLesson(lesson: Lesson): void {
    setActiveLesson(lesson);
    setStepIndex(0);
  }

  function exitLesson(): void {
    setActiveLesson(undefined);
    setStepIndex(0);
  }

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

  if (!isOpen) {
    return (
      <button
        type="button"
        onClick={() => setIsOpen(true)}
        // Filled even though it is a secondary action: this floats over whatever the
        // canvas is showing, and an outlined surface would sit at 1.03:1 against the page
        // plane (1.00:1 where it overlaps a panel), i.e. an invisible entry point.
        className="fixed right-4 top-20 z-10 rounded bg-(--accent) px-3 py-1.5 text-sm text-(--on-accent) shadow-lg hover:opacity-90"
      >
        レッスン
      </button>
    );
  }

  const totalSteps = activeLesson?.steps.length ?? 0;
  const clampedIndex = clampStepIndex(stepIndex, totalSteps);
  const step = activeLesson?.steps[clampedIndex];
  const resolvedAction =
    activeLesson !== undefined && step?.action !== undefined
      ? resolveLessonAction(step.action, world)
      : undefined;

  return (
    <aside
      className="fixed right-4 top-20 z-10 flex max-h-[calc(100vh-6rem)] w-80 flex-col gap-3 overflow-y-auto rounded border border-(--border) bg-(--surface-1) p-4 shadow-lg"
      aria-label="ガイド付きレッスン"
    >
      <div className="flex items-center justify-between">
        <h2 className="text-base font-semibold">レッスン</h2>
        <button
          type="button"
          onClick={() => setIsOpen(false)}
          className="text-sm text-(--text-secondary) hover:text-(--text-primary)"
        >
          閉じる
        </button>
      </div>

      {activeLesson === undefined ? (
        <ul className="flex flex-col gap-2">
          {lessons.map((lesson) => (
            <li key={lesson.id}>
              <button
                type="button"
                onClick={() => startLesson(lesson)}
                className="w-full rounded border border-(--text-muted) p-2 text-left text-sm hover:border-(--text-primary)"
              >
                <div className="font-medium">{lesson.title}</div>
                <div className="text-xs text-(--text-secondary)">{lesson.summary}</div>
              </button>
            </li>
          ))}
        </ul>
      ) : (
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
              onClick={() => setStepIndex((i) => clampStepIndex(i - 1, totalSteps))}
              disabled={clampedIndex === 0}
              className="rounded border border-(--text-muted) px-2 py-1 text-sm enabled:hover:border-(--text-primary) disabled:opacity-50"
            >
              戻る
            </button>
            <button
              type="button"
              onClick={exitLesson}
              className="text-xs text-(--text-secondary) hover:text-(--text-primary)"
            >
              レッスンを終了
            </button>
            <button
              type="button"
              onClick={() => setStepIndex((i) => clampStepIndex(i + 1, totalSteps))}
              disabled={clampedIndex === totalSteps - 1}
              className="rounded border border-(--text-muted) px-2 py-1 text-sm enabled:hover:border-(--text-primary) disabled:opacity-50"
            >
              次へ
            </button>
          </div>
        </div>
      )}
    </aside>
  );
}
