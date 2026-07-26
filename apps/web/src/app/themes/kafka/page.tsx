"use client";

import { type ReactNode, use, useEffect, useMemo, useRef, useState } from "react";
import { GATEWAY_URL } from "@/lib/gateway";
import { connectKafkaEvents, createBrowserEventSource } from "@/lib/sse";
import { useKafkaStore } from "@/lib/store";
import {
  closeDrawer,
  type DrawerState,
  initialDrawerState,
  toggleDrawer,
} from "@/themes/kafka/drawer/drawer-state";
import { LessonPanel } from "@/themes/kafka/lessons/LessonPanel";
import { lessons } from "@/themes/kafka/lessons/lessons";
import { findLessonByParam } from "@/themes/kafka/lessons/navigation";
import type { Lesson } from "@/themes/kafka/lessons/types";
import { PartitionBoard } from "@/themes/kafka/partition-board/PartitionBoard";
import { RebalanceDiagram } from "@/themes/kafka/rebalance-diagram/RebalanceDiagram";
import { ScenarioRunner } from "@/themes/kafka/scenario-form/ScenarioRunner";
import { TimeTravelBar } from "@/themes/kafka/time-travel/TimeTravelBar";
import { TimeTravelButton } from "@/themes/kafka/time-travel/TimeTravelButton";
import {
  buildTimeTravelIndex,
  seqRange,
  type TimeTravelIndex,
  transitionHistoryAtSeq,
  worldAtSeq,
} from "@/themes/kafka/time-travel/time-travel";
import { Timeline } from "@/themes/kafka/timeline/Timeline";
import { TopologyCanvas } from "@/themes/kafka/topology/TopologyCanvas";

/** Matches the gateway's DEMO_TOPIC / add-consumer's default groupId (server.ts,
 * consumer-scenarios.ts). The broker is shared infrastructure -- other processes
 * (integration tests, other scenarios) can create their own topics/groups against the
 * same broker, so picking topics[0]/groups[0] blindly could surface unrelated state;
 * preferring these known demo names keeps the dashboard showing what its own control
 * bar actually drives, falling back to "first seen" only if the demo names are absent. */
const DEMO_TOPIC_NAME = "glassbox.demo";
const DEMO_GROUP_ID = "glassbox-consumers";
/** Matches lib/store.ts's MAX_TRANSITION_HISTORY_PER_GROUP -- time travel's
 * reconstructed history is capped the same way the live store caps it. */
const MAX_TRANSITION_HISTORY_PER_GROUP = 10;

const DRAWER_ID = "kafka-side-drawer";
/** The four panels keep their own col-spans in both layouts; only the column count
 * changes when the drawer takes its share of the width. At 3 columns that lays out
 * as Topology(2)+Rebalance / Partitions+Timeline(2); at 2 it becomes a full-width
 * Topology, then Rebalance+Partitions, then a full-width Timeline -- so the panel
 * that needs the most width absorbs the loss instead of being squeezed. */
const PANEL_GRID_WIDE = "grid grid-cols-1 items-start gap-4 lg:grid-cols-3";
const PANEL_GRID_NARROW = "grid grid-cols-1 items-start gap-4 lg:grid-cols-2";
const PANEL_SECTION_CLASS =
  "flex min-w-0 flex-col gap-2 rounded border border-(--border) bg-(--surface-1) p-3";

export default function KafkaThemePage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const resolvedSearchParams = use(searchParams);
  const initialLesson = findLessonByParam(lessons, resolvedSearchParams.lesson);

  const events = useKafkaStore((state) => state.events);
  const latestSnapshot = useKafkaStore((state) => state.latestSnapshot);
  const groups = useKafkaStore((state) => state.world.groups);
  const addEvent = useKafkaStore((state) => state.addEvent);
  const reset = useKafkaStore((state) => state.reset);

  const [slowMotionEnabled, setSlowMotionEnabled] = useState(false);
  const [slowMotionFactorMs, setSlowMotionFactorMs] = useState(3000);
  const [scenarioError, setScenarioError] = useState<string | undefined>(undefined);
  const [consumerActionError, setConsumerActionError] = useState<string | undefined>(undefined);

  // The drawer and the reader's place in a lesson both live here rather than inside
  // LessonPanel: the panel grid has to react to the drawer's width, and owning the
  // lesson progress is what lets the drawer swap panes by conditional rendering
  // without losing that progress.
  const [drawer, setDrawer] = useState<DrawerState>(() =>
    initialDrawerState(initialLesson !== undefined),
  );
  const [activeLesson, setActiveLesson] = useState<Lesson | undefined>(initialLesson);
  const [stepIndex, setStepIndex] = useState(0);
  const drawerHeadingRef = useRef<HTMLHeadingElement>(null);
  const lessonToggleRef = useRef<HTMLButtonElement>(null);

  const isLessonOpen = drawer.isOpen && drawer.content === "lesson";

  // Focus moves into the drawer whenever it opens so keyboard users land in the new
  // content, and below `lg` (where the drawer sits above the grid) this doubles as
  // the scroll-into-view. Deliberately no focus trap: the drawer stays modeless,
  // every control underneath remains reachable.
  useEffect(() => {
    if (drawer.isOpen) {
      drawerHeadingRef.current?.focus();
    }
  }, [drawer.isOpen]);

  function dismissDrawer(): void {
    setDrawer(closeDrawer);
    lessonToggleRef.current?.focus();
  }

  // Time travel (Phase 4): entering it freezes a TimeTravelIndex built from the
  // event buffer at that moment (see time-travel.ts) -- live events keep
  // accumulating in the background (addEvent below is unaffected), but the scrub
  // timeline itself doesn't grow until the user exits and re-enters, per the plan
  // ("必要ならチェックポイントをメモ化"): this is that memoization boundary.
  const [timeTravel, setTimeTravel] = useState<
    { index: TimeTravelIndex; seekSeq: number } | undefined
  >(undefined);

  const addEventRef = useRef(addEvent);
  addEventRef.current = addEvent;
  const resetRef = useRef(reset);
  resetRef.current = reset;

  useEffect(() => {
    const eventSource = createBrowserEventSource(`${GATEWAY_URL}/api/events`);
    const cleanup = connectKafkaEvents(eventSource, {
      onEvent: (event) => addEventRef.current(event),
      onReset: () => resetRef.current(),
    });
    return cleanup;
  }, []);

  async function handleSlowMotionToggle(): Promise<void> {
    const nextEnabled = !slowMotionEnabled;
    setConsumerActionError(undefined);
    try {
      const response = await fetch(`${GATEWAY_URL}/api/themes/kafka/slow-motion`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: nextEnabled, factorMs: slowMotionFactorMs }),
      });
      if (!response.ok) {
        throw new Error(`gateway responded with ${response.status}`);
      }
      setSlowMotionEnabled(nextEnabled);
    } catch (error) {
      setConsumerActionError(error instanceof Error ? error.message : String(error));
    }
  }

  // While time traveling, every panel's data is reconstructed as of `seekSeq`
  // instead of read from the live store (see time-travel.ts's worldAtSeq); `events`
  // for the timeline/topology pulse animation is likewise sliced to only what had
  // already happened by that point.
  const isTimeTravel = timeTravel !== undefined;
  const displayWorld =
    timeTravel !== undefined ? worldAtSeq(timeTravel.index, timeTravel.seekSeq) : undefined;
  const displayTopics = displayWorld?.topics ?? latestSnapshot?.topics;
  const displayGroupsRecord = displayWorld?.groups ?? groups;
  const displayEvents = useMemo(() => {
    if (timeTravel === undefined) {
      return events;
    }
    const { ascendingEvents } = timeTravel.index;
    const { seekSeq } = timeTravel;
    // ascendingEvents is oldest-first; every other panel here expects newest-first
    // (see lib/store.ts's `events`), so this reverses back after filtering.
    return ascendingEvents.filter((event) => event.seq <= seekSeq).toReversed();
  }, [timeTravel, events]);

  const topic = displayTopics?.find((t) => t.name === DEMO_TOPIC_NAME) ?? displayTopics?.[0];
  const allGroups = useMemo(() => Object.values(displayGroupsRecord), [displayGroupsRecord]);
  const primaryGroup = displayGroupsRecord[DEMO_GROUP_ID] ?? allGroups[0];
  // Scoped to just the group this dashboard's own control bar drives, so an unrelated
  // group sharing the same broker (a different process's scenario/integration test)
  // never shows up as a stray node in the topology canvas or partition board.
  const displayGroups = useMemo(
    () => (primaryGroup === undefined ? [] : [primaryGroup]),
    [primaryGroup],
  );
  const rebalanceGroupId = primaryGroup?.groupId ?? DEMO_GROUP_ID;
  const rebalanceOverride = useMemo(() => {
    if (timeTravel === undefined) {
      return undefined;
    }
    return {
      state: displayGroupsRecord[rebalanceGroupId]?.state,
      history: transitionHistoryAtSeq(
        timeTravel.index.ascendingEvents,
        timeTravel.seekSeq,
        rebalanceGroupId,
        MAX_TRANSITION_HISTORY_PER_GROUP,
      ),
    };
  }, [timeTravel, displayGroupsRecord, rebalanceGroupId]);

  function enterTimeTravel(): void {
    const index = buildTimeTravelIndex(events);
    const range = seqRange(index.ascendingEvents);
    setTimeTravel({ index, seekSeq: range?.max ?? 0 });
  }

  return (
    // viz-root must sit on the page root, not on the width-limited inner div: the tokens
    // are only defined inside .viz-root, and a centered max-w container would leave the
    // page plane unpainted outside 80rem (see the home page for the same two-layer shape).
    <main className="viz-root min-h-screen bg-(--page-plane) text-(--text-primary)">
      {/* min-w-0 on the main column switches off the flex default of min-width:auto.
       * Without it the topology canvas's own min-width would widen this column and
       * push the whole page sideways, instead of scrolling inside the canvas. */}
      <div className="mx-auto flex max-w-7xl flex-col gap-4 p-6 lg:flex-row lg:items-start">
        <div className="flex min-w-0 flex-1 flex-col gap-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex flex-col gap-0.5">
              <h1 className="text-2xl font-bold">Kafka ダッシュボード</h1>
              <p className="text-sm text-(--text-secondary)">
                producer → topic → consumer group の流れをリアルタイムに観察する
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                ref={lessonToggleRef}
                aria-expanded={isLessonOpen}
                aria-controls={DRAWER_ID}
                onClick={() => setDrawer((state) => toggleDrawer(state, "lesson"))}
                className={
                  isLessonOpen
                    ? "rounded bg-(--accent) px-3 py-1.5 text-sm text-(--on-accent) hover:opacity-90"
                    : "rounded border border-(--text-muted) px-3 py-1.5 text-sm hover:border-(--text-primary)"
                }
              >
                レッスン
              </button>
              <TimeTravelButton onEnter={enterTimeTravel} disabled={isTimeTravel} />
            </div>
          </div>

          <section className="flex flex-wrap items-end gap-4 rounded border border-(--border) bg-(--surface-1) p-3">
            <ScenarioRunner gatewayUrl={GATEWAY_URL} onError={setScenarioError} />

            <label className="flex flex-col gap-1 text-sm">
              slow-motion factorMs
              <input
                type="number"
                min={0}
                max={10000}
                value={slowMotionFactorMs}
                onChange={(e) => setSlowMotionFactorMs(Number(e.target.value))}
                className="w-24 rounded border border-(--text-muted) bg-(--surface-1) px-2 py-1 text-(--text-primary)"
              />
            </label>
            {/* Filled while on, outlined while off -- the state has to survive without the
             * hue the old purple carried, and aria-pressed carries it for assistive tech. */}
            <button
              type="button"
              aria-pressed={slowMotionEnabled}
              onClick={() => {
                void handleSlowMotionToggle();
              }}
              className={
                slowMotionEnabled
                  ? "rounded bg-(--accent) px-3 py-1.5 text-sm text-(--on-accent) hover:opacity-90"
                  : "rounded border border-(--text-muted) px-3 py-1.5 text-sm hover:border-(--text-primary)"
              }
            >
              Slow-motion: {slowMotionEnabled ? "ON" : "OFF"}
            </button>

            {scenarioError && <ErrorLine>{scenarioError}</ErrorLine>}
            {consumerActionError && <ErrorLine>{consumerActionError}</ErrorLine>}
          </section>

          {timeTravel !== undefined && (
            <TimeTravelBar
              min={seqRange(timeTravel.index.ascendingEvents)?.min ?? 0}
              max={seqRange(timeTravel.index.ascendingEvents)?.max ?? 0}
              value={timeTravel.seekSeq}
              onExit={() => setTimeTravel(undefined)}
              onSeek={(seq) =>
                setTimeTravel((prev) => (prev === undefined ? prev : { ...prev, seekSeq: seq }))
              }
            />
          )}

          <div className={drawer.isOpen ? PANEL_GRID_NARROW : PANEL_GRID_WIDE}>
            <section
              className={`${PANEL_SECTION_CLASS} lg:col-span-2`}
              aria-labelledby="panel-topology"
            >
              <h2 id="panel-topology" className="text-lg font-semibold">
                Topology
              </h2>
              <TopologyCanvas topic={topic} groups={displayGroups} events={displayEvents} />
            </section>

            <section className={PANEL_SECTION_CLASS} aria-labelledby="panel-rebalance">
              <h2 id="panel-rebalance" className="text-lg font-semibold">
                Rebalance state {primaryGroup ? `— ${primaryGroup.groupId}` : ""}
              </h2>
              <RebalanceDiagram
                groupId={rebalanceGroupId}
                {...(rebalanceOverride === undefined ? {} : { override: rebalanceOverride })}
              />
            </section>

            <section className={PANEL_SECTION_CLASS} aria-labelledby="panel-partitions">
              <h2 id="panel-partitions" className="text-lg font-semibold">
                Partitions {topic ? `— ${topic.name}` : ""}
              </h2>
              <PartitionBoard topic={topic} groups={displayGroups} />
            </section>

            <section
              className={`${PANEL_SECTION_CLASS} lg:col-span-2`}
              aria-labelledby="panel-timeline"
            >
              <h2 id="panel-timeline" className="text-lg font-semibold">
                Event timeline
              </h2>
              <Timeline events={displayEvents} />
            </section>
          </div>
        </div>

        {/* order-first below `lg`: stacked vertically the drawer has to come before the
         * panels, or opening a lesson would place it thousands of pixels below the fold
         * and reading the step would scroll the visualisation it talks about off screen.
         * At `lg` and up it returns to being the right-hand column.
         * The sticky offset is the AppHeader's h-14 (3.5rem) plus its border. */}
        <aside
          id={DRAWER_ID}
          aria-label={drawer.content === "glossary" ? "用語集" : "ガイド付きレッスン"}
          onKeyDown={(event) => {
            if (event.key === "Escape") {
              dismissDrawer();
            }
          }}
          className={
            drawer.isOpen
              ? "order-first flex w-full shrink-0 flex-col gap-3 rounded border border-(--border) bg-(--surface-1) p-4 lg:sticky lg:top-[3.75rem] lg:order-none lg:max-h-[calc(100vh-4.75rem)] lg:w-80 lg:self-start lg:overflow-y-auto"
              : "hidden"
          }
        >
          <div className="flex items-center justify-between">
            <h2 ref={drawerHeadingRef} tabIndex={-1} className="text-base font-semibold">
              {drawer.content === "glossary" ? "用語集" : "レッスン"}
            </h2>
            <button
              type="button"
              onClick={dismissDrawer}
              className="text-sm text-(--text-secondary) hover:text-(--text-primary)"
            >
              閉じる
            </button>
          </div>

          {drawer.content === "lesson" && (
            <LessonPanel
              gatewayUrl={GATEWAY_URL}
              onError={setConsumerActionError}
              onSlowMotionChanged={setSlowMotionEnabled}
              activeLesson={activeLesson}
              stepIndex={stepIndex}
              onSelectLesson={(lesson) => {
                setActiveLesson(lesson);
                setStepIndex(0);
              }}
              onStepIndexChange={setStepIndex}
              onExitLesson={() => {
                setActiveLesson(undefined);
                setStepIndex(0);
              }}
            />
          )}
        </aside>
      </div>
    </main>
  );
}

/** Error text with a --status-critical dot instead of red type: the status hues are
 * validated as marks, not as glyphs (#d03b3b is 4.05:1 on the dark plane), so the
 * message itself stays at --text-primary and the hue rides on the dot -- the same
 * split the home screen's EnvironmentStatus uses. */
function ErrorLine({ children }: { children: ReactNode }) {
  return (
    <p className="flex w-full items-center gap-2 text-sm">
      <span
        aria-hidden="true"
        className="inline-block size-2 shrink-0 rounded-full"
        style={{ backgroundColor: "var(--status-critical)" }}
      />
      {children}
    </p>
  );
}
