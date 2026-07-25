"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { connectKafkaEvents, createBrowserEventSource } from "@/lib/sse";
import { useKafkaStore } from "@/lib/store";
import { LessonPanel } from "@/themes/kafka/lessons/LessonPanel";
import { PartitionBoard } from "@/themes/kafka/partition-board/PartitionBoard";
import { RebalanceDiagram } from "@/themes/kafka/rebalance-diagram/RebalanceDiagram";
import { ScenarioRunner } from "@/themes/kafka/scenario-form/ScenarioRunner";
import { TimeTravelBar } from "@/themes/kafka/time-travel/TimeTravelBar";
import {
  buildTimeTravelIndex,
  seqRange,
  type TimeTravelIndex,
  transitionHistoryAtSeq,
  worldAtSeq,
} from "@/themes/kafka/time-travel/time-travel";
import { Timeline } from "@/themes/kafka/timeline/Timeline";
import { TopologyCanvas } from "@/themes/kafka/topology/TopologyCanvas";

const GATEWAY_URL = process.env.NEXT_PUBLIC_GATEWAY_URL ?? "http://localhost:4000";
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

export default function KafkaThemePage() {
  const events = useKafkaStore((state) => state.events);
  const latestSnapshot = useKafkaStore((state) => state.latestSnapshot);
  const groups = useKafkaStore((state) => state.world.groups);
  const addEvent = useKafkaStore((state) => state.addEvent);
  const reset = useKafkaStore((state) => state.reset);

  const [slowMotionEnabled, setSlowMotionEnabled] = useState(false);
  const [slowMotionFactorMs, setSlowMotionFactorMs] = useState(3000);
  const [scenarioError, setScenarioError] = useState<string | undefined>(undefined);
  const [consumerActionError, setConsumerActionError] = useState<string | undefined>(undefined);

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
    <main className="mx-auto flex min-h-screen max-w-7xl flex-col gap-4 p-6">
      <h1 className="text-2xl font-bold">Kafka</h1>

      <section className="flex flex-wrap items-end gap-4 rounded border border-gray-300 p-3">
        <ScenarioRunner gatewayUrl={GATEWAY_URL} onError={setScenarioError} />

        <label className="flex flex-col gap-1 text-sm">
          slow-motion factorMs
          <input
            type="number"
            min={0}
            max={10000}
            value={slowMotionFactorMs}
            onChange={(e) => setSlowMotionFactorMs(Number(e.target.value))}
            className="w-24 rounded border border-gray-300 px-2 py-1"
          />
        </label>
        <button
          type="button"
          onClick={() => {
            void handleSlowMotionToggle();
          }}
          className="rounded bg-purple-600 px-3 py-1.5 text-sm text-white"
        >
          Slow-motion: {slowMotionEnabled ? "ON" : "OFF"}
        </button>

        {scenarioError && <p className="w-full text-sm text-red-600">{scenarioError}</p>}
        {consumerActionError && (
          <p className="w-full text-sm text-red-600">{consumerActionError}</p>
        )}
      </section>

      <TimeTravelBar
        isActive={isTimeTravel}
        min={timeTravel !== undefined ? (seqRange(timeTravel.index.ascendingEvents)?.min ?? 0) : 0}
        max={timeTravel !== undefined ? (seqRange(timeTravel.index.ascendingEvents)?.max ?? 0) : 0}
        value={timeTravel?.seekSeq ?? 0}
        onEnter={enterTimeTravel}
        onExit={() => setTimeTravel(undefined)}
        onSeek={(seq) =>
          setTimeTravel((prev) => (prev === undefined ? prev : { ...prev, seekSeq: seq }))
        }
      />

      <div className="grid grid-cols-1 items-start gap-4 lg:grid-cols-3">
        <section className="flex flex-col gap-2 rounded border border-gray-300 p-3 lg:col-span-2">
          <h2 className="text-lg font-semibold">Topology</h2>
          <TopologyCanvas topic={topic} groups={displayGroups} events={displayEvents} />
        </section>

        <section className="flex flex-col gap-2 rounded border border-gray-300 p-3">
          <h2 className="text-lg font-semibold">
            Rebalance state {primaryGroup ? `— ${primaryGroup.groupId}` : ""}
          </h2>
          <RebalanceDiagram
            groupId={rebalanceGroupId}
            {...(rebalanceOverride === undefined ? {} : { override: rebalanceOverride })}
          />
        </section>

        <section className="flex flex-col gap-2 rounded border border-gray-300 p-3">
          <h2 className="text-lg font-semibold">Partitions {topic ? `— ${topic.name}` : ""}</h2>
          <PartitionBoard topic={topic} groups={displayGroups} />
        </section>

        <section className="flex flex-col gap-2 rounded border border-gray-300 p-3 lg:col-span-2">
          <h2 className="text-lg font-semibold">Event timeline</h2>
          <Timeline events={displayEvents} />
        </section>
      </div>

      <LessonPanel
        gatewayUrl={GATEWAY_URL}
        onError={setConsumerActionError}
        onSlowMotionChanged={setSlowMotionEnabled}
      />
    </main>
  );
}
