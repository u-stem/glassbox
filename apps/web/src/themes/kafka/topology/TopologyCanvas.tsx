"use client";

import { type AdminSnapshotPayload, type GlassboxEvent, kafkaEventSchema } from "@glassbox/schema";
import {
  Background,
  type Edge,
  Handle,
  type Node,
  type NodeProps,
  type NodeTypes,
  Position,
  ReactFlow,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { AnimatePresence, motion } from "motion/react";
import { useEffect, useMemo, useRef, useState } from "react";
import { partitionColorVar } from "../colors";
import type { WorldGroup } from "../world-reducer";
import { computeTopologyLayout } from "./layout";

const CANVAS_HEIGHT = 320;
const NODE_WIDTH = 140;
const NODE_HEIGHT = 48;
const PULSE_DURATION_S = 0.6;
/** Thins bursts of produce/consume events to one pulse per path per window, per the
 * plan ("burst 時にジャンクしない程度に間引く"). */
const PULSE_THROTTLE_MS = 120;

function ProducerNode() {
  return (
    <div
      className="flex items-center justify-center rounded border px-3 py-2 text-sm font-medium"
      style={{
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
        background: "var(--surface-1)",
        borderColor: "var(--axis)",
        color: "var(--text-primary)",
      }}
    >
      Producer
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

function TopicNode({ data }: NodeProps & { data: { name: string; partitions: number[] } }) {
  return (
    <div
      className="flex flex-col gap-1 rounded border px-3 py-2 text-sm"
      style={{
        width: NODE_WIDTH,
        background: "var(--surface-1)",
        borderColor: "var(--axis)",
        color: "var(--text-primary)",
      }}
    >
      <span className="font-medium">{data.name}</span>
      <div className="flex flex-col gap-0.5">
        {data.partitions.map((partition) => (
          // relative + one named source handle per partition row: without this, every
          // assignment edge for this topic shares the node's single default handle and
          // they render exactly on top of each other, so only the last edge's color is
          // visible (see the Phase 3 review). Anchoring the handle inside its own row
          // (top: 50% via xyflow's default Position.Right styling) makes each edge
          // leave from that partition's actual lane instead.
          <div key={partition} className="relative flex items-center gap-1.5">
            <span
              className="inline-block h-2.5 w-2.5 rounded-sm"
              style={{ background: partitionColorVar(partition) }}
            />
            <span className="text-xs" style={{ color: "var(--text-secondary)" }}>
              P{partition}
            </span>
            <Handle type="source" position={Position.Right} id={`partition-${partition}`} />
          </div>
        ))}
      </div>
      <Handle type="target" position={Position.Left} />
    </div>
  );
}

function ConsumerNode({
  data,
}: NodeProps & { data: { clientId: string; groupId: string; liveness: "alive" | "lost" } }) {
  const isLost = data.liveness === "lost";
  return (
    <div
      className="flex flex-col rounded border px-3 py-2 text-sm"
      style={{
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
        background: "var(--surface-1)",
        borderColor: "var(--axis)",
        borderStyle: isLost ? "dashed" : "solid",
        color: isLost ? "var(--text-muted)" : "var(--text-primary)",
        opacity: isLost ? 0.6 : 1,
      }}
    >
      <span className="font-medium">{data.clientId}</span>
      <span className="text-xs" style={{ color: "var(--text-muted)" }}>
        {data.groupId}
        {isLost ? " · lost" : ""}
      </span>
      <Handle type="target" position={Position.Left} />
    </div>
  );
}

const nodeTypes: NodeTypes = {
  producer: ProducerNode,
  topic: TopicNode,
  consumer: ConsumerNode,
};

interface Pulse {
  id: string;
  fromX: number;
  fromY: number;
  toX: number;
  toY: number;
  color: string;
}

interface MessagePulses {
  pulses: Pulse[];
  /** Called from the pulse's onAnimationComplete once it's done animating, so the
   * array doesn't grow without bound over a long-running session (see the Phase 3
   * review). */
  removePulse: (id: string) => void;
}

/** Tracks the newest event this hook has already animated, so re-renders triggered
 * by unrelated state don't replay the same pulse. Newest-first per lib/store.ts. */
function useMessagePulses(
  events: readonly GlassboxEvent[],
  layout: ReturnType<typeof computeTopologyLayout>,
): MessagePulses {
  const [pulses, setPulses] = useState<Pulse[]>([]);
  const lastSeenIdRef = useRef<string | undefined>(undefined);
  const lastPulseAtRef = useRef<Record<string, number>>({});

  useEffect(() => {
    const newest = events[0];
    if (newest === undefined || newest.id === lastSeenIdRef.current) {
      return;
    }
    lastSeenIdRef.current = newest.id;

    const now = Date.now();
    function throttled(key: string): boolean {
      const last = lastPulseAtRef.current[key];
      if (last !== undefined && now - last < PULSE_THROTTLE_MS) {
        return true;
      }
      lastPulseAtRef.current[key] = now;
      return false;
    }

    if (newest.type === "producer.send.end" && !throttled("producer-topic")) {
      setPulses((prev) => [
        ...prev,
        {
          id: newest.id,
          fromX: layout.producer.x + NODE_WIDTH,
          fromY: layout.producer.y + NODE_HEIGHT / 2,
          toX: layout.topic.x,
          toY: layout.topic.y + NODE_HEIGHT / 2,
          color: "var(--text-secondary)",
        },
      ]);
    }

    if (newest.type === "consumer.message") {
      const parsed = kafkaEventSchema.safeParse({ type: newest.type, payload: newest.payload });
      if (!parsed.success || parsed.data.type !== "consumer.message") {
        return;
      }
      const { clientId, partition } = parsed.data.payload;
      const consumer = layout.consumers.find((c) => c.id === clientId);
      if (consumer !== undefined && !throttled(`topic-${clientId}`)) {
        setPulses((prev) => [
          ...prev,
          {
            id: newest.id,
            fromX: layout.topic.x + NODE_WIDTH,
            fromY: layout.topic.y + NODE_HEIGHT / 2,
            toX: consumer.x,
            toY: consumer.y + NODE_HEIGHT / 2,
            color: partitionColorVar(partition),
          },
        ]);
      }
    }
  }, [events, layout]);

  const removePulse = (id: string) => {
    setPulses((prev) => prev.filter((pulse) => pulse.id !== id));
  };

  return { pulses, removePulse };
}

/**
 * 3c: topology canvas. Producer -> Topic (partition lanes) -> Consumer Group members,
 * assignment edges colored per-partition (same fixed palette as 3a). Layout is fixed
 * (computeTopologyLayout), not force-directed; pan/zoom/drag are disabled so the
 * pixel coordinates from that layout match the rendered screen coordinates exactly,
 * which the motion pulse overlay below relies on.
 */
export function TopologyCanvas({
  topic,
  groups,
  events,
}: {
  topic: AdminSnapshotPayload["topics"][number] | undefined;
  groups: readonly WorldGroup[];
  events: readonly GlassboxEvent[];
}) {
  const layout = useMemo(() => computeTopologyLayout(topic?.name ?? "", groups), [topic, groups]);

  const nodes: Node[] = useMemo(() => {
    const list: Node[] = [
      { id: "producer", type: "producer", position: layout.producer, data: {}, draggable: false },
      {
        id: "topic",
        type: "topic",
        position: layout.topic,
        data: {
          name: topic?.name ?? "(no topic)",
          partitions: topic?.partitions.map((p) => p.index) ?? [],
        },
        draggable: false,
      },
    ];
    for (const consumer of layout.consumers) {
      list.push({
        id: consumer.id,
        type: "consumer",
        position: { x: consumer.x, y: consumer.y },
        data: {
          clientId: consumer.clientId,
          groupId: consumer.groupId,
          liveness: consumer.liveness,
        },
        draggable: false,
      });
    }
    return list;
  }, [layout, topic]);

  const edges: Edge[] = useMemo(() => {
    const list: Edge[] = [
      {
        id: "producer-topic",
        source: "producer",
        target: "topic",
        style: { stroke: "var(--axis)" },
      },
    ];
    for (const edge of layout.edges) {
      list.push({
        id: edge.id,
        source: "topic",
        // Named per-partition handle (see TopicNode) so edges to the same consumer for
        // different partitions don't all leave from the same point and overlap.
        sourceHandle: `partition-${edge.partition}`,
        target: edge.consumerId,
        style: { stroke: edge.color, strokeWidth: 2 },
      });
    }
    return list;
  }, [layout]);

  const { pulses, removePulse } = useMessagePulses(events, layout);

  const partitionSwatches = topic?.partitions.map((p) => p.index) ?? [];

  return (
    <div className="viz-root flex flex-col gap-2">
      <div className="relative" style={{ height: CANVAS_HEIGHT, background: "var(--surface-1)" }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          defaultViewport={{ x: 0, y: 0, zoom: 1 }}
          panOnDrag={false}
          panOnScroll={false}
          zoomOnScroll={false}
          zoomOnPinch={false}
          zoomOnDoubleClick={false}
          nodesDraggable={false}
          nodesConnectable={false}
          proOptions={{ hideAttribution: true }}
        >
          {/* ReactFlow's own dot color (#91919a) is a light-mode constant that only
           * changes behind its `.dark` class, which we never set -- on the dark plane it
           * ends up more prominent than on the light one. The handle colors are pinned to
           * roles in globals.css for the same reason. */}
          <Background color="var(--gridline)" />
        </ReactFlow>

        <svg
          className="pointer-events-none absolute inset-0"
          width="100%"
          height="100%"
          aria-hidden="true"
        >
          <AnimatePresence>
            {pulses.map((pulse) => (
              <motion.circle
                key={pulse.id}
                r={4}
                fill={pulse.color}
                initial={{ cx: pulse.fromX, cy: pulse.fromY, opacity: 1 }}
                animate={{ cx: pulse.toX, cy: pulse.toY, opacity: 0 }}
                transition={{ duration: PULSE_DURATION_S, ease: "linear" }}
                onAnimationComplete={() => removePulse(pulse.id)}
              />
            ))}
          </AnimatePresence>
        </svg>
      </div>

      {partitionSwatches.length > 0 && (
        <div className="flex flex-wrap gap-3 text-xs" style={{ color: "var(--text-secondary)" }}>
          {partitionSwatches.map((partition) => (
            <span key={partition} className="flex items-center gap-1">
              <span
                className="inline-block h-2.5 w-2.5 rounded-sm"
                style={{ background: partitionColorVar(partition) }}
              />
              partition {partition}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
