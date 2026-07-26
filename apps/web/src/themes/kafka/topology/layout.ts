import { partitionColorVar } from "../colors";
import type { WorldGroup, WorldGroupMember } from "../world-reducer";

const PRODUCER_X = 40;
const TOPIC_X = 320;
const CONSUMER_X = 620;
const CONSUMER_ROW_HEIGHT = 72;
const TOP_MARGIN = 40;

/** Rendered width of a node box. Lives here rather than in TopologyCanvas because
 * TOPOLOGY_CONTENT_WIDTH below needs it; the canvas imports it back for its own
 * node styles so there is still exactly one definition. */
export const NODE_WIDTH = 140;
const RIGHT_MARGIN = 20;

/**
 * Width the canvas needs before anything is clipped: the consumer column is the
 * rightmost thing laid out (it has no right-side handle -- see TopologyCanvas's
 * ConsumerNode), so its box plus a small margin bounds the whole drawing.
 *
 * The canvas can't shrink to fit: pan/zoom are disabled precisely so the layout
 * coordinates below equal on-screen pixels, which the message-pulse overlay relies
 * on (see TopologyCanvas). So when the drawer narrows the column past this, the
 * canvas scrolls horizontally instead of scaling.
 */
export const TOPOLOGY_CONTENT_WIDTH = CONSUMER_X + NODE_WIDTH + RIGHT_MARGIN;

export interface TopologyConsumerNode {
  id: string;
  clientId: string;
  groupId: string;
  liveness: WorldGroupMember["liveness"];
  x: number;
  y: number;
}

export interface TopologyEdge {
  id: string;
  consumerId: string;
  partition: number;
  color: string;
}

export interface TopologyLayout {
  producer: { id: string; x: number; y: number };
  topic: { id: string; x: number; y: number };
  consumers: TopologyConsumerNode[];
  edges: TopologyEdge[];
}

/**
 * Pure position + edge computation for the 3c topology canvas: producer -> topic ->
 * one node per consumer across every group, laid out in fixed columns (no
 * force-directed layout needed -- see the Phase 3 plan). Edges are keyed per
 * (consumer, partition) and colored with the same fixed partition palette as 3a
 * (colors.ts), so identity reads consistently across panels.
 */
export function computeTopologyLayout(
  topicName: string,
  groups: readonly WorldGroup[],
): TopologyLayout {
  const consumers: TopologyConsumerNode[] = [];
  const edges: TopologyEdge[] = [];
  let row = 0;

  for (const group of groups) {
    for (const member of group.members) {
      // Keyed by clientId, not memberId: classic protocol assigns a fresh memberId on
      // every rejoin (see world-reducer.ts), but consumer.message events (used to
      // drive the message-flow animation) carry clientId, so using it here avoids a
      // memberId->clientId lookup at animation time.
      const id = member.clientId;
      const y = TOP_MARGIN + row * CONSUMER_ROW_HEIGHT;
      consumers.push({
        id,
        clientId: member.clientId,
        groupId: group.groupId,
        liveness: member.liveness,
        x: CONSUMER_X,
        y,
      });
      row += 1;

      for (const assignment of member.assignments) {
        if (assignment.topic !== topicName) {
          continue;
        }
        for (const partition of assignment.partitions) {
          edges.push({
            id: `${id}-${topicName}-${partition}`,
            consumerId: id,
            partition,
            color: partitionColorVar(partition),
          });
        }
      }
    }
  }

  const topicY =
    TOP_MARGIN + (Math.max(row, 1) * CONSUMER_ROW_HEIGHT) / 2 - CONSUMER_ROW_HEIGHT / 2;

  return {
    producer: { id: "producer", x: PRODUCER_X, y: topicY },
    topic: { id: "topic", x: TOPIC_X, y: topicY },
    consumers,
    edges,
  };
}
