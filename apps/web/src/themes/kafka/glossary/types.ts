/**
 * One Kafka term as the dashboard explains it. Japanese leads because that is the
 * reading language, but the English spelling is always carried alongside: it is what
 * appears in the gateway's own event names, in the scenario parameters, and in
 * Kafka's documentation, so a reader who only ever saw the Japanese would not
 * recognise the same concept elsewhere.
 */
export interface GlossaryEntry {
  ja: string;
  en: string;
  /** Shown in the popover. One to three sentences -- long enough to be an answer,
   * short enough to read without leaving the panel you were looking at. */
  body: string;
  category: GlossaryCategory;
  /** Terms reachable as chips from this one's popover, for the follow-up question
   * the body inevitably raises ("committed offset" -> "what is a commit?"). */
  related?: readonly GlossaryTermId[];
}

export type GlossaryCategory = "basics" | "offset" | "group" | "dashboard";

/**
 * Every term the UI can point at. A union rather than plain strings so a
 * `<GlossaryTerm id="...">` naming a term that was never written is a compile
 * error, and so the GLOSSARY record below cannot be missing an entry.
 */
export type GlossaryTermId =
  // basics
  | "topic"
  | "partition"
  | "broker"
  | "producer"
  | "consumer"
  | "message-key"
  | "partitioner"
  // offset
  | "offset"
  | "end-offset"
  | "committed-offset"
  | "commit"
  | "lag"
  // group
  | "consumer-group"
  | "group-id"
  | "member"
  | "assignment"
  | "rebalance"
  | "group-state"
  | "session-timeout"
  | "leave-group"
  | "lost-member"
  // dashboard
  | "topology"
  | "event"
  | "actor-id"
  | "scenario"
  | "slow-motion"
  | "time-travel"
  | "admin-snapshot";

/** A term paired with its id, which the id-keyed GLOSSARY record drops. */
export type IdentifiedGlossaryEntry = GlossaryEntry & { id: GlossaryTermId };

export interface GlossaryGroup {
  category: GlossaryCategory;
  label: string;
  entries: readonly IdentifiedGlossaryEntry[];
}
