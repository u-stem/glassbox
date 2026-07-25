/**
 * Fixed-order categorical palette per the dataviz skill (references/palette.md),
 * validated with scripts/validate_palette.js in both light and dark mode (see the
 * Phase 3 report for the exact command output). Colors follow the entity (partition
 * index), never a sort rank -- see anti-patterns.md "Recolor-on-filter".
 */
const SERIES_SLOT_COUNT = 8;

/** CSS custom property for a partition's fixed color slot. Wraps past 8 partitions
 * (folds back to slot 1) rather than generating a 9th hue -- acceptable here because
 * lanes that share a hue are never adjacent in the all-pairs sense the validator
 * checks (see anti-patterns.md "Cycling / generating hues past 8"); a topic with more
 * than 8 partitions is out of scope for this learning demo. */
export function partitionColorVar(partitionIndex: number): string {
  const slot = (partitionIndex % SERIES_SLOT_COUNT) + 1;
  return `var(--series-${slot})`;
}
