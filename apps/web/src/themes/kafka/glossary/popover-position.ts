export interface Rect {
  left: number;
  top: number;
  right: number;
  bottom: number;
}

export interface Size {
  width: number;
  height: number;
}

/** Distance between the trigger and the popover edge facing it. */
const DEFAULT_GAP = 6;
/** Smallest distance the popover keeps from the viewport edges. */
const DEFAULT_MARGIN = 8;

function clamp(value: number, min: number, max: number): number {
  // min wins when the available span is smaller than the popover (max < min), which
  // is how the too-large cases degrade to sitting at the margin instead of off screen.
  return Math.max(min, Math.min(value, max));
}

/**
 * Places a term popover against its trigger, in viewport coordinates for
 * `position: fixed`.
 *
 * Below the trigger and left-aligned by default; flips above when the space below
 * runs out, and slides horizontally to stay inside the viewport. The horizontal
 * clamp is not cosmetic: glossary markers appear inside the right-hand drawer, so
 * left-aligning them would put most of the card past the right edge of the screen.
 */
export function computePopoverPosition({
  anchor,
  popover,
  viewport,
  gap = DEFAULT_GAP,
  margin = DEFAULT_MARGIN,
}: {
  anchor: Rect;
  popover: Size;
  viewport: Size;
  gap?: number;
  margin?: number;
}): { left: number; top: number } {
  const below = anchor.bottom + gap;
  const fitsBelow = below + popover.height + margin <= viewport.height;
  const preferredTop = fitsBelow ? below : anchor.top - gap - popover.height;

  return {
    left: clamp(anchor.left, margin, viewport.width - popover.width - margin),
    top: clamp(preferredTop, margin, viewport.height - popover.height - margin),
  };
}
