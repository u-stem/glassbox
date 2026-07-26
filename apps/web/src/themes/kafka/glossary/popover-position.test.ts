import { describe, expect, test } from "bun:test";
import { computePopoverPosition } from "./popover-position";

const VIEWPORT = { width: 1000, height: 800 };
const POPOVER = { width: 288, height: 160 };
const GAP = 6;
const MARGIN = 8;

/** A trigger comfortably inside the viewport, away from every edge. */
const CENTERED = { left: 400, top: 300, right: 420, bottom: 320 };

describe("computePopoverPosition", () => {
  test("sits below the trigger when there is room", () => {
    const { top } = computePopoverPosition({
      anchor: CENTERED,
      popover: POPOVER,
      viewport: VIEWPORT,
    });

    expect(top).toBe(CENTERED.bottom + GAP);
  });

  test("aligns to the trigger's left edge", () => {
    const { left } = computePopoverPosition({
      anchor: CENTERED,
      popover: POPOVER,
      viewport: VIEWPORT,
    });

    expect(left).toBe(CENTERED.left);
  });

  test("flips above the trigger when it would overflow the bottom", () => {
    const nearBottom = { left: 400, top: 700, right: 420, bottom: 720 };

    const { top } = computePopoverPosition({
      anchor: nearBottom,
      popover: POPOVER,
      viewport: VIEWPORT,
    });

    expect(top).toBe(nearBottom.top - GAP - POPOVER.height);
  });

  test("clamps to the right margin for a trigger near the right edge", () => {
    // The case that matters most: markers inside the drawer sit against the right
    // edge of the screen, so left-aligning them unclamped would run off it.
    const nearRight = { left: 960, top: 300, right: 980, bottom: 320 };

    const { left } = computePopoverPosition({
      anchor: nearRight,
      popover: POPOVER,
      viewport: VIEWPORT,
    });

    expect(left).toBe(VIEWPORT.width - POPOVER.width - MARGIN);
  });

  test("clamps to the left margin for a trigger near the left edge", () => {
    const nearLeft = { left: 2, top: 300, right: 22, bottom: 320 };

    const { left } = computePopoverPosition({
      anchor: nearLeft,
      popover: POPOVER,
      viewport: VIEWPORT,
    });

    expect(left).toBe(MARGIN);
  });

  test("falls back to the margin when the popover is wider than the viewport", () => {
    const { left } = computePopoverPosition({
      anchor: CENTERED,
      popover: { width: 2000, height: 160 },
      viewport: VIEWPORT,
    });

    expect(left).toBe(MARGIN);
  });

  test("keeps a too-tall popover on screen rather than above the top edge", () => {
    const nearBottom = { left: 400, top: 700, right: 420, bottom: 720 };

    const { top } = computePopoverPosition({
      anchor: nearBottom,
      popover: { width: 288, height: 2000 },
      viewport: VIEWPORT,
    });

    expect(top).toBe(MARGIN);
  });
});
