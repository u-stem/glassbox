import { describe, expect, test } from "bun:test";
import {
  CLOSED_DRAWER,
  closeDrawer,
  type DrawerState,
  initialDrawerState,
  openGlossaryAt,
  toggleDrawer,
} from "./drawer-state";

const OPEN_LESSON: DrawerState = { isOpen: true, content: "lesson" };

describe("initialDrawerState", () => {
  test("opens on the lesson pane when a deep-linked lesson was resolved", () => {
    expect(initialDrawerState(true)).toEqual({ isOpen: true, content: "lesson" });
  });

  test("stays closed without a deep-linked lesson", () => {
    expect(initialDrawerState(false)).toEqual(CLOSED_DRAWER);
  });
});

describe("toggleDrawer", () => {
  test("opens a closed drawer on the requested pane", () => {
    expect(toggleDrawer(CLOSED_DRAWER, "lesson")).toEqual({ isOpen: true, content: "lesson" });
  });

  test("closes the drawer when the open pane is requested again", () => {
    expect(toggleDrawer(OPEN_LESSON, "lesson").isOpen).toBe(false);
  });

  test("switches pane without closing when a different pane is requested", () => {
    expect(toggleDrawer(OPEN_LESSON, "glossary")).toEqual({ isOpen: true, content: "glossary" });
  });

  test("drops focusTermId when leaving the glossary pane", () => {
    const focused: DrawerState = { isOpen: true, content: "glossary", focusTermId: "partition" };

    expect(toggleDrawer(focused, "lesson")).not.toHaveProperty("focusTermId");
  });
});

describe("openGlossaryAt", () => {
  test("opens the glossary pane", () => {
    expect(openGlossaryAt("partition").content).toBe("glossary");
  });

  test("carries the requested term so the list can scroll to it", () => {
    expect(openGlossaryAt("lag").focusTermId).toBe("lag");
  });
});

describe("closeDrawer", () => {
  test("drops focusTermId so reopening does not jump to a stale term", () => {
    const focused: DrawerState = { isOpen: true, content: "glossary", focusTermId: "partition" };

    expect(closeDrawer(focused)).not.toHaveProperty("focusTermId");
  });
});
