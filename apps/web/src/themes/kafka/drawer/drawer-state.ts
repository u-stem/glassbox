/** Which pane the side drawer is showing. Both panes share one `<aside>` (its
 * heading, close button and layout) so the dashboard never grows a second
 * overlay surface -- see docs/themes/kafka.md's 用語集 section. */
export type DrawerContent = "lesson" | "glossary";

export interface DrawerState {
  isOpen: boolean;
  content: DrawerContent;
  /** Term the glossary list should scroll to and focus, set when the drawer was
   * opened from a term popover's "用語集で見る". Dropped whenever the drawer
   * closes or leaves the glossary pane, so reopening it later doesn't jump to a
   * term the reader has since moved on from. */
  focusTermId?: string;
}

export const CLOSED_DRAWER: Readonly<DrawerState> = { isOpen: false, content: "lesson" };

/**
 * Initial drawer state for the theme page. A `?lesson=<id>` deep link that
 * resolved to a known lesson (see navigation.ts's findLessonByParam) opens the
 * drawer straight away -- this is the only path that opens it without a click,
 * and the home screen's lesson links depend on it.
 */
export function initialDrawerState(hasInitialLesson: boolean): DrawerState {
  return hasInitialLesson ? { isOpen: true, content: "lesson" } : { ...CLOSED_DRAWER };
}

/**
 * Header-row button behaviour: pressing the button for the pane that is already
 * showing closes the drawer, pressing the other one switches panes while
 * staying open. Never carries `focusTermId` across, since that only makes sense
 * for a glossary pane opened from a specific term.
 */
export function toggleDrawer(state: DrawerState, content: DrawerContent): DrawerState {
  if (state.isOpen && state.content === content) {
    return { isOpen: false, content };
  }
  return { isOpen: true, content };
}

/** Opens the glossary pane focused on one term (from a popover's "用語集で見る").
 * Takes no prior state: the result is the same whatever the drawer was showing. */
export function openGlossaryAt(termId: string): DrawerState {
  return { isOpen: true, content: "glossary", focusTermId: termId };
}

export function closeDrawer(state: DrawerState): DrawerState {
  return { isOpen: false, content: state.content };
}
