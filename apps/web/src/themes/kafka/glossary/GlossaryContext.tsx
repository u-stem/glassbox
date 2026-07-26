"use client";

import { createContext, useContext } from "react";
import type { GlossaryTermId } from "./types";

/**
 * How a popover's "用語集で見る" reaches the page's drawer state.
 *
 * A context rather than a prop: markers live inside panels several levels down
 * (the timeline's filter row, the scenario controls), and threading a callback
 * through would mean adding a prop to components that otherwise take only the data
 * they render -- including two that are memoized, where an unstable callback would
 * quietly undo the memoization.
 *
 * Deliberately not in the zustand store: this is transient screen state, not part of
 * the Kafka world the store models.
 *
 * The default is a no-op so a marker rendered outside a provider still works as a
 * popover; only the jump to the full list goes missing.
 */
export const GlossaryContext = createContext<{ openGlossary: (id: GlossaryTermId) => void }>({
  openGlossary: () => {},
});

export function useGlossary() {
  return useContext(GlossaryContext);
}
