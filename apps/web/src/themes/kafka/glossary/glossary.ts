import { CATEGORY_LABELS, CATEGORY_ORDER, GLOSSARY } from "./terms";
import type {
  GlossaryEntry,
  GlossaryGroup,
  GlossaryTermId,
  IdentifiedGlossaryEntry,
} from "./types";

type Glossary = Readonly<Record<GlossaryTermId, GlossaryEntry>>;

function identify(id: GlossaryTermId, glossary: Glossary): IdentifiedGlossaryEntry {
  return { id, ...glossary[id] };
}

/**
 * Groups the glossary for the drawer's list, in CATEGORY_ORDER. This is the single
 * authority on that ordering -- the list component walks what this returns rather
 * than sorting again, so the order can't drift between the two.
 *
 * Object.keys order is the declaration order in terms.ts, which is grouped by
 * category already; the filter below re-derives it from `category` regardless, so
 * reordering the literal can't silently reorder the UI.
 */
export function groupTerms(glossary: Glossary = GLOSSARY): readonly GlossaryGroup[] {
  const ids = Object.keys(glossary) as GlossaryTermId[];
  return CATEGORY_ORDER.map((category) => ({
    category,
    label: CATEGORY_LABELS[category],
    entries: ids
      .filter((id) => glossary[id].category === category)
      .map((id) => identify(id, glossary)),
  }));
}

/** Resolves a term's `related` ids into entries, for the chips in its popover. */
export function relatedEntries(
  id: GlossaryTermId,
  glossary: Glossary = GLOSSARY,
): readonly IdentifiedGlossaryEntry[] {
  return (glossary[id].related ?? []).map((relatedId) => identify(relatedId, glossary));
}
