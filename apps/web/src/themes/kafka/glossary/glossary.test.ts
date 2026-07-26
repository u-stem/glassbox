import { describe, expect, test } from "bun:test";
import { groupTerms, relatedEntries } from "./glossary";
import { CATEGORY_ORDER, GLOSSARY } from "./terms";

describe("groupTerms", () => {
  test("returns categories in reading order", () => {
    expect(groupTerms().map((group) => group.category)).toEqual([...CATEGORY_ORDER]);
  });

  test("places every term in exactly one category", () => {
    const grouped = groupTerms().flatMap((group) => group.entries);

    expect(grouped).toHaveLength(Object.keys(GLOSSARY).length);
  });

  test("labels each category in Japanese", () => {
    expect(groupTerms()[0]?.label).toBe("基本のしくみ");
  });

  test("carries the id alongside each entry so the list can anchor to it", () => {
    const first = groupTerms()[0]?.entries[0];

    expect(first?.id).toBe("topic");
  });
});

describe("relatedEntries", () => {
  test("resolves related ids to full entries", () => {
    expect(relatedEntries("lag").map((entry) => entry.id)).toEqual([
      "end-offset",
      "committed-offset",
      "slow-motion",
    ]);
  });

  test("returns nothing for a term with no related ids", () => {
    // Injected rather than reaching for a real term: every shipped entry currently
    // links somewhere, and this asserts the branch, not that fact.
    const { related: _related, ...withoutRelated } = GLOSSARY["group-id"];
    const glossary = { ...GLOSSARY, "group-id": withoutRelated };

    expect(relatedEntries("group-id", glossary)).toEqual([]);
  });
});
