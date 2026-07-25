import { describe, expect, test } from "bun:test";
import { applyThemePreference, nextThemePreference, parseThemePreference } from "./color-scheme";

describe("parseThemePreference", () => {
  test("accepts 'light'", () => {
    expect(parseThemePreference("light")).toBe("light");
  });

  test("accepts 'dark'", () => {
    expect(parseThemePreference("dark")).toBe("dark");
  });

  test("falls back to 'system' for an unknown string", () => {
    expect(parseThemePreference("blue")).toBe("system");
  });

  test("falls back to 'system' for null (nothing stored)", () => {
    expect(parseThemePreference(null)).toBe("system");
  });
});

describe("nextThemePreference", () => {
  test("cycles system to light", () => {
    expect(nextThemePreference("system")).toBe("light");
  });

  test("cycles light to dark", () => {
    expect(nextThemePreference("light")).toBe("dark");
  });

  test("cycles dark back to system", () => {
    expect(nextThemePreference("dark")).toBe("system");
  });
});

interface FakeRoot {
  attributes: Map<string, string>;
}

function makeFakeRoot(initial?: Record<string, string>): FakeRoot & {
  setAttribute: (name: string, value: string) => void;
  removeAttribute: (name: string) => void;
} {
  const attributes = new Map(Object.entries(initial ?? {}));
  return {
    attributes,
    setAttribute: (name, value) => attributes.set(name, value),
    removeAttribute: (name) => attributes.delete(name),
  };
}

describe("applyThemePreference", () => {
  test("sets data-theme for an explicit preference", () => {
    const root = makeFakeRoot();

    applyThemePreference(root, "dark");

    expect(root.attributes.get("data-theme")).toBe("dark");
  });

  test("removes data-theme for 'system' so the OS preference wins", () => {
    const root = makeFakeRoot({ "data-theme": "light" });

    applyThemePreference(root, "system");

    expect(root.attributes.has("data-theme")).toBe(false);
  });
});
