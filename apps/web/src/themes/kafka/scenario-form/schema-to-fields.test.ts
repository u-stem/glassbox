import { describe, expect, test } from "bun:test";
import { schemaToFields } from "./schema-to-fields";

describe("schemaToFields", () => {
  test("maps a string property with a default", () => {
    const fields = schemaToFields({
      type: "object",
      properties: { topic: { type: "string", default: "glassbox.demo" } },
    });

    expect(fields).toEqual([
      { kind: "string", name: "topic", defaultValue: "glassbox.demo", required: false },
    ]);
  });

  test("maps an integer property with min/max bounds", () => {
    const fields = schemaToFields({
      type: "object",
      properties: { count: { type: "integer", default: 10, minimum: 1, maximum: 1000 } },
    });

    expect(fields).toEqual([
      { kind: "number", name: "count", defaultValue: 10, min: 1, max: 1000, required: false },
    ]);
  });

  test("adjusts an integer field's exclusiveMinimum/exclusiveMaximum to the nearest inclusive bound", () => {
    const fields = schemaToFields({
      type: "object",
      properties: {
        sessionTimeoutMs: { type: "integer", exclusiveMinimum: 0 },
        rebalanceTimeoutMs: { type: "integer", exclusiveMaximum: 100 },
      },
    });

    // exclusiveMinimum 0 -> the smallest valid integer is 1, not 0 itself.
    expect(fields[0]).toEqual({
      kind: "number",
      name: "sessionTimeoutMs",
      defaultValue: undefined,
      min: 1,
      max: undefined,
      required: false,
    });
    // exclusiveMaximum 100 -> the largest valid integer is 99, not 100 itself.
    expect(fields[1]).toEqual({
      kind: "number",
      name: "rebalanceTimeoutMs",
      defaultValue: undefined,
      min: undefined,
      max: 99,
      required: false,
    });
  });

  test("passes a non-integer field's exclusiveMinimum/exclusiveMaximum through unadjusted", () => {
    const fields = schemaToFields({
      type: "object",
      properties: { rateFactor: { type: "number", exclusiveMinimum: 0, exclusiveMaximum: 1 } },
    });

    expect(fields[0]).toEqual({
      kind: "number",
      name: "rateFactor",
      defaultValue: undefined,
      min: 0,
      max: 1,
      required: false,
    });
  });

  test("prefers inclusive minimum/maximum over exclusive when both are present", () => {
    const fields = schemaToFields({
      type: "object",
      properties: { count: { type: "integer", minimum: 1, exclusiveMinimum: 0 } },
    });

    expect(fields[0]).toEqual({
      kind: "number",
      name: "count",
      defaultValue: undefined,
      min: 1,
      max: undefined,
      required: false,
    });
  });

  test("maps an enum property, defaulting to its own default or first option", () => {
    const withDefault = schemaToFields({
      type: "object",
      properties: {
        keyStrategy: { type: "string", enum: ["round-robin", "keyed"], default: "keyed" },
      },
    });
    expect(withDefault[0]).toEqual({
      kind: "enum",
      name: "keyStrategy",
      options: ["round-robin", "keyed"],
      defaultValue: "keyed",
      required: false,
    });

    const withoutDefault = schemaToFields({
      type: "object",
      properties: { mode: { type: "string", enum: ["graceful", "kill"] } },
    });
    expect(withoutDefault[0]?.defaultValue).toBe("graceful");
  });

  test("marks a property without a default as required when listed in `required`", () => {
    const fields = schemaToFields({
      type: "object",
      properties: { consumerId: { type: "string" } },
      required: ["consumerId"],
    });

    expect(fields[0]).toEqual({
      kind: "string",
      name: "consumerId",
      defaultValue: "",
      required: true,
    });
  });

  test("falls back to a JSON text field for array/object properties", () => {
    const fields = schemaToFields({
      type: "object",
      properties: { topics: { type: "array", default: ["glassbox.demo"] } },
    });

    expect(fields[0]).toEqual({
      kind: "json",
      name: "topics",
      defaultValue: JSON.stringify(["glassbox.demo"]),
      required: false,
    });
  });

  test("returns an empty list for a schema without properties", () => {
    expect(schemaToFields({ type: "object" })).toEqual([]);
  });

  test("preserves property declaration order", () => {
    const fields = schemaToFields({
      type: "object",
      properties: {
        b: { type: "string" },
        a: { type: "string" },
      },
    });

    expect(fields.map((f) => f.name)).toEqual(["b", "a"]);
  });
});
