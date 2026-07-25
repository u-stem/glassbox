import { describe, expect, test } from "bun:test";
import { buildScenarioBody, initialFormValues } from "./form-values";
import type { ScenarioFormField } from "./schema-to-fields";

describe("initialFormValues", () => {
  test("stringifies a number field's default", () => {
    const fields: ScenarioFormField[] = [
      { kind: "number", name: "count", defaultValue: 10, min: 1, max: 1000, required: false },
    ];

    expect(initialFormValues(fields)).toEqual({ count: "10" });
  });

  test("leaves a number field without a default as an empty string", () => {
    const fields: ScenarioFormField[] = [
      {
        kind: "number",
        name: "n",
        defaultValue: undefined,
        min: undefined,
        max: undefined,
        required: false,
      },
    ];

    expect(initialFormValues(fields)).toEqual({ n: "" });
  });

  test("uses a string/enum/json field's defaultValue verbatim", () => {
    const fields: ScenarioFormField[] = [
      { kind: "string", name: "topic", defaultValue: "glassbox.demo", required: false },
      { kind: "enum", name: "mode", options: ["a", "b"], defaultValue: "a", required: false },
      { kind: "json", name: "topics", defaultValue: '["x"]', required: false },
    ];

    expect(initialFormValues(fields)).toEqual({
      topic: "glassbox.demo",
      mode: "a",
      topics: '["x"]',
    });
  });
});

describe("buildScenarioBody", () => {
  test("omits a blank optional field so the server default applies", () => {
    const fields: ScenarioFormField[] = [
      { kind: "string", name: "topic", defaultValue: "glassbox.demo", required: false },
    ];

    const { body, errors } = buildScenarioBody(fields, { topic: "" });

    expect(body).toEqual({});
    expect(errors).toEqual({});
  });

  test("reports a blank required field as an error instead of submitting it", () => {
    const fields: ScenarioFormField[] = [
      { kind: "string", name: "consumerId", defaultValue: "", required: true },
    ];

    const { body, errors } = buildScenarioBody(fields, { consumerId: "" });

    expect(body).toEqual({});
    expect(errors).toEqual({ consumerId: "required" });
  });

  test("coerces a number field to a number", () => {
    const fields: ScenarioFormField[] = [
      { kind: "number", name: "count", defaultValue: 10, min: 1, max: 1000, required: false },
    ];

    const { body, errors } = buildScenarioBody(fields, { count: "42" });

    expect(body).toEqual({ count: 42 });
    expect(errors).toEqual({});
  });

  test("reports a non-numeric value for a number field", () => {
    const fields: ScenarioFormField[] = [
      { kind: "number", name: "count", defaultValue: 10, min: 1, max: 1000, required: false },
    ];

    const { body, errors } = buildScenarioBody(fields, { count: "abc" });

    expect(body).toEqual({});
    expect(errors).toEqual({ count: "must be a number" });
  });

  test("passes an enum value through as-is", () => {
    const fields: ScenarioFormField[] = [
      {
        kind: "enum",
        name: "keyStrategy",
        options: ["round-robin", "keyed"],
        defaultValue: "round-robin",
        required: false,
      },
    ];

    const { body } = buildScenarioBody(fields, { keyStrategy: "keyed" });

    expect(body).toEqual({ keyStrategy: "keyed" });
  });

  test("parses a json field's value", () => {
    const fields: ScenarioFormField[] = [
      { kind: "json", name: "topics", defaultValue: '["glassbox.demo"]', required: false },
    ];

    const { body, errors } = buildScenarioBody(fields, { topics: '["a","b"]' });

    expect(body).toEqual({ topics: ["a", "b"] });
    expect(errors).toEqual({});
  });

  test("reports invalid JSON in a json field", () => {
    const fields: ScenarioFormField[] = [
      { kind: "json", name: "topics", defaultValue: '["glassbox.demo"]', required: false },
    ];

    const { body, errors } = buildScenarioBody(fields, { topics: "{not json" });

    expect(body).toEqual({});
    expect(errors).toEqual({ topics: "must be valid JSON" });
  });
});
