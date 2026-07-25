import { describe, expect, test } from "bun:test";
import { z } from "zod";
import { createScenarioRegistry, defineScenario } from "./registry";

describe("dispatch via createScenarioRegistry/defineScenario", () => {
  test("returns not-found for an unregistered scenario id", () => {
    const registry = createScenarioRegistry([]);

    const result = registry.dispatch("nope", {}, () => {});

    expect(result).toEqual({ status: "not-found" });
  });

  test("parses params with the scenario's own schema and accepts", () => {
    const registry = createScenarioRegistry([
      defineScenario({
        id: "greet",
        paramsSchema: z.object({ name: z.string().default("world") }),
        start: async () => {},
      }),
    ]);

    const result = registry.dispatch("greet", {}, () => {});

    expect(result).toEqual({ status: "accepted", params: { name: "world" } });
  });

  test("returns conflict without calling start when isConflicting is true", () => {
    let started = false;
    const registry = createScenarioRegistry([
      defineScenario({
        id: "busy",
        paramsSchema: z.object({}),
        isConflicting: () => true,
        start: async () => {
          started = true;
        },
      }),
    ]);

    const result = registry.dispatch("busy", {}, () => {});

    expect(result).toEqual({ status: "conflict" });
    expect(started).toBe(false);
  });

  test("returns capacity without calling start when isAtCapacity is true", () => {
    let started = false;
    const registry = createScenarioRegistry([
      defineScenario({
        id: "full",
        paramsSchema: z.object({}),
        isAtCapacity: () => true,
        start: async () => {
          started = true;
        },
      }),
    ]);

    const result = registry.dispatch("full", {}, () => {});

    expect(result).toEqual({ status: "capacity" });
    expect(started).toBe(false);
  });

  test("forwards a rejected start() to onError without throwing", async () => {
    let capturedError: unknown;
    const registry = createScenarioRegistry([
      defineScenario({
        id: "failing",
        paramsSchema: z.object({}),
        start: async () => {
          throw new Error("boom");
        },
      }),
    ]);

    registry.dispatch("failing", {}, (error) => {
      capturedError = error;
    });

    // start() runs asynchronously; give its rejection a tick to reach onError.
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect((capturedError as Error).message).toBe("boom");
  });

  test("describe() reports id/title/paramsSchema as a JSON Schema", () => {
    const handle = defineScenario({
      id: "greet",
      title: "Greet",
      description: "says hello",
      paramsSchema: z.object({ name: z.string().default("world") }),
      start: async () => {},
    });

    const description = handle.describe();

    expect(description.id).toBe("greet");
    expect(description.title).toBe("Greet");
    expect(description.description).toBe("says hello");
    expect(description.paramsSchema.type).toBe("object");
  });

  test("describe() falls back to id as title when title is omitted", () => {
    const handle = defineScenario({
      id: "greet",
      paramsSchema: z.object({}),
      start: async () => {},
    });

    expect(handle.describe().title).toBe("greet");
    expect(handle.describe().description).toBeUndefined();
  });

  test("describe() marks defaulted params as not required (io: input)", () => {
    const handle = defineScenario({
      id: "greet",
      paramsSchema: z.object({ name: z.string().default("world") }),
      start: async () => {},
    });

    const required = handle.describe().paramsSchema.required;

    expect(required).toBeUndefined();
  });

  test("registry.describeAll() lists every registered scenario", () => {
    const registry = createScenarioRegistry([
      defineScenario({ id: "a", paramsSchema: z.object({}), start: async () => {} }),
      defineScenario({ id: "b", paramsSchema: z.object({}), start: async () => {} }),
    ]);

    expect(registry.describeAll().map((d) => d.id)).toEqual(["a", "b"]);
  });

  test("supports multiple independently-typed scenarios in one registry", () => {
    const registry = createScenarioRegistry([
      defineScenario({
        id: "a",
        paramsSchema: z.object({ count: z.number() }),
        start: async () => {},
      }),
      defineScenario({
        id: "b",
        paramsSchema: z.object({ label: z.string() }),
        start: async () => {},
      }),
    ]);

    expect(registry.dispatch("a", { count: 1 }, () => {})).toEqual({
      status: "accepted",
      params: { count: 1 },
    });
    expect(registry.dispatch("b", { label: "x" }, () => {})).toEqual({
      status: "accepted",
      params: { label: "x" },
    });
  });
});
