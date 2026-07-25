import { describe, expect, test } from "bun:test";
import {
  buildMessageKey,
  createProduceBurstRunner,
  ProduceBurstAlreadyRunningError,
  produceBurstParamsSchema,
  runProduceBurst,
} from "./produce-burst";

describe("produceBurstParamsSchema", () => {
  test("applies documented defaults when no fields are given", () => {
    const params = produceBurstParamsSchema.parse({});

    expect(params).toEqual({
      topic: "glassbox.demo",
      count: 10,
      rateMs: 50,
      keyStrategy: "round-robin",
    });
  });

  test("rejects a count above 1000", () => {
    expect(() => produceBurstParamsSchema.parse({ count: 1001 })).toThrow();
  });

  test("rejects a count below 1", () => {
    expect(() => produceBurstParamsSchema.parse({ count: 0 })).toThrow();
  });

  test("rejects a rateMs above 1000", () => {
    expect(() => produceBurstParamsSchema.parse({ rateMs: 1001 })).toThrow();
  });

  test("rejects an unknown keyStrategy", () => {
    expect(() => produceBurstParamsSchema.parse({ keyStrategy: "random" })).toThrow();
  });
});

describe("buildMessageKey", () => {
  test("returns undefined for round-robin", () => {
    expect(buildMessageKey("round-robin", 3)).toBeUndefined();
  });

  test("returns a stable key for a given index under keyed strategy", () => {
    expect(buildMessageKey("keyed", 0)).toBe("key-0");
    expect(buildMessageKey("keyed", 5)).toBe("key-1");
  });
});

describe("runProduceBurst", () => {
  test("sends the configured number of messages", async () => {
    const sent: unknown[] = [];
    const fakeActor = {
      send: async (messages: unknown[]) => {
        sent.push(...messages);
      },
    };
    const published: unknown[] = [];
    const fakeBus = { publish: (event: unknown) => published.push(event) };

    await runProduceBurst(fakeActor, fakeBus, {
      topic: "glassbox.demo",
      count: 3,
      rateMs: 0,
      keyStrategy: "round-robin",
    });

    expect(sent.length).toBe(3);
  });

  test("publishes scenario.started before sending and scenario.finished after", async () => {
    const fakeActor = { send: async () => {} };
    const published: Array<{ type: string }> = [];
    const fakeBus = { publish: (event: { type: string }) => published.push(event) };

    await runProduceBurst(fakeActor, fakeBus, {
      topic: "glassbox.demo",
      count: 1,
      rateMs: 0,
      keyStrategy: "round-robin",
    });

    expect(published.map((e) => e.type)).toEqual(["scenario.started", "scenario.finished"]);
  });

  test("publishes scenario.finished with an error when sending fails", async () => {
    const fakeActor = {
      send: async () => {
        throw new Error("broker unreachable");
      },
    };
    const published: Array<{ type: string; payload: unknown }> = [];
    const fakeBus = {
      publish: (event: { type: string; payload: unknown }) => published.push(event),
    };

    await expect(
      runProduceBurst(fakeActor, fakeBus, {
        topic: "glassbox.demo",
        count: 1,
        rateMs: 0,
        keyStrategy: "round-robin",
      }),
    ).rejects.toThrow("broker unreachable");

    const finished = published.find((e) => e.type === "scenario.finished");
    expect(finished?.payload).toMatchObject({ error: "broker unreachable" });
  });

  test("tags scenario.started and scenario.finished with the same scenarioRunId", async () => {
    const fakeActor = { send: async () => {} };
    const published: Array<{ type: string; payload: { scenarioRunId?: string } }> = [];
    const fakeBus = {
      publish: (event: { type: string; payload: { scenarioRunId?: string } }) =>
        published.push(event),
    };

    await runProduceBurst(fakeActor, fakeBus, {
      topic: "glassbox.demo",
      count: 1,
      rateMs: 0,
      keyStrategy: "round-robin",
    });

    const started = published.find((e) => e.type === "scenario.started");
    const finished = published.find((e) => e.type === "scenario.finished");
    expect(started?.payload.scenarioRunId).toBeDefined();
    expect(finished?.payload.scenarioRunId).toBe(started?.payload.scenarioRunId);
  });

  test("tags each sent message with the run's scenarioRunId in its metadata", async () => {
    const sent: Array<{ metadata?: { scenarioRunId?: string } }> = [];
    const fakeActor = {
      send: async (messages: Array<{ metadata?: { scenarioRunId?: string } }>) => {
        sent.push(...messages);
      },
    };
    const published: Array<{ type: string; payload: { scenarioRunId?: string } }> = [];
    const fakeBus = {
      publish: (event: { type: string; payload: { scenarioRunId?: string } }) =>
        published.push(event),
    };

    await runProduceBurst(fakeActor, fakeBus, {
      topic: "glassbox.demo",
      count: 2,
      rateMs: 0,
      keyStrategy: "round-robin",
    });

    const started = published.find((e) => e.type === "scenario.started");
    expect(sent.every((m) => m.metadata?.scenarioRunId === started?.payload.scenarioRunId)).toBe(
      true,
    );
  });
});

describe("createProduceBurstRunner", () => {
  function makeParams() {
    return {
      topic: "glassbox.demo",
      count: 1,
      rateMs: 0,
      keyStrategy: "round-robin" as const,
    };
  }

  test("runs normally when not already running", async () => {
    const fakeActor = { send: async () => {} };
    const fakeBus = { publish: () => {} };
    const runner = createProduceBurstRunner(fakeActor, fakeBus);

    await runner.run(makeParams());

    expect(runner.isRunning()).toBe(false);
  });

  test("reports isRunning() as true while a run is in flight", async () => {
    let resolveSend: (() => void) | undefined;
    const fakeActor = {
      send: () =>
        new Promise<void>((resolve) => {
          resolveSend = resolve;
        }),
    };
    const fakeBus = { publish: () => {} };
    const runner = createProduceBurstRunner(fakeActor, fakeBus);

    const runPromise = runner.run(makeParams());
    expect(runner.isRunning()).toBe(true);

    resolveSend?.();
    await runPromise;
    expect(runner.isRunning()).toBe(false);
  });

  test("rejects a concurrent run with ProduceBurstAlreadyRunningError", async () => {
    let resolveSend: (() => void) | undefined;
    const fakeActor = {
      send: () =>
        new Promise<void>((resolve) => {
          resolveSend = resolve;
        }),
    };
    const fakeBus = { publish: () => {} };
    const runner = createProduceBurstRunner(fakeActor, fakeBus);

    const firstRun = runner.run(makeParams());
    await expect(runner.run(makeParams())).rejects.toThrow(ProduceBurstAlreadyRunningError);

    resolveSend?.();
    await firstRun;
  });

  test("resets isRunning after a run throws, allowing another run", async () => {
    const fakeActor = {
      send: async () => {
        throw new Error("broker unreachable");
      },
    };
    const fakeBus = { publish: () => {} };
    const runner = createProduceBurstRunner(fakeActor, fakeBus);

    await expect(runner.run(makeParams())).rejects.toThrow("broker unreachable");
    expect(runner.isRunning()).toBe(false);

    await expect(runner.run(makeParams())).rejects.toThrow("broker unreachable");
    expect(runner.isRunning()).toBe(false);
  });
});
