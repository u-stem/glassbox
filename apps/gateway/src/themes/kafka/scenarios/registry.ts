import { z } from "zod";

export interface ScenarioDefinition<Params> {
  id: string;
  /** Human-readable name shown by the web UI's auto-generated scenario form
   * (GET /api/themes/kafka/scenarios). Falls back to `id` if omitted. */
  title?: string;
  /** Short explanation shown alongside the form; omitted if not provided. */
  description?: string;
  paramsSchema: z.ZodType<Params>;
  /**
   * Optional synchronous conflict check (e.g. produce-burst's single-flight guard).
   * Checked before start() is invoked, so the HTTP handler can respond 409 without
   * waiting on start()'s own async lifecycle.
   */
  isConflicting?: () => boolean;
  /**
   * Optional synchronous capacity check (e.g. add-consumer's MAX_CONSUMERS guard).
   * Checked before start() is invoked, same timing as isConflicting, but reported as
   * its own "capacity" status (mapped to HTTP 422) rather than "conflict" (409):
   * this is a resource limit, not a mutual-exclusion conflict.
   */
  isAtCapacity?: () => boolean;
  /**
   * Kicks off the scenario. The returned promise represents the scenario's full
   * async lifecycle and is intentionally NOT awaited by the dispatcher (see
   * ScenarioHandle.dispatch): scenarios publish their own scenario.started/finished
   * events and the HTTP response returns as soon as the scenario is accepted.
   */
  start: (params: Params) => Promise<void>;
}

/**
 * Params-erased description of a scenario for the web UI's form generator: id/title/
 * description plus paramsSchema rendered as a JSON Schema object. Uses zod's native
 * `z.toJSONSchema` (available since zod 4, no extra dependency) with `io: "input"` so
 * that defaulted params (e.g. produce-burst's `count`) are NOT listed as `required` --
 * the form generator treats them as optional fields pre-filled with their default,
 * matching what paramsSchema.parse(body) actually accepts.
 */
export interface ScenarioDescription {
  id: string;
  title: string;
  description?: string;
  paramsSchema: Record<string, unknown>;
}

export type DispatchResult =
  | { status: "accepted"; params: unknown }
  | { status: "conflict" }
  | { status: "capacity" }
  | { status: "not-found" };

export interface ScenarioHandle {
  id: string;
  dispatch: (body: unknown, onError: (error: unknown) => void) => DispatchResult;
  describe: () => ScenarioDescription;
}

/**
 * Wraps a ScenarioDefinition<Params> into a Params-erased ScenarioHandle. This is
 * where the generic Params type is "closed over": the returned closure captures
 * paramsSchema/isConflicting/start while Params is still concrete, so a heterogeneous
 * array of ScenarioHandle (one per scenario, each with a different Params) can be
 * built without ever needing an unsound `as` cast to erase Params after the fact.
 */
export function defineScenario<Params>(definition: ScenarioDefinition<Params>): ScenarioHandle {
  return {
    id: definition.id,
    dispatch(body, onError) {
      const params = definition.paramsSchema.parse(body);

      if (definition.isConflicting?.()) {
        return { status: "conflict" };
      }
      if (definition.isAtCapacity?.()) {
        return { status: "capacity" };
      }

      void definition.start(params).catch(onError);

      return { status: "accepted", params };
    },
    describe() {
      return {
        id: definition.id,
        title: definition.title ?? definition.id,
        ...(definition.description === undefined ? {} : { description: definition.description }),
        paramsSchema: z.toJSONSchema(definition.paramsSchema, { io: "input" }),
      };
    },
  };
}

export interface ScenarioRegistry {
  dispatch(id: string, body: unknown, onError: (error: unknown) => void): DispatchResult;
  describeAll(): ScenarioDescription[];
}

/**
 * A minimal registry keyed by scenario id, dispatching to the Params-erased
 * ScenarioHandle built by defineScenario.
 */
export function createScenarioRegistry(handles: ScenarioHandle[]): ScenarioRegistry {
  const map = new Map(handles.map((handle) => [handle.id, handle]));

  return {
    dispatch(id, body, onError) {
      const handle = map.get(id);
      if (handle === undefined) {
        return { status: "not-found" };
      }
      return handle.dispatch(body, onError);
    },
    describeAll() {
      return handles.map((handle) => handle.describe());
    },
  };
}
