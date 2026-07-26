import cors from "@fastify/cors";
import {
  brokerActionRequestSchema,
  type GlassboxEvent,
  type HealthzResponse,
} from "@glassbox/schema";
import { Admin } from "@platformatic/kafka";
import Fastify from "fastify";
import { ulid } from "ulid";
import { ZodError, z } from "zod";
import { parseEnv } from "./env";
import { EventBus } from "./event-bus";
import { createConsumerRegistry } from "./themes/kafka/actors/consumer-registry";
import { ProducerActor } from "./themes/kafka/actors/producer-actor";
import { createAdminPoller } from "./themes/kafka/admin-poller";
import { resolveComposePath } from "./themes/kafka/broker/compose-path";
import { createBrokerController } from "./themes/kafka/broker/controller";
import { createExecFileDockerRunner } from "./themes/kafka/broker/docker-cli";
import { toBrokerHttpStatus } from "./themes/kafka/broker/http-status";
import { startKafkaCollector } from "./themes/kafka/collector";
import { wireGroupStateTracker } from "./themes/kafka/group-state-wiring";
import {
  createAddConsumerScenario,
  createRemoveConsumerScenario,
  createSlowConsumerScenario,
} from "./themes/kafka/scenarios/consumer-scenarios";
import {
  createProduceBurstRunner,
  produceBurstParamsSchema,
} from "./themes/kafka/scenarios/produce-burst";
import { createScenarioRegistry, defineScenario } from "./themes/kafka/scenarios/registry";

const env = parseEnv(process.env);
const bootstrapBrokers = env.KAFKA_BROKERS.split(",").map((broker) => broker.trim());

const DEMO_TOPIC = "glassbox.demo";
const DEMO_TOPIC_PARTITIONS = 4;

const app = Fastify({ logger: true });
const eventBus = new EventBus({ capacity: env.EVENT_BUFFER_CAPACITY });

/**
 * Last-resort safety net for a genuine upstream bug found while investigating
 * repeated add-consumer/remove-consumer(kill) cycling crashing the gateway (Phase 2
 * follow-up investigation): @platformatic/kafka@2.8.0's Consumer#handleError calls
 * `kafkaError.findBy(...)` on an error object that, in at least one code path
 * (repeated metadata-refresh failures against a connection this gateway is
 * deliberately keeping severed for the kill scenario -- see
 * ConsumerActor#close's "kill" branch), turns out not to have a `findBy` method,
 * throwing a plain synchronous TypeError with no enclosing try/catch anywhere in the
 * call stack. This is NOT an EventEmitter 'error' emission (those are separately
 * handled via onClientError on every actor/admin client; see their docs) and cannot
 * be intercepted with a `.on('error', ...)` listener -- Node's only hook for a bare
 * uncaught synchronous exception is `process.on('uncaughtException', ...)`.
 *
 * This is a deliberate, narrow safety net for this specific class of third-party bug
 * that our own kill-scenario design is prone to trigger (repeatedly destroying a
 * consumer's connections is an edge case the library's own tests likely don't cover),
 * not a general "swallow every crash" policy: it only logs and keeps the process
 * alive, it never suppresses errors we can otherwise handle at their source.
 */
process.on("uncaughtException", (error) => {
  app.log.error(error, "uncaughtException (see server.ts's doc on this handler)");
});
process.on("unhandledRejection", (reason) => {
  app.log.error(reason, "unhandledRejection (see server.ts's doc on this handler)");
});

const admin = new Admin({ clientId: "glassbox-gateway-admin", bootstrapBrokers });
// Required, not just nice-to-have: @platformatic/kafka clients extend Node's
// EventEmitter, and Node throws (crashing the whole process) when 'error' is emitted
// with zero listeners attached. Every client instance in this process needs one.
admin.on("error", (error) => {
  app.log.error(error, "admin client error");
});

const producerActor = new ProducerActor({
  clientId: "glassbox-gateway-producer",
  bootstrapBrokers,
  eventBus,
  onClientError: (error) => {
    app.log.error(error, "producer client error");
  },
});

const stopCollector = startKafkaCollector(eventBus);
const stopGroupStateTracker = wireGroupStateTracker(eventBus);
const produceBurstRunner = createProduceBurstRunner(producerActor, eventBus);

const consumerRegistry = createConsumerRegistry({
  bootstrapBrokers,
  eventBus,
  onConsumeError: (clientId, error) => {
    app.log.error(error, `consumer ${clientId} stopped consuming`);
  },
  onTeardownError: (clientId, error) => {
    app.log.error(error, `consumer ${clientId} kill-mode teardown failed`);
  },
  onClientError: (clientId, error) => {
    app.log.error(error, `consumer ${clientId} client error`);
  },
});

const scenarioRegistry = createScenarioRegistry([
  defineScenario({
    id: "produce-burst",
    title: "Produce burst",
    description: "Sends `count` messages to a topic, one every `rateMs`.",
    paramsSchema: produceBurstParamsSchema,
    isConflicting: () => produceBurstRunner.isRunning(),
    start: (params) => produceBurstRunner.run(params),
  }),
  defineScenario({
    ...createAddConsumerScenario(consumerRegistry, eventBus),
    title: "Add consumer",
    description: "Joins a fresh consumer to the group, triggering a rebalance.",
  }),
  defineScenario({
    ...createRemoveConsumerScenario(consumerRegistry, eventBus),
    title: "Remove consumer",
    description: "Removes a consumer, gracefully (immediate) or by kill (session timeout).",
  }),
  defineScenario({
    ...createSlowConsumerScenario(consumerRegistry, eventBus),
    title: "Slow consumer",
    description: "Adds a per-message processing delay to an existing consumer, so lag accrues.",
  }),
]);

const slowMotionParamsSchema = z.object({
  enabled: z.boolean(),
  factorMs: z.number().int().min(0).max(10_000).default(3000),
});

const adminPoller = createAdminPoller(admin, {
  intervalMs: env.ADMIN_POLL_INTERVAL_MS,
  onSnapshot: (snapshot) => {
    eventBus.publish({
      id: ulid(),
      ts: Date.now(),
      theme: "kafka",
      source: { kind: "admin" },
      type: "admin.snapshot",
      payload: snapshot,
    });
  },
  onError: (error) => {
    app.log.error(error, "admin-poller tick failed");
  },
  // Reconciled rather than done once at boot (ADR-0005): this is what allows the
  // gateway to start with no broker at all, and it also re-creates the topic after a
  // stop/start cycle or a container recreation (compose.yaml declares no named
  // volume, and auto-create is disabled, so a recreated container comes back empty).
  onReachable: async () => {
    await ensureDemoTopic();
  },
});

async function ensureDemoTopic(): Promise<void> {
  const topics = await admin.listTopics({ includeInternals: false });
  if (topics.includes(DEMO_TOPIC)) {
    return;
  }
  await admin.createTopics({
    topics: [DEMO_TOPIC],
    partitions: DEMO_TOPIC_PARTITIONS,
    replicas: 1,
  });
}

app.get("/healthz", async (): Promise<HealthzResponse> => {
  return { ok: true, kafka: adminPoller.kafkaStatus() };
});

/**
 * Broker lifecycle control (ADR-0005). This lives in the gateway rather than in the
 * web app because the gateway is the process that already owns side effects, and it
 * is reachable only on loopback (env.HOST) and only from WEB_ORIGIN (the cors plugin
 * registered in start()) -- the same threat model as the scenario routes below.
 *
 * `/healthz`'s `kafka` field answers a different question (can the gateway talk to
 * the broker *right now*) and both are deliberately kept: the container can be up
 * while the broker is not yet answering.
 */
const brokerController = createBrokerController({
  run: createExecFileDockerRunner(),
  composePath: resolveComposePath({ configuredPath: env.GLASSBOX_COMPOSE_FILE }),
});

app.get("/api/themes/kafka/broker", async (_request, reply) => {
  const outcome = await brokerController.status();
  return reply.code(toBrokerHttpStatus(outcome.broker)).send(outcome);
});

app.post("/api/themes/kafka/broker", async (request, reply) => {
  let action: "start" | "stop";
  try {
    action = brokerActionRequestSchema.parse(request.body).action;
  } catch (error) {
    if (error instanceof ZodError) {
      return reply.code(400).send({ error: error.message });
    }
    throw error;
  }

  const result = await brokerController.apply(action);
  if (result.status === "conflict") {
    return reply.code(409).send({ error: "a broker action is already running" });
  }
  return reply.code(toBrokerHttpStatus(result.outcome.broker)).send(result.outcome);
});

/**
 * SSE stream. On reconnect, resumes from Last-Event-ID via the ring buffer; if the
 * requested seq is older than the buffer retains, sends a control.reset frame
 * (without an `id:` line, so it does not affect the client's Last-Event-ID) followed
 * by the full buffer, per ADR-0002.
 */
app.get("/api/events", (request, reply) => {
  reply.hijack();
  const res = reply.raw;
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    // reply.hijack() bypasses Fastify's normal onSend pipeline, so @fastify/cors's
    // header (registered as a plugin) never reaches this raw response -- it must be
    // set by hand here, or every browser EventSource to this endpoint is blocked by
    // CORS (curl/server-to-server calls don't enforce CORS, so this was invisible
    // until testing against a real browser for Phase 3).
    "Access-Control-Allow-Origin": env.WEB_ORIGIN,
  });
  // Node buffers headers until the first write/flushHeaders; without this, a client
  // with no backlog to resume (no Last-Event-ID) would never see a response at all.
  res.flushHeaders();

  function writeEvent(event: GlassboxEvent, includeId: boolean): void {
    if (includeId) {
      res.write(`id: ${event.seq}\n`);
    }
    res.write(`data: ${JSON.stringify(event)}\n\n`);
  }

  const lastEventIdHeader = request.headers["last-event-id"];
  const isStrictlyNumeric =
    typeof lastEventIdHeader === "string" && /^\d+$/.test(lastEventIdHeader);
  const lastEventId = isStrictlyNumeric ? Number.parseInt(lastEventIdHeader, 10) : undefined;

  const liveListener = (event: GlassboxEvent): void => {
    writeEvent(event, true);
  };

  // getSinceAndSubscribe atomically combines reading the backlog and registering the
  // live listener, so no event published in between can be lost (see event-bus.ts).
  // Always routed through here, even for a brand-new connection with no Last-Event-ID
  // (lastEventId ?? -1): seq starts at 0, so getSince(-1) resolves gap:false and
  // returns the whole current buffer -- a plain subscribe() here would skip that
  // backlog entirely, meaning a fresh page load could miss the last admin.snapshot
  // (topics/groups rendering empty until the next poll tick) or, worse, a client
  // resuming with no Last-Event-ID after a gateway restart would show no history at
  // all until something new happens to be published.
  const { result: resumed, unsubscribe } = eventBus.getSinceAndSubscribe(
    lastEventId ?? -1,
    liveListener,
  );

  if (resumed.gap) {
    const anchorSeq = resumed.events[0]?.seq ?? 0;
    writeEvent(
      {
        id: ulid(),
        seq: anchorSeq,
        ts: Date.now(),
        theme: "kafka",
        source: { kind: "admin" },
        type: "control.reset",
        payload: { reason: "gap" },
      },
      false,
    );
  }
  for (const event of resumed.events) {
    writeEvent(event, true);
  }

  request.raw.on("close", () => {
    unsubscribe();
  });
});

/**
 * Backs the web UI's auto-generated scenario form (Phase 4): lists every registered
 * scenario's id/title/description plus its paramsSchema as a JSON Schema object, so
 * the form generator never needs its own hardcoded copy of each scenario's shape.
 */
app.get("/api/themes/kafka/scenarios", async () => {
  return { scenarios: scenarioRegistry.describeAll() };
});

const scenarioRouteParamsSchema = z.object({ scenarioId: z.string() });

/**
 * Common dispatch for every kafka scenario (produce-burst, add-consumer,
 * remove-consumer, slow-consumer, ...), each registered via defineScenario() above
 * with its own Zod paramsSchema. isConflicting()/start() are still per-scenario (e.g.
 * produce-burst's single-flight guard); this route only owns the HTTP status mapping.
 */
app.post("/api/themes/kafka/scenarios/:scenarioId", async (request, reply) => {
  const { scenarioId } = scenarioRouteParamsSchema.parse(request.params);

  let result: ReturnType<typeof scenarioRegistry.dispatch>;
  try {
    result = scenarioRegistry.dispatch(scenarioId, request.body, (error: unknown) => {
      app.log.error(error, `${scenarioId} scenario failed`);
    });
  } catch (error) {
    if (error instanceof ZodError) {
      return reply.code(400).send({ error: error.message });
    }
    throw error;
  }

  if (result.status === "not-found") {
    return reply.code(404).send({ error: `unknown scenario: ${scenarioId}` });
  }
  if (result.status === "conflict") {
    return reply.code(409).send({ error: `${scenarioId} is already running` });
  }
  if (result.status === "capacity") {
    return reply.code(422).send({ error: `${scenarioId} is at capacity` });
  }
  return reply.code(202).send({ scenarioId, params: result.params });
});

/**
 * Slow-motion toggle: stretches PreparingRebalance to a human-observable length by
 * delaying every consumer's joinGroup protocol metadata resolution (see
 * ConsumerActor's protocolsMetadata hook doc for why this is a real, not simulated,
 * delay, and its limitation re: CompletingRebalance).
 */
app.post("/api/themes/kafka/slow-motion", async (request, reply) => {
  const { enabled, factorMs } = slowMotionParamsSchema.parse(request.body);
  consumerRegistry.setSlowMotion(enabled ? factorMs : 0);
  return reply.code(200).send({ enabled, factorMs: enabled ? factorMs : 0 });
});

/**
 * Deliberately does not touch Kafka: the demo topic is reconciled by the admin
 * poller's onReachable hook instead (ADR-0005). Awaiting a broker RPC here used to
 * mean the gateway could not start at all while the broker was down -- which in turn
 * made "start the broker from the UI" impossible, since the process serving that UI's
 * API was the one that could not boot.
 */
async function start(): Promise<void> {
  await app.register(cors, { origin: env.WEB_ORIGIN });
  adminPoller.start();
  await app.listen({ port: env.PORT, host: env.HOST });
}

start().catch((error: unknown) => {
  app.log.error(error);
  process.exit(1);
});

async function shutdown(): Promise<void> {
  adminPoller.stop();
  stopCollector();
  stopGroupStateTracker();
  await Promise.all(consumerRegistry.all().map((actor) => actor.close("graceful")));
  await producerActor.close();
  await admin.close();
  await app.close();
}

for (const signal of ["SIGTERM", "SIGINT"] as const) {
  process.on(signal, () => {
    void shutdown().finally(() => process.exit(0));
  });
}
