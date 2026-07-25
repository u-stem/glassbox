import { z } from "zod";

const envSchema = z.object({
  PORT: z.coerce.number().int().positive().default(4000),
  // Defaults to loopback-only: this gateway has no authentication of its own, so
  // binding to all interfaces by default would expose scenario-trigger/admin routes
  // to the whole network. Set explicitly (e.g. "0.0.0.0") only in a trusted, isolated
  // environment (see security-auditor's Major finding on this).
  HOST: z.string().default("127.0.0.1"),
  KAFKA_BROKERS: z.string().default("localhost:9092"),
  EVENT_BUFFER_CAPACITY: z.coerce.number().int().min(1).default(1000),
  ADMIN_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(1000),
  WEB_ORIGIN: z.string().default("http://localhost:3000"),
});

export type Env = z.infer<typeof envSchema>;

export function parseEnv(source: Record<string, string | undefined>): Env {
  return envSchema.parse(source);
}
