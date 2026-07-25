/** The gateway's base URL (Fastify, see README's setup steps). Centralized here so
 * every caller (the kafka theme page, the home screen's health check, ...) reads the
 * same env var the same way instead of each re-declaring its own fallback. */
export const GATEWAY_URL = process.env.NEXT_PUBLIC_GATEWAY_URL ?? "http://localhost:4000";
