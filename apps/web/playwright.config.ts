import { defineConfig } from "@playwright/test";

/**
 * Playwright E2E smoke config (Phase 4). Kept separate from `bun test` (see
 * package.json's `test:e2e` script) since these tests need a real gateway + web +
 * Kafka broker running, not just in-process unit/integration tests.
 *
 * Deliberately does not manage the gateway/web/broker lifecycle itself (no
 * `webServer` config): this theme's dev workflow already keeps them running
 * separately (see README's setup steps), and gateway state persists across a single
 * long-lived broker (consumer ids are auto-numbered across the whole process
 * lifetime, see ConsumerRegistry) in a way that's easier to reason about with one
 * shared, already-running instance than a fresh one spun up per test run.
 */
export default defineConfig({
  testDir: "./e2e",
  // Named *.e2e.ts (not Playwright's own default *.spec.ts) so `bun test` -- which
  // matches *.spec.ts by default just like Playwright does -- never tries to load
  // these files itself and collides with Playwright's own test() registration.
  testMatch: "**/*.e2e.ts",
  // Generous: lesson-b.e2e.ts's rebalance waits can legitimately take up to the
  // broker's own session/rebalance timeout when a previous run's killed consumer is
  // still broker-side pending (see that test's own doc).
  timeout: 180_000,
  // A "kill"-mode removal severing one consumer's connection has been observed to
  // also disrupt a sibling consumer's in-flight metadata refresh against the same
  // gateway process (a real, pre-existing @platformatic/kafka@2.8.0 characteristic
  // this theme's kill scenario is prone to trigger -- see
  // docs/adr/0004-process-resilience.md; the gateway itself stays up thanks to that
  // ADR's uncaughtException net, but the affected consumer can still drop out of the
  // group and need to rejoin). Retrying absorbs that pre-existing flakiness rather
  // than papering over a bug introduced by this test.
  retries: 2,
  use: {
    baseURL: process.env.E2E_WEB_URL ?? "http://localhost:3000",
    trace: "retain-on-failure",
  },
});
