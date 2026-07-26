import { expect, test } from "@playwright/test";

const GATEWAY_URL = process.env.E2E_GATEWAY_URL ?? "http://localhost:4000";
/** Comfortably above any consumerId this lesson's own runs (or a previous failed
 * attempt's retries) could have produced -- ConsumerRegistry auto-numbers
 * "consumer-1", "consumer-2", ... across the whole gateway process lifetime, so a
 * failed/retried run leaves earlier-numbered members behind rather than reusing
 * their ids. */
const MAX_PLAUSIBLE_CONSUMER_INDEX = 50;

/**
 * Gracefully removes every consumerId in [1, MAX_PLAUSIBLE_CONSUMER_INDEX] from the
 * demo group before the lesson starts. This test shares one long-lived gateway
 * across repeated runs (see playwright.config.ts's doc); without this, a member left
 * over from a previous failed attempt (retries add a *new* member each time rather
 * than replacing the old one, since the lesson's "add a consumer" step always joins
 * fresh) accumulates across attempts and can make the "add 2nd consumer ->
 * PreparingRebalance" wait below never resolve, because there's no single quiescent
 * moment to observe once several consumers are already mid-join/mid-leave from
 * earlier failed attempts. remove-consumer for a consumerId that was never actually
 * registered is a cheap no-op (ConsumerRegistry#remove returns immediately when the
 * given clientId isn't found; see its own doc) -- this doesn't try to enumerate the
 * group's real membership first because the gateway has no "list members" endpoint.
 */
async function resetConsumerGroup(): Promise<void> {
  await Promise.all(
    Array.from({ length: MAX_PLAUSIBLE_CONSUMER_INDEX }, (_, i) => i + 1).map((index) =>
      fetch(`${GATEWAY_URL}/api/themes/kafka/scenarios/remove-consumer`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ consumerId: `consumer-${index}`, mode: "graceful" }),
      }).catch(() => {
        // Best-effort cleanup; a single failed cleanup call shouldn't fail the test
        // before it has even started.
      }),
    ),
  );
}

const REBALANCE_DIAGRAM_SELECTOR =
  'svg[aria-label="consumer group rebalance state diagram"] circle';
/** Matches RebalanceDiagram's NODES order: Empty, PreparingRebalance,
 * CompletingRebalance, Stable. */
const PREPARING_CIRCLE_INDEX = 1;
const STABLE_CIRCLE_INDEX = 3;
/** RebalanceDiagram renders the current state's circle with this literal `fill`
 * attribute (an SVG presentation attribute, not a computed style -- see
 * RebalanceDiagram.tsx), so this is a stable, non-flaky assertion target. */
const CURRENT_STATE_FILL = "var(--accent)";

/**
 * Long timeout for the two rebalance waits below (not the typical case, which
 * settles in a few seconds): this test shares one long-lived gateway/broker across
 * repeated runs (see playwright.config.ts's doc), and a "kill"-mode member from a
 * *previous* run is still known to the broker (only the gateway/UI already show it
 * as "lost" -- see docs/themes/kafka.md's known limitations) until its own session
 * timeout elapses. A rebalance triggered while that zombie member is still
 * broker-side pending can stall until the broker's own session/rebalance timeout
 * (library defaults: 60s / 102s), not just this lesson's own slow-motion delay.
 */
const REBALANCE_WAIT_TIMEOUT_MS = 150_000;

/**
 * Smoke test for lesson B ("Consumer group リバランス", see
 * apps/web/src/themes/kafka/lessons/lessons.ts): drives it start to finish against a
 * real running gateway + Kafka broker, asserting the state-transition diagram
 * actually highlights the states the lesson claims it will, and that killing a
 * consumer surfaces the "lost" indicator. Requires gateway (4000), web (3000), and
 * the Kafka broker already running (see README's setup steps) -- run via
 * `bun run test:e2e`, not part of the regular `bun test` suite.
 */
test("lesson B: consumer group rebalance walkthrough", async ({ page }) => {
  await resetConsumerGroup();
  await page.goto("/themes/kafka");

  // exact: accessible-name matching is substring by default, and "レッスンを終了"
  // becomes visible once a lesson is open.
  await page.getByRole("button", { name: "レッスン", exact: true }).click();
  await page.getByRole("button", { name: /Consumer group リバランス/ }).click();

  const runStep = page.getByRole("button", { name: "このステップを実行" });
  const nextStep = page.getByRole("button", { name: "次へ" });
  const rebalanceCircles = page.locator(REBALANCE_DIAGRAM_SELECTOR);

  // Step: intro (no action) -> advance.
  await nextStep.click();

  // Step: add the 1st consumer -> the group should settle into Stable.
  await runStep.click();
  await expect(rebalanceCircles.nth(STABLE_CIRCLE_INDEX)).toHaveAttribute(
    "fill",
    CURRENT_STATE_FILL,
    { timeout: REBALANCE_WAIT_TIMEOUT_MS },
  );
  await nextStep.click();

  // Step: enable slow-motion.
  await runStep.click();
  await nextStep.click();

  // Step: add a 2nd consumer while slow-motion is on -> PreparingRebalance should be
  // observably highlighted (this is the whole point of turning slow-motion on first).
  await runStep.click();
  await expect(rebalanceCircles.nth(PREPARING_CIRCLE_INDEX)).toHaveAttribute(
    "fill",
    CURRENT_STATE_FILL,
    { timeout: REBALANCE_WAIT_TIMEOUT_MS },
  );
  await nextStep.click();

  // Step: kill the most recently joined consumer -> its "lost" indicator appears
  // immediately (consumer.connection.lost is published synchronously on kill, well
  // before the broker's own session timeout -- see docs/adr/0004-process-resilience.md's
  // sibling doc in consumer-actor.ts).
  await runStep.click();
  // Scoped to the topology panel, and .first() because a killed member from an
  // earlier run can still be shown as lost alongside this run's: "lost" also appears
  // in the event timeline (consumer.connection.lost) and, once the glossary lands,
  // in its own entry -- an unscoped match would resolve to several elements.
  // The name regex spans both the English and Japanese panel headings so it survives
  // the heading rename.
  await expect(
    page
      .getByRole("region", { name: /Topology|トポロジー/ })
      .getByText(/lost/)
      .first(),
  ).toBeVisible({ timeout: 20_000 });

  await nextStep.click();

  // Step: disable slow-motion (cleanup, so later manual/automated runs aren't left
  // in slow-motion mode).
  await runStep.click();
});
