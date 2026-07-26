import { expect, test } from "@playwright/test";

/**
 * The acceptance condition for moving the lesson out of a fixed overlay and into a
 * side drawer: a lesson tells you to look at a panel, so it must never be sitting on
 * top of one. Playwright's default 1280x720 viewport is above the `lg` breakpoint,
 * which is where the drawer becomes a column beside the grid.
 *
 * Deliberately independent of broker state -- the layout is the same whether or not
 * any events have arrived -- so this does not need the long rebalance waits
 * lesson-b.e2e.ts does. Requires gateway (4000) and web (3000) running.
 */
test("lesson drawer sits beside the visualisation, never over it", async ({ page }) => {
  await page.goto("/themes/kafka");

  await page.getByRole("button", { name: "レッスン", exact: true }).click();

  const drawer = page.getByRole("complementary", { name: "ガイド付きレッスン" });
  await expect(drawer).toBeVisible();

  const canvasBox = await page.getByRole("region", { name: /Topology|トポロジー/ }).boundingBox();
  const drawerBox = await drawer.boundingBox();
  if (canvasBox === null || drawerBox === null) {
    throw new Error("expected both the topology panel and the drawer to be laid out");
  }

  // 1px of slack for subpixel rounding of the flex gap.
  expect(canvasBox.x + canvasBox.width).toBeLessThanOrEqual(drawerBox.x + 1);
});

test("the lesson drawer toggles shut when its own button is pressed again", async ({ page }) => {
  await page.goto("/themes/kafka");
  const toggle = page.getByRole("button", { name: "レッスン", exact: true });

  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-expanded", "true");

  await toggle.click();

  await expect(toggle).toHaveAttribute("aria-expanded", "false");
});

/**
 * Guards the one path that opens the drawer without a click. The home screen's lesson
 * links rely on it, and it is easy to lose: the state that used to do this lived in
 * LessonPanel and moved to the page during the drawer rework.
 */
test("a ?lesson= deep link opens the drawer already on that lesson", async ({ page }) => {
  await page.goto("/themes/kafka?lesson=partitioning-101");

  await expect(
    page.getByRole("complementary", { name: "ガイド付きレッスン" }).getByText("Partitioning 入門"),
  ).toBeVisible();
});
