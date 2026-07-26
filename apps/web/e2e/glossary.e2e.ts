import { expect, test } from "@playwright/test";

/**
 * The in-screen glossary: a marker beside a panel heading opens a short explanation
 * without leaving the dashboard, and the card can hand off to the full list in the
 * drawer.
 *
 * Independent of broker state -- headings, markers and the drawer render whether or
 * not any Kafka events have arrived -- so unlike lesson-b.e2e.ts this needs no
 * consumer-group setup or long rebalance waits. Requires gateway (4000) and web
 * (3000) running.
 */
test("a heading marker explains its term in place", async ({ page }) => {
  await page.goto("/themes/kafka");

  const marker = page.getByRole("button", { name: "トポロジー とは" });
  await marker.click();

  const card = page.locator(`#${await marker.getAttribute("aria-controls")}`);
  await expect(card).toContainText("トポロジー");
  await expect(card).toContainText("topology");
  await expect(card).toContainText("パルス");
});

test("Escape closes the term card and returns focus to its marker", async ({ page }) => {
  await page.goto("/themes/kafka");
  const marker = page.getByRole("button", { name: "パーティション とは" });

  await marker.click();
  await expect(marker).toHaveAttribute("aria-expanded", "true");

  await page.keyboard.press("Escape");

  await expect(marker).toHaveAttribute("aria-expanded", "false");
  await expect(marker).toBeFocused();
});

test("「用語集で見る」 opens the drawer's glossary at that term", async ({ page }) => {
  await page.goto("/themes/kafka");

  await page.getByRole("button", { name: "パーティション とは" }).click();
  await page.getByRole("button", { name: "用語集で見る" }).click();

  const glossary = page.getByRole("complementary", { name: "用語集" });
  await expect(glossary).toBeVisible();
  await expect(glossary.getByText("追記専用のログ")).toBeVisible();
});

test("the glossary drawer sits beside the visualisation, never over it", async ({ page }) => {
  await page.goto("/themes/kafka");

  await page.getByRole("button", { name: "用語集", exact: true }).click();

  const drawer = page.getByRole("complementary", { name: "用語集" });
  const canvasBox = await page.getByRole("region", { name: /トポロジー/ }).boundingBox();
  const drawerBox = await drawer.boundingBox();
  if (canvasBox === null || drawerBox === null) {
    throw new Error("expected both the topology panel and the drawer to be laid out");
  }

  expect(canvasBox.x + canvasBox.width).toBeLessThanOrEqual(drawerBox.x + 1);
});
