import { expect, test } from "@playwright/test";

/**
 * Smoke tests for the home screen (Phase C) and deep-linking straight into a lesson
 * (Phase B): opening `/themes/kafka?lesson=<id>` should land on the theme page with
 * that lesson's panel already expanded, instead of requiring the "レッスン" button +
 * lesson list click every time. Requires gateway (4000) and web (3000) already
 * running (see README's setup steps) -- run via `bun run test:e2e`, not part of the
 * regular `bun test` suite.
 */
test("deep link: opening the URL with ?lesson= opens that lesson's panel", async ({ page }) => {
  await page.goto("/themes/kafka?lesson=partitioning-101");

  const lessonPanel = page.getByRole("complementary", { name: "ガイド付きレッスン" });
  await expect(lessonPanel.getByText("Partitioning 入門")).toBeVisible();
  await expect(lessonPanel.getByText(/1 \/ \d+/)).toBeVisible();
  await expect(page.getByRole("heading", { name: "パーティションとは" })).toBeVisible();
});

test("home: shows the Kafka card, both lesson links, and settles on ready", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Kafka" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Partitioning 入門" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Consumer group リバランス" })).toBeVisible();
  await expect(page.getByText("Gateway 接続 OK / Kafka 稼働中")).toBeVisible();
});

test("home: clicking a lesson link deep-links into that lesson", async ({ page }) => {
  await page.goto("/");

  await page.getByRole("link", { name: "Partitioning 入門" }).click();

  await expect(page).toHaveURL(/\/themes\/kafka\?lesson=partitioning-101$/);
  await expect(
    page.getByRole("complementary", { name: "ガイド付きレッスン" }).getByText(/1 \/ \d+/),
  ).toBeVisible();
});

test("theme toggle: cycles system -> light -> dark -> system and persists", async ({ page }) => {
  // The reload below (with a stored theme applied pre-hydration by the layout's
  // inline script) is exactly the scenario that once triggered a React hydration
  // mismatch on <html data-theme>; collect console errors to catch a regression.
  const hydrationErrors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error" && /hydrat/i.test(message.text())) {
      hydrationErrors.push(message.text());
    }
  });
  await page.goto("/");
  const toggle = page.getByRole("button", { name: "配色テーマを切り替える" });
  const html = page.locator("html");

  await expect(toggle).toHaveAttribute("title", "配色テーマ: システム");
  await expect(html).not.toHaveAttribute("data-theme");

  await toggle.click();
  await expect(html).toHaveAttribute("data-theme", "light");

  await toggle.click();
  await expect(html).toHaveAttribute("data-theme", "dark");

  await page.reload();
  await expect(html).toHaveAttribute("data-theme", "dark");
  await expect(toggle).toHaveAttribute("title", "配色テーマ: ダーク");

  await toggle.click();
  await expect(html).not.toHaveAttribute("data-theme");

  expect(hydrationErrors).toEqual([]);
});
