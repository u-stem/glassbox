import Link from "next/link";
import { ThemeToggle } from "@/color-scheme/ThemeToggle";

/** Shared chrome across all pages: the brand (a link back home) on the left and the
 * theme toggle on the right. Theme navigation is intentionally absent -- themes are
 * picked from the home screen's cards. */
export function AppHeader() {
  return (
    <header className="viz-root sticky top-0 z-40 border-b border-(--border) bg-(--surface-1)">
      <div className="mx-auto flex h-14 max-w-7xl items-center justify-between px-8">
        <Link href="/" className="text-base font-bold text-(--text-primary)">
          glassbox
        </Link>
        <ThemeToggle />
      </div>
    </header>
  );
}
