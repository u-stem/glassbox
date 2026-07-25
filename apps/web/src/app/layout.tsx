import type { Metadata } from "next";
import type { ReactNode } from "react";
import { AppHeader } from "./AppHeader";
import "./globals.css";

export const metadata: Metadata = {
  title: "glassbox",
  description: "ミドルウェア可視化学習プラットフォーム",
};

/** Applies the stored theme preference before first paint so a forced light/dark
 * choice never flashes the wrong scheme. Must stay a static string (no interpolation)
 * and mirror THEME_STORAGE_KEY / the data-theme contract in color-scheme.ts. */
const THEME_INIT_SCRIPT = `try{var t=localStorage.getItem("glassbox-theme");if(t==="light"||t==="dark")document.documentElement.setAttribute("data-theme",t)}catch(e){}`;

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    // suppressHydrationWarning is scoped to this element's own attributes: the theme
    // init script above sets data-theme on <html> before hydration, which React would
    // otherwise report as a server/client mismatch.
    <html lang="ja" suppressHydrationWarning>
      <body>
        {/* biome-ignore lint/security/noDangerouslySetInnerHtml: static theme-init script, no dynamic input */}
        <script dangerouslySetInnerHTML={{ __html: THEME_INIT_SCRIPT }} />
        <AppHeader />
        {children}
      </body>
    </html>
  );
}
