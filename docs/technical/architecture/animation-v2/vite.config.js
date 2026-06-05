import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { viteSingleFile } from "vite-plugin-singlefile";

// `build` produces a single self-contained `dist/index.html` so the demo stays
// drop-anywhere — same UX as the old plain-JS file. `dev` runs an HMR server.
export default defineConfig({
  plugins: [svelte(), viteSingleFile()],
  build: {
    target: "es2020",
    cssCodeSplit: false,
    assetsInlineLimit: 100000000,
    rollupOptions: { output: { inlineDynamicImports: true } },
  },
});
