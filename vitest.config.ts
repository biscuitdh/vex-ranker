import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: [
      "apps/**/*.test.ts",
      "packages/**/*.test.ts",
      "workers/**/*.test.ts"
    ],
    globals: true,
    environment: "node"
  },
  resolve: {
    alias: {
      "@vex-ranker/db": new URL("./packages/db/src/index.ts", import.meta.url).pathname,
      "@vex-ranker/ranking-engine": new URL("./packages/ranking-engine/src/index.ts", import.meta.url).pathname,
      "@vex-ranker/vex-client": new URL("./packages/vex-client/src/index.ts", import.meta.url).pathname,
      "@vex-ranker/collector": new URL("./workers/collector/src/index.ts", import.meta.url).pathname
    }
  }
});
