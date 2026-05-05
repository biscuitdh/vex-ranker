import { createPool } from "@vex-ranker/db";

declare global {
  // eslint-disable-next-line no-var
  var __vexRankerPool: ReturnType<typeof createPool> | undefined;
}

export function getWebPool(): ReturnType<typeof createPool> {
  if (!globalThis.__vexRankerPool) {
    globalThis.__vexRankerPool = createPool();
  }
  return globalThis.__vexRankerPool;
}
