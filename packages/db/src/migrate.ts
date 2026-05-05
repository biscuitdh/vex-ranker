import { readdir, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createPool } from "./index.js";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const migrationsDir = join(root, "migrations");

export async function runMigrations(): Promise<void> {
  const pool = createPool();
  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query(`
      create table if not exists schema_migrations (
        version text primary key,
        applied_at timestamptz not null default now()
      )
    `);
    const files = (await readdir(migrationsDir)).filter((file) => file.endsWith(".sql")).sort();
    for (const file of files) {
      const version = file.replace(/\.sql$/, "");
      const existing = await client.query("select 1 from schema_migrations where version = $1", [version]);
      if (existing.rowCount) continue;
      const sql = await readFile(join(migrationsDir, file), "utf8");
      await client.query(sql);
      await client.query("insert into schema_migrations (version) values ($1)", [version]);
      process.stdout.write(JSON.stringify({ level: "info", message: "migration applied", version }) + "\n");
    }
    await client.query("commit");
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runMigrations().catch((error) => {
    process.stderr.write(JSON.stringify({ level: "error", message: "migration failed", error: String(error) }) + "\n");
    process.exitCode = 1;
  });
}
