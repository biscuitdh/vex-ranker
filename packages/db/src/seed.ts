import { createPool } from "./index.js";

export async function seedDefaultData(): Promise<void> {
  const pool = createPool();
  const client = await pool.connect();
  try {
    await client.query("begin");
    const event = await client.query<{ id: string }>(
      `
      insert into events (
        event_sku, source_event_id, name, start_at, end_at, city, region, country, raw_json
      ) values (
        'RE-V5RC-26-4025',
        4025,
        '2026 VEX Robotics World Championship',
        '2026-04-21T00:00:00Z',
        '2026-04-24T23:59:59Z',
        'Dallas',
        'TX',
        'United States',
        '{}'::jsonb
      )
      on conflict (event_sku) do update set updated_at = now()
      returning id
      `
    );
    const division = await client.query<{ id: string }>(
      `
      insert into event_divisions (event_id, source_division_id, name)
      values ($1, 1, 'Technology')
      on conflict (event_id, name) do update set updated_at = now()
      returning id
      `,
      [event.rows[0].id]
    );
    const team = await client.query<{ id: string }>(
      `
      insert into teams (team_number, team_name, organization)
      values ('7157B', 'Mystery Machine', 'Chittenango High School, NY')
      on conflict (team_number) do update set
        team_name = excluded.team_name,
        organization = excluded.organization,
        updated_at = now()
      returning id
      `
    );
    await client.query(
      `
      insert into event_teams (event_id, division_id, team_id)
      values ($1, $2, $3)
      on conflict do nothing
      `,
      [event.rows[0].id, division.rows[0].id, team.rows[0].id]
    );
    await client.query("commit");
    process.stdout.write(JSON.stringify({ level: "info", message: "seed complete", team: "7157B" }) + "\n");
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  seedDefaultData().catch((error) => {
    process.stderr.write(JSON.stringify({ level: "error", message: "seed failed", error: String(error) }) + "\n");
    process.exitCode = 1;
  });
}
