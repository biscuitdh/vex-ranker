import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

if (process.argv.includes("--check")) {
  process.exit(0);
}

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const fixtures = {
  event: JSON.parse(readFileSync(join(root, "fixtures/event.json"), "utf8")),
  rankings: JSON.parse(readFileSync(join(root, "fixtures/rankings.json"), "utf8")),
  matches: JSON.parse(readFileSync(join(root, "fixtures/matches.json"), "utf8"))
};

const port = Number(process.env.PORT || 4010);

createServer((request, response) => {
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);
  response.setHeader("content-type", "application/json");

  if (url.pathname === "/health") {
    response.writeHead(200);
    response.end(JSON.stringify({ ok: true }));
    return;
  }

  if (url.pathname === "/api/v2/events") {
    response.writeHead(200);
    response.end(JSON.stringify(fixtures.event));
    return;
  }

  if (url.pathname === "/api/v2/events/4025/divisions/1/rankings") {
    response.writeHead(200);
    response.end(JSON.stringify(fixtures.rankings));
    return;
  }

  if (url.pathname === "/api/v2/events/4025/divisions/1/matches") {
    response.writeHead(200);
    response.end(JSON.stringify(fixtures.matches));
    return;
  }

  response.writeHead(404);
  response.end(JSON.stringify({ error: "not found", path: url.pathname }));
}).listen(port, "0.0.0.0", () => {
  process.stdout.write(JSON.stringify({ level: "info", message: "mock vex api listening", port }) + "\n");
});
