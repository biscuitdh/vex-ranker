import { describe, expect, it } from "vitest";
import { normalizeEvent, normalizeMatches, normalizeRankings } from "../src/index.js";

describe("RobotEvents normalization", () => {
  it("normalizes event metadata and divisions", () => {
    const event = normalizeEvent({
      id: 4025,
      sku: "re-v5rc-26-4025",
      name: "Worlds",
      start: "2026-04-21",
      end: "2026-04-24",
      location: { city: "Dallas", region: "TX", country: "United States" },
      divisions: [{ id: 1, name: "Technology" }]
    });

    expect(event.eventSku).toBe("RE-V5RC-26-4025");
    expect(event.divisions[0]).toEqual({ sourceDivisionId: 1, name: "Technology" });
  });

  it("normalizes ranking rows", () => {
    const rankings = normalizeRankings({
      eventSku: "RE-V5RC-26-4025",
      divisionName: "Technology",
      eventName: "Worlds",
      fetchedAt: "2026-04-21T12:00:00Z",
      payload: {
        data: [{
          rank: 2,
          wins: 5,
          losses: 1,
          ties: 0,
          wp: 10,
          ap: 20,
          sp: 30,
          average_score: 42,
          team: { number: "7157b", team_name: "Mystery Machine", organization: "Chittenango" }
        }]
      }
    });

    expect(rankings[0]).toMatchObject({
      teamNumber: "7157B",
      recordText: "5-1-0",
      rank: 2,
      source: "robotevents"
    });
  });

  it("normalizes match rows", () => {
    const matches = normalizeMatches({
      eventSku: "RE-V5RC-26-4025",
      divisionName: "Technology",
      fetchedAt: "2026-04-21T12:00:00Z",
      payload: {
        data: [{
          id: 100,
          name: "Q1",
          round: "Q",
          instance: 1,
          scheduled: "2026-04-21T12:00:00Z",
          started: "2026-04-21T12:15:00Z",
          field: { name: "Google" },
          alliances: [
            { score: 20, teams: [{ team: { number: "7157B" } }, { team: { number: "1234A" } }] },
            { score: 18, teams: [{ team: { number: "9999X" } }, { team: { number: "8888B" } }] }
          ]
        }]
      }
    });

    expect(matches[0]).toMatchObject({
      matchKey: "100",
      roundLabel: "Q1",
      status: "completed",
      redTeams: ["7157B", "1234A"],
      blueTeams: ["9999X", "8888B"]
    });
  });
});
