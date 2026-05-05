import { describe, expect, it } from "vitest";
import { matchSnapshotHash, rankDelta, rankSnapshotHash } from "../src/index.js";

describe("snapshot hashes", () => {
  it("keeps unchanged rank states stable", () => {
    const base = {
      eventSku: "re-v5rc-26-4025",
      divisionName: "Technology",
      teamNumber: "7157b",
      rank: 5,
      wins: 4,
      losses: 1,
      ties: 0,
      wp: 8,
      ap: 20,
      sp: 30,
      averageScore: 42,
      recordText: "4-1-0"
    };

    expect(rankSnapshotHash(base)).toEqual(rankSnapshotHash({ ...base, teamNumber: "7157B" }));
    expect(rankSnapshotHash(base)).not.toEqual(rankSnapshotHash({ ...base, rank: 4 }));
  });

  it("keeps unchanged match states stable", () => {
    const base = {
      eventSku: "RE-V5RC-26-4025",
      divisionName: "Technology",
      matchKey: "Q1",
      matchType: "Q",
      roundLabel: "Q1",
      instance: 1,
      status: "completed" as const,
      scheduledTime: "2026-04-21T12:00:00Z",
      completedTime: "2026-04-21T12:15:00Z",
      fieldName: "Google",
      redScore: 20,
      blueScore: 18,
      redTeams: ["7157B", "1234A"],
      blueTeams: ["9999X", "8888B"]
    };

    expect(matchSnapshotHash(base)).toEqual(matchSnapshotHash({ ...base, redTeams: ["1234A", "7157B"] }));
    expect(matchSnapshotHash(base)).not.toEqual(matchSnapshotHash({ ...base, blueScore: 21 }));
  });
});

describe("rank delta", () => {
  it("reports upward movement when numeric rank decreases", () => {
    expect(rankDelta({ rank: 3, recordText: "5-1-0" }, { rank: 7, recordText: "4-1-0" })).toEqual({
      rankChange: 4,
      rankDirection: "up",
      recordChanged: true
    });
  });
});
