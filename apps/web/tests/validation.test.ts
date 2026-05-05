import { describe, expect, it } from "vitest";
import { eventSkuSchema, refreshBodySchema, teamNumberSchema } from "../src/lib/validation";

describe("api validation", () => {
  it("accepts normalized VEX identifiers", () => {
    expect(teamNumberSchema.parse("7157b")).toBe("7157B");
    expect(eventSkuSchema.parse("re-v5rc-26-4025")).toBe("RE-V5RC-26-4025");
  });

  it("rejects malformed identifiers", () => {
    expect(teamNumberSchema.safeParse("../../etc/passwd").success).toBe(false);
    expect(eventSkuSchema.safeParse("javascript:alert(1)").success).toBe(false);
  });

  it("validates refresh payload source values", () => {
    expect(refreshBodySchema.safeParse({ teamNumber: "7157B", eventSku: "RE-V5RC-26-4025", source: "mock" }).success).toBe(true);
    expect(refreshBodySchema.safeParse({ teamNumber: "7157B", eventSku: "RE-V5RC-26-4025", source: "aws-lambda-but-worse" }).success).toBe(false);
  });
});
