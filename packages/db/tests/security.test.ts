import { describe, expect, it } from "vitest";
import { verifyAdminToken } from "../src/index.js";

describe("admin token verification", () => {
  it("accepts only exact tokens", () => {
    expect(verifyAdminToken("local-admin-token", "local-admin-token")).toBe(true);
    expect(verifyAdminToken("local-admin-token ", "local-admin-token")).toBe(false);
    expect(verifyAdminToken("", "local-admin-token")).toBe(false);
    expect(verifyAdminToken("local-admin-token", "")).toBe(false);
  });
});
