import { NextResponse } from "next/server";
import { z } from "zod";
import {
  isValidEventSku,
  isValidTeamNumber,
  normalizeEventSku,
  normalizeTeamNumber
} from "@vex-ranker/ranking-engine";

export const teamNumberSchema = z.string().transform(normalizeTeamNumber).refine(isValidTeamNumber, {
  message: "Invalid VEX team number"
});

export const eventSkuSchema = z.string().transform(normalizeEventSku).refine(isValidEventSku, {
  message: "Invalid RobotEvents SKU"
});

export const refreshBodySchema = z.object({
  eventSku: eventSkuSchema.optional(),
  teamNumber: teamNumberSchema.optional(),
  source: z.enum(["mock", "live"]).optional()
});

export function jsonError(status: number, code: string, message: string): NextResponse {
  return NextResponse.json({ error: { code, message } }, { status });
}

export function getBearerToken(request: Request): string {
  const header = request.headers.get("authorization") ?? "";
  if (!header.toLowerCase().startsWith("bearer ")) return "";
  return header.slice("bearer ".length).trim();
}

export function getClientRateKey(request: Request): string {
  const forwarded = request.headers.get("x-forwarded-for")?.split(",")[0]?.trim();
  const realIp = request.headers.get("x-real-ip")?.trim();
  return `admin:${forwarded || realIp || "local"}`;
}
