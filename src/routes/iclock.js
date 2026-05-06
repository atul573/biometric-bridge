/**
 * iclock.js
 * iClock / ZKTeco ADMS push protocol endpoints.
 *
 * The MORX BioFace MSD1K calls these over plain HTTP:
 *   GET  /iclock/cdata?SN=xxx          → handshake / heartbeat
 *   POST /iclock/cdata?SN=xxx&table=ATTLOG → attendance push
 *   GET  /iclock/getrequest?SN=xxx     → poll for commands
 */

import { Router } from "express";
import { parseAttendanceLines } from "../utils/parser.js";
import { sendHeartbeat, sendPunchLogs, fetchPendingCommand } from "../services/aimify.js";

const router = Router();

// ─── GET /iclock/cdata — Handshake & Heartbeat ────────────────────────
router.get("/cdata", async (req, res) => {
  const sn = req.query.SN || req.query.sn;
  if (!sn) return res.status(400).send("ERROR: Missing SN");

  const ip = req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "unknown";
  console.log(`[iClock] ♥  Heartbeat  SN=${sn}  IP=${ip}`);

  // Async — don't block response
  sendHeartbeat(sn, ip).catch(() => {});

  // Device MUST receive plain "OK" — no JSON, no HTML
  res.set("Content-Type", "text/plain");
  return res.send("OK");
});

// ─── POST /iclock/cdata — Attendance Log Push ─────────────────────────
router.post("/cdata", async (req, res) => {
  const sn = req.query.SN || req.query.sn;
  const table = (req.query.table || req.query.Table || "").toUpperCase();

  // Always ACK first so device doesn't retry aggressively
  res.set("Content-Type", "text/plain");
  res.send("OK");

  if (!sn) return;

  // Log raw body for debugging
  const rawBody = typeof req.body === "string" ? req.body : String(req.body || "");
  console.log(`[iClock] 📥 POST cdata  SN=${sn}  table=${table || "none"}  bodyLen=${rawBody.length}`);

  if (rawBody.length > 0) {
    console.log(`[iClock]    Raw body:\n${rawBody.slice(0, 300)}`);
  }

  // Only process ATTLOG table (attendance records)
  if (table && table !== "ATTLOG") {
    console.log(`[iClock]    Skipping non-ATTLOG table: ${table}`);
    return;
  }

  const records = parseAttendanceLines(rawBody);
  if (records.length === 0) {
    console.log(`[iClock]    No valid attendance records parsed`);
    return;
  }

  console.log(`[iClock]    Parsed ${records.length} record(s) → forwarding to Aimify`);
  await sendPunchLogs(sn, records);
});

// ─── GET /iclock/getrequest — Command Poll ────────────────────────────
router.get("/getrequest", async (req, res) => {
  const sn = req.query.SN || req.query.sn;

  res.set("Content-Type", "text/plain");

  if (!sn) return res.send("OK");

  console.log(`[iClock] 📡 getrequest  SN=${sn}`);

  const cmd = await fetchPendingCommand(sn);
  if (cmd) {
    console.log(`[iClock]    Serving command: ${cmd}`);
    return res.send(cmd + "\n\n");
  }

  return res.send("OK");
});

export default router;
