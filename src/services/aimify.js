/**
 * aimify.js
 * Forwards parsed device data to the Aimify backend via HTTPS REST.
 *
 * Uses TWO forwarding strategies:
 *   1. iClock-compatible POST /iclock/cdata?SN=xxx&table=ATTLOG
 *      (matches biometricPush.controller.js exactly)
 *   2. JSON bridge API /biometric/bridge/push
 *      (for richer data, if available)
 */

import axios from "axios";
import crypto from "crypto";
import * as dotenv from "dotenv";
dotenv.config();

const BASE = process.env.AIMIFY_BACKEND_URL;
const API_KEY = process.env.BRIDGE_API_KEY;

// ── JSON client (for future bridge-specific endpoints) ──
const jsonClient = axios.create({
  baseURL: BASE,
  timeout: 15000,
  headers: {
    "Content-Type": "application/json",
    "x-bridge-key": API_KEY,
  },
});

// ── Plain text client (for iClock-compatible endpoint) ──
const textClient = axios.create({
  baseURL: BASE,
  timeout: 15000,
  headers: {
    "Content-Type": "text/plain",
    "x-bridge-key": API_KEY,
  },
});

/**
 * Forward punch logs using the iClock-compatible format.
 * This directly feeds into biometricPush.controller.js → handleAttendancePush()
 *
 * @param {string} serialNumber - Device serial number
 * @param {Array} records - Array of { pin, timestamp, status, verifyMode, rawLine }
 */
export async function sendPunchLogs(serialNumber, records) {
  if (!BASE) {
    console.error("[Aimify] ✗ AIMIFY_BACKEND_URL not configured");
    return null;
  }

  try {
    // Build tab-delimited body matching ZKTeco ATTLOG format:
    // PIN\tTimestamp\tStatus\tVerifyMode\tWorkCode\tReserved1\tReserved2
    const body = records.map(r => r.rawLine).join("\n");

    const url = `/iclock/cdata?SN=${encodeURIComponent(serialNumber)}&table=ATTLOG`;

    console.log(`[Aimify] 📤 Forwarding ${records.length} record(s) to ${BASE}${url}`);
    console.log(`[Aimify]    Body: ${body}`);

    const res = await textClient.post(url, body);

    console.log(`[Aimify] ✅ ${records.length} punch(es) forwarded → ${res.status} ${res.data}`);
    return res.data;
  } catch (err) {
    const errMsg = err.response
      ? `${err.response.status} ${JSON.stringify(err.response.data)}`
      : err.message;
    console.error(`[Aimify] ✗ Forward failed: ${errMsg}`);
    throw err; // Re-throw so caller can handle retry
  }
}

/**
 * Send heartbeat to Aimify backend (optional).
 */
export async function sendHeartbeat(serialNumber, ipAddress) {
  if (!BASE) return;
  try {
    // Use the iClock GET handshake endpoint
    await textClient.get(`/iclock/cdata?SN=${encodeURIComponent(serialNumber)}`);
    console.log(`[Aimify] ♥  Heartbeat forwarded for ${serialNumber}`);
  } catch (err) {
    console.error(`[Aimify] ✗ Heartbeat failed: ${err.message}`);
  }
}

/**
 * Fetch pending commands for this device from Aimify.
 */
export async function fetchPendingCommand(serialNumber) {
  if (!BASE) return null;
  try {
    const res = await textClient.get(
      `/iclock/getrequest?SN=${encodeURIComponent(serialNumber)}`
    );
    const cmd = res.data?.trim();
    if (cmd && cmd !== "OK") return cmd;
    return null;
  } catch (err) {
    console.error(`[Aimify] ✗ Command fetch failed: ${err.message}`);
    return null;
  }
}
