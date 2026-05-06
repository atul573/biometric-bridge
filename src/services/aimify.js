/**
 * aimify.js
 * Forwards parsed device data to the Aimify Render backend via HTTPS REST.
 */

import axios from "axios";
import * as dotenv from "dotenv";
dotenv.config();

const BASE = process.env.AIMIFY_BACKEND_URL;
const API_KEY = process.env.BRIDGE_API_KEY;

const client = axios.create({
  baseURL: BASE,
  timeout: 10000,
  headers: {
    "Content-Type": "application/json",
    "x-bridge-key": API_KEY,
  },
});

/**
 * Send heartbeat to Aimify backend.
 * Updates device.lastHeartbeat and ipAddress in MongoDB.
 */
export async function sendHeartbeat(serialNumber, ipAddress) {
  try {
    await client.post("/biometric/bridge/heartbeat", { serialNumber, ipAddress });
    console.log(`[Aimify] ♥  Heartbeat forwarded for ${serialNumber}`);
  } catch (err) {
    console.error(`[Aimify] ✗ Heartbeat failed: ${err.message}`);
  }
}

/**
 * Forward parsed attendance punch records to Aimify.
 * Aimify will do PIN→teacher mapping and save TeacherAttendance.
 */
export async function sendPunchLogs(serialNumber, records) {
  try {
    const res = await client.post("/biometric/bridge/push", {
      serialNumber,
      records,
    });
    console.log(`[Aimify] ✅ ${records.length} punch(es) forwarded →`, res.data?.message || "OK");
    return res.data;
  } catch (err) {
    console.error(`[Aimify] ✗ Push failed: ${err.response?.data?.message || err.message}`);
    return null;
  }
}

/**
 * Fetch pending commands for this device from Aimify.
 * Returns a command string like "C:123:DATA UPDATE attlog" or null.
 */
export async function fetchPendingCommand(serialNumber) {
  try {
    const res = await client.get(`/biometric/bridge/commands/${serialNumber}`);
    return res.data?.command || null;
  } catch (err) {
    console.error(`[Aimify] ✗ Command fetch failed: ${err.message}`);
    return null;
  }
}
