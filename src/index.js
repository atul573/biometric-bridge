import express from "express";
import net from "net";
import { WebSocketServer } from "ws";
import http from "http";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { createRequire } from "module";
import { sendHeartbeat, sendPunchLogs } from "./services/aimify.js";

const require = createRequire(import.meta.url);
const ZKLib = require("zkteco-js");

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ═══════════════════════════════════════════════════════════════════
// MORX BioFace-MSD1K — Biometric Bridge (Cloud Edition)
// Dual-mode: HTTP (iclock) + Raw TCP (Mantra eBioServer XML protocol)
// ═══════════════════════════════════════════════════════════════════

const PORT = process.env.PORT || 80;
const TCP_PORT = process.env.TCP_PORT || 1018;
const WS_PORT = process.env.WS_PORT || 1018; // WebSocket on same port as TCP

// In-memory store
const state = {
  heartbeats: 0,
  lastHeartbeat: null,
  records: [],        // Parsed attendance records
  rawHits: [],        // Raw event log (for dashboard)
  deviceSN: null,
  startedAt: new Date().toISOString(),
  tcpConnections: 0,
  processedTransIDs: new Set(), // Dedup set
  forwarded: 0,       // Successfully forwarded to Aimify
  forwardErrors: 0,   // Failed forwards
  lastForward: null,
  devices: {},        // Track connected devices: { serialNo: { uid, lastSeen, ip } }
};

function log(msg) {
  const ts = new Date().toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata" });
  console.log(`[${ts}] ${msg}`);
}

// ═══════════════════════════════════════════════════════════════════
// XML PARSER — Parse Mantra eBioServer XML messages
// ═══════════════════════════════════════════════════════════════════

function parseMantraXML(xmlString) {
  // Normalize: strip \0, collapse \r\r\n → \n, remove stray \r
  const clean = xmlString
    .replace(/\0/g, "")
    .replace(/\r\r\n/g, "\n")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "");

  // First extract inner content of <Message>...</Message> wrapper
  const msgMatch = clean.match(/<Message>([\s\S]*?)<\/Message>/);
  const inner = msgMatch ? msgMatch[1] : clean;

  const result = {};
  const tagPattern = /<(\w+)>(.*?)<\/\1>/g;
  let match;
  while ((match = tagPattern.exec(inner)) !== null) {
    result[match[1]] = match[2].trim();
  }
  return result;
}

// ACK byte constants for TCP push protocol
const ACK_BYTE = Buffer.from([0x06]);        // Primary: ASCII ACK
const ACK_OK   = Buffer.from('OK');           // Fallback: plain "OK"

// ═══════════════════════════════════════════════════════════════════
// ZKTeco SDK — Remote device management via UDP port 4370
// ═══════════════════════════════════════════════════════════════════

let clearingInProgress = false;

async function clearDeviceLogs(deviceIP) {
  if (clearingInProgress) {
    log(`⏳ Clear already in progress, skipping`);
    return false;
  }
  clearingInProgress = true;
  log(`🔧 ZK SDK: Connecting to device at ${deviceIP}:4370 to clear logs...`);

  try {
    const zk = new ZKLib(deviceIP, 4370, 5000, 4000);
    await zk.createSocket();
    log(`🔧 ZK SDK: Connected! Clearing attendance log...`);

    await zk.clearAttendanceLog();
    log(`✅ ZK SDK: Attendance log CLEARED successfully!`);

    await zk.disconnect();
    clearingInProgress = false;
    return true;
  } catch (err) {
    log(`❌ ZK SDK: Failed to clear logs: ${err.message}`);
    clearingInProgress = false;
    return false;
  }
}

/**
 * Map Mantra AttendStat → iClock status code
 *   "Duty On"  → 0 (Check-In)
 *   "Duty Off" → 1 (Check-Out)
 *   Others     → 0
 */
function mapAttendStat(stat) {
  if (!stat) return 0;
  const s = stat.toLowerCase();
  if (s.includes("off") || s.includes("out")) return 1;
  return 0;
}

/**
 * Map Mantra VerifMode → iClock verifyMode code
 *   "FP"   → 1 (Fingerprint)
 *   "Face" → 15
 *   "Card" → 4
 */
function mapVerifyMode(mode) {
  if (!mode) return 0;
  const m = mode.toLowerCase();
  if (m === "fp" || m.includes("finger")) return 1;
  if (m.includes("face")) return 15;
  if (m.includes("card")) return 4;
  return 0;
}

/**
 * Build a timestamp string from Mantra XML date fields
 * Returns: "2026-05-05 18:08:15"
 */
function buildTimestamp(parsed) {
  const y = parsed.Year || "2026";
  const mo = (parsed.Month || "1").padStart(2, "0");
  const d = (parsed.Day || "1").padStart(2, "0");
  const h = (parsed.Hour || "0").padStart(2, "0");
  const mi = (parsed.Minute || "0").padStart(2, "0");
  const s = (parsed.Second || "0").padStart(2, "0");
  return `${y}-${mo}-${d} ${h}:${mi}:${s}`;
}

// ═══════════════════════════════════════════════════════════════════
// PROCESS ATTENDANCE — Parse XML, forward to Aimify
// NOTE: ACK is sent BEFORE this function is called (in TCP handler)
// ═══════════════════════════════════════════════════════════════════

async function processMantraMessage(data, remoteAddr) {
  const raw = data.toString("utf8");

  log(`📦 Processing ${data.length} bytes from ${remoteAddr}`);

  const parsed = parseMantraXML(raw);

  // Debug: log what we parsed
  log(`🔍 Parsed: TransID=${parsed.TransID} User=${parsed.UserID} Event=${parsed.Event}`);

  // Validate required fields
  if (!parsed.TransID || !parsed.DeviceSerialNo) {
    log(`⚠️ Invalid XML — missing TransID or SerialNo. Keys: ${Object.keys(parsed).join(", ")}`);
    return;
  }

  const transID = parsed.TransID;
  const serialNo = parsed.DeviceSerialNo;
  const deviceUID = parsed.DeviceUID || "unknown";
  const event = parsed.Event || "Unknown";

  // Track device
  state.devices[serialNo] = {
    uid: deviceUID,
    terminalType: parsed.TerminalType || "Unknown",
    lastSeen: new Date().toISOString(),
    ip: remoteAddr.split(":")[0],
  };
  state.deviceSN = serialNo;

  // ── Always send heartbeat to Aimify (even on duplicates) ──
  state.heartbeats++;
  state.lastHeartbeat = new Date().toISOString();
  sendHeartbeat(serialNo, remoteAddr.split(":")[0]).catch(() => {});

  // ── Check for duplicate (device may resend before ACK arrives) ──
  // Use composite key: TransID can reset to 0 after device log clear
  const dedupKey = `${transID}|${parsed.UserID || ""}|${parsed.Year}-${parsed.Month}-${parsed.Day}-${parsed.Hour}-${parsed.Minute}-${parsed.Second}`;
  if (state.processedTransIDs.has(dedupKey)) {
    log(`⏭️ Duplicate ${dedupKey} — skipping`);
    return;
  }
  state.processedTransIDs.add(dedupKey);

  // Keep dedup set manageable (max 10000 entries)
  if (state.processedTransIDs.size > 10000) {
    const arr = Array.from(state.processedTransIDs);
    state.processedTransIDs = new Set(arr.slice(-5000));
  }

  // ── Process based on event type ──
  if (event === "TimeLog") {
    const userID = parsed.UserID;
    const timestamp = buildTimestamp(parsed);
    const attendStat = parsed.AttendStat || "Unknown";
    const verifMode = parsed.VerifMode || "Unknown";
    const status = mapAttendStat(attendStat);
    const verifyCode = mapVerifyMode(verifMode);

    log(`📋 ATTENDANCE: User ${userID} | ${timestamp} | ${attendStat} | ${verifMode}`);

    // Store record
    const record = {
      transID,
      serialNo,
      userID,
      timestamp,
      attendStat,
      verifMode,
      status,
      verifyCode,
      receivedAt: new Date().toISOString(),
      forwarded: false,
    };
    state.records.push(record);

    // Keep records manageable
    if (state.records.length > 1000) {
      state.records = state.records.slice(-500);
    }

    // ── Forward to Aimify API ──
    try {
      const punchRecord = {
        pin: userID,
        timestamp,
        status,
        verifyMode: verifyCode,
        rawLine: `${userID}\t${timestamp}\t${status}\t${verifyCode}\t0\t0\t0`,
      };

      await sendPunchLogs(serialNo, [punchRecord]);
      record.forwarded = true;
      state.forwarded++;
      state.lastForward = new Date().toISOString();
      log(`✅ Forwarded to Aimify: User ${userID} @ ${timestamp}`);
    } catch (err) {
      state.forwardErrors++;
      log(`❌ Forward FAILED: ${err.message}`);
      // TODO: Phase 4 — queue to SQLite for retry
    }

  } else {
    log(`ℹ️ Non-attendance event: ${event} (TransID: ${transID})`);
  }

  // Save to file log
  try {
    const logDir = process.env.LOG_DIR || "/root";
    const logLine = `\n=== ${new Date().toISOString()} from ${remoteAddr} ===\n${JSON.stringify(parsed, null, 2)}\n`;
    fs.appendFileSync(path.join(logDir, "tcp_messages.log"), logLine);
  } catch (e) { /* ignore log write errors */ }
}

// ═══════════════════════════════════════════════════════════════════
// HTTP SERVER — Dashboard + iClock fallback
// ═══════════════════════════════════════════════════════════════════

const app = express();
app.use(express.json({ limit: "1mb" }));               // Minop JSON protocol
app.use(express.text({ type: "text/*", limit: "1mb" })); // iClock text protocol  
app.use(express.urlencoded({ extended: true, limit: "1mb" }));
app.disable("x-powered-by");

// ── Request logger ──
app.use((req, _res, next) => {
  const body = typeof req.body === "object" ? JSON.stringify(req.body) : req.body;
  log(`${req.method} ${req.originalUrl} from=${req.ip}`);
  if (body && body !== "undefined") log(`  Body: ${String(body).substring(0, 300)}`);
  state.rawHits.push({
    time: new Date().toISOString(),
    method: req.method,
    path: req.originalUrl,
    from: req.ip,
    bodyPreview: body ? String(body).substring(0, 200) : "",
  });
  if (state.rawHits.length > 500) state.rawHits = state.rawHits.slice(-250);
  next();
});

// ── Dashboard ──
app.get("/", (_req, res) => {
  const uptime = Math.floor((Date.now() - new Date(state.startedAt).getTime()) / 1000);
  const h = Math.floor(uptime / 3600);
  const m = Math.floor((uptime % 3600) / 60);
  const s = uptime % 60;

  const deviceList = Object.entries(state.devices).map(([sn, d]) => `
    <tr>
      <td>${sn}</td>
      <td>${d.terminalType}</td>
      <td>${d.uid}</td>
      <td>${d.ip}</td>
      <td>${d.lastSeen}</td>
    </tr>
  `).join("") || "<tr><td colspan=5>No devices connected yet</td></tr>";

  const recentRecords = state.records.slice(-20).reverse().map(r => `
    <tr class="${r.forwarded ? 'ok' : 'err'}">
      <td>${r.timestamp}</td>
      <td>${r.userID}</td>
      <td>${r.attendStat}</td>
      <td>${r.verifMode}</td>
      <td>${r.forwarded ? "✅" : "❌"}</td>
      <td>${r.transID}</td>
    </tr>
  `).join("") || "<tr><td colspan=6>No attendance records yet</td></tr>";

  const recentHits = state.rawHits.slice(-15).reverse().map(h => `
    <tr>
      <td>${h.time}</td>
      <td>${h.method}</td>
      <td>${h.path}</td>
      <td>${h.from}</td>
    </tr>
  `).join("");

  res.send(`<!DOCTYPE html><html lang="en"><head>
    <meta charset="utf-8"><title>Biometric Bridge — Aimify</title>
    <meta http-equiv="refresh" content="15">
    <style>
      *{margin:0;padding:0;box-sizing:border-box}
      body{background:#0a0e17;color:#e0e6ed;font:14px/1.6 'Segoe UI',system-ui,sans-serif;padding:20px}
      h1{color:#38bdf8;margin-bottom:4px;font-size:24px}
      .sub{color:#64748b;margin-bottom:20px}
      .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:24px}
      .card{background:#111827;border:1px solid #1e293b;border-radius:8px;padding:16px;text-align:center}
      .card .num{font-size:28px;font-weight:700;color:#38bdf8}
      .card .label{color:#94a3b8;font-size:12px;text-transform:uppercase}
      .card.green .num{color:#34d399}
      .card.red .num{color:#f87171}
      .card.yellow .num{color:#fbbf24}
      table{width:100%;border-collapse:collapse;margin-bottom:24px;font-size:13px}
      th{background:#1e293b;padding:8px 12px;text-align:left;color:#94a3b8;font-weight:600}
      td{padding:6px 12px;border-bottom:1px solid #1e293b}
      tr.ok td:nth-child(5){color:#34d399}
      tr.err td:nth-child(5){color:#f87171}
      h2{color:#38bdf8;margin:20px 0 8px;font-size:16px}
      .pulse{display:inline-block;width:8px;height:8px;border-radius:50%;background:#34d399;margin-right:6px;animation:pulse 2s infinite}
      @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
    </style></head><body>
    <h1>🌉 Biometric Bridge</h1>
    <p class="sub">Mantra eBioServer XML → Aimify API | Uptime: ${h}h ${m}m ${s}s</p>

    <div class="grid">
      <div class="card"><div class="num">${state.heartbeats}</div><div class="label">Messages</div></div>
      <div class="card green"><div class="num">${state.forwarded}</div><div class="label">Forwarded</div></div>
      <div class="card red"><div class="num">${state.forwardErrors}</div><div class="label">Errors</div></div>
      <div class="card"><div class="num">${state.tcpConnections}</div><div class="label">TCP Connects</div></div>
      <div class="card yellow"><div class="num">${state.records.length}</div><div class="label">Records</div></div>
      <div class="card"><div class="num">${Object.keys(state.devices).length}</div><div class="label">Devices</div></div>
    </div>

    <h2><span class="pulse"></span>Connected Devices</h2>
    <table>
      <tr><th>Serial No</th><th>Type</th><th>Device UID</th><th>IP</th><th>Last Seen</th></tr>
      ${deviceList}
    </table>

    <h2>Recent Attendance Records</h2>
    <table>
      <tr><th>Timestamp</th><th>User ID</th><th>Status</th><th>Verify</th><th>Fwd</th><th>TransID</th></tr>
      ${recentRecords}
    </table>

    <h2>Raw Event Log (last 15)</h2>
    <table>
      <tr><th>Time</th><th>Method</th><th>Path</th><th>From</th></tr>
      ${recentHits}
    </table>

    <p style="color:#475569;font-size:12px;margin-top:20px">
      Last forward: ${state.lastForward || "never"} | 
      Last heartbeat: ${state.lastHeartbeat || "never"} |
      Auto-refresh: 15s
    </p>
  </body></html>`);
});

// ── Health check ──
app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    uptime: Math.floor((Date.now() - new Date(state.startedAt).getTime()) / 1000),
    heartbeats: state.heartbeats,
    forwarded: state.forwarded,
    errors: state.forwardErrors,
    devices: Object.keys(state.devices),
    lastHeartbeat: state.lastHeartbeat,
  });
});

// ── API: Get recent records (for external tools) ──
app.get("/api/records", (_req, res) => {
  res.json({
    total: state.records.length,
    records: state.records.slice(-50).reverse(),
  });
});

// ── iClock / ADMS HTTP Protocol (Production) ──
// This is the PROPER protocol for Mantra devices.
// Device must be configured: Menu → Communication → Server → HTTP mode
// Server URL: http://168.144.119.199:80/iclock/cdata
//
// Flow: 1) Device GETs /iclock/cdata → server sends config
//       2) Device POSTs /iclock/cdata?table=ATTLOG → server receives data, responds OK
//       3) Device GETs /iclock/getrequest → server can send CLEAR LOG command
//       4) Device processes OK → clears its internal queue ✅

// Command queue for device (CLEAR LOG, etc.)
const deviceCommandQueue = [];

app.get("/iclock/cdata", (req, res) => {
  const sn = req.query.SN || req.query.sn || "unknown";
  log(`💓 iClock Handshake from SN=${sn}`);
  state.heartbeats++;
  state.lastHeartbeat = new Date().toISOString();
  state.deviceSN = sn;
  state.devices[sn] = {
    ...(state.devices[sn] || {}),
    lastSeen: new Date().toISOString(),
    ip: req.ip,
    mode: "HTTP/ADMS",
  };
  // Tell device to push AttLog in realtime
  res.send(`GET OPTION FROM: ${sn}\r\nATTLOGStamp=0\r\nOPERLOGStamp=0\r\nATTPHOTOStamp=0\r\nErrorDelay=30\r\nDelay=10\r\nTransTimes=00:00;14:05\r\nTransInterval=1\r\nTransFlag=TransData AttLog\r\nTimeZone=5.5\r\nRealtime=1\r\nEncrypt=0\r\n`);
});

app.post("/iclock/cdata", async (req, res) => {
  const sn = req.query.SN || req.query.sn || "unknown";
  const table = req.query.table || "unknown";
  log(`📥 iClock POST from SN=${sn}, table=${table}`);

  if (table === "ATTLOG") {
    // Parse tab-separated attendance lines
    // Format: UserID\tTimestamp\tStatus\tVerify\t...
    const body = typeof req.body === "string" ? req.body : req.body?.toString() || "";
    const lines = body.split("\n").filter(l => l.trim());
    const records = [];

    for (const line of lines) {
      const parts = line.split("\t");
      if (parts.length >= 2) {
        const userID = parts[0].trim();
        const timestamp = parts[1].trim();
        const status = parts[2]?.trim() || "0";
        const verify = parts[3]?.trim() || "0";

        // Dedup
        const dedupKey = `http|${userID}|${timestamp}`;
        if (state.processedTransIDs.has(dedupKey)) {
          log(`⏭️ HTTP duplicate ${dedupKey} — skipping`);
          continue;
        }
        state.processedTransIDs.add(dedupKey);

        log(`📋 HTTP ATTENDANCE: User ${userID} | ${timestamp} | status=${status}`);
        records.push({ userID, timestamp, status, verify });

        state.records.push({
          source: "HTTP/ADMS",
          userID,
          timestamp,
          status,
          receivedAt: new Date().toISOString(),
        });
      }
    }

    // Forward to Aimify
    if (records.length > 0) {
      try {
        const attlogBody = records
          .map(r => `${r.userID}\t${r.timestamp}\t${r.status}\t${r.verify}\t0\t0\t0`)
          .join("\n");
        await sendPunchLogs(sn, attlogBody);
        state.forwarded += records.length;
        state.lastForward = new Date().toISOString();
        log(`✅ HTTP: Forwarded ${records.length} record(s) to Aimify`);
      } catch (e) {
        state.forwardErrors++;
        log(`❌ HTTP forward failed: ${e.message}`);
      }
    }
  }

  // CRITICAL: Responding with "OK" tells the device to CLEAR this record from its queue
  res.send("OK");
});

// ═══════════════════════════════════════════════════════════════════
// MINOP-COMPATIBLE API — Device pushes JSON via HTTP POST
// This is the protocol that actually clears the device queue!
// ═══════════════════════════════════════════════════════════════════

// ── Heartbeat / Transactional Response ──
app.post("/api/DeviceApi/getAttendance", (req, res) => {
  const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
  const dvcSrNo = body?.dvcSrNo || 'unknown';
  const dvcTime = body?.dvcTime || new Date().toISOString();

  log(`📡 MINOP HEARTBEAT: Device ${dvcSrNo} time=${dvcTime}`);

  state.devices[dvcSrNo] = {
    ...state.devices[dvcSrNo],
    lastSeen: new Date().toISOString(),
    ip: req.ip,
    terminalType: 'Minop-HTTP',
  };
  state.deviceSN = dvcSrNo;
  state.heartbeats++;
  state.lastHeartbeat = new Date().toISOString();

  sendHeartbeat(dvcSrNo, req.ip).catch(() => {});

  res.json({ status: 1 });
});

// ── Push Response — receives attendance records and ACKs each txnId ──
app.post("/api/DeviceApi/saveAttendance", async (req, res) => {
  const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
  const trans = body?.trans || [];

  log(`📥 MINOP PUSH: ${trans.length} record(s) received`);

  const transStatus = [];
  const newRecords = [];

  for (const t of trans) {
    const txnId = t.txnId;
    const punchId = String(t.punchId || '');
    const timestamp = t.txnDateTime || '';
    const mode = (t.mode || 'IN').toUpperCase();
    const dvcId = t.dvcId || 1;
    const dvcIP = t.dvcIP || req.ip;

    log(`📋 MINOP: txnId=${txnId} punch=${punchId} time=${timestamp} mode=${mode}`);

    // Dedup
    const dedupKey = `minop|${txnId}|${punchId}|${timestamp}`;
    if (!state.processedTransIDs.has(dedupKey)) {
      state.processedTransIDs.add(dedupKey);

      const status = mode === 'OUT' ? 1 : 0;
      const record = {
        transID: String(txnId),
        serialNo: state.deviceSN || `DVC-${dvcId}`,
        userID: punchId,
        timestamp,
        attendStat: mode === 'OUT' ? 'Duty Off' : 'Duty On',
        verifMode: 'FP',
        status,
        verifyCode: 1,
        receivedAt: new Date().toISOString(),
        forwarded: false,
      };
      state.records.push(record);
      newRecords.push(record);
    }

    // ACK this txnId — THIS is what clears the device queue
    transStatus.push({ txnId, status: 1 });
  }

  // Forward new records to Aimify
  if (newRecords.length > 0) {
    try {
      const sn = state.deviceSN || 'M2025011735';
      const body = newRecords.map(r =>
        `${r.userID}\t${r.timestamp}\t${r.status}\t${r.verifyCode}\t0\t0\t0`
      ).join('\n');

      await sendPunchLogs(sn, body);
      newRecords.forEach(r => r.forwarded = true);
      state.forwarded += newRecords.length;
      state.lastForward = new Date().toISOString();
      log(`✅ MINOP: Forwarded ${newRecords.length} record(s) to Aimify`);
    } catch (e) {
      state.forwardErrors++;
      log(`❌ MINOP forward failed: ${e.message}`);
    }
  }

  // CRITICAL: Return transStatus with status:1 for each txnId
  // This tells the device "I got it, clear from your queue"
  log(`📤 MINOP ACK: ${JSON.stringify({ transStatus })}`);
  res.json({ transStatus });
});

// ── SSL Webhook Mode — simple attendance webhook ──
app.post("/api/DeviceApi/saveEtimeAttendance", async (req, res) => {
  const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
  log(`📥 SSL WEBHOOK: ${JSON.stringify(body).substring(0, 500)}`);

  // Process whatever format arrives
  if (body?.trans) {
    // Same as saveAttendance
    for (const t of body.trans) {
      log(`📋 SSL: txnId=${t.txnId} punch=${t.punchId} time=${t.txnDateTime} mode=${t.mode}`);
    }
  }

  // Response: plain text "success" — this is what the SSL mode expects
  res.send("success");
});

app.get("/iclock/getrequest", (req, res) => {
  const sn = req.query.SN || req.query.sn || "unknown";
  // If there are pending commands, send one
  if (deviceCommandQueue.length > 0) {
    const cmd = deviceCommandQueue.shift();
    log(`📤 Sending command to device ${sn}: ${cmd}`);
    res.send(cmd);
  } else {
    res.send("OK");
  }
});

// ── Admin endpoint to clear device logs via ZK SDK (port 4370 UDP) ──
app.post("/admin/clear-device-log", async (req, res) => {
  // Get device IP from last known connection
  const deviceInfo = Object.values(state.devices)[0];
  if (!deviceInfo || !deviceInfo.ip) {
    return res.json({ success: false, message: "No device IP known yet" });
  }

  const ip = deviceInfo.ip.replace('::ffff:', '');
  log(`🗑️ Admin: Clearing logs on device at ${ip} via ZK SDK...`);

  const result = await clearDeviceLogs(ip);
  res.json({
    success: result,
    message: result ? "Attendance log cleared via ZK SDK!" : "Failed to clear — see logs",
    deviceIP: ip,
  });
});

// ── Catch-all ──
app.all("*", (req, res) => {
  log(`❓ CATCH-ALL: ${req.method} ${req.originalUrl}`);
  res.send("OK");
});

// ═══════════════════════════════════════════════════════════════════
// RAW TCP SERVER (Mantra eBioServer XML protocol on port 1018)
// CRITICAL: Send 0x06 ACK IMMEDIATELY on complete message
//           BEFORE any parsing, DB, or API work
// ═══════════════════════════════════════════════════════════════════

const tcpServer = net.createServer((socket) => {
  state.tcpConnections++;
  const remote = `${socket.remoteAddress}:${socket.remotePort}`;
  log(`🔌 TCP CONNECTION #${state.tcpConnections} from ${remote}`);

  let buffer = Buffer.alloc(0);

  socket.on("data", (data) => {
    // Accumulate data in buffer (message may arrive in chunks)
    buffer = Buffer.concat([buffer, data]);

    // Check if we have a complete message (ends with </Message>)
    const text = buffer.toString("utf8");
    if (!text.includes("</Message>")) {
      log(`📦 Buffering ${data.length} bytes from ${remote} (waiting for complete message)`);
      return;
    }

    log(`📦 TCP DATA from ${remote} (${buffer.length} bytes)`);

    // ══════════════════════════════════════════════════════
    // STEP 1: Send ACK IMMEDIATELY — before ANY processing
    // This is the critical fix: device expects raw 0x06
    // ══════════════════════════════════════════════════════
    try {
      socket.write(Buffer.from('{"status":1}'));
      log(`✅ ACK {"status":1} sent IMMEDIATELY to ${remote}`);
    } catch (e) {
      log(`⚠️ Failed to send ACK: ${e.message}`);
    }

    // Capture buffer before clearing
    const completeMessage = buffer;
    buffer = Buffer.alloc(0);

    // ══════════════════════════════════════════════════════
    // STEP 2: Process data AFTER ACK (async, non-blocking)
    // ══════════════════════════════════════════════════════
    processMantraMessage(completeMessage, remote).catch((err) => {
      log(`❌ Process error: ${err.message}`);
    });
  });

  socket.on("close", () => {
    log(`🔌 TCP DISCONNECTED: ${remote}`);
  });

  socket.on("error", (err) => {
    log(`❌ TCP ERROR from ${remote}: ${err.message}`);
  });
});

// ═══════════════════════════════════════════════════════════════════
// WEBSOCKET SERVER (Mantra WebSocket mode on port 1018)
// ═══════════════════════════════════════════════════════════════════

// Create HTTP server for Express (port 80)
const httpServer = http.createServer(app);

// Attach WebSocket to the HTTP server (port 80) for upgrade requests
const wss80 = new WebSocketServer({ server: httpServer });

// Standalone WebSocket server on port 1018
const wsServer1018 = http.createServer((req, res) => {
  // Handle plain HTTP requests on 1018 as fallback
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('OK');
});
const wss1018 = new WebSocketServer({ server: wsServer1018 });

function handleWebSocket(ws, req, label) {
  const remote = req.socket.remoteAddress + ':' + req.socket.remotePort;
  state.tcpConnections++;
  log(`🌐 WEBSOCKET CONNECTION #${state.tcpConnections} from ${remote} (${label})`);
  log(`🌐 WS URL: ${req.url}`);
  log(`🌐 WS Headers: ${JSON.stringify(req.headers)}`);

  ws.on('message', async (data, isBinary) => {
    const raw = isBinary ? data : data.toString('utf8');
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    log(`🌐 WS MESSAGE from ${remote}: ${buf.length} bytes`);

    // Check if it's the familiar XML format
    const text = buf.toString('utf8');
    if (text.includes('<Message>') || text.includes('</Message>')) {
      // Send ACK immediately via WebSocket
      try {
        ws.send(Buffer.from([0x06]));
        log(`✅ WS ACK 0x06 sent to ${remote}`);
      } catch (e) {
        log(`⚠️ WS ACK send failed: ${e.message}`);
      }

      // Process after ACK
      processMantraMessage(buf, remote).catch((err) => {
        log(`❌ WS process error: ${err.message}`);
      });
    } else {
      // Unknown format — log and ACK
      log(`🌐 WS UNKNOWN format: ${text.substring(0, 200)}`);
      try {
        ws.send(Buffer.from([0x06]));
      } catch (e) {
        log(`⚠️ WS generic ACK failed: ${e.message}`);
      }
    }
  });

  ws.on('close', (code, reason) => {
    log(`🌐 WS DISCONNECTED: ${remote} (code=${code}, reason=${reason})`);
  });

  ws.on('error', (err) => {
    log(`❌ WS ERROR from ${remote}: ${err.message}`);
  });

  // Send initial handshake — some devices expect a greeting
  try {
    ws.send(JSON.stringify({ Return: "True", status: 1 }));
    log(`🌐 WS initial handshake sent to ${remote}`);
  } catch (e) {
    log(`⚠️ WS handshake failed: ${e.message}`);
  }
}

wss80.on('connection', (ws, req) => handleWebSocket(ws, req, 'port-80'));
wss1018.on('connection', (ws, req) => handleWebSocket(ws, req, 'port-1018'));

// ── Start all servers ──
httpServer.listen(PORT, "0.0.0.0", () => {
  console.log("\n" + "═".repeat(56));
  console.log("  🌉 BIOMETRIC BRIDGE (Cloud) — Mantra eBioServer");
  console.log("═".repeat(56));
  console.log(`  HTTP + WS       → port ${PORT}`);
  console.log(`  TCP + WS        → port ${TCP_PORT}`);
  console.log(`  Aimify Backend  → ${process.env.AIMIFY_BACKEND_URL || "not configured"}`);
  console.log("═".repeat(56) + "\n");
});

// TCP server handles raw TCP connections
tcpServer.listen(TCP_PORT, "0.0.0.0", () => {
  log(`🔌 TCP server listening on port ${TCP_PORT}`);
});

// WebSocket server on port 1019 (device WebSocket mode)
wsServer1018.listen(1019, "0.0.0.0", () => {
  log(`🌐 WebSocket server listening on port 1019`);
});