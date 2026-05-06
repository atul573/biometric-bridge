import express from "express";
import net from "net";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { sendHeartbeat, sendPunchLogs } from "./services/aimify.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ═══════════════════════════════════════════════════════════════════
// MORX BioFace-MSD1K — Biometric Bridge (Cloud Edition)
// Dual-mode: HTTP (iclock) + Raw TCP (Mantra eBioServer XML protocol)
// ═══════════════════════════════════════════════════════════════════

const PORT = process.env.PORT || 80;
const TCP_PORT = process.env.TCP_PORT || 1018;

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

function buildAckXML(transID) {
  // Match device format: single continuous XML line, null-terminated
  return `<?xml version="1.0"?><Message><TransID>${transID}</TransID><Result>1</Result><Status>OK</Status></Message>\x00`;
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
// PROCESS ATTENDANCE — Parse XML, ACK device, forward to Aimify
// ═══════════════════════════════════════════════════════════════════

async function processMantraMessage(data, socket, remoteAddr) {
  const raw = data.toString("utf8");

  // DEEP PROTOCOL ANALYSIS: Full hex dump
  log(`🔬 FULL MESSAGE: ${data.length} bytes total`);
  log(`🔬 FULL HEX: ${data.toString("hex")}`);
  // Check what comes AFTER </Message> — checksum? length? null bytes?
  const msgEndIdx = raw.indexOf('</Message>');
  if (msgEndIdx >= 0) {
    const afterMsg = data.slice(msgEndIdx + 10); // bytes after </Message>
    log(`🔬 AFTER </Message>: ${afterMsg.length} bytes → hex: ${afterMsg.toString("hex")} → text: "${afterMsg.toString("utf8")}"`);
    // Check bytes BEFORE <?xml — any length prefix?
    const xmlStartIdx = raw.indexOf('<?xml');
    if (xmlStartIdx > 0) {
      const beforeXml = data.slice(0, xmlStartIdx);
      log(`🔬 BEFORE <?xml>: ${beforeXml.length} bytes → hex: ${beforeXml.toString("hex")}`);
    }
  }

  const parsed = parseMantraXML(raw);

  // Debug: log what we parsed
  log(`🔍 DEBUG parsed keys: ${JSON.stringify(Object.keys(parsed))}`);
  log(`🔍 DEBUG parsed: ${JSON.stringify(parsed).substring(0, 500)}`);

  // Validate required fields
  if (!parsed.TransID || !parsed.DeviceSerialNo) {
    log(`⚠️ Invalid XML message — missing TransID (${parsed.TransID}) or SerialNo (${parsed.DeviceSerialNo})`);
    // Try alternate key names
    log(`⚠️ All keys: ${Object.keys(parsed).join(", ")}`);
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

  // ── Send ACK with correct protocol format ──
  // PROTOCOL: Double null-byte terminator + CRLF line endings matching device format
  try {
    const ack = buildAckXML(transID);
    log(`📤 ACK sent: ${ack.length} bytes, hex: ${ack.toString('hex')}`);
    socket.write(ack);
    log(`✅ ACK sent for TransID ${transID}`);
  } catch (e) {
    log(`⚠️ Failed to send ACK: ${e.message}`);
  }

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
app.use(express.text({ type: "*/*", limit: "1mb" }));
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

// ── iClock fallback (if device switches to HTTP mode) ──
app.get("/iclock/cdata", (req, res) => {
  const sn = req.query.SN || req.query.sn || "unknown";
  log(`💓 iClock Handshake from SN=${sn}`);
  state.heartbeats++;
  state.lastHeartbeat = new Date().toISOString();
  state.deviceSN = sn;
  res.send(`GET OPTION FROM: ${sn}\r\nATTLOGStamp=0\r\nOPERLOGStamp=0\r\nATTPHOTOStamp=0\r\nErrorDelay=30\r\nDelay=10\r\nTransTimes=00:00;14:05\r\nTransInterval=1\r\nTransFlag=TransData AttLog\r\nTimeZone=5.5\r\nRealtime=1\r\nEncrypt=0\r\n`);
});

app.post("/iclock/cdata", (req, res) => {
  const sn = req.query.SN || req.query.sn || "unknown";
  log(`📥 iClock PUSH from SN=${sn}, table=${req.query.table}`);
  res.send("OK");
});

app.get("/iclock/getrequest", (req, res) => {
  res.send("OK");
});

// ── Catch-all ──
app.all("*", (req, res) => {
  log(`❓ CATCH-ALL: ${req.method} ${req.originalUrl}`);
  res.send("OK");
});

// ═══════════════════════════════════════════════════════════════════
// RAW TCP SERVER (Mantra eBioServer XML protocol on port 1018)
// ═══════════════════════════════════════════════════════════════════

const tcpServer = net.createServer((socket) => {
  state.tcpConnections++;
  const remote = `${socket.remoteAddress}:${socket.remotePort}`;
  log(`🔌 TCP CONNECTION #${state.tcpConnections} from ${remote}`);

  let buffer = Buffer.alloc(0);

  socket.on("data", async (data) => {
    // Accumulate data in buffer (message may arrive in chunks)
    buffer = Buffer.concat([buffer, data]);

    // Check if we have a complete message (ends with null byte \0 or </Message>)
    const text = buffer.toString("utf8");
    if (!text.includes("</Message>")) {
      log(`📦 Buffering ${data.length} bytes from ${remote} (waiting for complete message)`);
      return;
    }

    log(`📦 TCP DATA from ${remote} (${buffer.length} bytes)`);

    // Process the complete message
    await processMantraMessage(buffer, socket, remote);

    // Clear buffer
    buffer = Buffer.alloc(0);
  });

  socket.on("close", () => {
    log(`🔌 TCP DISCONNECTED: ${remote}`);
  });

  socket.on("error", (err) => {
    log(`❌ TCP ERROR from ${remote}: ${err.message}`);
  });
});

// ── Start both servers ──
app.listen(PORT, "0.0.0.0", () => {
  console.log("\n" + "═".repeat(56));
  console.log("  🌉 BIOMETRIC BRIDGE (Cloud) — Mantra eBioServer XML");
  console.log("═".repeat(56));
  console.log(`  HTTP Dashboard  → port ${PORT}`);
  console.log(`  TCP Listener    → port ${TCP_PORT}`);
  console.log(`  Aimify Backend  → ${process.env.AIMIFY_BACKEND_URL || "not configured"}`);
  console.log("═".repeat(56) + "\n");
});

tcpServer.listen(TCP_PORT, "0.0.0.0", () => {
  log(`🔌 TCP server listening on port ${TCP_PORT}`);
});