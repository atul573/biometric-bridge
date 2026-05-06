import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ═══════════════════════════════════════════════════════════════════
// MORX BioFace-MSD1K — Biometric Bridge (Cloud Edition)
// Receives attendance data via ZKTeco PUSH protocol (iclock)
// ═══════════════════════════════════════════════════════════════════

const PORT = process.env.PORT || 3001;

// In-memory store (will reset on redeploy — fine for now)
const state = {
  heartbeats: 0,
  lastHeartbeat: null,
  records: [],
  rawHits: [],
  deviceSN: null,
  startedAt: new Date().toISOString(),
};

function log(msg) {
  const ts = new Date().toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata" });
  console.log(`[${ts}] ${msg}`);
}

// ─── Express App ──────────────────────────────────────────────────
const app = express();

// Serve the UI
app.use(express.static(path.join(__dirname, "ui")));

// Parse text bodies (ZKTeco sends text/plain)
app.use(express.text({ type: "*/*", limit: "5mb" }));

// ── API: State for UI dashboard ──
app.get("/api/state", (req, res) => {
  res.json(state);
});

// ── Health check ──
app.get("/health", (req, res) => {
  res.json({ status: "ok", uptime: process.uptime(), heartbeats: state.heartbeats });
});

// ── Log EVERY request ──
app.use((req, res, next) => {
  const from = req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "?";
  const hit = {
    time: new Date().toISOString(),
    method: req.method,
    path: req.originalUrl,
    from: from,
    bodyPreview: typeof req.body === "string" ? req.body.substring(0, 500) : "",
  };
  state.rawHits.push(hit);
  if (state.rawHits.length > 500) state.rawHits.shift();

  log(`${req.method} ${req.originalUrl} from=${from}`);
  if (req.body) {
    const bodyStr = typeof req.body === "string" ? req.body : JSON.stringify(req.body);
    if (bodyStr.trim() && bodyStr !== "{}") {
      log(`  Body: ${bodyStr.substring(0, 300)}`);
    }
  }
  next();
});

// ── ZKTeco PUSH: Heartbeat (GET /iclock/cdata) ──
app.get("/iclock/cdata", (req, res) => {
  const sn = req.query.SN || "unknown";
  state.deviceSN = sn;
  state.heartbeats++;
  state.lastHeartbeat = new Date().toISOString();
  log(`♥ HEARTBEAT #${state.heartbeats} from SN=${sn}`);

  const response = [
    "GET OPTION FROM: " + sn,
    "ATTLOGStamp=0",
    "OPERLOGStamp=0",
    "ATTPHOTOStamp=0",
    "ErrorDelay=60",
    "Delay=10",
    "TransTimes=00:00;23:59",
    "TransInterval=1",
    "TransFlag=TransData AttLog\tOpLog",
    "Realtime=1",
    "TimeZone=5",
  ].join("\n");

  res.set("Content-Type", "text/plain");
  res.send(response);
});

// ── ZKTeco PUSH: Attendance data (POST /iclock/cdata) ──
app.post("/iclock/cdata", (req, res) => {
  const sn = req.query.SN || "unknown";
  const table = req.query.table || "unknown";
  const body = String(req.body || "");

  log(`📥 DATA PUSH SN=${sn} table=${table}`);
  log(`  Raw: ${body.substring(0, 500)}`);

  if (table === "ATTLOG" && body.trim()) {
    const lines = body.trim().split("\n");
    lines.forEach((line) => {
      const parts = line.split("\t");
      if (parts.length >= 2) {
        const rec = {
          pin: parts[0]?.trim(),
          timestamp: parts[1]?.trim(),
          status: parts[2]?.trim() || "0",
          verify: parts[3]?.trim() || "0",
          sn: sn,
          receivedAt: new Date().toISOString(),
        };
        state.records.push(rec);
        log(`  ✅ PIN=${rec.pin} Time=${rec.timestamp} Status=${rec.status}`);
      }
    });
  }

  res.send("OK");
});

// ── Other ZKTeco endpoints ──
app.all("/iclock/getrequest", (req, res) => {
  log(`📡 getrequest SN=${req.query.SN || "?"}`);
  res.set("Content-Type", "text/plain");
  res.send("OK");
});

app.all("/iclock/devicecmd", (req, res) => {
  log(`⚙️ devicecmd: ${JSON.stringify(req.query)}`);
  res.send("OK");
});

// ── Catch-all ──
app.all("*", (req, res) => {
  log(`❓ CATCH-ALL: ${req.method} ${req.originalUrl}`);
  res.send("OK");
});

// ── Start ──
app.listen(PORT, "0.0.0.0", () => {
  console.log("\n" + "═".repeat(50));
  console.log("  🌉 BIOMETRIC BRIDGE (Cloud)");
  console.log("═".repeat(50));
  console.log(`  Listening on port ${PORT}`);
  console.log(`  Dashboard: /`);
  console.log("═".repeat(50) + "\n");
});