/**
 * parser.js
 * Parse the raw iClock ATTLOG body sent by the MORX BioFace MSD1K.
 *
 * Format (tab-delimited per line):
 *   PIN \t Timestamp \t Status \t VerifyMode \t WorkCode \t ...
 * Example:
 *   1001\t2026-05-06 09:15:30\t0\t15\t0\t0\t0
 */

export function parseAttendanceLines(body) {
  if (!body || typeof body !== "string") return [];

  return body
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .reduce((acc, line) => {
      const parts = line.split("\t");
      if (parts.length < 4) return acc;

      const [pin, timestampStr, statusStr, verifyModeStr] = parts;
      const timestamp = new Date(timestampStr);
      if (isNaN(timestamp.getTime())) return acc;

      acc.push({
        pin: pin.trim(),
        timestamp: timestamp.toISOString(),
        status: parseInt(statusStr, 10) || 0,
        verifyMode: parseInt(verifyModeStr, 10) || 0,
        rawLine: line,
      });
      return acc;
    }, []);
}

export function mapVerifyMode(code) {
  const n = parseInt(code, 10);
  if (n === 1)  return "fingerprint";
  if (n === 15) return "face";
  if (n === 4)  return "card";
  return "unknown";
}
