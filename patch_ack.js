const fs = require('fs');
const file = '/root/biometric-bridge/src/index.js';
let content = fs.readFileSync(file, 'utf8');

// Replace the buildAckXML function
const oldFunc = /function buildAckXML\(transID\)\s*\{[^}]+\}/;
const newFunc = `function buildAckXML(transID, serialNo) {
  // eBioServer protocol: <Response> with DeviceSerialNo + TransIDs + SUCCESS
  // This tells the device firmware to CLEAR the acknowledged record from its queue
  const xml = \`<?xml version="1.0" encoding="UTF-8"?><Response><DeviceSerialNo>\${serialNo || ""}</DeviceSerialNo><TransIDs>\${transID}</TransIDs><Status>SUCCESS</Status></Response>\`;
  // Null-terminate like the device does
  return Buffer.concat([Buffer.from(xml, 'utf8'), Buffer.from([0x00])]);
}`;

content = content.replace(oldFunc, newFunc);

// Update the call site to pass serialNo
content = content.replace(
  /const ack = buildAckXML\(transID\);/,
  'const ack = buildAckXML(transID, serialNo);'
);

fs.writeFileSync(file, content);
console.log('Patched successfully');

// Verify
const check = fs.readFileSync(file, 'utf8');
if (check.includes('<Response><DeviceSerialNo>')) {
  console.log('VERIFIED: New Response format found');
} else {
  console.log('ERROR: Patch may not have applied');
}
