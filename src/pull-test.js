/**
 * pull-test.js — Direct device communication test
 * 
 * Connects to MORX BioFace MSD1K via TCP (port 4370)
 * and pulls attendance logs + device info directly.
 * 
 * Usage: node src/pull-test.js
 */

import ZKLib from "zkteco-js";

const DEVICE_IP = "192.168.1.73";
const DEVICE_PORT = 5005; // Device shows TCP Port: 5005

async function main() {
  console.log("=".repeat(50));
  console.log("🔌 Connecting to device at", DEVICE_IP + ":" + DEVICE_PORT);
  console.log("=".repeat(50));

  const device = new ZKLib(DEVICE_IP, DEVICE_PORT, 10000, 4000);

  try {
    // 1. Connect
    console.log("\n⏳ Creating socket connection...");
    await device.createSocket();
    console.log("✅ Connected!\n");

    // 2. Get device info
    console.log("📋 Device Info:");
    try {
      const info = await device.getInfo();
      console.log("   ", JSON.stringify(info, null, 2));
    } catch (e) {
      console.log("   (getInfo not supported:", e.message + ")");
    }

    // 3. Get serial number
    try {
      const serial = await device.getSerialNumber();
      console.log("   Serial:", serial);
    } catch (e) {
      console.log("   (getSerialNumber:", e.message + ")");
    }

    // 4. Get users
    console.log("\n👤 Users on device:");
    try {
      const users = await device.getUsers();
      if (users?.data?.length) {
        users.data.forEach((u) => {
          console.log(`   PIN=${u.pin || u.uid}  Name=${u.name || "?"}`);
        });
        console.log(`   Total: ${users.data.length} users`);
      } else {
        console.log("   No users found or empty response");
        console.log("   Raw:", JSON.stringify(users));
      }
    } catch (e) {
      console.log("   (getUsers error:", e.message + ")");
    }

    // 5. Get attendance logs
    console.log("\n📊 Attendance Logs:");
    try {
      const logs = await device.getAttendances();
      if (logs?.data?.length) {
        console.log(`   Found ${logs.data.length} records:`);
        // Show last 20
        const recent = logs.data.slice(-20);
        recent.forEach((log) => {
          console.log(
            `   PIN=${String(log.pin || log.deviceUserId || log.uid).padEnd(8)} ` +
            `Time=${log.recordTime || log.timestamp || "?"}  ` +
            `Type=${log.type ?? log.status ?? "?"}`
          );
        });
        if (logs.data.length > 20) {
          console.log(`   ... and ${logs.data.length - 20} more`);
        }
      } else {
        console.log("   No attendance logs found");
        console.log("   Raw:", JSON.stringify(logs));
      }
    } catch (e) {
      console.log("   (getAttendances error:", e.message + ")");
    }

    // 6. Disconnect
    await device.disconnect();
    console.log("\n✅ Disconnected successfully");

  } catch (err) {
    console.error("\n❌ Connection failed:", err.message);
    console.log("\nTroubleshooting:");
    console.log("  1. Is device on? Ping it: ping", DEVICE_IP);
    console.log("  2. Try port 5005 instead of 4370");
    console.log("  3. Check Communication Password is OFF on device");
  }

  console.log("\n" + "=".repeat(50));
}

main();
