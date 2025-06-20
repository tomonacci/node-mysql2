// This file contains the BinlogEventHeader class and a helper for CRC32 calculation.

'use strict';

const zlib = require('zlib'); // Node.js built-in zlib for CRC32
const Packet = require('./Packet.js'); // Assuming Packet.js is in the same directory

/**
 * Helper function for CRC32 validation, equivalent to Python's zlib.crc32.
 * @param {Buffer} buffer - The buffer to calculate CRC32 for.
 * @returns {number} The calculated unsigned 32-bit CRC32 value.
 */
function calculateCrc32(buffer) {
  // zlib.crc32 returns a signed 32-bit integer, but Python's crc32 behaves as unsigned.
  // The `>>> 0` operator converts a signed 32-bit integer to an unsigned 32-bit integer.
  return zlib.crc32(buffer) >>> 0;
}

/**
 * Class representing the common 19-byte MySQL binlog event header.
 */
class BinlogEventHeader {
  /**
   * Parses the common 19-byte MySQL binlog event header.
   * @param {Packet} packet - The Packet instance to read from. The packet's offset
   * should be at the start of the 19-byte binlog event header.
   */
  constructor(packet) {
    this.timestamp = packet.readInt32(); // 4 bytes
    this.eventType = packet.readInt8();  // 1 byte
    this.serverId = packet.readInt32();  // 4 bytes
    this.eventSize = packet.readInt32(); // 4 bytes (total size of event including header)
    this.logPos = packet.readInt32();    // 4 bytes
    this.flags = packet.readInt16();     // 2 bytes
    // Total 19 bytes read. After this constructor, packet.offset will be at the
    // start of the event's specific payload data.
  }
}

module.exports = {
  BinlogEventHeader,
  calculateCrc32,
};
