// This file defines the base BinLogEvent class.

'use strict';

const Packet = require('../packets/packet.js');
const NativeBuffer = require('buffer').Buffer; // Still needed for NativeBuffer.alloc
const Long = require('long'); // Still needed for Long class
const { BinlogEventHeader } = require('./event_header.js'); // Import BinlogEventHeader

/**
 * Base class for all MySQL Binlog Events.
 * This class provides common properties and methods for all binlog events.
 */
class BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance with its offset positioned at the start of the event's payload (after the 19-byte header).
   * @param {BinlogEventHeader} eventHeader - The already parsed BinlogEventHeader object.
   * @param {Object} tableMap - Reference to the table map (will be fleshed out later).
   * @param {Object} ctlConnection - Control connection object (will be fleshed out later).
   * @param {Object} options - Additional options for the event.
   * @param {Array<number>} [options.mysqlVersion=[0, 0, 0]] - MySQL server version as a tuple/array.
   * @param {Array<string>} [options.onlyTables=null] - List of tables to include.
   * @param {Array<string>} [options.ignoredTables=null] - List of tables to ignore.
   * @param {Array<string>} [options.onlySchemas=null] - List of schemas to include.
   * @param {Array<string>} [options.ignoredSchemas=null] - List of schemas to ignore.
   * @param {boolean} [options.freezeSchema=false] - Whether to freeze the schema.
   * @param {boolean} [options.ignoreDecodeErrors=false] - Whether to ignore decoding errors.
   * @param {boolean} [options.optionalMetaData=false] - Whether to include optional meta data.
   */
  constructor(
    fromPacket,
    eventHeader,
    tableMap,
    ctlConnection,
    {
      mysqlVersion = [0, 0, 0],
      onlyTables = null,
      ignoredTables = null,
      onlySchemas = null,
      ignoredSchemas = null,
      freezeSchema = false,
      ignoreDecodeErrors = false,
      optionalMetaData = false,
    } = {}
  ) {
    this.packet = fromPacket;
    this.tableMap = tableMap;
    this.eventHeader = eventHeader;

    this.eventType = this.eventHeader.eventType;
    this.timestamp = this.eventHeader.timestamp;
    this.eventSize = this.eventHeader.eventSize;
    this.ctlConnection = ctlConnection;
    this.mysqlVersion = mysqlVersion;
    this.ignoreDecodeErrors = ignoreDecodeErrors;
    this.isEventValid = true; // Assume valid if factory didn't throw

    this.processed = true; // The event has been fully processed; if false, the event will be skipped.
    this.complete = true;

    this.dbms = this.ctlConnection ? this.ctlConnection._getDbms() : null;
  }

  /**
   * Reads a 6-byte table ID from the packet and converts it to a number.
   * Corresponds to Python's `_read_table_id`.
   * @returns {number} The table ID.
   */
  readTableId() {
    const tableIdBuffer = this.packet.readBuffer(6);
    const paddedBuffer = NativeBuffer.alloc(8);
    tableIdBuffer.copy(paddedBuffer, 0);
    const low = paddedBuffer.readUInt32LE(0);
    const high = paddedBuffer.readUInt32LE(4);
    const longValue = new Long(low, high, true); // true for unsigned
    return longValue.toNumber(); // Convert to JavaScript number. Be aware of precision limits for very large IDs.
  }

  /**
   * Gets the formatted timestamp of the event in ISO 8601 format.
   * @returns {string} The formatted timestamp.
   */
  get formattedTimestamp() {
    return new Date(this.timestamp * 1000).toISOString();
  }
}

module.exports = BinLogEvent;
