// lib/binlog/event_factory.js
// This file provides a factory for creating BinLogEvent instances.

'use strict';

const Packet = require('../packets/packet.js'); // Updated path
const { BinlogEventHeader, calculateCrc32 } = require('./event_header.js'); // Updated path
const BinLogEvent = require('./event_base.js'); // Updated path

// Import all specific MySQL event types
const {
  RotateEvent,
  FormatDescriptionEvent,
  StopEvent,
  XAPrepareEvent,
  XidEvent,
  HeartbeatLogEvent,
  QueryEvent,
  BeginLoadQueryEvent,
  ExecuteLoadQueryEvent,
  IntvarEvent,
  RandEvent,
  UserVarEvent,
  GtidEvent,
  PreviousGtidsEvent,
  NotImplementedEvent,
} = require('./event_types.js'); // Updated path

// Import all specific MariaDB event types
const MariadbEventTypes = require('./mariadb_event_types.js'); // Updated path

// Import all specific Row event types
const RowEventTypes = require('./row_event_types.js'); // Updated path

// Import TableMapEvent
const { TableMapEvent } = require('./table_event_types.js'); // Updated path

const Types = require('../constants/binlog_event_types.js'); // Updated path

/**
 * Consolidate all event classes into a single object for factory lookup.
 * This map directly links numerical event type codes from MySQL/MariaDB to their
 * corresponding JavaScript class constructors.
 */
const ALL_EVENT_CLASSES = {
  [Types.ROTATE_EVENT]: RotateEvent,
  [Types.FORMAT_DESCRIPTION_EVENT]: FormatDescriptionEvent,
  [Types.STOP_EVENT]: StopEvent,
  [Types.XA_PREPARE_LOG_EVENT]: XAPrepareEvent,
  [Types.XID_EVENT]: XidEvent,
  [Types.HEARTBEAT_LOG_EVENT]: HeartbeatLogEvent,
  [Types.QUERY_EVENT]: QueryEvent,
  [Types.BEGIN_LOAD_QUERY_EVENT]: BeginLoadQueryEvent,
  [Types.EXEC_LOAD_EVENT]: ExecuteLoadQueryEvent,
  [Types.INTVAR_EVENT]: IntvarEvent,
  [Types.RAND_EVENT]: RandEvent,
  [Types.USER_VAR_EVENT]: UserVarEvent,
  [Types.GTID_LOG_EVENT]: GtidEvent,
  [Types.PREVIOUS_GTIDS_LOG_EVENT]: PreviousGtidsEvent,

  // Table Map Event
  [Types.TABLE_MAP_EVENT]: TableMapEvent,

  // Row specific events
  // Note: RowsQueryLogEvent is effectively replaced by RowsEvent and its subclasses
  [Types.WRITE_ROWS_EVENT]: RowEventTypes.WriteRowsEvent, // V1
  [Types.WRITE_ROWS_EVENT_V2]: RowEventTypes.WriteRowsEvent, // V2
  [Types.DELETE_ROWS_EVENT]: RowEventTypes.DeleteRowsEvent, // V1
  [Types.DELETE_ROWS_EVENT_V2]: RowEventTypes.DeleteRowsEvent, // V2
  [Types.UPDATE_ROWS_EVENT]: RowEventTypes.UpdateRowsEvent, // V1
  [Types.UPDATE_ROWS_EVENT_V2]: RowEventTypes.UpdateRowsEvent, // V2
  [Types.PARTIAL_UPDATE_ROWS_EVENT]: RowEventTypes.PartialUpdateRowsEvent,

  // MariaDB specific events
  [Types.MARIADB_GTID_EVENT]: MariadbEventTypes.MariadbGtidEvent,
  [Types.MARIADB_BINLOG_CHECKPOINT_EVENT]:
    MariadbEventTypes.MariadbBinLogCheckPointEvent,
  [Types.MARIADB_ANNOTATE_ROWS_EVENT]:
    MariadbEventTypes.MariadbAnnotateRowsEvent,
  [Types.MARIADB_GTID_LIST_EVENT]: MariadbEventTypes.MariadbGtidListEvent,
  [Types.MARIADB_START_ENCRYPTION_EVENT]:
    MariadbEventTypes.MariadbStartEncryptionEvent,
};

const encounteredUnimplementedEventNames = new Set();

/**
 * A factory class for creating instances of BinLogEvent and its subclasses.
 */
class BinlogEventFactory {
  /**
   * Factory method to create a BinLogEvent instance based on the event type.
   * Parses the event header and performs checksum verification.
   *
   * @param {Packet} packet - The Packet instance containing the raw binlog event data.
   * The packet's offset should be positioned at the start of the MySQL binlog event
   * (i.e., after the 4-byte MySQL packet length and 1-byte sequence ID from the TCP stream).
   * @param {Object} tableMap - Reference to the table map (will be fleshed out later).
   * @param {Object} ctlConnection - Control connection object (will be fleshed out later).
   * @param {Object} options - Options object.
   * @param {boolean} [options.verifyChecksum=false] - Whether to verify CRC32 checksums.
   * @returns {BinLogEvent} An instance of the appropriate BinLogEvent subclass.
   * @throws {Error} If checksum verification fails or an unknown event type is encountered.
   */
  static fromPacket(packet, tableMap, ctlConnection, options = {}) {
    packet.skip(1);

    // Store original offset before header parsing. This is the start of the current binlog event.
    const binlogEventStartOffset = packet.offset;

    // 1. Parse header
    const eventHeader = new BinlogEventHeader(packet);

    // 2. Perform checksum verification if requested
    if (options.verifyChecksum) {
      // The CRC32 checksum is calculated over the entire binlog event (header + payload).
      const p0 = packet.clone();
      p0.reset();
      p0.skip(1);
      const calculatedCrc = calculateCrc32(
        p0.readBuffer(eventHeader.eventSize - 4)
      );
      const footerCrc = p0.readInt32();

      if (calculatedCrc !== footerCrc) {
        console.error(
          `CRC32 checksum failed for event type ${eventHeader.eventType} (0x${eventHeader.eventType.toString(16)}). ` +
            `Calculated: 0x${calculatedCrc.toString(16)}, Expected: 0x${footerCrc.toString(16)}.`
        );
        throw new Error(
          `CRC32 checksum failed for event type ${eventHeader.eventType}`
        );
      }

      // Make adjustment here
      eventHeader.eventSize -= 4;
      packet.end -= 4;
    }

    // Determine the correct subclass based on eventType.
    // Use the ALL_EVENT_CLASSES object for direct lookup.
    let EventClass = ALL_EVENT_CLASSES[eventHeader.eventType];
    if (!EventClass) {
      const name = Types[eventHeader.eventType];
      if (name) {
        if (!encounteredUnimplementedEventNames.has(name)) {
          encounteredUnimplementedEventNames.add(name);
          console.warn(`Handling of ${name} is not implemented yet`);
        }
        EventClass = NotImplementedEvent;
      } else throw Error(`Unknown event type ${eventHeader.eventType}`);
    }

    // Create an instance of the appropriate event class.
    // The `packet` object's `offset` is currently at the start of the event's payload
    // (after the 19-byte header). The subclass constructor will read from here.
    const eventInstance = new EventClass(
      packet,
      eventHeader, // Pass the parsed header object directly
      tableMap,
      ctlConnection,
      options
    );

    // After the event instance is created and has read its payload,
    // the packet.offset should be at the end of the current event's payload.
    // The factory's responsibility is to ensure the `packet.offset` is correctly
    // positioned for the *next* binlog event header.
    // This includes advancing past the payload and the final 4-byte checksum if present.
    // The `eventHeader.eventSize` already includes the 19-byte header itself.
    // So, `eventHeader.eventSize` covers the header + payload.
    // We then need to add the 4-byte checksum if verifyChecksum is true.
    //const totalBytesConsumedByEvent = eventHeader.eventSize + (options.verifyChecksum ? 4 : 0);
    //packet.offset = binlogEventStartOffset + totalBytesConsumedByEvent;
    if (packet.numRemainingBytes() < 0) {
      console.warn('Buffer overrun', packet, eventInstance);
      throw Error('Buffer overrun');
    }

    return eventInstance;
  }
}

module.exports = BinlogEventFactory;
