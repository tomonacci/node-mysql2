// This file contains implementations for MariaDB-specific MySQL Binlog Event types.

'use strict';

const BinLogEvent = require('./event_base'); // Import base BinLogEvent
const Packet = require('../packets/packet'); // Import Packet class
const Long = require('long'); // Still needed for Long operations

/**
 * Represents an individual GTID object used within MariadbGtidListEvent.
 * This is a data structure, not a BinLogEvent itself.
 */
class MariadbGtidObject {
  /**
   * @param {Packet} packet - The packet to read from.
   *
   * @property {number} domainId - Replication Domain ID.
   * @property {number} serverId - Server ID.
   * @property {number|string} gtidSeqNo - GTID sequence number.
   * @property {string} gtid - Formatted GTID string: 'domainId-serverId-gtidSeqNo'.
   */
  constructor(packet) {
    this.domainId = packet.readInt32(); // Corresponds to Python's read_uint32()
    this.serverId = packet.readInt32(); // Corresponds to Python's read_uint32()
    this.gtidSeqNo = packet.readInt64(); // Corresponds to Python's read_uint64()
    this.gtid = `${this.domainId}-${this.serverId}-${this.gtidSeqNo}`;
  }
}

/**
 * GTID (Global Transaction Identifier) change in binlog event in MariaDB.
 *
 * For more information: `[see details] <https://mariadb.com/kb/en/gtid_event/>`_.
 */
class MariadbGtidEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number} serverId - The ID of the server where the GTID event occurred.
   * @property {number|string} gtidSeqNo - The sequence number of the GTID event.
   * @property {number} domainId - The domain ID associated with the GTID event.
   * @property {number} flags - Flags related to the GTID event.
   * @property {string} gtid - The Global Transaction Identifier in the format ‘domain_id-server_id-gtid_seq_no’.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    this.serverId = this.eventHeader.serverId; // From event header
    this.gtidSeqNo = this.packet.readInt64(); // Corresponds to Python's read_uint64()
    this.domainId = this.packet.readInt32(); // Corresponds to Python's read_uint32()
    this.flags = this.packet.readInt8(); // Corresponds to Python's read_uint8()
    this.gtid = `${this.domainId}-${this.serverId}-${this.gtidSeqNo}`;
  }
}

/**
 * Represents a checkpoint in a binlog event in MariaDB.
 *
 * More details are available in the MariaDB Knowledge Base:
 * https://mariadb.com/kb/en/binlog_checkpoint_event/
 */
class MariadbBinLogCheckPointEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {string} filename - The name of the file saved at the checkpoint.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    const filenameLength = this.packet.readInt32(); // Corresponds to Python's read_uint32()
    this.filename = this.packet.readString(filenameLength, 'utf8'); // Corresponds to Python's .decode()
  }
}

/**
 * Annotate rows event.
 * If you want to check this binlog, change the value of the flag (line 382 of the 'binlogstream.py') option to 2.
 * https://mariadb.com/kb/en/annotate_rows_event/
 */
class MariadbAnnotateRowsEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {Buffer} sqlStatement - The SQL statement (raw buffer).
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);
    // Payload size is total event size - common header size (19 bytes).
    const payloadBytes = this.eventSize - 19;
    this.sqlStatement = this.packet.readBuffer(payloadBytes); // Corresponds to Python's read(event_size)
  }
}

/**
 * GTID List event
 * https://mariadb.com/kb/en/gtid_list_event/
 */
class MariadbGtidListEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number} gtidLength - Number of GTIDs in the list.
   * @property {Array<MariadbGtidObject>} gtidList - List of 'MariadbGtidObject' instances.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    this.gtidLength = this.packet.readInt32(); // Corresponds to Python's read_uint32()
    this.gtidList = [];

    for (let i = 0; i < this.gtidLength; i++) {
      this.gtidList.push(new MariadbGtidObject(this.packet));
    }
  }
}

/**
 * Since MariaDB 10.1.7, the START_ENCRYPTION event is written to every binary log file
 * if `encrypt_binlog` is set to ON.
 * This event is written just once, after the Format Description event.
 */
class MariadbStartEncryptionEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number} schema - The Encryption scheme, always set to 1 for system files.
   * @property {number} keyVersion - The Encryption key version.
   * @property {Buffer} nonce - Nonce (12 random bytes) of current binlog file.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    this.schema = this.packet.readInt8();
    this.keyVersion = this.packet.readInt32();
    this.nonce = this.packet.readBuffer(12);
  }
}

module.exports = {
  MariadbGtidObject,
  MariadbGtidEvent,
  MariadbBinLogCheckPointEvent,
  MariadbAnnotateRowsEvent,
  MariadbGtidListEvent,
  MariadbStartEncryptionEvent,
};
