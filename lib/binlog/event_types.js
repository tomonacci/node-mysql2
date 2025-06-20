// lib/binlog/event_types.js
// This file contains implementations for various specific MySQL Binlog Event types.

'use strict';

const BinLogEvent = require('./event_base.js'); // Import base BinLogEvent
const { parseDecimalFromBytes, StatusVariableMismatch } = require('./event_helpers.js'); // Import helpers
const NativeBuffer = require('buffer').Buffer; // Still needed for Buffer operations
const Long = require('long'); // Still needed for Long operations (e.g., in UserVarEvent._readInt)
const Types = require('./constants/types.js'); // Assuming this contains relevant event type constants


/**
 * Represents a Rotate Event in the MySQL binary log.
 * Changes the MySQL binlog file.
 *
 * For more information: `[see details] <https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Rotate__event.html>`_.
 */
class RotateEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance with its offset positioned at the start of the event's payload.
   * @param {BinlogEventHeader} eventHeader - The already parsed BinlogEventHeader object.
   * @param {Object} tableMap - Reference to the table map.
   * @param {Object} ctlConnection - Control connection object.
   * @param {Object} options - Additional options for the event.
   *
   * @property {number} position - Position inside next binlog.
   * @property {string} nextBinlog - Name of next binlog file.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    this.position = this.packet.readInt64JSNumber(); // Corresponds to Python's struct.unpack("<Q", self.packet.read(8))[0]

    // Payload size is total event size - common header size (19 bytes).
    // Remaining bytes after reading 'position' (8 bytes).
    const payloadSize = this.eventSize - 19;
    const remainingPayloadBytes = payloadSize - 8;
    this.nextBinlog = this.packet.readString(remainingPayloadBytes, 'utf8'); // Corresponds to Python's .decode()
  }
}

/**
 * Represents a Format Description Event in the MySQL binary log.
 * This event is written at the start of a binary log file for binlog version 4.
 * It provides the necessary information to decode subsequent events in the file.
 */
class FormatDescriptionEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance with its offset positioned at the start of the event's payload.
   * @param {BinlogEventHeader} eventHeader - The already parsed BinlogEventHeader object.
   * @param {Object} tableMap - Reference to the table map.
   * @param {Object} ctlConnection - Control connection object.
   * @param {Object} options - Additional options for the event.
   *
   * @property {number} binlogVersion - Version of the binary log format.
   * @property {string} mysqlVersionStr - Server's MySQL version in string format.
   * @property {number} created - Timestamp when the binlog was created.
   * @property {number} commonHeaderLen - Length of the common event header.
   * @property {Array<number>} postHeaderLen - Array of post-header lengths for various event types.
   * @property {Array<number>} mysqlVersion - Server version split into major, minor, patch.
   * @property {number} numberOfEventTypes - Number of different event types.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    // The `this.packet.offset` is already at the end of the 19-byte common event header due to super() call.
    // The following fields are part of the Format Description Event's *payload*.

    this.binlogVersion = this.packet.readInt16(); // Corresponds to Python's struct.unpack("<H", self.packet.read(2))

    // Read 50 bytes, remove trailing nulls, decode to string.
    this.mysqlVersionStr = this.packet.readBuffer(50).toString('utf8').replace(/\0+$/, '');

    // Split MySQL version string into major, minor, patch numbers.
    const numbers = this.mysqlVersionStr.split('-')[0];
    this.mysqlVersion = numbers.split('.').map(Number); // Convert to array of numbers

    this.created = this.packet.readInt32(); // Corresponds to Python's struct.unpack("<I", self.packet.read(4))[0]

    this.commonHeaderLen = this.packet.readInt8(); // Corresponds to Python's struct.unpack("<B", self.packet.read(1))[0]

    // Calculate `n` for `postHeaderLen`.
    // `this.eventSize` (from super's eventHeader) is the total event size including its 19-byte header.
    // The payload starts after the 19-byte header.
    // The length of the FormatDescriptionEvent's payload is `this.eventSize - 19`.
    // Fixed bytes already read in the payload:
    // 2 (binlogVersion) + 50 (mysqlVersionStr) + 4 (created) + 1 (commonHeaderLen) = 57 bytes.
    const fixedPayloadBytesRead = 2 + 50 + 4 + 1;
    const checksumAlgorithmLen = 1; // MySQL 5.6+ binlog-checksum=CRC32 uses 1 byte for algorithm
    const checksumLen = 4; // CRC32 checksum itself is 4 bytes

    // `n` is the number of bytes for `postHeaderLen` array.
    const payloadSize = this.eventSize - 19; // Total payload size for FDE
    const n = payloadSize - fixedPayloadBytesRead - checksumAlgorithmLen - checksumLen;

    this.postHeaderLen = Array.from(this.packet.readBuffer(n)); // Corresponds to Python's struct.unpack(f"<{n}B", self.packet.read(n))

    this.serverVersionSplit = Array.from(this.packet.readBuffer(3)); // Corresponds to Python's struct.unpack("<3B", self.packet.read(3))

    this.numberOfEventTypes = this.packet.readInt8(); // Corresponds to Python's struct.unpack("<B", self.packet.read(1))[0]
  }
}

/**
 * Represents a Stop Event in the MySQL binary log.
 * This event is logged when the server stops.
 */
class StopEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);
    // StopEvent has no additional payload fields according to the Python code,
    // so no further reads are performed here.
  }
}

/**
 * An XA prepare event is generated for an XA prepared transaction.
 * Like XidEvent, it contains XID of the **prepared** transaction.
 *
 * For more information: `[see details] <https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html>`_.
 */
class XAPrepareEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {boolean} onePhase - Current XA transaction commit method (true for ONE PHASE, false for PREPARE).
   * @property {number} xidFormatId - A number that identifies the format used by the gtrid and bqual values.
   * @property {Buffer} xidGtrid - The global transaction ID part of XID.
   * @property {Buffer} xidBqual - The branch qualifier part of XID.
   * @property {string} xid - The combined decoded XID (getter).
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    // onePhase is True: XA COMMIT ... ONE PHASE
    // onePhase is False: XA PREPARE
    this.onePhase = this.packet.readBuffer(1)[0] !== 0x00;
    this.xidFormatId = this.packet.readInt32(); // Corresponds to Python's struct.unpack("<I", self.packet.read(4))[0]
    const gtridLength = this.packet.readInt32(); // Corresponds to Python's struct.unpack("<I", self.packet.read(4))[0]
    const bqualLength = this.packet.readInt32(); // Corresponds to Python's struct.unpack("<I", self.packet.read(4))[0]
    this.xidGtrid = this.packet.readBuffer(gtridLength);
    this.xidBqual = this.packet.readBuffer(bqualLength);
  }

  /**
   * Returns the combined and decoded XID (xidGtrid + xidBqual).
   * @returns {string} The XID.
   */
  get xid() {
    return this.xidGtrid.toString('utf8') + this.xidBqual.toString('utf8');
  }
}

/**
 * A COMMIT event generated when COMMIT of a transaction that modifies one or more tables of an XA-capable storage engine occurs.
 *
 * For more information: `[see details] <https://mariadb.com/kb/en/xid_event/>`_.
 */
class XidEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number|string} xid - Transaction ID for 2 Phase Commit (uint64, can be number or string).
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);
    this.xid = this.packet.readInt64(); // Corresponds to Python's struct.unpack("<Q", self.packet.read(8))[0]
  }
}


/**
 * A Heartbeat event.
 * Heartbeats are sent by the master when there are no unsent events in the binary log file
 * after a certain period of time (defined by MASTER_HEARTBEAT_PERIOD connection setting).
 *
 * `[see MASTER_HEARTBEAT_PERIOD] <https://dev.mysql.com/doc/refman/8.0/en/change-master-to.html>`_.
 */
class HeartbeatLogEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {string} ident - Name of the current binlog.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);
    // Payload size is total event size - common header size (19 bytes).
    const payloadBytes = this.eventSize - 19;
    this.ident = this.packet.readString(payloadBytes, 'utf8'); // Corresponds to Python's .decode()
  }
}


/**
 * QueryEvent is generated for each query that modified database.
 * If row-based replication is used, DML will be logged as RowsEvent instead.
 */
class QueryEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number} slaveProxyId - The id of the thread that issued this statement on the master server.
   * @property {number} executionTime - The time from when the query started to when it was logged in the binlog, in seconds.
   * @property {number} schemaLength - The length of the name of the currently selected database.
   * @property {number} errorCode - Error code generated by the master.
   * @property {number} statusVarsLength - The length of the status variable.
   * @property {Buffer} schema - The name of the currently selected database (raw buffer).
   * @property {string} query - The query executed.
   * @property {number} [flags2] - Q_FLAGS2_CODE status variable value.
   * @property {number|string} [sqlMode] - Q_SQL_MODE_CODE status variable value.
   * @property {number} [autoIncrementIncrement] - Q_AUTO_INCREMENT status variable value.
   * @property {number} [autoIncrementOffset] - Q_AUTO_INCREMENT status variable value.
   * @property {number} [characterSetClient] - Q_CHARSET_CODE status variable value.
   * @property {number} [collationConnection] - Q_CHARSET_CODE status variable value.
   * @property {number} [collationServer] - Q_CHARSET_CODE status variable value.
   * @property {Buffer} [timeZone] - Q_TIME_ZONE_CODE status variable value.
   * @property {Buffer} [catalogNzCode] - Q_CATALOG_NZ_CODE status variable value.
   * @property {number} [lcTimeNamesNumber] - Q_LC_TIME_NAMES_CODE status variable value.
   * @property {number} [charsetDatabaseNumber] - Q_CHARSET_DATABASE_CODE status variable value.
   * @property {number|string} [tableMapForUpdate] - Q_TABLE_MAP_FOR_UPDATE_CODE status variable value.
   * @property {Buffer} [user] - Q_INVOKER user.
   * @property {Buffer} [host] - Q_INVOKER host.
   * @property {Array<string>} [mtsAccessedDbNames] - Q_UPDATED_DB_NAMES status variable value.
   * @property {number} [microseconds] - Q_MICROSECONDS status variable value.
   * @property {number} [explicitDefaultsTs] - Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP status variable value.
   * @property {number|string} [ddlXid] - Q_DDL_LOGGED_WITH_XID status variable value.
   * @property {number} [defaultCollationForUtf8mb4Number] - Q_DEFAULT_COLLATION_FOR_UTF8MB4 status variable value.
   * @property {number} [sqlRequirePrimaryKey] - Q_SQL_REQUIRE_PRIMARY_KEY status variable value.
   * @property {number} [defaultTableEncryption] - Q_DEFAULT_TABLE_ENCRYPTION status variable value.
   * @property {number} [hrnow] - Q_HRNOW status variable value.
   * @property {number|string} [xid] - Q_XID status variable value.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    // Post-header
    this.slaveProxyId = this.packet.readInt32();
    this.executionTime = this.packet.readInt32();
    this.schemaLength = this.packet.readInt8();
    this.errorCode = this.packet.readInt16();
    this.statusVarsLength = this.packet.readInt16();

    // Payload - Status Variables
    const statusVarsEndPos = this.packet.offset + this.statusVarsLength;
    while (this.packet.offset < statusVarsEndPos) {
      const statusVarsKey = this.packet.readInt8();
      this._readStatusVarsValueForKey(statusVarsKey);
    }

    this.schema = this.packet.readBuffer(this.schemaLength);
    this.packet.skip(1); // Skip the null byte after schema

    // Calculate remaining bytes for the query string
    // Total event size - common header (19) - post-header (4+4+1+2+2 = 13) - statusVarsLength - schemaLength - 1 (null byte after schema)
    const queryLength = this.eventSize - 19 - 13 - this.statusVarsLength - this.schemaLength - 1;
    this.query = this.packet.readString(queryLength, 'utf8');
  }

  /**
   * Parses status variable VALUE for a given KEY.
   * A status variable in query events is a sequence of status KEY-VALUE pairs.
   * @param {number} key - Key for the status variable.
   * @private
   */
  _readStatusVarsValueForKey(key) {
    switch (key) {
      case Types.Q_FLAGS2_CODE: // 0x00
        this.flags2 = this.packet.readInt32();
        break;
      case Types.Q_SQL_MODE_CODE: // 0x01
        this.sqlMode = this.packet.readInt64();
        break;
      case Types.Q_CATALOG_CODE: // 0x02 for MySQL 5.0.x - no value to read
        break;
      case Types.Q_AUTO_INCREMENT: // 0x03
        this.autoIncrementIncrement = this.packet.readInt16();
        this.autoIncrementOffset = this.packet.readInt16();
        break;
      case Types.Q_CHARSET_CODE: // 0x04
        this.characterSetClient = this.packet.readInt16();
        this.collationConnection = this.packet.readInt16();
        this.collationServer = this.packet.readInt16();
        break;
      case Types.Q_TIME_ZONE_CODE: // 0x05
        const timeZoneLen = this.packet.readInt8();
        if (timeZoneLen) {
          this.timeZone = this.packet.readBuffer(timeZoneLen);
        }
        break;
      case Types.Q_CATALOG_NZ_CODE: // 0x06
        const catalogLen = this.packet.readInt8();
        if (catalogLen) {
          this.catalogNzCode = this.packet.readBuffer(catalogLen);
        }
        break;
      case Types.Q_LC_TIME_NAMES_CODE: // 0x07
        this.lcTimeNamesNumber = this.packet.readInt16();
        break;
      case Types.Q_CHARSET_DATABASE_CODE: // 0x08
        this.charsetDatabaseNumber = this.packet.readInt16();
        break;
      case Types.Q_TABLE_MAP_FOR_UPDATE_CODE: // 0x09
        this.tableMapForUpdate = this.packet.readInt64();
        break;
      case Types.Q_MASTER_DATA_WRITTEN_CODE: // 0x0A - no value to read
        break;
      case Types.Q_INVOKER: // 0x0B
        const userLen = this.packet.readInt8();
        if (userLen) {
          this.user = this.packet.readBuffer(userLen);
        }
        const hostLen = this.packet.readInt8();
        if (hostLen) {
          this.host = this.packet.readBuffer(hostLen);
        }
        break;
      case Types.Q_UPDATED_DB_NAMES: // 0x0C
        const mtsAccessedDbs = this.packet.readInt8();
        if (mtsAccessedDbs === 254) {
          return; // Special case: 1 db, name is ""
        }
        const dbs = [];
        for (let i = 0; i < mtsAccessedDbs; i++) {
          const dbName = this.packet.readLengthCodedString('utf8');
          dbs.push(dbName);
        }
        this.mtsAccessedDbNames = dbs;
        break;
      case Types.Q_MICROSECONDS: // 0x0D
        this.microseconds = this.packet.readInt24();
        break;
      case Types.Q_COMMIT_TS: // 0x0E - no value to read
        break;
      case Types.Q_COMMIT_TS2: // 0x0F - no value to read
        break;
      case Types.Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP: // 0x10
        this.explicitDefaultsTs = this.packet.readInt8();
        break;
      case Types.Q_DDL_LOGGED_WITH_XID: // 0x11
        this.ddlXid = this.packet.readInt64();
        break;
      case Types.Q_DEFAULT_COLLATION_FOR_UTF8MB4: // 0x12
        this.defaultCollationForUtf8mb4Number = this.packet.readInt16();
        break;
      case Types.Q_SQL_REQUIRE_PRIMARY_KEY: // 0x13
        this.sqlRequirePrimaryKey = this.packet.readInt8();
        break;
      case Types.Q_DEFAULT_TABLE_ENCRYPTION: // 0x14
        this.defaultTableEncryption = this.packet.readInt8();
        break;
      case Types.Q_HRNOW:
        this.hrnow = this.packet.readInt24();
        break;
      case Types.Q_XID:
        this.xid = this.packet.readInt64();
        break;
      default:
        throw new StatusVariableMismatch(`Unknown status variable key: 0x${key.toString(16)}`);
    }
  }
}

/**
 * This event is written into the binary log file for LOAD DATA INFILE events
 * if the server variable `binlog_mode` was set to "STATEMENT".
 */
class BeginLoadQueryEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number} fileId - The id of the file.
   * @property {Buffer} blockData - Data block about "LOAD DATA INFILE".
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    // Payload
    this.fileId = this.packet.readInt32(); // Corresponds to Python's read_uint32()
    // Remaining bytes: total event size - common header size (19) - fileId size (4)
    const remainingPayloadBytes = this.eventSize - 19 - 4;
    this.blockData = this.packet.readBuffer(remainingPayloadBytes);
  }
}

/**
 * This event handles "LOAD DATA INFILE" statement.
 * It is similar to a `QueryEvent` except that it has extra static fields.
 */
class ExecuteLoadQueryEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number} slaveProxyId - The id of the thread that issued this statement on the master server.
   * @property {number} executionTime - The number of seconds that the statement took to execute.
   * @property {number} schemaLength - The length of the default database's name when the statement was executed.
   * @property {number} errorCode - The error code resulting from execution of the statement on the master.
   * @property {number} statusVarsLength - The length of the status variable block.
   * @property {number} fileId - The id of the loaded file.
   * @property {number} startPos - Offset from the start of the statement to the beginning of the filename.
   * @property {number} endPos - Offset from the start of the statement to the end of the filename.
   * @property {number} dupHandlingFlags - How LOAD DATA INFILE handles duplicated data (0x0: error, 0x1: ignore, 0x2: replace).
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    // Post-header
    this.slaveProxyId = this.packet.readInt32();
    this.executionTime = this.packet.readInt32();
    this.schemaLength = this.packet.readInt8();
    this.errorCode = this.packet.readInt16();
    this.statusVarsLength = this.packet.readInt16();

    // Payload
    this.fileId = this.packet.readInt32();
    this.startPos = this.packet.readInt32();
    this.endPos = this.packet.readInt32();
    this.dupHandlingFlags = this.packet.readInt8();
  }
}


/**
 * Stores the value of auto-increment variables.
 * This event will be created just before a QueryEvent.
 */
class IntvarEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number} type - 1 byte identifying the type of variable stored. Can be either LAST_INSERT_ID_EVENT (1) or INSERT_ID_EVENT (2).
   * @property {number} value - The value of the variable.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    // Payload
    this.type = this.packet.readInt8(); // Corresponds to Python's read_uint8()
    this.value = this.packet.readInt32(); // Corresponds to Python's read_uint32()
  }
}

/**
 * RandEvent is generated every time a statement uses the RAND() function.
 * Indicates the seed values to use for generating a random number with RAND() in the next statement.
 *
 * RandEvent only works in statement-based logging (need to set binlog_format as 'STATEMENT')
 * and only works when the seed number is not specified.
 */
class RandEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number|string} seed1 - Value for the first seed.
   * @property {number|string} seed2 - Value for the second seed.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);
    // Payload
    this.seed1 = this.packet.readInt64(); // Corresponds to Python's read_uint64()
    this.seed2 = this.packet.readInt64(); // Corresponds to Python's read_uint64()
  }
}


/**
 * UserVarEvent is generated every time a statement uses a user variable.
 * Indicates the value to use for the user variable in the next statement.
 */
class UserVarEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number} nameLen - Length of user variable name.
   * @property {string} name - User variable name.
   * @property {string|number|Buffer} value - Value of the user variable. Can be string, number, or Buffer/other depending on type.
   * @property {number} type - Type of the user variable.
   * @property {number} charset - The number of the character set for the user variable.
   * @property {number} isNull - Non-zero if the variable value is the SQL NULL value, 0 otherwise.
   * @property {number} flags - Extra flags associated with the user variable.
   * @property {number} valueLen - Length of the user variable's value in bytes.
   * @property {Object.<number, Array<string|Function>>} typeToCodesAndMethod - Maps user variable type codes to their names and parsing methods.
   * @property {Buffer} tempValueBuffer - Temporary buffer holding the raw value bytes before parsing.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    // Payload
    this.nameLen = this.packet.readInt32(); // Corresponds to Python's read_uint32()
    this.name = this.packet.readString(this.nameLen, 'utf8'); // .decode()
    this.isNull = this.packet.readInt8(); // Corresponds to Python's read_uint8()

    this.typeToCodesAndMethod = {
      0x00: ["STRING_RESULT", this._readString.bind(this)],
      0x01: ["REAL_RESULT", this._readReal.bind(this)],
      0x02: ["INT_RESULT", this._readInt.bind(this)],
      0x03: ["ROW_RESULT", this._readDefault.bind(this)],
      0x04: ["DECIMAL_RESULT", this._readDecimal.bind(this)],
    };

    this.value = null;
    this.flags = null;
    this.tempValueBuffer = NativeBuffer.alloc(0); // Initialize as empty Buffer

    if (!this.isNull) {
      this.type = this.packet.readInt8(); // Corresponds to Python's read_uint8()
      this.charset = this.packet.readInt32(); // Corresponds to Python's read_uint32()
      this.valueLen = this.packet.readInt32(); // Corresponds to Python's read_uint32()
      this.tempValueBuffer = this.packet.readBuffer(this.valueLen);
      this.flags = this.packet.readInt8(); // Corresponds to Python's read_uint8()
      this._setValueFromTempBuffer();
    } else {
      // If null, all related properties are null
      this.type = null;
      this.charset = null;
      this.valueLen = null;
      this.value = null;
      this.flags = null;
    }
  }

  /**
   * Sets the `value` property from the `tempValueBuffer` based on `this.type`.
   * @private
   */
  _setValueFromTempBuffer() {
    if (this.tempValueBuffer.length > 0) {
      const typeCodeEntry = this.typeToCodesAndMethod[this.type];
      if (typeCodeEntry) {
        const [codeName, readMethod] = typeCodeEntry;
        if (codeName === "INT_RESULT") {
          this.value = readMethod(this.tempValueBuffer, this.flags);
        } else {
          this.value = readMethod(this.tempValueBuffer);
        }
      } else {
        // Fallback for unknown type codes, similar to Python's _read_default
        this.value = this._readDefault(this.tempValueBuffer);
      }
    }
  }

  /**
   * Reads string data from a buffer.
   * @param {Buffer} buffer - The buffer containing the string data.
   * @returns {string} The decoded string.
   * @private
   */
  _readString(buffer) {
    return buffer.toString('utf8'); // Corresponds to Python's .decode()
  }

  /**
   * Reads real (double) data from a buffer.
   * @param {Buffer} buffer - The buffer containing the double data.
   * @returns {number} The decoded double.
   * @private
   */
  _readReal(buffer) {
    return buffer.readDoubleLE(0); // Corresponds to Python's struct.unpack("<d", buffer)[0]
  }

  /**
   * Reads integer data from a buffer.
   * Handles various integer sizes and signed/unsigned based on flags.
   * @param {Buffer} buffer - The buffer containing the integer data.
   * @param {number} flags - Flags to determine signed/unsigned (1 for unsigned, 0 for signed).
   * @returns {number|string} The decoded integer (number or string for large integers).
   * @private
   */
  _readInt(buffer, flags) {
    // In Python, <Q is unsigned 64-bit, <q is signed 64-bit.
    // NodeJS Buffer.readUInt/IntXYLE methods handle sizes up to 64-bit for BigInt or 32-bit for Number.
    // For 64-bit numbers that might exceed Number.MAX_SAFE_INTEGER, return as string.
    if (buffer.length === 8) {
      const low = buffer.readUInt32LE(0);
      const high = buffer.readUInt32LE(4);
      const l = new Long(low, high, flags === 1); // true for unsigned if flags == 1
      const num = l.toNumber();
      // If the number exceeds safe integer limits, return as string
      if (num.toString() !== l.toString()) {
        return l.toString();
      }
      return num;
    } else if (buffer.length === 4) {
      return flags === 1 ? buffer.readUInt32LE(0) : buffer.readInt32LE(0);
    } else if (buffer.length === 2) {
      return flags === 1 ? buffer.readUInt16LE(0) : buffer.readInt16LE(0);
    } else if (buffer.length === 1) {
      return flags === 1 ? buffer.readUInt8(0) : buffer.readInt8(0);
    }
    console.warn(`UserVarEvent._readInt: Unexpected buffer length for integer: ${buffer.length}`);
    return buffer.toString('hex'); // Fallback for unexpected lengths
  }

  /**
   * Reads decimal data from a buffer using the external `parseDecimalFromBytes` helper.
   * @param {Buffer} buffer - The buffer containing the decimal data.
   * @returns {string} The decoded decimal as a string.
   * @private
   */
  _readDecimal(buffer) {
    // `this.tempValueBuffer` contains precision and decimals in its first two bytes.
    // `buffer` here is `tempValueBuffer.slice(2)`.
    if (this.tempValueBuffer.length < 2) {
      console.warn("UserVarEvent._readDecimal: tempValueBuffer too short for precision/decimals.");
      return buffer.toString('hex');
    }
    const precisionFromBuffer = this.tempValueBuffer[0];
    const decimalsFromBuffer = this.tempValueBuffer[1];
    const rawDecimalBytes = this.tempValueBuffer.slice(2); // This matches the `buffer` passed in Python

    return parseDecimalFromBytes(rawDecimalBytes, precisionFromBuffer, decimalsFromBuffer);
  }

  /**
   * Reads default data (raw buffer). Used when the type is unknown or null.
   * @param {Buffer} buffer - The buffer containing the data.
   * @returns {Buffer} The raw buffer.
   * @private
   */
  _readDefault(buffer) {
    // In Python, this would read from `self.packet`. In JS, `tempValueBuffer`
    // has already been read from `this.packet`, so we return that.
    return buffer;
  }
}

/**
 * Used as a temporary class for events that have not yet been implemented.
 * The event referencing this class skips parsing its payload.
 */
class NotImplementedEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);
    // Skip the rest of the payload. The payload size is `eventSize - 19` bytes.
    // The Packet's offset is already at the start of the payload due to super() call.
    const payloadBytesToSkip = this.eventSize - 19;
    this.packet.skip(payloadBytesToSkip);
  }
}

/**
 * GTID change in binlog event
 *
 * For more information: `[GTID] <https://mariadb.com/kb/en/gtid/>`_ `[see also] <https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Gtid__event.html>`_
 */
class GtidEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {boolean} commitFlag - True if transaction may have changes logged with SBR.
   * @property {Buffer} sid - 16 byte sequence - UUID representing the SID.
   * @property {number|string} gno - Group number, second component of GTID.
   * @property {number} ltType - The type of logical timestamp used in the logical clock fields.
   * @property {number|string} [lastCommitted] - Store the transaction's commit parent sequenceNumber (for MySQL 5.7+).
   * @property {number|string} [sequenceNumber] - The transaction's logical timestamp assigned at prepare phase (for MySQL 5.7+).
   * @property {string} gtid - The GTID string (getter).
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    this.commitFlag = this.packet.readInt8() === 1; // struct.unpack("!B", self.packet.read(1))[0] == 1
    this.sid = this.packet.readBuffer(16); // 16 byte sequence - UUID
    this.gno = this.packet.readInt64(); // struct.unpack("<Q", self.packet.read(8))[0] (uint64)
    this.ltType = this.packet.readInt8(); // self.packet.read(1)[0] (uint8)

    // Conditional fields for MySQL 5.7+
    if (this.mysqlVersion[0] >= 5 && this.mysqlVersion[1] >= 7) {
      this.lastCommitted = this.packet.readInt64(); // struct.unpack("<Q", self.packet.read(8))[0] (uint64)
      this.sequenceNumber = this.packet.readInt64(); // struct.unpack("<Q", self.packet.read(8))[0] (uint64)
    }
  }

  /**
   * GTID = source_id:transaction_id
   * Eg: 3E11FA47-71CA-11E1-9E33-C80AA9429562:23
   * See: http://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html
   * @returns {string} The formatted GTID string.
   */
  get gtid() {
    const nibbles = this.sid.toString('hex'); // binascii.hexlify(self.sid).decode("ascii")
    const gtidString = (
      `${nibbles.substring(0, 8)}-` +
      `${nibbles.substring(8, 12)}-` +
      `${nibbles.substring(12, 16)}-` +
      `${nibbles.substring(16, 20)}-` +
      `${nibbles.substring(20)}:` +
      `${this.gno}`
    );
    return gtidString;
  }
}

/**
 * PreviousGtidsEvent contains the Gtids executed in the last binary log file.
 * Attributes:
 * nSid: Which size is the gtid_set
 * sid: 16bytes UUID as a binary
 * nIntervals: How many intervals are sent
 * Eg: [4c9e3dfc-9d25-11e9-8d2e-0242ac1cfd7e:1-100, 4c9e3dfc-9d25-11e9-8d2e-0242ac1cfd7e:1-10:20-30]
 */
class PreviousGtidsEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   *
   * @property {number|string} nSid - The number of SIDs in the GTID set.
   * @property {Array<string>} gtids - List of formatted GTID strings.
   * @property {string} previousGtids - Comma-separated string of GTIDs.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    this.nSid = this.packet.readInt64(); // Corresponds to Python's read_int64()
    this.gtids = [];

    for (let i = 0; i < this.nSid; i++) {
      const sid = this.packet.readBuffer(16); // 16 bytes UUID
      const nIntervals = this.packet.readInt64(); // uint64
      const intervals = [];
      for (let j = 0; j < nIntervals; j++) {
        const start = this.packet.readInt64(); // int64
        const end = this.packet.readInt64(); // uint64
        intervals.push(`${start}-${end}`);
      }

      const nibbles = sid.toString('hex'); // binascii.hexlify(sid).decode("ascii")
      const gtid = (
        `${nibbles.substring(0, 8)}-` +
        `${nibbles.substring(8, 12)}-` +
        `${nibbles.substring(12, 16)}-` +
        `${nibbles.substring(16, 20)}-` +
        `${nibbles.substring(20)}:` +
        `${intervals.join(':')}`
      );
      this.gtids.push(gtid);
    }

    this.previousGtids = this.gtids.join(',');
  }
}

module.exports = {
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
  NotImplementedEvent, // Still export explicitly for direct reference if needed
  GtidEvent,
  PreviousGtidsEvent,
};
