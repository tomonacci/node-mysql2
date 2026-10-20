// lib/binlog/row_event_types.js
// This file contains implementations for MySQL row-based binlog event types.

'use strict';

const BinLogEvent = require('./event_base.js'); // Import base BinLogEvent
const { decimalByteSize, parseDecimalFromBytes } = require('./helpers.js'); // Import helpers
const { decodeJson, decodeJsonDiffs } = require('./json_binary.js');
const Types = require('../constants/types.js'); // Assuming this contains relevant event type constants
const EventTypes = require('../constants/binlog_event_types.js'); // Assuming this contains relevant event type constants

class NotImplementedError extends Error {
  constructor(message) {
    super(message);
    this.name = 'NotImplementedError';
  }
}

// --- Enums and Constants ---

/**
 * Enum for sources of NULL values in row events.
 * Corresponds to Python's NONE_SOURCE.
 */
const NONE_SOURCE = {
  NULL: 'null',
  COLS_BITMAP: 'cols_bitmap',
  OUT_OF_DATETIME_RANGE: 'out_of_datetime_range',
  OUT_OF_DATE_RANGE: 'out_of_date_range',
  OUT_OF_DATETIME2_RANGE: 'out_of_datetime2_range',
  EMPTY_SET: 'empty_set',
  JSON_PARTIAL_UPDATE: 'json_partial_update',
};

/**
 * Enum for row image types (used in RowsEvent V2).
 * Corresponds to Python's RowImageType.
 */
const RowImageType = {
  Undefined: 0,
  Full: 1, // Full row image
  UpdateBI: 2, // Before image for update
  UpdateAI: 3, // After image for update
  Delete: 4, // Delete image
  Write: 5, // Write image
  Minimal: 6, // Minimal row image
};

const BINARY_CHARSET_ID = 63;
const TIME_INTEGRAL_OFFSET = 0x800000;
const TIME_FRACTION_OFFSET = 0x800000000000n;

const pad = (value, width) => String(value).padStart(width, '0');

function formatTime(negative, hours, minutes, seconds, microseconds, fsp) {
  const fraction = fsp > 0 ? `.${pad(microseconds, 6).slice(0, fsp)}` : '';
  return `${negative ? '-' : ''}${pad(hours, 2)}:${pad(minutes, 2)}:${pad(seconds, 2)}${fraction}`;
}

// --- Helper Functions for Bit Manipulation ---

/**
 * Counts the number of set bits (1s) in a buffer or number.
 * Corresponds to Python's BitCount.
 * @param {Buffer|number} value - The buffer or number to count bits in.
 * @returns {number} The count of set bits.
 */
function BitCount(value) {
  if (Buffer.isBuffer(value)) {
    let count = 0;
    for (let i = 0; i < value.length; i++) {
      let byte = value[i];
      while (byte > 0) {
        byte &= byte - 1; // Brian Kernighan's algorithm
        count++;
      }
    }
    return count;
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    let count = 0;
    let n = BigInt(value);
    while (n > 0n) {
      n &= n - 1n;
      count++;
    }
    return count;
  }
  return 0;
}

/**
 * Checks if a specific bit at a given position is set in a buffer or number.
 * Corresponds to Python's BitGet.
 * @param {Buffer|number|bigint} value - The buffer or number to check.
 * @param {number} position - The bit position to check (0-indexed).
 * @returns {number} 1 if the bit is set, 0 otherwise.
 */
function BitGet(value, position) {
  if (Buffer.isBuffer(value)) {
    const byteIndex = Math.floor(position / 8);
    const bitOffset = position % 8;
    if (byteIndex >= value.length) {
      return 0; // Out of bounds
    }
    return value[byteIndex] & (1 << bitOffset) ? 1 : 0;
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    return BigInt(value) & (1n << BigInt(position)) ? 1 : 0;
  }
  return 0;
}

// --- Row Event Classes ---

/**
 * Base class for all MySQL row-based replication events (Write, Update, Delete).
 * It handles common logic for parsing row data and column values.
 */
class RowsEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options, including filtering.
   * @param {Array<string>} [options.onlyTables=null] - List of tables to include.
   * @param {Array<string>} [options.ignoredTables=null] - List of tables to ignore.
   * @param {Array<string>} [options.onlySchemas=null] - List of schemas to include.
   * @param {Array<string>} [options.ignoredSchemas=null] - List of schemas to ignore.
   * @param {boolean} [options.bigNumberStrings=false] - Decode every BIGINT as a string, not only those beyond 2^53.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    this.tableMap = tableMap;
    this.__rows = null;
    this.__only_tables = options.onlyTables || null;
    this.__ignored_tables = options.ignoredTables || null;
    this.__only_schemas = options.onlySchemas || null;
    this.__ignored_schemas = options.ignoredSchemas || null;
    this.__big_number_strings = options.bigNumberStrings === true;
    this.__none_sources = {}; // To track why a value is None/null

    // Header - Table ID is common for all RowsEvents
    this.tableId = this.readTableId();

    // Additional information from table map
    try {
      this.primaryKey = tableMap[this.tableId].data.primary_key;
      this.schema = tableMap[this.tableId].schema;
      this.table = tableMap[this.tableId].table;
    } catch (e) {
      // If the corresponding TableMap Event was filtered or not found
      // This event should not be processed.
      console.warn('TableMapEvent filtered?', e, this.tableId, tableMap);
      this.processed = false;
      return;
    }

    // Apply filtering based on options
    if (
      this.__only_tables !== null &&
      !this.__only_tables.includes(this.table)
    ) {
      this.processed = false;
      return;
    }
    if (
      this.__ignored_tables !== null &&
      this.__ignored_tables.includes(this.table)
    ) {
      this.processed = false;
      return;
    }
    if (
      this.__only_schemas !== null &&
      !this.__only_schemas.includes(this.schema)
    ) {
      this.processed = false;
      return;
    }
    if (
      this.__ignored_schemas !== null &&
      this.__ignored_schemas.includes(this.schema)
    ) {
      this.processed = false;
      return;
    }
    this.processed = true;

    // Event V2 specific header (only for WRITE_ROWS_EVENT_V2, DELETE_ROWS_EVENT_V2, UPDATE_ROWS_EVENT_V2, PARTIAL_UPDATE_ROWS_EVENT)
    if (
      this.eventType === EventTypes.WRITE_ROWS_EVENT_V2 ||
      this.eventType === EventTypes.DELETE_ROWS_EVENT_V2 ||
      this.eventType === EventTypes.UPDATE_ROWS_EVENT_V2 ||
      this.eventType === EventTypes.PARTIAL_UPDATE_ROWS_EVENT
    ) {
      this.flags = this.packet.readInt16(); // H
      this.extraDataLength = this.packet.readInt16(); // H
      // extra_data_length includes its own 2 bytes, so we subtract to get actual extra data bytes
      if (this.extraDataLength > 2) {
        this.extraDataType = this.packet.readInt8(); // B
        // NDB information
        if (this.extraDataType === 0) {
          this.nbdInfoLength = this.packet.readInt8(); // B
          this.nbdInfoFormat = this.packet.readInt8(); // B
          this.nbdInfo = this.packet.readBuffer(this.nbdInfoLength - 2);
        }
        // Partition information
        else if (this.extraDataType === 1) {
          if (
            this.eventType === EventTypes.UPDATE_ROWS_EVENT_V2 ||
            this.eventType === EventTypes.PARTIAL_UPDATE_ROWS_EVENT
          ) {
            this.partitionId = this.packet.readInt16(); // H
            this.sourcePartitionId = this.packet.readInt16(); // H
          } else {
            this.partitionId = this.packet.readInt16(); // H
          }
        }
        // etc - remaining extra data
        else {
          // 3 bytes read (1 for extraDataType, 2 for length/format or partition ID part)
          this.extraData = this.packet.readBuffer(this.extraDataLength - 3);
        }
      }
    } else {
      // For V1 events, flags are directly after table_id (2 bytes)
      this.flags = this.packet.readInt16(); // H
    }

    // Body
    this.numberOfColumns = this.packet.readLengthCodedNumber();
    this.columns = this.tableMap[this.tableId].columns;
  }

  /**
   * Checks if a column value is NULL based on the null bitmap.
   * @param {Buffer} nullBitmap - The null bitmap buffer.
   * @param {number} position - The position of the column in the null bitmap.
   * @returns {boolean} True if the column is null, false otherwise.
   * @private
   */
  _is_null(nullBitmap, position) {
    return BitGet(nullBitmap, position) !== 0;
  }

  /**
   * Reads column data based on the columns present bitmap.
   * This method is used by WRITE, UPDATE, and DELETE events.
   * @param {Buffer} colsBitmap - Bitmap indicating which columns are present.
   * @param {number} [rowImageType=RowImageType.Undefined] - Type of row image (e.g., UpdateAI).
   * @returns {Object.<string, any>} A hash map of column names to their values.
   * @private
   */
  _read_column_data(colsBitmap, rowImageType = RowImageType.Undefined) {
    this.isPartialJsonUpdate = false;
    let partialBitmap = null;

    if (
      this.eventType === EventTypes.PARTIAL_UPDATE_ROWS_EVENT &&
      rowImageType === RowImageType.UpdateAI
    ) {
      const binlogRowValueOption = this.packet.readLengthCodedNumber();
      this.isPartialJsonUpdate = (binlogRowValueOption & 1) !== 0;
      if (this.isPartialJsonUpdate) {
        partialBitmap = this.packet.readBuffer(
          Math.ceil(this._json_column_count() / 8)
        );
      }
    }

    const values = {};

    // Null bitmap length = (bits set in 'columns-present-bitmap' + 7) / 8
    // See http://dev.mysql.com/doc/internals/en/rows-event.html
    const nullBitmapLength = Math.ceil(BitCount(colsBitmap) / 8);
    const nullBitmap = this.packet.readBuffer(nullBitmapLength);
    this.nullBitmap = nullBitmap;

    let nullBitmapIndex = 0;
    let partialBitmapIndex = 0;
    const nbColumns = this.columns.length;

    for (let i = 0; i < nbColumns; i++) {
      let isPartial = false;
      const column = this.columns[i];
      let name = column.name;
      const unsigned = column.unsigned;

      if (
        this.isPartialJsonUpdate &&
        rowImageType === RowImageType.UpdateAI &&
        column.type === Types.JSON
      ) {
        if (BitGet(partialBitmap, partialBitmapIndex) > 0) {
          isPartial = true;
        }
        partialBitmapIndex++;
      }

      if (!name) {
        // If using minimal binlog_row_metadata, column names might not be available.
        // If you know column information,
        // MySQL 5.7+ can set binlog_row_metadata = "FULL".
        name = `UNKNOWN_COL${i}`;
      }

      values[name] = this._read_values_name(
        column,
        nullBitmap,
        nullBitmapIndex,
        isPartial,
        colsBitmap,
        unsigned,
        i
      );

      // Only advance null_bitmap_index if the column is present in colsBitmap
      if (BitGet(colsBitmap, i) !== 0) {
        nullBitmapIndex++;
      }
    }
    return values;
  }

  /**
   * Reads the value for a single column based on its type and other properties.
   * @param {Object} column - The column definition object.
   * @param {Buffer} nullBitmap - The null bitmap for the current row.
   * @param {number} nullBitmapIndex - The current index in the null bitmap.
   * @param {boolean} isPartial - True if it's a partial JSON update.
   * @param {Buffer} colsBitmap - The columns present bitmap for the row.
   * @param {boolean} unsigned - True if the integer column is unsigned.
   * @param {number} i - The index of the column.
   * @returns {any} The parsed column value.
   * @private
   */
  _read_values_name(
    column,
    nullBitmap,
    nullBitmapIndex,
    isPartial,
    colsBitmap,
    unsigned,
    i
  ) {
    const name = this.tableMap[this.tableId].columns[i].name;

    if (BitGet(colsBitmap, i) === 0) {
      // This block is only executed when binlog_row_image = MINIMAL.
      // When binlog_row_image = FULL, this block does not execute.
      this.__none_sources[name] = NONE_SOURCE.COLS_BITMAP;
      return null;
    }

    if (this._is_null(nullBitmap, nullBitmapIndex)) {
      this.__none_sources[name] = NONE_SOURCE.NULL;
      return null;
    }

    switch (column.type) {
      case Types.TINY:
        return unsigned ? this.packet.readInt8() : this.packet.readSInt8();
      case Types.SHORT:
        return unsigned ? this.packet.readInt16() : this.packet.readSInt16();
      case Types.LONG:
        return unsigned ? this.packet.readInt32() : this.packet.readSInt32();
      case Types.INT24:
        return unsigned ? this.packet.readInt24() : this.packet.readSInt24();
      case Types.FLOAT:
        return this.packet.readFloat();
      case Types.DOUBLE:
        return this.packet.readDouble();
      case Types.VARCHAR:
      case Types.STRING:
      case Types.VAR_STRING:
        return column.max_length > 255
          ? this._read_string(2, column)
          : this._read_string(1, column);
      case Types.NEWDECIMAL:
        return this._read_new_decimal(column);
      case Types.BLOB:
      case Types.TINY_BLOB:
      case Types.MEDIUM_BLOB:
      case Types.LONG_BLOB:
        return this._read_string(column.length_size, column);
      case Types.DATETIME:
        const datetimeVal = this._read_datetime();
        if (datetimeVal === null) {
          this.__none_sources[name] = NONE_SOURCE.OUT_OF_DATETIME_RANGE;
        }
        return datetimeVal;
      case Types.TIME:
        return this._read_time();
      case Types.DATE:
        const dateVal = this._read_date();
        if (dateVal === null) {
          this.__none_sources[name] = NONE_SOURCE.OUT_OF_DATE_RANGE;
        }
        return dateVal;
      case Types.TIMESTAMP:
        return new Date(this.packet.readInt32() * 1000); // Unix timestamp to milliseconds
      case Types.DATETIME2:
        const datetime2Val = this._read_datetime2(column);
        if (datetime2Val === null) {
          this.__none_sources[name] = NONE_SOURCE.OUT_OF_DATETIME2_RANGE;
        }
        return datetime2Val;
      case Types.TIME2:
        return this._read_time2(column);
      case Types.TIMESTAMP2:
        // MySQL TIMESTAMP2 is 4 bytes for integer part, then FSP.
        // packet.readIntBeBySize(4) reads 4 bytes big-endian.
        const unixTimestamp = this.packet.readIntBeBySize(4);
        const baseDate = new Date(unixTimestamp * 1000); // Convert to milliseconds
        return this._add_fsp_to_time(baseDate, column);
      case Types.LONGLONG:
        if (this.__big_number_strings) {
          return unsigned
            ? this.packet.readInt64String()
            : this.packet.readSInt64String();
        }
        return unsigned ? this.packet.readInt64() : this.packet.readSInt64();
      case Types.YEAR: {
        const year = this.packet.readInt8();
        return year === 0 ? 0 : year + 1900;
      }
      case Types.ENUM: {
        const enumIndex = this.packet.readIntBySize(column.size);
        if (!column.enum_values) {
          return enumIndex;
        }
        return column.enum_values[enumIndex] ?? null;
      }
      case Types.SET: {
        const bitMask = this.packet.readIntBySize(column.size);
        if (!column.set_values) {
          return bitMask;
        }
        return column.set_values
          .filter((_, idx) => BitGet(bitMask, idx) !== 0)
          .join(',');
      }
      case Types.BIT:
        return this._read_bit(column);
      case Types.GEOMETRY:
        return this.packet.readBuffer(
          this.packet.readIntBySize(column.length_size)
        );
      case Types.JSON:
        return this._read_json(column, isPartial);
      default:
        throw new NotImplementedError(
          `Unknown MySQL column type: ${column.type}`
        );
    }
  }

  /**
   * Adds the fractional seconds part (FSP) to a Date/Time object.
   * @param {Date|Date} time - The base Date or Date object.
   * @param {Object} column - The column definition with fsp.
   * @returns {Date|Date} The Date/Date object with microseconds.
   * @private
   */
  _add_fsp_to_time(time, column) {
    const microsecond = this._read_fsp(column);
    if (microsecond > 0) {
      // Date.prototype.setMilliseconds handles up to 999.
      // For microseconds, we need to manually adjust.
      // A full `microseconds` property would require a custom Date object or library.
      // For now, we'll round to milliseconds.
      // If higher precision is required, a dedicated library like 'luxon' or custom object is needed.
      time.setMilliseconds(
        time.getMilliseconds() + Math.floor(microsecond / 1000)
      );
    }
    return time;
  }

  /**
   * Reads the fractional seconds part (FSP) from the packet.
   * @param {Object} column - The column definition with fsp.
   * @returns {number} The microseconds value.
   * @private
   */
  _read_fsp(column) {
    let readBytes = 0;
    if (column.fsp === 1 || column.fsp === 2) {
      readBytes = 1;
    } else if (column.fsp === 3 || column.fsp === 4) {
      readBytes = 2;
    } else if (column.fsp === 5 || column.fsp === 6) {
      readBytes = 3;
    }

    if (readBytes > 0) {
      let microsecond = this.packet.readIntBeBySize(readBytes);
      if (column.fsp % 2) {
        microsecond = Math.floor(microsecond / 10);
      }
      return microsecond * 10 ** (6 - column.fsp);
    }
    return 0;
  }

  /**
   * Helper to map MySQL charset names to Node.js Buffer encodings.
   * This is a simplified version; a full implementation would involve a comprehensive mapping.
   * @param {string} name - The MySQL charset name.
   * @returns {string} The corresponding Node.js Buffer encoding.
   * @private
   */
  charset_to_encoding(name) {
    // Simplified mapping. Add more as needed.
    const charsetMap = {
      utf8: 'utf8',
      utf8mb4: 'utf8', // Node.js 'utf8' handles utf8mb4 in most cases
      latin1: 'latin1',
      binary: 'binary',
      // Add other charsets as required
    };
    return charsetMap[name.toLowerCase()] || 'utf8'; // Default to utf8
  }

  /**
   * Reads a string from the packet.
   * @param {number} size - The size of the length field (1 or 2 bytes).
   * @param {Object} column - The column definition object.
   * @returns {string|Buffer} The decoded string, or the bytes of a binary column.
   * @private
   */
  _read_string(size, column) {
    const len = this.packet.readIntBySize(size);
    if (column.charset_id === BINARY_CHARSET_ID) {
      const bytes = this.packet.readBuffer(len);
      // The server strips the trailing zero padding of BINARY(n) values.
      return column.type === Types.STRING && len < column.max_length
        ? Buffer.concat([bytes, Buffer.alloc(column.max_length - len)])
        : bytes;
    }
    const encoding =
      column.character_set_name != null
        ? this.charset_to_encoding(column.character_set_name)
        : 'utf8';
    // TODO maybe do something with ignoreDecodeErrors
    return this.packet.readString(len, encoding);
  }

  /**
   * Reads MySQL BIT type.
   * @param {Object} column - The column definition object.
   * @returns {Buffer} The big-endian bytes of the BIT value, as a regular query returns it.
   * @private
   */
  _read_bit(column) {
    return this.packet.readBuffer(column.bytes);
  }

  /**
   * Reads MySQL TIME type (pre-5.6.4 format).
   * @returns {string} The time as [-]HH:MM:SS.
   * @private
   */
  _read_time() {
    const timeValue = this.packet.readSInt24();
    const absolute = Math.abs(timeValue);
    const hours = Math.floor(absolute / 10000);
    const minutes = Math.floor((absolute % 10000) / 100);
    const seconds = absolute % 100;
    return formatTime(timeValue < 0, hours, minutes, seconds, 0, 0);
  }

  /**
   * Reads MySQL TIME2 type.
   * See my_time_packed_from_binary in sql-common/my_time.cc: the integral part
   * is stored with a 0x800000 offset, and for a negative value with a
   * fractional part the two are stored as if the sign applied to the integral
   * part only.
   * @param {Object} column - The column definition object with fsp.
   * @returns {string} The time as [-]HH:MM:SS[.ffffff].
   * @private
   */
  _read_time2(column) {
    const fractionBytes = Math.ceil(column.fsp / 2);
    let packed;
    if (fractionBytes === 3) {
      packed = BigInt(this.packet.readIntBeBySize(6)) - TIME_FRACTION_OFFSET;
    } else {
      let integral = this.packet.readIntBeBySize(3) - TIME_INTEGRAL_OFFSET;
      let fraction = fractionBytes
        ? this.packet.readIntBeBySize(fractionBytes)
        : 0;
      if (integral < 0 && fraction) {
        integral++;
        fraction -= 256 ** fractionBytes;
      }
      const fractionScale = fractionBytes === 1 ? 10000 : 100;
      packed = (BigInt(integral) << 24n) + BigInt(fraction * fractionScale);
    }

    const negative = packed < 0n;
    const absolute = negative ? -packed : packed;
    const time = absolute >> 24n;
    return formatTime(
      negative,
      Number((time >> 12n) & 0x3ffn),
      Number((time >> 6n) & 63n),
      Number(time & 63n),
      Number(absolute & 0xffffffn),
      column.fsp
    );
  }

  /**
   * Reads MySQL DATE type.
   * @returns {Date} A Date object representing the date, or null if invalid or zero date.
   * @private
   */
  _read_date() {
    const timeValue = this.packet.readInt24(); // read_uint24()
    if (timeValue === 0) {
      // Nasty MySQL 0000-00-00 dates
      return null;
    }

    // Date is stored as (year * 16 * 32) + (month * 32) + day
    // year = value >> 9
    // month = (value >> 5) & 0x0F
    // day = value & 0x1F

    const year = timeValue >> 9;
    const month = (timeValue >> 5) & 0x0f;
    const day = timeValue & 0x1f;

    if (year === 0 || month === 0 || day === 0) {
      return null;
    }

    // Month in Date constructor is 0-indexed.
    const date = new Date(year, month - 1, day);
    return date;
  }

  /**
   * Reads MySQL DATETIME type (old format).
   * @returns {Date} A Date object representing the datetime, or null if invalid or zero datetime.
   * @private
   */
  _read_datetime() {
    const value = this.packet.readInt64(); // read_uint64(), convert to Number (potential precision loss for very large values)
    if (value === 0) {
      // Nasty MySQL 0000-00-00 dates
      return null;
    }

    // Value isYYYYMMDDHHMMSS
    const datePart = Math.floor(value / 1000000);
    const timePart = value % 1000000;

    const year = Math.floor(datePart / 10000);
    const month = Math.floor((datePart % 10000) / 100);
    const day = datePart % 100;

    if (year === 0 || month === 0 || day === 0) {
      return null;
    }

    const hour = Math.floor(timePart / 10000);
    const minute = Math.floor((timePart % 10000) / 100);
    const second = timePart % 100;

    // Month in Date constructor is 0-indexed.
    const dateTime = new Date(year, month - 1, day, hour, minute, second);
    return dateTime;
  }

  /**
   * Reads MySQL DATETIME2 type (new format).
   * @param {Object} column - The column definition object with fsp.
   * @returns {Date} A Date object representing the datetime with FSP.
   * @private
   */
  _read_datetime2(column) {
    // DATETIME2 encoding:
    // 1 bit sign (1= non-negative, 0= negative)
    // 17 bits year*13+month (year 0-9999, month 0-12)
    // 5 bits day (0-31)
    // 5 bits hour (0-23)
    // 6 bits minute (0-59)
    // 6 bits second (0-59)
    // ---------------------------
    // 40 bits = 5 bytes

    const data = this.packet.readIntBeBySize(5);

    // Year and month are packed
    const yearMonth = this._read_binary_slice(data, 1, 17, 40);
    const year = Math.floor(yearMonth / 13);
    const month = yearMonth % 13;

    const day = this._read_binary_slice(data, 18, 5, 40);
    const hour = this._read_binary_slice(data, 23, 5, 40);
    const minute = this._read_binary_slice(data, 28, 6, 40);
    const second = this._read_binary_slice(data, 34, 6, 40);

    try {
      // Month in Date constructor is 0-indexed.
      const baseDateTime = new Date(year, month - 1, day, hour, minute, second);
      return this._add_fsp_to_time(baseDateTime, column);
    } catch (e) {
      // Handle invalid date/time parts.
      this._read_fsp(column); // Still read FSP to advance packet offset
      return null;
    }
  }

  /**
   * Reads MySQL's packed DECIMAL.
   * @param {Object} column - The column definition with precision and decimals.
   * @returns {string} The decoded decimal as a string.
   * @private
   */
  _read_new_decimal(column) {
    return parseDecimalFromBytes(
      this.packet.readBuffer(
        decimalByteSize(column.precision, column.decimals)
      ),
      column.precision,
      column.decimals
    );
  }

  /**
   * Reads a JSON column, or the diffs of a partially updated one.
   * @param {Object} column - The column definition object.
   * @param {boolean} isPartial - True if the column holds JSON diffs.
   * @returns {*} The document, the array of diffs, or null if empty.
   * @private
   */
  _read_json(column, isPartial) {
    const length = this.packet.readIntBySize(column.length_size);
    if (length === 0) {
      return null;
    }
    const bytes = this.packet.readBuffer(length);
    return isPartial ? decodeJsonDiffs(bytes) : decodeJson(bytes);
  }

  /**
   * Reads a part of binary data (represented as a number) and extracts a number.
   * Corresponds to Python's __read_binary_slice.
   * @param {number|bigint} binary - The binary data.
   * @param {number} start - From which bit to start reading (0-indexed).
   * @param {number} size - How many bits to read.
   * @param {number} dataLength - Total length of the binary data in bits.
   * @returns {number} The extracted number.
   * @private
   */
  _read_binary_slice(binary, start, size, dataLength) {
    let bigIntBinary = BigInt(binary);
    // Shift right to bring the desired bits to the least significant position
    bigIntBinary = bigIntBinary >> BigInt(dataLength - (start + size));
    // Create a mask with 'size' number of 1s
    const mask = (1n << BigInt(size)) - 1n;
    // Apply the mask to get the bits
    return Number(bigIntBinary & mask); // Convert back to number, careful with large values
  }

  /**
   * Counts the number of JSON columns in the table definition.
   * @returns {number} The count of JSON columns.
   * @private
   */
  _json_column_count() {
    let count = 0;
    for (const column of this.columns) {
      if (column.type === Types.JSON) {
        count++;
      }
    }
    return count;
  }

  /**
   * Identifies the source of `null` values for columns.
   * @param {Object.<string, any>} columnData - The parsed column data for a row.
   * @returns {Object.<string, string>} A map of column names to their `NONE_SOURCE`.
   * @private
   */
  _get_none_sources(columnData) {
    const result = {};
    for (const columnName in columnData) {
      if (columnData.hasOwnProperty(columnName)) {
        const value = columnData[columnName];
        if (columnName !== null && value === null) {
          const source = this.__none_sources[columnName] || NONE_SOURCE.NULL; // Default to 'null' if not explicitly set
          result[columnName] = source;
        }
      }
    }
    return result;
  }

  /**
   * Fetches all rows for this event. A failed decode leaves the event
   * undecoded, so the next access reports the error again.
   * @private
   */
  _fetch_rows() {
    const rows = [];
    if (this.processed) {
      const start = this.packet.offset;
      try {
        while (this.packet.haveMoreData()) {
          rows.push(this._fetch_one_row());
        }
      } catch (e) {
        this.packet.offset = start;
        throw e;
      }
    }
    this.__rows = rows;
  }

  /**
   * Getter for the rows data. Lazily loads rows if not already fetched.
   * @type {Array<Object>}
   */
  get rows() {
    if (this.__rows === null) {
      this._fetch_rows();
    }
    return this.__rows;
  }

  /**
   * Dumps the event information to the console.
   * @override
   */
  _dump() {
    super._dump(); // Call base class dump
    console.log(`Table: ${this.schema}.${this.table}`);
    console.log(`Affected columns: ${this.numberOfColumns}`);
    console.log(`Changed rows: ${this.rows.length}`);
    // Assuming tableMap[this.tableId].column_name_flag exists from TableMapEvent
    // console.log(`Column Name Information Flag: ${this.tableMap[this.tableId].column_name_flag}`);
  }
}

/**
 * This event is triggered when a row in the database is removed.
 * For each row, it contains a 'values' hash with the data of the removed line.
 */
class DeleteRowsEvent extends RowsEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   * @property {Buffer} columnsPresentBitmap - Bitmap indicating which columns are present in the event.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);
    if (this.processed) {
      // The bitmap size is (number_of_columns + 7) / 8 bytes.
      this.columnsPresentBitmap = this.packet.readBuffer(
        Math.ceil(this.numberOfColumns / 8)
      );
    }
  }

  /**
   * Fetches and parses one row of data for a delete event.
   * @returns {Object} An object containing 'values' and 'none_sources' for the row.
   * @private
   */
  _fetch_one_row() {
    const row = {};
    // For DELETE events, only the "before image" is relevant, which corresponds to RowImageType.Delete.
    row.values = this._read_column_data(
      this.columnsPresentBitmap,
      RowImageType.Delete
    );
    row.none_sources = this._get_none_sources(row.values);
    return row;
  }

  /**
   * Dumps the event information to the console, including row values.
   * @override
   */
  _dump() {
    super._dump();
    console.log('Values:');
    for (const row of this.rows) {
      console.log('--');
      for (const key in row.values) {
        if (row.values.hasOwnProperty(key)) {
          const noneSource = row.none_sources[key] || '';
          if (noneSource) {
            console.log(`* ${key} : ${row.values[key]} (${noneSource})`);
          } else {
            console.log(`* ${key} : ${row.values[key]}`);
          }
        }
      }
    }
  }
}

/**
 * This event is triggered when a row in the database is added.
 * For each row, it contains a 'values' hash with the data of the new line.
 */
class WriteRowsEvent extends RowsEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   * @property {Buffer} columnsPresentBitmap - Bitmap indicating which columns are present in the event.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);
    if (this.processed) {
      // The bitmap size is (number_of_columns + 7) / 8 bytes.
      this.columnsPresentBitmap = this.packet.readBuffer(
        Math.ceil(this.numberOfColumns / 8)
      );
    }
  }

  /**
   * Fetches and parses one row of data for a write event.
   * @returns {Object} An object containing 'values' and 'none_sources' for the row.
   * @private
   */
  _fetch_one_row() {
    const row = {};
    // For WRITE events, the "after image" is relevant, which corresponds to RowImageType.Write.
    row.values = this._read_column_data(
      this.columnsPresentBitmap,
      RowImageType.Write
    );
    row.none_sources = this._get_none_sources(row.values);
    return row;
  }

  /**
   * Dumps the event information to the console, including row values.
   * @override
   */
  _dump() {
    super._dump();
    console.log('Values:');
    for (const row of this.rows) {
      console.log('--');
      for (const key in row.values) {
        if (row.values.hasOwnProperty(key)) {
          const noneSource = row.none_sources[key] || '';
          if (noneSource) {
            console.log(`* ${key} : ${row.values[key]} (${noneSource})`);
          } else {
            console.log(`* ${key} : ${row.values[key]}`);
          }
        }
      }
    }
  }
}

/**
 * This event is triggered when a row in the database is changed (updated).
 * For each row, it provides 'before_values' and 'after_values'.
 * The content depends on MySQL's `binlog_row_image` setting.
 * http://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#sysvar_binlog_row_image
 */
class UpdateRowsEvent extends RowsEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   * @property {Buffer} columnsPresentBitmap - Bitmap for "before" image columns.
   * @property {Buffer} columnsPresentBitmap2 - Bitmap for "after" image columns.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    if (this.processed) {
      // Body
      // Bitmap for columns present in the "before image"
      this.columnsPresentBitmap = this.packet.readBuffer(
        Math.ceil(this.numberOfColumns / 8)
      );
      // Bitmap for columns present in the "after image"
      this.columnsPresentBitmap2 = this.packet.readBuffer(
        Math.ceil(this.numberOfColumns / 8)
      );
    }
  }

  /**
   * Fetches and parses one row of data for an update event.
   * @returns {Object} An object containing 'before_values', 'after_values', and their respective 'none_sources'.
   * @private
   */
  _fetch_one_row() {
    const row = {};

    row.before_values = this._read_column_data(
      this.columnsPresentBitmap,
      RowImageType.UpdateBI
    );
    row.before_none_sources = this._get_none_sources(row.before_values);

    row.after_values = this._read_column_data(
      this.columnsPresentBitmap2,
      RowImageType.UpdateAI
    );
    row.after_none_sources = this._get_none_sources(row.after_values);
    return row;
  }

  /**
   * Dumps the event information to the console, including before and after row values.
   * @override
   */
  _dump() {
    super._dump();
    console.log('Values:');
    for (const row of this.rows) {
      console.log('--');
      for (const key in row.before_values) {
        if (row.before_values.hasOwnProperty(key)) {
          const beforeNoneSource = row.before_none_sources[key] || '';
          const afterNoneSource = row.after_none_sources[key] || '';

          const beforeValueInfo = beforeNoneSource
            ? `${row.before_values[key]} (${beforeNoneSource})`
            : row.before_values[key];
          const afterValueInfo = afterNoneSource
            ? `${row.after_values[key]} (${afterNoneSource})`
            : row.after_values[key];

          console.log(`* ${key}: ${beforeValueInfo} => ${afterValueInfo}`);
        }
      }
    }
  }
}

/**
 * PartialUpdateRowsEvent handles partial JSON updates.
 * It extends UpdateRowsEvent.
 */
class PartialUpdateRowsEvent extends UpdateRowsEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The table map.
   * @param {Object} ctlConnection - The control connection.
   * @param {Object} options - Additional options.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);
    // No additional constructor logic needed here, super handles flags and bitmaps
  }

  /**
   * Fetches and parses one row of data for a partial update event.
   * Explicitly sets RowImageType for before/after images.
   * @returns {Object} An object containing 'before_values', 'after_values', and their respective 'none_sources'.
   * @private
   */
  _fetch_one_row() {
    const row = {};
    let rowImageType = RowImageType.UpdateBI;
    row.before_values = this._read_column_data(
      this.columnsPresentBitmap,
      rowImageType
    );
    row.before_none_sources = this._get_none_sources(row.before_values);

    rowImageType = RowImageType.UpdateAI;
    row.after_values = this._read_column_data(
      this.columnsPresentBitmap2,
      rowImageType
    );
    row.after_none_sources = this._get_none_sources(row.after_values);

    return row;
  }

  /**
   * Dumps the event information to the console.
   * @override
   */
  _dump() {
    super._dump(); // Relies on UpdateRowsEvent's dump
  }
}

module.exports = {
  RowsEvent,
  DeleteRowsEvent,
  WriteRowsEvent,
  UpdateRowsEvent,
  PartialUpdateRowsEvent,
};
