// lib/binlog/row_event_types.js
// This file contains implementations for MySQL row-based binlog event types.

'use strict';

const BinLogEvent = require('./event_base.js'); // Import base BinLogEvent
const { parseDecimalFromBytes, StatusVariableMismatch } = require('./helpers.js'); // Import helpers
const NativeBuffer = require('buffer').Buffer; // Still needed for Buffer operations
const Long = require('long'); // Still needed for Long operations
const Types = require('../constants/types.js'); // Assuming this contains relevant event type constants

class NotImplementedError extends Error {
  constructor(message) {
    super(message);
    this.name = "NotImplementedError";
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
        byte &= (byte - 1); // Brian Kernighan's algorithm
        count++;
      }
    }
    return count;
  } else if (typeof value === 'number' || typeof value === 'bigint') {
    let count = 0;
    let n = BigInt(value);
    while (n > 0n) {
      n &= (n - 1n);
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
    return (value[byteIndex] & (1 << bitOffset)) ? 1 : 0;
  } else if (typeof value === 'number' || typeof value === 'bigint') {
    return (BigInt(value) & (1n << BigInt(position))) ? 1 : 0;
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
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    this.__rows = null;
    this.__only_tables = options.onlyTables || null;
    this.__ignored_tables = options.ignoredTables || null;
    this.__only_schemas = options.onlySchemas || null;
    this.__ignored_schemas = options.ignoredSchemas || null;
    this.__none_sources = {}; // To track why a value is None/null

    // Header - Table ID is common for all RowsEvents
    this.tableId = this.readTableId();

    // Additional information from table map
    try {
      this.primaryKey = this.tableMap[this.tableId].data.primary_key;
      this.schema = this.tableMap[this.tableId].schema;
      this.table = this.tableMap[this.tableId].table;
    } catch (e) {
      // If the corresponding TableMap Event was filtered or not found
      // This event should not be processed.
      this.processed = false;
      return;
    }

    // Apply filtering based on options
    if (this.__only_tables !== null && !this.__only_tables.includes(this.table)) {
      this.processed = false;
      return;
    }
    if (this.__ignored_tables !== null && this.__ignored_tables.includes(this.table)) {
      this.processed = false;
      return;
    }
    if (this.__only_schemas !== null && !this.__only_schemas.includes(this.schema)) {
      this.processed = false;
      return;
    }
    if (this.__ignored_schemas !== null && this.__ignored_schemas.includes(this.schema)) {
      this.processed = false;
      return;
    }

    // Event V2 specific header (only for WRITE_ROWS_EVENT_V2, DELETE_ROWS_EVENT_V2, UPDATE_ROWS_EVENT_V2, PARTIAL_UPDATE_ROWS_EVENT)
    if (
      this.eventType === Types.WRITE_ROWS_EVENT_V2 ||
      this.eventType === Types.DELETE_ROWS_EVENT_V2 ||
      this.eventType === Types.UPDATE_ROWS_EVENT_V2 ||
      this.eventType === Types.PARTIAL_UPDATE_ROWS_EVENT
    ) {
      this.flags = this.packet.readUInt16LE(); // H
      this.extraDataLength = this.packet.readUInt16LE(); // H
      // extra_data_length includes its own 2 bytes, so we subtract to get actual extra data bytes
      if (this.extraDataLength > 2) {
        this.extraDataType = this.packet.readUInt8(); // B
        // NDB information
        if (this.extraDataType === 0) {
          this.nbdInfoLength = this.packet.readUInt8(); // B
          this.nbdInfoFormat = this.packet.readUInt8(); // B
          this.nbdInfo = this.packet.readBuffer(this.nbdInfoLength - 2);
        }
        // Partition information
        else if (this.extraDataType === 1) {
          if (
            this.eventType === Types.UPDATE_ROWS_EVENT_V2 ||
            this.eventType === Types.PARTIAL_UPDATE_ROWS_EVENT
          ) {
            this.partitionId = this.packet.readUInt16LE(); // H
            this.sourcePartitionId = this.packet.readUInt16LE(); // H
          } else {
            this.partitionId = this.packet.readUInt16LE(); // H
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
      this.flags = this.packet.readUInt16LE(); // H
    }

    // Body
    this.numberOfColumns = this.packet.readLengthCodedBinary();
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
      this.eventType === Types.PARTIAL_UPDATE_ROWS_EVENT &&
      rowImageType === RowImageType.UpdateAI
    ) {
      const binlogRowValueOption = this.packet.readLengthCodedBinary();
      this.isPartialJsonUpdate = (binlogRowValueOption & 0b10000001) !== 0;
      if (this.isPartialJsonUpdate) {
        partialBitmap = this.packet.readBuffer(Math.ceil(this._json_column_count() / 8));
      }
    }

    const values = {};

    // Null bitmap length = (bits set in 'columns-present-bitmap' + 7) / 8
    // See http://dev.mysql.com/doc/internals/en/rows-event.html
    const nullBitmapLength = Math.ceil(BitCount(colsBitmap) / 8);
    const nullBitmap = this.packet.readBuffer(nullBitmapLength);

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
        name = 'UNKNOWN_COL' + i;
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
        return unsigned ? this.packet.readUInt8() : this.packet.readInt8();
      case Types.SHORT:
        return unsigned ? this.packet.readUInt16LE() : this.packet.readInt16LE();
      case Types.LONG:
        return unsigned ? this.packet.readUInt32LE() : this.packet.readInt32LE();
      case Types.INT24:
        return unsigned ? this.packet.readUInt24() : this.packet.readInt24();
      case Types.FLOAT:
        return this.packet.readFloatLE();
      case Types.DOUBLE:
        return this.packet.readDoubleLE();
      case Types.VARCHAR:
      case Types.STRING:
      case Types.VAR_STRING:
        return column.max_length > 255 ?
          this._read_string(2, column) :
          this._read_string(1, column);
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
        return new Date(this.packet.readUInt32LE() * 1000); // Unix timestamp to milliseconds
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
        return unsigned ? this.packet.readUInt64() : this.packet.readInt64();
      case Types.YEAR:
        return this.packet.readUInt8() + 1900;
      case Types.ENUM:
        const enumIndex = this.packet.readUIntBySize(column.size);
        if (column.enum_values && column.enum_values[enumIndex] !== undefined) {
          return column.enum_values[enumIndex];
        }
        return null;
      case Types.SET:
        const bitMask = this.packet.readUIntBySize(column.size);
        if (column.set_values) {
          const resultSet = new Set();
          for (let idx = 0; idx < column.set_values.length; idx++) {
            if (BitGet(bitMask, idx) !== 0) {
              resultSet.add(column.set_values[idx]);
            }
          }
          if (resultSet.size === 0) {
            this.__none_sources[column.name] = NONE_SOURCE.EMPTY_SET;
            return null;
          }
          return Array.from(resultSet); // Return as array for easier JS handling
        }
        this.__none_sources[column.name] = NONE_SOURCE.EMPTY_SET;
        return null;
      case Types.BIT:
        return this._read_bit(column);
      case Types.GEOMETRY:
        return this.packet.readLengthCodedPascalString(column.length_size);
      case Types.JSON:
        const jsonValue = this.packet.readBinaryJson(column.length_size, isPartial);
        if (jsonValue === null && isPartial) { // Python returns None for empty partial JSON update
            this.__none_sources[column.name] = NONE_SOURCE.JSON_PARTIAL_UPDATE;
        }
        return jsonValue;
      default:
        throw new NotImplementedError(`Unknown MySQL column type: ${column.type}`);
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
      time.setMilliseconds(time.getMilliseconds() + Math.floor(microsecond / 1000));
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
      return microsecond * (10 ** (6 - column.fsp));
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
      'utf8': 'utf8',
      'utf8mb4': 'utf8', // Node.js 'utf8' handles utf8mb4 in most cases
      'latin1': 'latin1',
      'binary': 'binary',
      // Add other charsets as required
    };
    return charsetMap[name.toLowerCase()] || 'utf8'; // Default to utf8
  }

  /**
   * Reads a string from the packet.
   * @param {number} size - The size of the length field (1 or 2 bytes).
   * @param {Object} column - The column definition object.
   * @returns {string} The decoded string.
   * @private
   */
  _read_string(size, column) {
    const buffer = this.packet.readLengthCodedPascalString(size);
    if (!buffer) {
        return null; // Handle cases where readLengthCodedPascalString returns null
    }

    let decodedString = buffer;
    const decodeErrors = this.ignoreDecodeErrors ? 'ignore' : 'utf8'; // 'ignore' for toString() is a custom handler or defaults

    if (column.character_set_name !== null) {
      const encoding = this.charset_to_encoding(column.character_set_name);
      try {
        decodedString = buffer.toString(encoding);
      } catch (e) {
        // Fallback if encoding is not supported by Node.js or decoding fails.
        // In Python, it would return origin_string if LookupError. Here, just the raw buffer.
        console.warn(`Failed to decode string with encoding ${encoding}: ${e.message}`);
        decodedString = buffer.toString('hex'); // Fallback to hex representation
      }
    } else {
      // MySQL 5.x version - try UTF-8 by default
      decodedString = buffer.toString('utf8');
    }
    return decodedString;
  }

  /**
   * Reads MySQL BIT type.
   * @param {Object} column - The column definition object.
   * @returns {string} The binary string representation of the BIT value.
   * @private
   */
  _read_bit(column) {
    let resp = '';
    for (let byteIndex = 0; byteIndex < column.bytes; byteIndex++) {
      let currentByteBits = '';
      const data = this.packet.readUInt8();
      let endBit = 8;
      if (byteIndex === 0) {
        if (column.bytes === 1) {
          endBit = column.bits;
        } else {
          endBit = column.bits % 8;
          if (endBit === 0) {
            endBit = 8;
          }
        }
      }
      for (let bit = 0; bit < endBit; bit++) {
        if (data & (1 << bit)) {
          currentByteBits += '1';
        } else {
          currentByteBits += '0';
        }
      }
      resp += currentByteBits.split('').reverse().join(''); // Reverse to match big-endian bit order
    }
    return resp;
  }

  /**
   * Reads MySQL TIME type.
   * @returns {Date} A Date object representing the time, or null if invalid.
   * @private
   */
  _read_time() {
    const timeValue = this.packet.readUInt24(); // read_uint24()
    if (timeValue === 0) {
      return null;
    }

    // Time is stored as HHMMSS (e.g., 123045 for 12:30:45)
    const hours = Math.floor(timeValue / 10000);
    const minutes = Math.floor((timeValue % 10000) / 100);
    const seconds = timeValue % 100;

    // Create a Date object for today and set the time
    const date = new Date();
    date.setHours(hours, minutes, seconds, 0);
    return date;
  }

  /**
   * Reads MySQL TIME2 type.
   * @param {Object} column - The column definition object with fsp.
   * @returns {Date} A Date object representing the time with FSP.
   * @private
   */
  _read_time2(column) {
    // TIME encoding for non-fractional part:
    // 1 bit sign (1= non-negative, 0= negative)
    // 1 bit unused (reserved for future extensions)
    // 10 bits hour (0-838)
    // 6 bits minute (0-59)
    // 6 bits second (0-59)
    // ---------------------
    // 24 bits = 3 bytes

    let data = this.packet.readIntBeBySize(3); // read_int_be_by_size(3)

    const sign = (this._read_binary_slice(data, 0, 1, 24) === 1) ? 1 : -1;
    if (sign === -1) {
      // Negative integers are stored as 2's complement.
      // Take 2's complement again to get the right value.
      // Need to handle BigInt for bitwise operations if `data` can be large.
      data = (~data + 1);
    }

    const hours = this._read_binary_slice(data, 2, 10, 24);
    const minutes = this._read_binary_slice(data, 12, 6, 24);
    const seconds = this._read_binary_slice(data, 18, 6, 24);
    const microseconds = this._read_fsp(column);

    // Create a Date object for today and set the time.
    // For `timedelta` equivalent, we can return an object or use a library.
    // Here, we'll return a Date object with adjusted milliseconds for microseconds.
    const date = new Date();
    date.setHours(hours, minutes, seconds, 0);
    date.setMilliseconds(date.getMilliseconds() + Math.floor(microseconds / 1000));

    // Apply sign for total duration, if converting to duration object.
    // If we're returning a Date object, the sign is implicitly handled by Date arithmetic.
    // For duration, we might do `new Date(Date.now() + (sign * durationMs))`.
    // The Python `* sign` applies to timedelta object, so the hours/minutes/seconds
    // would be negative. Javascript Date objects don't handle negative time components directly.
    // For a pure time duration, a custom object would be better. For now, assume positive time.
    return date;
  }

  /**
   * Reads MySQL DATE type.
   * @returns {Date} A Date object representing the date, or null if invalid or zero date.
   * @private
   */
  _read_date() {
    const timeValue = this.packet.readUInt24(); // read_uint24()
    if (timeValue === 0) { // Nasty MySQL 0000-00-00 dates
      return null;
    }

    // Date is stored as (year * 16 * 32) + (month * 32) + day
    // year = value >> 9
    // month = (value >> 5) & 0x0F
    // day = value & 0x1F

    const year = (timeValue >> 9);
    const month = (timeValue >> 5) & 0x0F;
    const day = timeValue & 0x1F;

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
    const value = this.packet.readUInt64().toNumber(); // read_uint64(), convert to Number (potential precision loss for very large values)
    if (value === 0) { // Nasty MySQL 0000-00-00 dates
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

    let data = this.packet.readIntBeBySize(5); // read_int_be_by_size(5)

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
   * Reads MySQL's new DECIMAL format introduced in MySQL 5.
   * This is a complex parsing, directly translated from the Python logic.
   * @param {Object} column - The column definition with precision and decimals.
   * @returns {string} The decoded decimal as a string.
   * @private
   */
  _read_new_decimal(column) {
    const digitsPerInteger = 9;
    const compressedBytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    const integralDigits = column.precision - column.decimals;
    const uncompressedIntegral = Math.floor(integralDigits / digitsPerInteger);
    const uncompressedFractional = Math.floor(column.decimals / digitsPerInteger);
    const compressedIntegral = integralDigits - (uncompressedIntegral * digitsPerInteger);
    const compressedFractional = column.decimals - (uncompressedFractional * digitsPerInteger);

    // Read the sign byte
    let value = this.packet.readUInt8();
    let res = '';
    let mask = 0; // Mask for XORing to get actual value

    if ((value & 0x80) !== 0) { // Highest bit set indicates non-negative
      res = ''; // Start with empty for positive numbers
      mask = 0; // No mask needed for XOR if positive
    } else { // Highest bit not set indicates negative
      res = '-'; // Start with '-' for negative numbers
      mask = -1; // Use -1 (all bits set in 2's complement) for XOR if negative
    }
    // Rewind and flip the sign bit for parsing the magnitude
    this.packet.unread(NativeBuffer.from([value ^ 0x80])); // Invert the sign bit for reading the magnitude

    // Read integral part
    let size = compressedBytes[compressedIntegral];
    if (size > 0) {
      // Read big-endian integer, then XOR with mask
      const val = this.packet.readIntBeBySize(size) ^ mask;
      res += val.toString();
    }

    for (let i = 0; i < uncompressedIntegral; i++) {
      // Read 4 bytes big-endian, then XOR with mask
      const val = this.packet.readIntBeBySize(4) ^ mask;
      res += val.toString().padStart(digitsPerInteger, '0'); // Pad with leading zeros to 9 digits
    }

    res += '.'; // Add decimal point

    // Read fractional part
    for (let i = 0; i < uncompressedFractional; i++) {
      // Read 4 bytes big-endian, then XOR with mask
      const val = this.packet.readIntBeBySize(4) ^ mask;
      res += val.toString().padStart(digitsPerInteger, '0'); // Pad with leading zeros to 9 digits
    }

    size = compressedBytes[compressedFractional];
    if (size > 0) {
      // Read big-endian integer, then XOR with mask
      const val = this.packet.readIntBeBySize(size) ^ mask;
      res += val.toString().padStart(compressedFractional, '0'); // Pad with leading zeros based on compressedFractional
    }

    // Return as a string. For actual decimal arithmetic, a library like 'decimal.js' would be needed.
    return res;
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
   * Fetches all rows for this event.
   * @private
   */
  _fetch_rows() {
    this.__rows = [];

    if (!this.processed) {
      return;
    }

    // packet.readBytes tracks total bytes read by the packet from its original start
    // this.eventSize is the total size of this specific event, including its 19-byte header.
    // The packet.offset is currently at the start of the payload after the header.
    // So, we need to read until the end of this event's payload, which is
    // (start of event payload) + (total event size - header size).
    const endOfEventPayload = this.packet.offset + (this.eventSize - 19);

    while (this.packet.offset < endOfEventPayload) {
      this.__rows.push(this._fetch_one_row());
    }
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
    row.values = this._read_column_data(this.columnsPresentBitmap, RowImageType.Delete);
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
    row.values = this._read_column_data(this.columnsPresentBitmap, RowImageType.Write);
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

    row.before_values = this._read_column_data(this.columnsPresentBitmap, RowImageType.UpdateBI);
    row.before_none_sources = this._get_none_sources(row.before_values);

    row.after_values = this._read_column_data(this.columnsPresentBitmap2, RowImageType.UpdateAI);
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

          const beforeValueInfo = beforeNoneSource ? `${row.before_values[key]} (${beforeNoneSource})` : row.before_values[key];
          const afterValueInfo = afterNoneSource ? `${row.after_values[key]} (${afterNoneSource})` : row.after_values[key];

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
