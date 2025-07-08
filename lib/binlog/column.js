// lib/binlog/column.js
// This file defines the Column class, used to represent a database table column
// within the context of MySQL binlog events.

'use strict';

// Import the MySQL field type constants directly from types.js.
const Types = require('../constants/types.js');
// Packet class is implicitly used through the static fromPacket method,
// but not directly required in this file's top level.

/**
 * Represents a database table column as described in a MySQL binlog's TableMapEvent.
 * This class stores essential metadata about a column, such as its type, name,
 * and specific attributes relevant for parsing row data.
 */
class Column {
  /**
   * Initializes a new Column instance.
   * The constructor is now cleaner and primarily used for setting initial properties
   * from an object, or for creating an empty column instance.
   * Parsing from a packet is handled by the static `fromPacket` factory method.
   * @param {Object} [properties={}] - An optional object to initialize column properties.
   */
  constructor(properties = {}) {
    // Default properties
    this.type = null;
    this.name = null;
    this.unsigned = false;
    this.is_primary = false;
    this.charset_id = null;
    this.character_set_name = null;
    this.collation_name = null;
    this.enum_values = null;
    this.set_values = null;
    this.visibility = false; // Default to false as per Python, will be set true if explicitly visible
    this.max_length = 0;
    this.length_size = 0;
    this.fsp = 0;
    this.bits = 0;
    this.bytes = 0;
    this.size = 0; // For ENUM/SET, indicates storage size in bytes

    // Assign provided properties, overriding defaults
    Object.assign(this, properties);
  }

  /**
   * Factory method to create a Column instance by parsing its definition from a packet.
   * This method encapsulates the logic for reading column-specific metadata directly
   * from the binlog stream.
   * @param {number} columnType - The MySQL FIELD_TYPE of the column.
   * @param {Object} packet - The Packet instance to read metadata from.
   * @returns {Column} A newly created Column instance with parsed metadata.
   */
  static fromPacket(columnType, packet) {
    const column = new Column({ type: columnType }); // Create a new Column instance with its type set
    column._parseColumnDefinition(packet); // Call the private parsing method on the new instance
    return column;
  }

  /**
   * Parses the column definition from the provided packet.
   * This method initializes column properties based on its MySQL FIELD_TYPE
   * and reads specific metadata from the binlog packet.
   * @param {Object} packet - The Packet instance to read metadata from.
   * @private
   */
  _parseColumnDefinition(packet) {
    // Note: this.type is already set by the static fromPacket method.

    switch (this.type) {
      case Types.VARCHAR:
        // For VARCHAR, max_length is stored in 2 bytes.
        this.max_length = packet.readInt16();
        break;
      case Types.DOUBLE:
      case Types.FLOAT:
        // For FLOAT/DOUBLE, a single byte indicating precision/scale is read.
        this.size = packet.readInt8();
        break;
      case Types.TIMESTAMP2:
      case Types.DATETIME2:
      case Types.TIME2:
        // For new date/time types, fractional seconds precision (FSP) is read in 1 byte.
        this.fsp = packet.readInt8();
        break;
      case Types.VAR_STRING:
      case Types.STRING:
        // For VAR_STRING and STRING, special metadata is read to determine
        // the true type (if it's an ENUM/SET) or the max_length.
        this._readStringMetadata(packet);
        break;
      case Types.BLOB:
      case Types.TINY_BLOB:
      case Types.MEDIUM_BLOB:
      case Types.LONG_BLOB:
      case Types.GEOMETRY:
      case Types.JSON:
        // For BLOB, GEOMETRY, and JSON types, the size of the length prefix
        // (how many bytes indicate the data's length) is read in 1 byte.
        this.length_size = packet.readInt8();
        break;
      case Types.NEWDECIMAL:
        // For NEWDECIMAL, both precision and scale (decimals) are read in 1 byte each.
        this.precision = packet.readInt8();
        this.decimals = packet.readInt8();
        break;
      case Types.BIT:
        // For BIT type, two bytes are read:
        // The first byte contains the number of "excess" bits (0-7).
        // The second byte contains the number of full bytes.
        const bits_val = packet.readInt8();
        const bytes_val = packet.readInt8();
        // Calculate the total number of bits in the column.
        this.bits = bytes_val * 8 + bits_val;
        // Calculate the number of bytes required to store this bit field (rounded up).
        this.bytes = Math.ceil(this.bits / 8);
        break;
      default:
        // For other types (e.g., TINY, SHORT, LONG, LONGLONG, INT24, DATE, TIME, DATETIME, YEAR, NULL),
        // no additional specific metadata bytes are read directly by the Column definition itself.
        break;
    }
  }

  /**
   * Reads specific string metadata for `VAR_STRING` and `STRING` types.
   * This determines if the actual type is `ENUM` or `SET`, or calculates `max_length`.
   * @param {Object} packet - The Packet instance to read from.
   * @private
   */
  _readStringMetadata(packet) {
    // Reads two bytes that encode both a "real type" and a length/size component.
    const b1 = packet.readInt8();
    const b2 = packet.readInt8();
    const metadata = (b1 << 8) | b2; //packet.readInt16();
    const real_type = b1; // The high byte determines the effective type.

    if (real_type === Types.SET || real_type === Types.ENUM) {
      // If the real type indicates ENUM or SET, update the column's type
      // and extract the size (number of bytes used to store the ENUM/SET value).
      this.type = real_type;
      this.size = b2; // The low byte is the size (1 or 2 bytes).
    } else {
      // Otherwise, for standard string types, the metadata encodes the maximum length.
      // This is a specific MySQL formula:
      // max_length = (((metadata_high_byte_bits_4_5_shifted) ^ 0x300) + metadata_low_byte)
      // `((metadata >> 4) & 0x300)` extracts two bits (originally bits 4 and 5 of the high byte)
      // and shifts them to a higher position (0x100 or 0x200 or 0x300).
      // `^ 0x300` seems to adjust this value based on an internal MySQL encoding.
      // `(metadata & 0x00FF)` extracts the lower 8 bits directly.
      this.max_length = (((metadata >> 4) & 0x300) ^ 0x300) + b2;
    }
  }

  /**
   * Provides a serializable representation of the column's data.
   * This getter returns an object containing all public properties of the column,
   * excluding any private or internal properties (those starting with `_`).
   * @returns {Object} A plain object containing the column's public data.
   */
  get data() {
    const serializable = {};
    for (const key in this) {
      // Check if it's an own property and not a method or private/internal property.
      if (
        Object.prototype.hasOwnProperty.call(this, key) &&
        typeof this[key] !== 'function' &&
        !key.startsWith('_')
      ) {
        serializable[key] = this[key];
      }
    }
    return serializable;
  }
}

module.exports = Column;
