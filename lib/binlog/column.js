// column.js
// Mock Column class. In a real scenario, this would parse detailed column
// metadata from the binlog stream within the TableMapEvent.

'use strict';

const Types = require('./constants/types.js'); // Assuming Types contains FIELD_TYPE constants

/**
 * Represents a database table column.
 * This is a simplified mock. A complete implementation would parse
 * detailed column metadata (e.g., length, decimals, charset) from the binlog.
 */
class Column {
  /**
   * @param {number} type - The MySQL FIELD_TYPE of the column.
   * @param {Packet} fromPacket - The packet instance (mocked for now, not directly used for parsing here).
   */
  constructor(type, fromPacket) {
    this.type = type;
    this.name = null; // Will be set later by TableMapEvent's _sync_column_info
    this.unsigned = false;
    this.max_length = 0; // Placeholder
    this.length_size = 0; // Placeholder for BLOB/TEXT length prefix size
    this.fsp = 0; // Fractional seconds precision for TIME/DATETIME/TIMESTAMP
    this.bits = 0; // For BIT type
    this.bytes = 0; // For BIT type
    this.enum_values = null; // Array of strings for ENUM
    this.set_values = null; // Array of strings for SET
    this.is_primary = false;
    this.visibility = true; // Default visibility (true for visible columns)
    this.character_set_name = null; // Character set name (e.g., 'utf8', 'latin1')
    this.collation_name = null; // Collation name (e.g., 'utf8_general_ci')

    // Initialize specific properties based on type if known, or as placeholders
    switch (this.type) {
      case Types.FIELD_TYPE.TINY:
      case Types.FIELD_TYPE.SHORT:
      case Types.FIELD_TYPE.LONG:
      case Types.FIELD_TYPE.INT24:
      case Types.FIELD_TYPE.LONGLONG:
      case Types.FIELD_TYPE.YEAR:
        // Numeric types, unsigned property will be set later by optional metadata
        break;
      case Types.FIELD_TYPE.FLOAT:
      case Types.FIELD_TYPE.DOUBLE:
      case Types.FIELD_TYPE.NEWDECIMAL:
        // Floating point/decimal types
        break;
      case Types.FIELD_TYPE.VARCHAR:
      case Types.FIELD_TYPE.STRING:
        // String types, max_length would be parsed from metadata
        this.max_length = 255; // Default for VARCHAR/STRING if not explicitly read
        break;
      case Types.FIELD_TYPE.BLOB:
        // BLOB types, length_size (1, 2, 3, 4 bytes) for actual length would be parsed
        this.length_size = 1; // Default
        break;
      case Types.FIELD_TYPE.DATETIME:
      case Types.FIELD_TYPE.TIMESTAMP:
      case Types.FIELD_TYPE.TIME:
        // Old date/time types (no FSP by default in binlog)
        break;
      case Types.FIELD_TYPE.DATETIME2:
      case Types.FIELD_TYPE.TIMESTAMP2:
      case Types.FIELD_TYPE.TIME2:
        // New date/time types with FSP (FSP would be parsed from metadata)
        this.fsp = 0; // Default, will be updated from TableMapEvent's optional metadata
        break;
      case Types.FIELD_TYPE.ENUM:
        this.size = 1; // Default size in bytes (1 or 2)
        break;
      case Types.FIELD_TYPE.SET:
        this.size = 1; // Default size in bytes (1, 2, 3, 4, 8)
        break;
      case Types.FIELD_TYPE.BIT:
        // `bits` and `bytes` would be parsed from metadata
        this.bits = 1; // Default
        this.bytes = 1; // Default
        break;
      case Types.FIELD_TYPE.GEOMETRY:
        this.length_size = 4; // GEOMETRY uses 4-byte length prefix
        break;
      case Types.FIELD_TYPE.JSON:
        this.length_size = 4; // JSON uses 4-byte length prefix
        break;
      default:
        // Unknown type, handle as generic
        break;
    }
  }

  // Add methods for parsing specific column metadata if needed in the future.
  // For this translation, `TableMapEvent`'s `_sync_column_info` updates these properties.
}

module.exports = Column;
