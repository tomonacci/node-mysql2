// column.js
// This file defines the Column class, used to represent a database table column
// within the context of MySQL binlog events.

'use strict';

// Import the MySQL field type constants directly from types.js.
// The types.js file exports these constants directly on its module.exports object.
const Types = require('../constants/types.js');

/**
 * Represents a database table column as described in a MySQL binlog's TableMapEvent.
 * This class stores essential metadata about a column, such as its type, name,
 * and specific attributes relevant for parsing row data.
 *
 * Note: In a more complex implementation, the Column constructor might itself
 * parse additional metadata directly from the binlog packet if the TableMapEvent
 * provides a stream offset for column-specific details. For this current setup,
 * many properties are initialized as placeholders and later populated by
 * TableMapEvent's `_sync_column_info` method, based on optional metadata.
 */
class Column {
  /**
   * @param {number} type - The MySQL FIELD_TYPE (e.g., Types.TINY, Types.VARCHAR) of the column.
   * @param {Packet} fromPacket - The packet instance. While passed here, this mock
   * constructor does not directly parse column-specific metadata from it.
   */
  constructor(type, fromPacket) {
    this.type = type;
    this.name = null; // Populated by TableMapEvent's _sync_column_info
    this.unsigned = false; // Populated by TableMapEvent's _sync_column_info for numeric types
    this.max_length = 0; // Maximum length for string types (e.g., VARCHAR, STRING)
    this.length_size = 0; // Size in bytes of the length prefix for BLOB/TEXT/JSON/GEOMETRY
    this.fsp = 0; // Fractional seconds precision for TIME/DATETIME/TIMESTAMP2 types
    this.bits = 0; // Number of bits for BIT type
    this.bytes = 0; // Number of bytes for BIT type
    this.enum_values = null; // Array of strings for ENUM values (1-indexed, so often includes empty string at [0])
    this.set_values = null; // Array of strings for SET values
    this.is_primary = false; // True if this column is part of the primary key
    this.visibility = true; // True if the column is visible (false for invisible columns in MySQL 8.0+)
    this.character_set_name = null; // The character set name (e.g., 'utf8', 'latin1')
    this.collation_name = null; // The collation name (e.g., 'utf8_general_ci')

    // Initialize properties specific to certain column types.
    // These are default or minimum values; actual values are often
    // derived from the TableMapEvent's metadata block.
    switch (this.type) {
      case Types.TINY:
      case Types.SHORT:
      case Types.LONG:
      case Types.INT24:
      case Types.LONGLONG:
      case Types.YEAR:
        // Numeric integer types. 'unsigned' will be set by optional metadata.
        break;
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.NEWDECIMAL:
        // Floating-point and fixed-point decimal types.
        break;
      case Types.VARCHAR:
      case Types.STRING:
      case Types.VAR_STRING: // VAR_STRING can also map to VARCHAR/VARBINARY
        this.max_length = 255; // Common default, actual length from metadata if available
        break;
      case Types.BLOB:
      case Types.TINY_BLOB:
      case Types.MEDIUM_BLOB:
      case Types.LONG_BLOB:
        this.length_size = 1; // Default length size (1, 2, 3, or 4 bytes for BLOBs)
        break;
      case Types.DATETIME: // Old DATETIME format (no FSP in binlog, FSP is 0 implicitly)
      case Types.TIMESTAMP: // Old TIMESTAMP format (no FSP in binlog, FSP is 0 implicitly)
      case Types.TIME: // Old TIME format (no FSP in binlog, FSP is 0 implicitly)
        this.fsp = 0;
        break;
      case Types.DATETIME2: // New DATETIME format with FSP
      case Types.TIMESTAMP2: // New TIMESTAMP format with FSP
      case Types.TIME2: // New TIME format with FSP
        this.fsp = 0; // Placeholder, actual FSP comes from metadata
        break;
      case Types.ENUM:
        this.size = 1; // Storage size in bytes (1 or 2, depending on number of enum values)
        break;
      case Types.SET:
        this.size = 1; // Storage size in bytes (1, 2, 3, 4, or 8 depending on number of set members)
        break;
      case Types.BIT:
        this.bits = 1; // Placeholder, actual bits and bytes come from metadata
        this.bytes = 1; // Placeholder
        break;
      case Types.GEOMETRY:
        this.length_size = 4; // GEOMETRY data is length-coded with a 4-byte prefix
        break;
      case Types.JSON:
        this.length_size = 4; // JSON data is length-coded with a 4-byte prefix
        break;
      case Types.DECIMAL: // Alias for NEWDECIMAL (old DECIMAL is not common in binlogs)
      case Types.NULL: // Represents NULL type, typically for prepared statements result set metadata
      case Types.NEWDATE: // An internal type, rarely seen
        break; // No specific initializations beyond the defaults
      default:
        // Log a warning or handle unknown types if necessary.
        console.warn(`Column: Unknown or unhandled MySQL FIELD_TYPE: 0x${type.toString(16)}`);
        break;
    }
  }

  // No additional methods for parsing specific column metadata are defined here
  // as `TableMapEvent._sync_column_info` is responsible for populating these details.
}

module.exports = Column;
