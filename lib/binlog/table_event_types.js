// lib/binlog/table_event_types.js
// Implements the MySQL TableMapEvent.

'use strict';

const BinLogEvent = require('./event_base.js');
const Column = require('./column.js'); // Updated path
const Table = require('./table.js'); // Updated path
const CharsetToEncoding = require('../constants/charset_encodings.js'); // Updated path
const MetadataFieldType = require('./metadata_field_type.js'); // Updated path
const Types = require('../constants/types.js'); // Updated path

/**
 * Finds character set information by its ID.
 * @param {string|number} charsetId - The MySQL character set ID.
 * @param {string} [dbms='mysql'] - The database management system (e.g., 'mysql', 'mariadb').
 * @returns {Charset|null} The charset object if found, otherwise null.
 */
function charset_by_id(charsetId, dbms = 'mysql') {
  const id = parseInt(charsetId, 10);
  return CharsetToEncoding[id] || null;
}

/**
 * This event describes the structure of a table.
 * It's sent before a change happens on a table.
 * An end user of the lib should have no usage of this.
 */
class TableMapEvent extends BinLogEvent {
  /**
   * @param {Packet} fromPacket - The Packet instance.
   * @param {BinlogEventHeader} eventHeader - The event header.
   * @param {Object} tableMap - The global table map (object to store Table objects).
   * @param {Object} ctlConnection - The control connection object.
   * @param {Object} options - Additional options for filtering and schema freezing.
   * @param {Array<string>} [options.onlyTables=null] - Tables to include.
   * @param {Array<string>} [options.ignoredTables=null] - Tables to ignore.
   * @param {Array<string>} [options.onlySchemas=null] - Schemas to include.
   * @param {Array<string>} [options.ignoredSchemas=null] - Schemas to ignore.
   * @param {boolean} [options.freezeSchema=false] - Whether to freeze schema once mapped.
   * @param {boolean} [options.optionalMetaData=true] - Whether to read optional metadata.
   */
  constructor(fromPacket, eventHeader, tableMap, ctlConnection, options = {}) {
    super(fromPacket, eventHeader, tableMap, ctlConnection, options);

    this.__only_tables = options.onlyTables || null;
    this.__ignored_tables = options.ignoredTables || null;
    this.__only_schemas = options.onlySchemas || null;
    this.__ignored_schemas = options.ignoredSchemas || null;
    this.__freeze_schema = options.freezeSchema || false;
    this.__optional_meta_data = options.optionalMetaData !== false; // Default true

    // Post-Header
    this.tableId = this.readTableId();

    if (this.tableId in tableMap && this.__freeze_schema) {
      this.processed = false;
      return;
    }

    this.flags = this.packet.readInt16();

    // Payload
    this.schemaLength = this.packet.readInt8(); // !B
    this.schema = this.packet.readString(this.schemaLength, 'utf8');
    this.packet.skip(1); // Skip null byte
    this.tableLength = this.packet.readInt8(); // !B
    this.table = this.packet.readString(this.tableLength, 'utf8');

    // Apply filtering
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

    this.packet.skip(1); // Skip null byte
    this.columnCount = this.packet.readLengthCodedNumber();

    this.columns = [];

    // Read column types
    const columnTypes = this.packet.readBuffer(this.columnCount);
    const metadataLength = this.packet.readLengthCodedNumber(); // Read metadata block length (ignored here as we read directly)
    const metadataStartOffset = this.packet.offset;

    // Use the new Column.fromPacket factory method, passing the original fromPacket
    // so that the Column parsing methods can read from it.
    for (let i = 0; i < columnTypes.length; i++) {
      const columnType = columnTypes[i];
      // Create Column instance by parsing from the packet's current position
      const col = Column.fromPacket(columnType, fromPacket); // Pass 'fromPacket' which is the main Packet instance
      this.columns.push(col);
    }
    if (this.packet.offset !== metadataStartOffset + metadataLength) {
      console.warn(
        `column data offset mismatch for table ${this.table}: got ${this.packet.offset}, expected ${metadataStartOffset + metadataLength}`
      );
      throw Error('column data offset mismatch');
    }

    // ith column is nullable if (i - 1)th bit is set to True, not nullable otherwise
    this.nullBitmask = this.packet.readBuffer(Math.ceil(this.columnCount / 8));

    this.tableObj = new Table(
      this.tableId,
      this.schema,
      this.table,
      this.columns
    );
    tableMap[this.tableId] = this.tableObj;

    // Read optional metadata if enabled
    this.optionalMetadata = this._get_optional_meta_data();
    this._sync_column_info(); // Sync column info from optional metadata
  }

  /**
   * Returns the Table object associated with this event.
   * @returns {Table} The Table object.
   */
  get_table() {
    return this.tableObj;
  }

  /**
   * Dumps the event information to the console.
   * @override
   */
  _dump() {
    super._dump();
    console.log(`Table id: ${this.tableId}`);
    console.log(`Schema: ${this.schema}`);
    console.log(`Table: ${this.table}`);
    console.log(`Columns: ${this.columnCount}`);
    if (this.__optional_meta_data) {
      this.optionalMetadata.dump();
    }
  }

  /**
   * Parses the optional metadata section of the TableMapEvent.
   * TLV format data (TYPE, LENGTH, VALUE).
   * @returns {OptionalMetaData} An OptionalMetaData object containing parsed metadata.
   * @private
   */
  _get_optional_meta_data() {
    const optionalMetadata = new OptionalMetaData();
    // Ensure we don't read past the event's payload.
    // this.eventSize is total event size. 19 is common header.
    const payloadEndOffset = this.eventSize - 19;

    // The packet.offset is already at the start of the optional metadata after fixed parts.
    const startOfOptionalMetadata = this.packet.offset;

    while (this.packet.haveMoreData()) {
      if (this.packet.numRemainingBytes() < 1) {
        // Ensure enough bytes for type
        break; // Reached end of packet prematurely
      }
      const optionMetadataType = this.packet.readInt8(); // Read 1 byte for type

      if (this.packet.numRemainingBytes() < 1) {
        // Ensure enough bytes for length-coded binary length
        break;
      }
      const length = this.packet.readLengthCodedNumber(); // Read length-coded binary length

      const fieldType = MetadataFieldType.by_index(optionMetadataType);
      const dataEndOffset = this.packet.offset + length; // Calculate where the current field's data should end

      // Check if we have enough bytes for the declared length
      if (this.packet.numRemainingBytes() < length) {
        console.warn(
          `Optional metadata field type ${optionMetadataType} declared length ${length} but only ${this.packet.numRemainingBytes()} bytes remaining.`
        );
        this.packet.offset = this.packet.end;
        break;
      }

      switch (fieldType) {
        case MetadataFieldType.SIGNEDNESS:
          optionalMetadata.unsigned_column_list =
            this._convert_include_non_numeric_column(
              this._read_bool_list(length, true)
            );
          break;
        case MetadataFieldType.DEFAULT_CHARSET:
          const [defaultCharsetCollation, charsetCollation] =
            this._read_default_charset(length);
          optionalMetadata.default_charset_collation = defaultCharsetCollation;
          optionalMetadata.charset_collation = charsetCollation;
          optionalMetadata.charset_collation_list =
            this._parsed_column_charset_by_default_charset(
              optionalMetadata.default_charset_collation,
              optionalMetadata.charset_collation,
              this._is_character_column
            );
          break;
        case MetadataFieldType.COLUMN_CHARSET:
          optionalMetadata.column_charset = this._read_ints(length);
          optionalMetadata.charset_collation_list =
            this._parsed_column_charset_by_column_charset(
              optionalMetadata.column_charset,
              this._is_character_column
            );
          break;
        case MetadataFieldType.COLUMN_NAME:
          optionalMetadata.column_name_list = this._read_column_names(length);
          break;
        case MetadataFieldType.SET_STR_VALUE:
          optionalMetadata.set_str_value_list = this._read_type_values(length);
          break;
        case MetadataFieldType.ENUM_STR_VALUE:
          optionalMetadata.set_enum_str_value_list =
            this._read_type_values(length);
          break;
        case MetadataFieldType.GEOMETRY_TYPE:
          optionalMetadata.geometry_type_list = this._read_ints(length);
          break;
        case MetadataFieldType.SIMPLE_PRIMARY_KEY:
          optionalMetadata.simple_primary_key_list = this._read_ints(length);
          break;
        case MetadataFieldType.PRIMARY_KEY_WITH_PREFIX:
          optionalMetadata.primary_keys_with_prefix =
            this._read_primary_keys_with_prefix(length);
          break;
        case MetadataFieldType.ENUM_AND_SET_DEFAULT_CHARSET:
          const [enumSetDefaultCharset, enumSetCharsetCollation] =
            this._read_default_charset(length);
          optionalMetadata.enum_and_set_default_charset = enumSetDefaultCharset;
          optionalMetadata.enum_and_set_charset_collation =
            enumSetCharsetCollation;
          optionalMetadata.enum_and_set_collation_list =
            this._parsed_column_charset_by_default_charset(
              optionalMetadata.enum_and_set_default_charset,
              optionalMetadata.enum_and_set_charset_collation,
              this._is_enum_or_set_column
            );
          break;
        case MetadataFieldType.ENUM_AND_SET_COLUMN_CHARSET:
          optionalMetadata.enum_and_set_default_column_charset_list =
            this._read_ints(length);
          optionalMetadata.enum_and_set_collation_list =
            this._parsed_column_charset_by_column_charset(
              optionalMetadata.enum_and_set_default_column_charset_list,
              this._is_enum_or_set_column
            );
          break;
        case MetadataFieldType.VISIBILITY:
          optionalMetadata.visibility_list = this._read_bool_list(
            length,
            false
          );
          break;
        case MetadataFieldType.UNKNOWN_METADATA_FIELD_TYPE:
        default:
          console.warn(
            `Unknown optional metadata field type: 0x${optionMetadataType.toString(16)} with length ${length}. Skipping ${length} bytes.`
          );
          this.packet.skip(length); // Skip unknown data
          break;
      }
      // Ensure the packet's offset is precisely at `dataEndOffset` after reading the field's data.
      // This is crucial if the `_read_` methods don't consume exactly `length` bytes.
      // (Though ideally, they should).
      if (this.packet.offset !== dataEndOffset) {
        // This indicates a parsing mismatch within one of the _read_ methods.
        // Force jump to the expected end to continue parsing.
        console.warn(
          'dataEndOffset mismatch',
          this.packet.offset,
          dataEndOffset
        );
        this.packet.offset = dataEndOffset;
      }
    }
    return optionalMetadata;
  }

  /**
   * Synchronizes column information with the parsed optional metadata.
   * @private
   */
  _sync_column_info() {
    if (!this.__optional_meta_data) {
      return; // If optional_meta_data is False, do not sync.
    }
    if (this.optionalMetadata.column_name_list.length === 0) {
      // May be BINLOG_ROW_METADATA = FULL now but was MINIMAL before.
      return;
    }

    let charsetPos = 0;
    let enumOrSetPos = 0;
    let enumPos = 0;
    let setPos = 0;

    for (let columnIdx = 0; columnIdx < this.columnCount; columnIdx++) {
      const columnType = this.columns[columnIdx].type;
      const columnData = this.columns[columnIdx];

      // Set column name
      if (columnIdx < this.optionalMetadata.column_name_list.length) {
        columnData.name = this.optionalMetadata.column_name_list[columnIdx];
      }

      // Handle character set for character columns
      if (this._is_character_column(columnType, this.dbms)) {
        if (charsetPos < this.optionalMetadata.charset_collation_list.length) {
          const charsetId =
            this.optionalMetadata.charset_collation_list[charsetPos];
          charsetPos++;
          const charsetInfo = charset_by_id(charsetId, this.dbms);
          if (charsetInfo) {
            columnData.collation_name = charsetInfo.collation;
            columnData.character_set_name = charsetInfo.encoding; // Node.js encoding name
          }
        }
      }

      // Handle character set and values for ENUM/SET columns
      if (this._is_enum_or_set_column(columnType, this.dbms)) {
        if (
          enumOrSetPos <
          this.optionalMetadata.enum_and_set_collation_list.length
        ) {
          const charsetId =
            this.optionalMetadata.enum_and_set_collation_list[enumOrSetPos];
          enumOrSetPos++;
          const charsetInfo = charset_by_id(charsetId, this.dbms);
          if (charsetInfo) {
            columnData.collation_name = charsetInfo.collation;
            columnData.character_set_name = charsetInfo.encoding;
          }
        }

        if (this._is_enum_column(columnType)) {
          if (enumPos < this.optionalMetadata.set_enum_str_value_list.length) {
            const enumColumnInfo =
              this.optionalMetadata.set_enum_str_value_list[enumPos];
            columnData.enum_values = ['', ...enumColumnInfo]; // MySQL ENUMs are 1-indexed, so prepend empty string.
            enumPos++;
          }
        }

        if (this._is_set_column(columnType)) {
          if (setPos < this.optionalMetadata.set_str_value_list.length) {
            const setColumnInfo =
              this.optionalMetadata.set_str_value_list[setPos];
            columnData.set_values = setColumnInfo;
            setPos++;
          }
        }
      }

      // Set unsigned flag for numeric columns
      if (
        this.optionalMetadata.unsigned_column_list &&
        columnIdx < this.optionalMetadata.unsigned_column_list.length
      ) {
        if (this.optionalMetadata.unsigned_column_list[columnIdx]) {
          columnData.unsigned = true;
        }
      }

      // Set primary key flag
      if (
        this.optionalMetadata.simple_primary_key_list &&
        this.optionalMetadata.simple_primary_key_list.includes(columnIdx)
      ) {
        columnData.is_primary = true;
      }
      // Note: Primary keys with prefix are handled directly in OptionalMetaData, not directly assigned to Column.is_primary

      // Set visibility flag
      if (
        this.optionalMetadata.visibility_list &&
        columnIdx < this.optionalMetadata.visibility_list.length
      ) {
        if (this.optionalMetadata.visibility_list[columnIdx]) {
          columnData.visibility = true;
        }
      }
    }

    // After updating columns, recreate tableObj with column_name_flag set to true
    this.tableObj = new Table(
      this.tableId,
      this.schema,
      this.table,
      this.columns,
      true // column_name_flag is true because we have column names
    );
  }

  /**
   * Converts a boolean list for signedness, including non-numeric columns as false.
   * @param {Array<boolean>} signednessBoolList - List of signedness flags for numeric columns.
   * @returns {Array<boolean>} Full boolean list for all columns.
   * @private
   */
  _convert_include_non_numeric_column(signednessBoolList) {
    const boolList = [];
    let position = 0;
    for (let i = 0; i < this.columnCount; i++) {
      const columnType = this.columns[i].type;
      if (this._is_numeric_column(columnType)) {
        if (position < signednessBoolList.length) {
          boolList.push(signednessBoolList[position]);
        } else {
          boolList.push(false); // Default if bitmap is shorter than expected
        }
        position++;
      } else {
        boolList.push(false); // Non-numeric columns are not signed
      }
    }
    return boolList;
  }

  /**
   * Parses column character sets when a default charset is provided.
   * @param {number} defaultCharsetCollation - The default charset collation ID.
   * @param {Object.<number, number>} columnCharsetCollation - Map of column index to explicit charset collation ID.
   * @param {Function} columnTypeDetectFunction - Function to determine if a column type is relevant (e.g., character, enum/set).
   * @returns {Array<number>} List of charset collation IDs for relevant columns.
   * @private
   */
  _parsed_column_charset_by_default_charset(
    defaultCharsetCollation,
    columnCharsetCollation,
    columnTypeDetectFunction
  ) {
    const columnCharset = [];
    let position = 0;
    for (let i = 0; i < this.columnCount; i++) {
      const columnType = this.columns[i].type;
      if (!columnTypeDetectFunction(columnType, this.dbms)) {
        continue;
      } else {
        if (columnCharsetCollation.hasOwnProperty(position)) {
          columnCharset.push(columnCharsetCollation[position]);
        } else {
          columnCharset.push(defaultCharsetCollation);
        }
        position++;
      }
    }
    return columnCharset;
  }

  /**
   * Parses column character sets when explicit column charsets are provided.
   * @param {Array<number>} columnCharsetList - List of charset collation IDs for columns.
   * @param {Function} columnTypeDetectFunction - Function to determine if a column type is relevant.
   * @returns {Array<number>} List of charset collation IDs for relevant columns.
   * @private
   */
  _parsed_column_charset_by_column_charset(
    columnCharsetList,
    columnTypeDetectFunction
  ) {
    const columnCharset = [];
    let position = 0;
    if (columnCharsetList.length === 0) {
      return []; // Return empty if no list provided
    }
    for (let i = 0; i < this.columnCount; i++) {
      const columnType = this.columns[i].type;
      if (!columnTypeDetectFunction(columnType, this.dbms)) {
        continue;
      } else {
        if (position < columnCharsetList.length) {
          columnCharset.push(columnCharsetList[position]);
        } else {
          // This should ideally not happen if data is well-formed.
          // Fallback: use a default or log warning. For now, pushing 0.
          columnCharset.push(0);
        }
        position++;
      }
    }
    return columnCharset;
  }

  /**
   * Reads a boolean list from the packet. Used for signedness and visibility.
   * @param {number} readByteLength - The number of bytes to read for the bitmap.
   * @param {boolean} signednessFlag - True if reading signedness list (implies only numeric columns).
   * @returns {Array<boolean>} A list of booleans.
   * @private
   */
  _read_bool_list(readByteLength, signednessFlag) {
    const boolList = [];
    const bytesData = this.packet.readBuffer(readByteLength);

    let byteIdx = 0;
    let bitIdx = 0; // Current bit position within the byte (0-7)

    for (let i = 0; i < this.columnCount; i++) {
      const columnType = this.columns[i].type;
      if (signednessFlag && !this._is_numeric_column(columnType)) {
        continue; // Skip non-numeric columns for signedness
      }

      if (byteIdx >= bytesData.length) {
        // Ran out of bytes in the bitmap, assume false for remaining columns
        boolList.push(false);
        continue;
      }

      const byte = bytesData[byteIdx];
      boolList.push(((byte >> bitIdx) & 1) !== 0); // Read LSB first (Python's `1 << bit` then `&` is effectively checking bit by index)

      bitIdx++;
      if (bitIdx === 8) {
        bitIdx = 0;
        byteIdx++;
      }
    }
    return boolList;
  }

  /**
   * Reads default character set information.
   * @param {number} length - The length of the data to read.
   * @returns {Array<number|Object>} An array containing default_charset_collation and a map of column_index to charset_collation.
   * @private
   */
  _read_default_charset(length) {
    const charset = {};
    const readUntilOffset = this.packet.offset + length;

    if (this.packet.offset >= readUntilOffset) {
      return [null, {}]; // No default charset or columns specified
    }

    const defaultCharsetCollation = this.packet.readLengthCodedNumber();
    while (this.packet.offset < readUntilOffset) {
      const columnIndex = this.packet.readLengthCodedNumber();
      const charsetCollation = this.packet.readLengthCodedNumber();
      charset[columnIndex] = charsetCollation;
    }
    return [defaultCharsetCollation, charset];
  }

  /**
   * Reads a list of length-coded binary integers.
   * @param {number} length - The total length of the integer data in bytes.
   * @returns {Array<number>} An array of integers.
   * @private
   */
  _read_ints(length) {
    const result = [];
    const readUntilOffset = this.packet.offset + length;
    while (this.packet.offset < readUntilOffset) {
      result.push(this.packet.readLengthCodedNumber());
    }
    return result;
  }

  /**
   * Reads a list of column names (variable-length strings).
   * @param {number} length - The total length of the column name data in bytes.
   * @returns {Array<string>} An array of column names.
   * @private
   */
  _read_column_names(length) {
    const result = [];
    const readUntilOffset = this.packet.offset + length;
    while (this.packet.offset < readUntilOffset) {
      const buffer = this.packet.readVariableLengthString();
      try {
        result.push(buffer.toString('utf8'));
      } catch (e) {
        console.warn(
          `Failed to decode column name: ${e.message}. Using hex representation.`
        );
        result.push(buffer.toString('hex'));
      }
    }
    return result;
  }

  /**
   * Reads a list of type values (e.g., SET or ENUM string values).
   * @param {number} length - The total length of the type value data in bytes.
   * @returns {Array<Array<string>>} An array of arrays of strings.
   * @private
   */
  _read_type_values(length) {
    const result = [];
    const readUntilOffset = this.packet.offset + length;
    if (this.packet.offset >= readUntilOffset) {
      return [];
    }
    while (this.packet.offset < readUntilOffset) {
      const typeValueList = [];
      const valueCount = this.packet.readLengthCodedNumber();
      for (let i = 0; i < valueCount; i++) {
        const valueBuffer = this.packet.readVariableLengthString();
        let decodedValue = '';
        try {
          decodedValue = valueBuffer.toString('utf8');
        } catch (e) {
          console.warn(
            `Failed to decode type value string: ${e.message}. Skipping.`
          );
          // Python's `pass` means it will just remain `""` as initialized.
        }
        typeValueList.push(decodedValue);
      }
      result.push(typeValueList);
    }
    return result;
  }

  /**
   * Reads primary keys with prefix information.
   * @param {number} length - The total length of the data.
   * @returns {Object.<number, number>} A map of column index to prefix length.
   * @private
   */
  _read_primary_keys_with_prefix(length) {
    const ints = this._read_ints(length);
    const result = {};
    for (let i = 0; i < ints.length; i += 2) {
      result[ints[i]] = ints[i + 1];
    }
    return result;
  }

  /**
   * Checks if a column type is a character column.
   * @param {number} columnType - The MySQL FIELD_TYPE.
   * @param {string} [dbms='mysql'] - The database management system.
   * @returns {boolean} True if it's a character column.
   * @private
   */
  _is_character_column(columnType, dbms = 'mysql') {
    if (
      [
        Types.STRING,
        Types.VAR_STRING,
        Types.VARCHAR,
        Types.BLOB,
        Types.TINY_BLOB, // Added BLOB subtypes
        Types.MEDIUM_BLOB,
        Types.LONG_BLOB,
      ].includes(columnType)
    ) {
      return true;
    }
    // MariaDB can treat GEOMETRY as character column in some contexts
    if (columnType === Types.GEOMETRY && dbms === 'mariadb') {
      return true;
    }
    return false;
  }

  /**
   * Checks if a column type is an ENUM column.
   * @param {number} columnType - The MySQL FIELD_TYPE.
   * @returns {boolean} True if it's an ENUM column.
   * @private
   */
  _is_enum_column(columnType) {
    return columnType === Types.ENUM;
  }

  /**
   * Checks if a column type is a SET column.
   * @param {number} columnType - The MySQL FIELD_TYPE.
   * @returns {boolean} True if it's a SET column.
   * @private
   */
  _is_set_column(columnType) {
    return columnType === Types.SET;
  }

  /**
   * Checks if a column type is an ENUM or SET column.
   * @param {number} columnType - The MySQL FIELD_TYPE.
   * @param {string} [dbms='mysql'] - The database management system (not directly used here but kept for signature consistency).
   * @returns {boolean} True if it's an ENUM or SET column.
   * @private
   */
  _is_enum_or_set_column(columnType, dbms = 'mysql') {
    return [Types.ENUM, Types.SET].includes(columnType);
  }

  /**
   * Checks if a column type is a numeric column.
   * @param {number} columnType - The MySQL FIELD_TYPE.
   * @returns {boolean} True if it's a numeric column.
   * @private
   */
  _is_numeric_column(columnType) {
    return [
      Types.TINY,
      Types.SHORT,
      Types.INT24,
      Types.LONG,
      Types.LONGLONG,
      Types.NEWDECIMAL,
      Types.FLOAT,
      Types.DOUBLE,
      Types.YEAR,
    ].includes(columnType);
  }
}

/**
 * Class to hold optional metadata parsed from TableMapEvent.
 * Corresponds to Python's OptionalMetaData.
 */
class OptionalMetaData {
  constructor() {
    this.unsigned_column_list = [];
    this.default_charset_collation = null;
    this.charset_collation = {};
    this.column_charset = [];
    this.column_name_list = [];
    this.set_str_value_list = [];
    this.set_enum_str_value_list = [];
    this.geometry_type_list = [];
    this.simple_primary_key_list = [];
    this.primary_keys_with_prefix = {};
    this.enum_and_set_default_charset = null;
    this.enum_and_set_charset_collation = {};
    this.enum_and_set_default_column_charset_list = [];
    this.charset_collation_list = [];
    this.enum_and_set_collation_list = [];
    this.visibility_list = [];
  }

  dump() {
    console.log(`=== OptionalMetaData ===`);
    console.log(
      `unsigned_column_list: ${JSON.stringify(this.unsigned_column_list)}`
    );
    console.log(`default_charset_collation: ${this.default_charset_collation}`);
    console.log(`charset_collation: ${JSON.stringify(this.charset_collation)}`);
    console.log(`column_charset: ${JSON.stringify(this.column_charset)}`);
    console.log(`column_name_list: ${JSON.stringify(this.column_name_list)}`);
    console.log(
      `set_str_value_list : ${JSON.stringify(this.set_str_value_list)}`
    );
    console.log(
      `set_enum_str_value_list : ${JSON.stringify(this.set_enum_str_value_list)}`
    );
    console.log(
      `geometry_type_list : ${JSON.stringify(this.geometry_type_list)}`
    );
    console.log(
      `simple_primary_key_list: ${JSON.stringify(this.simple_primary_key_list)}`
    );
    console.log(
      `primary_keys_with_prefix: ${JSON.stringify(this.primary_keys_with_prefix)}`
    );
    console.log(`visibility_list: ${JSON.stringify(this.visibility_list)}`);
    console.log(
      `charset_collation_list: ${JSON.stringify(this.charset_collation_list)}`
    );
    console.log(
      `enum_and_set_collation_list: ${JSON.stringify(this.enum_and_set_collation_list)}`
    );
  }
}

// Helper function `find_charset` (from Python `find_charset` function)
// This function relies on `charset_by_id` from `constants/charset.js`
function find_charset(charsetId, dbms = 'mysql') {
  let encode = null;
  let collationName = null;
  let charsetName = null;
  const charset = charset_by_id(charsetId, dbms); // Use the imported helper

  if (charset === null) {
    encode = 'utf8';
    charsetName = 'utf8';
  } else {
    encode = charset.encoding;
    collationName = charset.collation;
    charsetName = charset.name;
  }
  return [encode, collationName, charsetName];
}

module.exports = {
  TableMapEvent,
  OptionalMetaData, // Exported for completeness, might not be directly used outside this module
  find_charset, // Exported as a helper that TableMapEvent uses
  MetadataFieldType, // Export the enum as well
};
