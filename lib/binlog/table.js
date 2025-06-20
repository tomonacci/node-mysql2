// table.js
// Mock Table class to store table metadata and column definitions.

'use strict';

/**
 * Represents a database table.
 */
class Table {
  /**
   * @param {number} tableId - The unique ID for the table in the binlog.
   * @param {string} schema - The database schema name.
   * @param {string} table - The table name.
   * @param {Array<Column>} columns - An array of Column objects representing the table's structure.
   * @param {boolean} [columnNameFlag=false] - Indicates if column names are available in the binlog metadata.
   */
  constructor(tableId, schema, table, columns, columnNameFlag = false) {
    this.table_id = tableId;
    this.schema = schema;
    this.table = table;
    this.columns = columns; // Array of Column instances
    this.column_name_flag = columnNameFlag; // From TableMapEvent's optional metadata
    this.data = { primary_key: [] }; // Mimic Python's structure, primary_key will be set later
  }
}

module.exports = Table;
