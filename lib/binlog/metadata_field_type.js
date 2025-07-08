// metadata-field-type.js
// Enum for metadata field types within TableMapEvent optional metadata.

'use strict';

/**
 * Enum for metadata field types present in the TableMapEvent's optional metadata section.
 * Corresponds to Python's MetadataFieldType.
 */
const MetadataFieldType = {
  SIGNEDNESS: 1,
  DEFAULT_CHARSET: 2,
  COLUMN_CHARSET: 3,
  COLUMN_NAME: 4,
  SET_STR_VALUE: 5,
  ENUM_STR_VALUE: 6,
  GEOMETRY_TYPE: 7,
  SIMPLE_PRIMARY_KEY: 8,
  PRIMARY_KEY_WITH_PREFIX: 9,
  ENUM_AND_SET_DEFAULT_CHARSET: 10,
  ENUM_AND_SET_COLUMN_CHARSET: 11,
  VISIBILITY: 12,
  UNKNOWN_METADATA_FIELD_TYPE: 128,

  /**
   * Static method to get the enum value by its numerical index.
   * @param {number} index - The numerical index of the metadata field type.
   * @returns {number} The corresponding enum value.
   */
  by_index: function (index) {
    // This assumes the index directly maps to one of the defined values.
    // In a stricter enum, one might check if `index` is a valid property value.
    const key = Object.keys(MetadataFieldType).find(
      (k) => MetadataFieldType[k] === index
    );
    return (
      MetadataFieldType[key] || MetadataFieldType.UNKNOWN_METADATA_FIELD_TYPE
    );
  },
};

module.exports = MetadataFieldType;
