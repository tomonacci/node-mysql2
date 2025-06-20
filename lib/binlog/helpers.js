// This file contains helper functions and custom error classes for binlog event parsing.

'use strict';

const NativeBuffer = require('buffer').Buffer;
const Long = require('long'); // Required for internal decimal parsing if BigInt not fully supported for certain ops.

/**
 * Custom decimal parsing for UserVarEvent, equivalent to Python's parse_decimal_from_bytes.
 * This is a placeholder and provides a simplified interpretation. A full, precise implementation
 * would require porting MySQL's complex internal decimal storage format parsing, potentially
 * involving a dedicated JavaScript arbitrary-precision decimal library (e.g., 'decimal.js' or 'big.js').
 *
 * @param {Buffer} buffer - The buffer containing the raw decimal bytes (excluding precision/decimals prefix).
 * @param {number} precision - The total number of decimal digits.
 * @param {number} decimals - The number of digits after the decimal point.
 * @returns {string} The decoded decimal as a string.
 */
function parseDecimalFromBytes(buffer, precision, decimals) {
  if (!buffer || buffer.length === 0) {
    return '0';
  }

  // Very simplified: assuming it's a signed integer string
  // This will NOT be correct for all MySQL DECIMAL types.
  let isNegative = false;
  // Check the highest bit of the first byte for the sign, then flip it.
  // MySQL stores decimals in a way that the most significant byte's highest bit
  // indicates the sign for the entire number.
  if (buffer[0] & 0x80) {
    isNegative = true;
    buffer[0] ^= 0x80; // Flip sign bit for parsing positive magnitude
  }

  // Convert the buffer to a BigInt for potentially large numbers.
  // This assumes the buffer represents the integer part of the decimal.
  let value = 0n;
  for (let i = 0; i < buffer.length; i++) {
    value = (value << 8n) | BigInt(buffer[i]);
  }

  let result = value.toString();

  // Insert decimal point
  if (decimals > 0) {
    const intPartLength = result.length - decimals;
    if (intPartLength <= 0) {
      // Pad with leading zeros for fractional parts like .001
      result = '0.' + '0'.repeat(Math.abs(intPartLength)) + result;
    } else {
      result = result.substring(0, intPartLength) + '.' + result.substring(intPartLength);
    }
  }

  if (isNegative) {
    result = '-' + result;
  }

  return result;
}

/**
 * Custom error class for Status Variable Mismatch in QueryEvent.
 */
class StatusVariableMismatch extends Error {
  constructor(message = "Status variable key mismatch or unknown.") {
    super(message);
    this.name = "StatusVariableMismatch";
  }
}

module.exports = {
  parseDecimalFromBytes,
  StatusVariableMismatch,
};
