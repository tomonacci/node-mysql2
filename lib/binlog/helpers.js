// This file contains helper functions and custom error classes for binlog event parsing.

'use strict';

const NativeBuffer = require('buffer').Buffer;

const DECIMAL_WORD_DIGITS = 9;
const COMPRESSED_BYTES = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

/**
 * Size in bytes of MySQL's packed DECIMAL(precision, decimals).
 * @param {number} precision - The total number of decimal digits.
 * @param {number} decimals - The number of digits after the decimal point.
 * @returns {number}
 */
function decimalByteSize(precision, decimals) {
  const integral = precision - decimals;
  return (
    Math.floor(integral / DECIMAL_WORD_DIGITS) * 4 +
    COMPRESSED_BYTES[integral % DECIMAL_WORD_DIGITS] +
    Math.floor(decimals / DECIMAL_WORD_DIGITS) * 4 +
    COMPRESSED_BYTES[decimals % DECIMAL_WORD_DIGITS]
  );
}

/**
 * Decodes MySQL's packed DECIMAL (see decimal2bin in strings/decimal.c).
 * The sign lives in the top bit of the first byte, and a negative value has
 * every byte inverted. The value is stored in big-endian words of 9 decimal
 * places, with a shorter leading word for the integral part and a shorter
 * trailing word for the fractional part.
 *
 * @param {Buffer} buffer - Exactly `decimalByteSize(precision, decimals)` bytes.
 * @param {number} precision - The total number of decimal digits.
 * @param {number} decimals - The number of digits after the decimal point.
 * @returns {string} The decoded decimal.
 */
function parseDecimalFromBytes(buffer, precision, decimals) {
  const bytes = NativeBuffer.from(buffer);
  const negative = (bytes[0] & 0x80) === 0;
  bytes[0] ^= 0x80;
  if (negative) {
    for (let i = 0; i < bytes.length; i++) bytes[i] ^= 0xff;
  }

  let offset = 0;
  const readWord = (size) => {
    let word = 0;
    for (let i = 0; i < size; i++) word = word * 256 + bytes[offset + i];
    offset += size;
    return word;
  };

  const integral = precision - decimals;
  let integralPart = '';
  const leadingPlaces = integral % DECIMAL_WORD_DIGITS;
  if (leadingPlaces) {
    integralPart += String(readWord(COMPRESSED_BYTES[leadingPlaces]));
  }
  for (let i = 0; i < Math.floor(integral / DECIMAL_WORD_DIGITS); i++) {
    integralPart += String(readWord(4)).padStart(DECIMAL_WORD_DIGITS, '0');
  }
  integralPart = integralPart.replace(/^0+(?=\d)/, '') || '0';

  let fractionalPart = '';
  for (let i = 0; i < Math.floor(decimals / DECIMAL_WORD_DIGITS); i++) {
    fractionalPart += String(readWord(4)).padStart(DECIMAL_WORD_DIGITS, '0');
  }
  const trailingPlaces = decimals % DECIMAL_WORD_DIGITS;
  if (trailingPlaces) {
    fractionalPart += String(
      readWord(COMPRESSED_BYTES[trailingPlaces])
    ).padStart(trailingPlaces, '0');
  }

  const sign = negative ? '-' : '';
  return fractionalPart
    ? `${sign}${integralPart}.${fractionalPart}`
    : `${sign}${integralPart}`;
}

/**
 * Custom error class for Status Variable Mismatch in QueryEvent.
 */
class StatusVariableMismatch extends Error {
  constructor(message = 'Status variable key mismatch or unknown.') {
    super(message);
    this.name = 'StatusVariableMismatch';
  }
}

module.exports = {
  decimalByteSize,
  parseDecimalFromBytes,
  StatusVariableMismatch,
};
