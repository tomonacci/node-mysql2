// Decoder for MySQL's binary JSON storage format (sql/json_binary.cc) and for
// the JSON diffs logged by binlog_row_value_options=PARTIAL_JSON.

'use strict';

const Types = require('../constants/types.js');
const { parseDecimalFromBytes, decimalByteSize } = require('./helpers.js');

const SMALL_OBJECT = 0x00;
const LARGE_OBJECT = 0x01;
const SMALL_ARRAY = 0x02;
const LARGE_ARRAY = 0x03;
const LITERAL = 0x04;
const INT16 = 0x05;
const UINT16 = 0x06;
const INT32 = 0x07;
const UINT32 = 0x08;
const INT64 = 0x09;
const UINT64 = 0x0a;
const DOUBLE = 0x0b;
const STRING = 0x0c;
const OPAQUE = 0x0f;

const LITERALS = [null, true, false];
const DIFF_OPERATIONS = ['REPLACE', 'INSERT', 'REMOVE'];

function readUInt(buffer, offset, size) {
  return size === 2 ? buffer.readUInt16LE(offset) : buffer.readUInt32LE(offset);
}

function readVariableLength(buffer, offset) {
  let length = 0;
  for (let i = 0; i < 5; i++) {
    const byte = buffer[offset + i];
    length += (byte & 0x7f) * 2 ** (7 * i);
    if ((byte & 0x80) === 0) return { length, next: offset + i + 1 };
  }
  throw new Error('Malformed JSON length prefix');
}

function readLengthEncoded(buffer, offset) {
  const first = buffer[offset];
  if (first < 0xfb) return { value: first, next: offset + 1 };
  if (first === 0xfc) {
    return { value: buffer.readUInt16LE(offset + 1), next: offset + 3 };
  }
  if (first === 0xfd) {
    return { value: buffer.readUIntLE(offset + 1, 3), next: offset + 4 };
  }
  return {
    value: Number(buffer.readBigUInt64LE(offset + 1)),
    next: offset + 9,
  };
}

function safeInteger(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) ? number : value.toString();
}

const pad = (value, width) => String(value).padStart(width, '0');

function decodePackedDateTime(packed, type) {
  const fraction = packed & 0xffffffn;
  const dateTime = packed >> 24n;
  const date = dateTime >> 17n;
  const time = dateTime & 0x1ffffn;
  const yearMonth = Number(date >> 5n);
  const day = Number(date & 31n);
  const year = Math.floor(yearMonth / 13);
  const month = yearMonth % 13;
  const ymd = `${pad(year, 4)}-${pad(month, 2)}-${pad(day, 2)}`;
  if (type === Types.DATE) return ymd;
  const hms = `${pad(Number(time >> 12n), 2)}:${pad(Number((time >> 6n) & 63n), 2)}:${pad(Number(time & 63n), 2)}`;
  return `${ymd} ${hms}.${pad(fraction, 6)}`;
}

function decodePackedTime(packed) {
  const negative = packed < 0n;
  const abs = negative ? -packed : packed;
  const time = abs >> 24n;
  const hours = Number((time >> 12n) & 0x3ffn);
  const minutes = Number((time >> 6n) & 63n);
  const seconds = Number(time & 63n);
  return `${negative ? '-' : ''}${pad(hours, 2)}:${pad(minutes, 2)}:${pad(seconds, 2)}.${pad(abs & 0xffffffn, 6)}`;
}

function decodeOpaque(fieldType, data) {
  switch (fieldType) {
    case Types.NEWDECIMAL: {
      const precision = data[0];
      const decimals = data[1];
      return Number(
        parseDecimalFromBytes(
          data.slice(2, 2 + decimalByteSize(precision, decimals)),
          precision,
          decimals
        )
      );
    }
    case Types.DATE:
    case Types.DATETIME:
    case Types.TIMESTAMP:
      return decodePackedDateTime(data.readBigInt64LE(0), fieldType);
    case Types.TIME:
      return decodePackedTime(data.readBigInt64LE(0));
    default:
      return data;
  }
}

function readContainer(buffer, start, large, isObject) {
  const offsetSize = large ? 4 : 2;
  const count = readUInt(buffer, start, offsetSize);
  const keyEntrySize = offsetSize + 2;
  const valueEntrySize = 1 + offsetSize;
  const keyEntries = start + 2 * offsetSize;
  const valueEntries = isObject
    ? keyEntries + count * keyEntrySize
    : keyEntries;

  const readEntry = (index) => {
    const entry = valueEntries + index * valueEntrySize;
    const type = buffer[entry];
    const inlineAt = entry + 1;
    switch (type) {
      case LITERAL:
        return LITERALS[buffer[inlineAt]];
      case INT16:
        return buffer.readInt16LE(inlineAt);
      case UINT16:
        return buffer.readUInt16LE(inlineAt);
      case INT32:
        if (large) return buffer.readInt32LE(inlineAt);
        break;
      case UINT32:
        if (large) return buffer.readUInt32LE(inlineAt);
        break;
      default:
    }
    return readValue(
      buffer,
      type,
      start + readUInt(buffer, inlineAt, offsetSize)
    );
  };

  if (!isObject) {
    const array = new Array(count);
    for (let i = 0; i < count; i++) array[i] = readEntry(i);
    return array;
  }
  const object = {};
  for (let i = 0; i < count; i++) {
    const keyEntry = keyEntries + i * keyEntrySize;
    const keyOffset = readUInt(buffer, keyEntry, offsetSize);
    const keyLength = buffer.readUInt16LE(keyEntry + offsetSize);
    const key = buffer.toString(
      'utf8',
      start + keyOffset,
      start + keyOffset + keyLength
    );
    object[key] = readEntry(i);
  }
  return object;
}

function readValue(buffer, type, offset) {
  switch (type) {
    case SMALL_OBJECT:
      return readContainer(buffer, offset, false, true);
    case LARGE_OBJECT:
      return readContainer(buffer, offset, true, true);
    case SMALL_ARRAY:
      return readContainer(buffer, offset, false, false);
    case LARGE_ARRAY:
      return readContainer(buffer, offset, true, false);
    case LITERAL:
      return LITERALS[buffer[offset]];
    case INT16:
      return buffer.readInt16LE(offset);
    case UINT16:
      return buffer.readUInt16LE(offset);
    case INT32:
      return buffer.readInt32LE(offset);
    case UINT32:
      return buffer.readUInt32LE(offset);
    case INT64:
      return safeInteger(buffer.readBigInt64LE(offset));
    case UINT64:
      return safeInteger(buffer.readBigUInt64LE(offset));
    case DOUBLE:
      return buffer.readDoubleLE(offset);
    case STRING: {
      const { length, next } = readVariableLength(buffer, offset);
      return buffer.toString('utf8', next, next + length);
    }
    case OPAQUE: {
      const { length, next } = readVariableLength(buffer, offset + 1);
      return decodeOpaque(buffer[offset], buffer.slice(next, next + length));
    }
    default:
      throw new Error(`Unknown JSON value type 0x${type.toString(16)}`);
  }
}

/**
 * @param {Buffer} buffer - A JSON document: a type byte followed by its value.
 * @returns {*} The decoded document.
 */
function decodeJson(buffer) {
  return readValue(buffer, buffer[0], 1);
}

/**
 * Decodes the diffs of a partially updated JSON column.
 * @param {Buffer} buffer - The diffs back to back, each as operation, path and
 * (unless removing) value, with the path and value length-encoded.
 * @returns {Array<{operation: string, path: string, value?: *}>}
 */
function decodeJsonDiffs(buffer) {
  const diffs = [];
  let offset = 0;
  while (offset < buffer.length) {
    const operation = DIFF_OPERATIONS[buffer[offset++]];
    const path = readLengthEncoded(buffer, offset);
    offset = path.next + path.value;
    const diff = {
      operation,
      path: buffer.toString('utf8', path.next, offset),
    };
    if (operation !== 'REMOVE') {
      const data = readLengthEncoded(buffer, offset);
      offset = data.next + data.value;
      diff.value = decodeJson(buffer.slice(data.next, offset));
    }
    diffs.push(diff);
  }
  return diffs;
}

module.exports = { decodeJson, decodeJsonDiffs };
