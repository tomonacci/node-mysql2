import { Buffer } from 'node:buffer';
import { describe, it, strict } from 'poku';
import Helpers from '../../../lib/binlog/helpers.js';
import JsonBinary from '../../../lib/binlog/json_binary.js';
import RowEventTypes from '../../../lib/binlog/row_event_types.js';
import Packet from '../../../lib/packets/packet.js';

const { parseDecimalFromBytes, decimalByteSize } = Helpers;
const { decodeJson, decodeJsonDiffs } = JsonBinary;
const { RowsEvent } = RowEventTypes;

// Byte vectors below were captured from a MySQL 8.0 binlog.

const readTime2 = (hex: string, fsp: number) => {
  const buf = Buffer.concat([Buffer.alloc(4), Buffer.from(hex, 'hex')]);
  const event = { packet: new Packet(0, buf, 0, buf.length) };

  // @ts-expect-error: internal access
  return RowsEvent.prototype._read_time2.call(event, { fsp });
};

describe('binlog DECIMAL', () => {
  const decimal = (hex: string, precision: number, scale: number) =>
    parseDecimalFromBytes(Buffer.from(hex, 'hex'), precision, scale);

  it('sizes the packed value by precision and scale', () => {
    strict.equal(decimalByteSize(5, 2), 3);
    strict.equal(decimalByteSize(10, 4), 5);
    strict.equal(decimalByteSize(9, 0), 4);
    strict.equal(decimalByteSize(30, 10), 14);
  });

  it('decodes positive and negative values', () => {
    strict.equal(decimal('7e1dbfe12d', 10, 4), '-123456.7890');
    strict.equal(decimal('44653600', 9, 0), '-999999999');
    strict.equal(decimal('8001', 4, 4), '0.0001');
  });

  it('decodes values spanning several 9-digit words', () => {
    strict.equal(
      decimal('8c149aa4350dfb38d2075bcd1501', 30, 10),
      '12345678901234567890.1234567891'
    );
  });
});

describe('binlog TIME(n)', () => {
  it('decodes negative values', () => {
    strict.equal(readTime2('7f3748', 0), '-12:34:56');
    strict.equal(readTime2('7fffffce', 1), '-00:00:00.5');
    strict.equal(readTime2('7fef7cee30', 3), '-01:02:03.456');
    strict.equal(readTime2('7fffffffff', 4), '-00:00:00.0001');
    strict.equal(readTime2('7f5ffffffff6', 5), '-10:00:00.00001');
    strict.equal(readTime2('4b9105000000', 6), '-838:59:59.000000');
  });

  it('decodes positive values', () => {
    strict.equal(readTime2('b46efa01e240', 6), '838:59:58.123456');
  });
});

describe('binlog JSON', () => {
  it('decodes a document', () => {
    const document = Buffer.from(
      '0003003700190001001a0001001b000100050100021c0000290061626303000d0005010005020005030001000e000b0001000c0c00640178',
      'hex'
    );

    strict.deepEqual(decodeJson(document), {
      a: 1,
      b: [1, 2, 3],
      c: { d: 'x' },
    });
  });

  it('decodes the diffs of a partial update', () => {
    strict.deepEqual(
      decodeJsonDiffs(Buffer.from('0003242e6103050200', 'hex')),
      [{ operation: 'REPLACE', path: '$.a', value: 2 }]
    );
    strict.deepEqual(decodeJsonDiffs(Buffer.from('0206242e625b315d', 'hex')), [
      { operation: 'REMOVE', path: '$.b[1]' },
    ]);
  });
});
