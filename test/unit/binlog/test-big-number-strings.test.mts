import { Buffer } from 'node:buffer';
import { describe, it, strict } from 'poku';
import RowEventTypes from '../../../lib/binlog/row_event_types.js';
import Types from '../../../lib/constants/types.js';
import Packet from '../../../lib/packets/packet.js';

const { RowsEvent } = RowEventTypes;

function readLongLong(
  value: bigint,
  {
    unsigned,
    bigNumberStrings,
  }: { unsigned: boolean; bigNumberStrings: boolean }
) {
  const buf = Buffer.alloc(12);
  buf.writeBigUInt64LE(BigInt.asUintN(64, value), 4);

  const event = {
    tableMap: { 1: { columns: [{ name: 'id' }] } },
    tableId: 1,
    packet: new Packet(0, buf, 0, buf.length),
    __none_sources: {},
    __big_number_strings: bigNumberStrings,
    // @ts-expect-error: internal access
    _is_null: RowsEvent.prototype._is_null,
  };

  // @ts-expect-error: internal access
  return RowsEvent.prototype._read_values_name.call(
    event,
    // @ts-expect-error: TODO: implement typings
    { type: Types.LONGLONG },
    Buffer.from([0]),
    0,
    false,
    Buffer.from([1]),
    unsigned,
    0
  );
}

describe('RowsEvent bigNumberStrings', () => {
  it('keeps safe BIGINT values as numbers by default', () => {
    const opts = { unsigned: true, bigNumberStrings: false };

    strict.equal(readLongLong(42n, opts), 42);
    strict.equal(readLongLong(2n ** 53n + 1n, opts), '9007199254740993');
  });

  it('returns every unsigned BIGINT as a string when enabled', () => {
    const opts = { unsigned: true, bigNumberStrings: true };

    strict.equal(readLongLong(42n, opts), '42');
    strict.equal(readLongLong(2n ** 64n - 1n, opts), '18446744073709551615');
  });

  it('returns every signed BIGINT as a string when enabled', () => {
    const opts = { unsigned: false, bigNumberStrings: true };

    strict.equal(readLongLong(-42n, opts), '-42');
    strict.equal(readLongLong(-(2n ** 63n), opts), '-9223372036854775808');
  });
});
