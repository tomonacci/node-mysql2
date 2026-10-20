import { Buffer } from 'node:buffer';
import { describe, it, strict } from 'poku';
import BinlogEventFactory from '../../../lib/binlog/event_factory.js';
import RowEventTypes from '../../../lib/binlog/row_event_types.js';
import TableEventTypes from '../../../lib/binlog/table_event_types.js';
import EventTypes from '../../../lib/constants/binlog_event_types.js';
import Types from '../../../lib/constants/types.js';
import Packet from '../../../lib/packets/packet.js';

const { RowsEvent } = RowEventTypes;
const { TableMapEvent } = TableEventTypes;

// @ts-expect-error: TODO: implement typings
const { TINY, YEAR, BIT, STRING, VARCHAR } = Types;
// @ts-expect-error: TODO: implement typings
const { ENUM, SET, LONG, DATETIME2 } = Types;

const MYSQL_PACKET_PREFIX = 5;
const HEADER_SIZE = 19;

const int = (value: number, size: number) => {
  const buf = Buffer.alloc(size);
  if (size === 8) buf.writeBigUInt64LE(BigInt(value));
  else buf.writeUIntLE(value, 0, size);
  return buf;
};

function decode(
  eventType: number,
  body: Buffer,
  tableMap: Record<number, unknown> = {}
): DecodedEvent {
  const header = Buffer.concat([
    int(0, 4),
    int(eventType, 1),
    int(1, 4),
    int(HEADER_SIZE + body.length, 4),
    int(0, 4),
    int(0, 2),
  ]);
  const wire = Buffer.concat([Buffer.alloc(MYSQL_PACKET_PREFIX), header, body]);

  return BinlogEventFactory.fromPacket(
    new Packet(0, wire, 0, wire.length),
    tableMap,
    {},
    {}
  );
}

interface DecodedEvent {
  constructor: { name: string };
  rows: { values: Record<string, unknown> }[];
  value: unknown;
  flags: number;
  xid: number;
  query: string;
  schema: string;
  fileId: number;
  dupHandlingFlags: number;
}

const queryHeader = (dbLength: number, statusVars: Buffer) =>
  Buffer.concat([
    int(0, 4),
    int(0, 4),
    int(dbLength, 1),
    int(0, 2),
    int(statusVars.length, 2),
  ]);

describe('binlog row values', () => {
  const readValue = (
    column: Record<string, unknown>,
    hex: string,
    unsigned = false
  ) => {
    const buf = Buffer.concat([Buffer.alloc(4), Buffer.from(hex, 'hex')]);
    const event = {
      tableMap: { 1: { columns: [{ name: 'v' }] } },
      tableId: 1,
      packet: new Packet(0, buf, 0, buf.length),
      __none_sources: {},
      // @ts-expect-error: internal access
      _is_null: RowsEvent.prototype._is_null,
      // @ts-expect-error: internal access
      _read_string: RowsEvent.prototype._read_string,
      // @ts-expect-error: internal access
      charset_to_encoding: RowsEvent.prototype.charset_to_encoding,
      // @ts-expect-error: internal access
      _read_bit: RowsEvent.prototype._read_bit,
      // @ts-expect-error: internal access
      _read_datetime2: RowsEvent.prototype._read_datetime2,
      // @ts-expect-error: internal access
      _read_binary_slice: RowsEvent.prototype._read_binary_slice,
      // @ts-expect-error: internal access
      _add_fsp_to_time: RowsEvent.prototype._add_fsp_to_time,
      // @ts-expect-error: internal access
      _read_fsp: RowsEvent.prototype._read_fsp,
    };

    // @ts-expect-error: internal access
    return RowsEvent.prototype._read_values_name.call(
      event,
      column,
      Buffer.from([0]),
      0,
      false,
      Buffer.from([1]),
      unsigned,
      0
    );
  };

  it('reads unsigned integers as unsigned', () => {
    strict.equal(readValue({ type: TINY }, 'fa', true), 250);
    strict.equal(readValue({ type: TINY }, 'fa'), -6);
  });

  it('reads YEAR 0000 as 0', () => {
    strict.equal(readValue({ type: YEAR }, '00'), 0);
    strict.equal(readValue({ type: YEAR }, '7c'), 2024);
  });

  it('reads BIT as its bytes', () => {
    strict.deepEqual(
      readValue({ type: BIT, bytes: 2, bits: 12 }, '0aaa'),
      Buffer.from('0aaa', 'hex')
    );
  });

  it('pads BINARY(n) values the server stripped', () => {
    const binary = { type: STRING, charset_id: 63, max_length: 4 };

    strict.deepEqual(
      readValue(binary, '026162'),
      Buffer.from('61620000', 'hex')
    );
    strict.deepEqual(
      readValue({ ...binary, type: VARCHAR }, '026162'),
      Buffer.from('6162', 'hex')
    );
  });

  it('keeps the stored value of ENUM and SET without member names', () => {
    strict.equal(readValue({ type: ENUM, size: 1 }, '02'), 2);
    strict.equal(readValue({ type: SET, size: 1 }, '05'), 5);
  });

  it('names the members of ENUM and SET', () => {
    const enumColumn = {
      type: ENUM,
      size: 1,
      enum_values: ['', 'a', 'b'],
    };
    const setColumn = { type: SET, size: 1, set_values: ['x', 'y', 'z'] };

    strict.equal(readValue(enumColumn, '02'), 'b');
    strict.equal(readValue(setColumn, '05'), 'x,z');
    strict.equal(readValue(setColumn, '00'), '');
  });

  it('reads DATETIME2 past 32 bits', () => {
    const yearMonth = BigInt(2024 * 13 + 2);
    const packed =
      (1n << 39n) |
      (yearMonth << 22n) |
      (29n << 17n) |
      (12n << 12n) |
      (34n << 6n) |
      56n;
    const hex = packed.toString(16).padStart(10, '0');

    strict.deepEqual(
      readValue({ type: DATETIME2, fsp: 0 }, hex),
      new Date(2024, 1, 29, 12, 34, 56)
    );
  });
});

describe('binlog row decoding', () => {
  it('reports the error again when a lazy decode fails', () => {
    const buf = Buffer.concat([Buffer.alloc(4), Buffer.from([1])]);
    const packet = new Packet(0, buf, 0, buf.length);
    const event = {
      processed: true,
      packet,
      __rows: null,
      _fetch_one_row() {
        packet.readInt8();
        throw new Error('decode failed');
      },
    };
    const fetchRows = () =>
      // @ts-expect-error: internal access
      RowsEvent.prototype._fetch_rows.call(event);

    strict.throws(fetchRows, /decode failed/);
    strict.throws(fetchRows, /decode failed/);
    strict.equal(event.__rows, null);
  });

  it('reads signedness bitmaps most significant bit first', () => {
    const buf = Buffer.concat([Buffer.alloc(4), Buffer.from([0x40])]);
    const event = {
      packet: new Packet(0, buf, 0, buf.length),
      columnCount: 3,
      columns: [{ type: LONG }, { type: TINY }, { type: VARCHAR }],
      // @ts-expect-error: internal access
      _is_numeric_column: TableMapEvent.prototype._is_numeric_column,
    };

    strict.deepEqual(
      // @ts-expect-error: internal access
      TableMapEvent.prototype._read_bool_list.call(event, 1, true),
      [false, true]
    );
  });
});

describe('binlog event factory', () => {
  const tableMap = {
    1: {
      schema: 's',
      table: 't',
      data: { primary_key: [] },
      columns: [{ type: TINY, name: 'a' }],
    },
  };
  const v1RowsBody = (rows: Buffer) =>
    Buffer.concat([int(1, 6), int(0, 2), int(1, 1), Buffer.from([1]), rows]);

  it('decodes V1 row events, as sent by MariaDB', () => {
    const event = decode(
      EventTypes.WRITE_ROWS_EVENT_V1,
      v1RowsBody(Buffer.from([0, 5])),
      tableMap
    );

    strict.equal(event.constructor.name, 'WriteRowsEvent');
    strict.deepEqual(event.rows[0].values, { a: 5 });
    strict.equal(
      decode(
        EventTypes.DELETE_ROWS_EVENT_V1,
        v1RowsBody(Buffer.alloc(0)),
        tableMap
      ).constructor.name,
      'DeleteRowsEvent'
    );
  });

  it('decodes EXECUTE_LOAD_QUERY as a query with load fields', () => {
    const statement = "LOAD DATA INFILE '/tmp/f' INTO TABLE t";
    const event = decode(
      EventTypes.EXECUTE_LOAD_QUERY_EVENT,
      Buffer.concat([
        queryHeader(1, Buffer.alloc(0)),
        int(7, 4),
        int(16, 4),
        int(22, 4),
        int(1, 1),
        Buffer.from(`d\0${statement}`),
      ])
    );

    strict.equal(event.constructor.name, 'ExecuteLoadQueryEvent');
    strict.equal(event.fileId, 7);
    strict.equal(event.dupHandlingFlags, 1);
    strict.equal(event.schema, 'd');
    strict.equal(event.query, statement);
  });

  it('decodes ROWS_QUERY', () => {
    const statement = 'INSERT INTO t VALUES (1)';
    const event = decode(
      EventTypes.ROWS_QUERY_LOG_EVENT,
      Buffer.concat([int(statement.length, 1), Buffer.from(statement)])
    );

    strict.equal(event.query, statement);
  });

  it('decodes anonymous GTIDs', () => {
    const event = decode(
      EventTypes.ANONYMOUS_GTID_LOG_EVENT,
      Buffer.concat([int(1, 1), Buffer.alloc(16), int(0, 8), int(2, 1)])
    );

    strict.equal(event.constructor.name, 'AnonymousGtidEvent');
  });

  it('accepts the MariaDB Q_XID status variable', () => {
    const statusVars = Buffer.concat([int(0x81, 1), int(9, 8)]);
    const event = decode(
      EventTypes.QUERY_EVENT,
      Buffer.concat([
        queryHeader(1, statusVars),
        statusVars,
        Buffer.from('d\0CREATE TABLE t (a INT)'),
      ])
    );

    strict.equal(event.xid, 9);
    strict.equal(event.query, 'CREATE TABLE t (a INT)');
  });

  describe('USER_VAR', () => {
    const userVar = (
      type: number,
      charset: number,
      value: Buffer,
      tail: Buffer
    ) =>
      decode(
        EventTypes.USER_VAR_EVENT,
        Buffer.concat([
          int(1, 4),
          Buffer.from('u'),
          int(0, 1),
          int(type, 1),
          int(charset, 4),
          int(value.length, 4),
          value,
          tail,
        ])
      );

    it('reads values that have no flags byte', () => {
      const real = Buffer.alloc(8);
      real.writeDoubleLE(1.5);

      strict.equal(userVar(1, 63, real, Buffer.alloc(0)).value, 1.5);
      strict.equal(
        userVar(0, 33, Buffer.from('abc'), Buffer.alloc(0)).value,
        'abc'
      );
    });

    it('reads the flags byte of integer values', () => {
      const event = userVar(2, 63, int(42, 8), int(1, 1));

      strict.equal(event.value, 42);
      strict.equal(event.flags, 1);
    });

    it('reads DECIMAL values', () => {
      const decimal = Buffer.concat([
        Buffer.from([3, 2]),
        Buffer.from('8119', 'hex'),
      ]);

      strict.equal(userVar(4, 63, decimal, Buffer.alloc(0)).value, '1.25');
    });
  });
});
