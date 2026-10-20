'use strict';
process.env.TZ = 'UTC';
// Dumps every binlog event the driver decodes for a fixed workload and, with a
// baseline given, reports events that differ from it.
// usage: node harness.js <repoRoot> <out.json> [minimal|full] [baseline.json]
// Needs a ROW-format, binlog-enabled server (BINLOG_PORT, default 3399).
const path = require('path');
const fs = require('fs');
const root = path.resolve(process.argv[2]);
const out = process.argv[3];
const baselinePath = process.argv[5];
const mode = (process.argv[4] || 'minimal').toUpperCase();
const mysql = require(root + '/index.js');
const cfg = { host: '127.0.0.1', port: Number(process.env.BINLOG_PORT || 3399), user: 'root', database: 'test', multipleStatements: true };

const typeCases = {
  tinyint: ['TINYINT', ['-5', '127', 'NULL']],
  utinyint: ['TINYINT UNSIGNED', ['250', '0', 'NULL']],
  smallint: ['SMALLINT', ['-300', '32767', 'NULL']],
  mediumint: ['MEDIUMINT', ['-70000', '8388607', 'NULL']],
  int: ['INT', ['-2000000000', '2147483647', 'NULL']],
  bigint: ['BIGINT', ['-9007199254740993', '9223372036854775807', 'NULL']],
  ubigint: ['BIGINT UNSIGNED', ['18446744073709551615', '0', 'NULL']],
  float: ['FLOAT', ['1.5', '-0.25', 'NULL']],
  double: ['DOUBLE', ['2.25', '-1e100', 'NULL']],
  decimal: ['DECIMAL(20,5)', ["'123456789012345.12345'", "'-0.00001'", 'NULL']],
  decimal_small: ['DECIMAL(5,2)', ["'-12.50'", "'0'", 'NULL']],
  char: ['CHAR(10)', ["'abc'", "''", 'NULL']],
  varchar: ['VARCHAR(300)', ["'héllo wörld'", "''", 'NULL']],
  varchar_long: ['VARCHAR(1000)', ["REPEAT('é',400)", "'x'", 'NULL']],
  text: ['TEXT', ["'long text'", "''", 'NULL']],
  blob: ['BLOB', ["'blobdata'", "''", 'NULL']],
  binary: ['BINARY(4)', ["'ab'", "'abcd'", 'NULL']],
  varbinary: ['VARBINARY(20)', ["'vb'", "''", 'NULL']],
  date: ['DATE', ["'2024-02-29'", "'1000-01-01'", 'NULL']],
  time: ['TIME', ["'-12:34:56'", "'838:59:59'", 'NULL']],
  time3: ['TIME(3)', ["'01:02:03.456'", "'00:00:00.000'", 'NULL']],
  datetime: ['DATETIME', ["'2024-02-29 12:34:56'", "'1000-01-01 00:00:00'", 'NULL']],
  datetime6: ['DATETIME(6)', ["'2024-02-29 12:34:56.123456'", "'1000-01-01 00:00:00.000000'", 'NULL']],
  timestamp: ['TIMESTAMP NULL', ["'2024-02-29 12:34:56'", "'1970-01-01 00:00:01'", 'NULL']],
  timestamp3: ['TIMESTAMP(3) NULL', ["'2024-02-29 12:34:56.789'", "'1970-01-01 00:00:01.000'", 'NULL']],
  year: ['YEAR', ['2024', '1901', 'NULL']],
  enum: ["ENUM('a','b','c')", ["'b'", "'c'", 'NULL']],
  set: ["SET('x','y','z')", ["'x,z'", "''", 'NULL']],
  bit: ['BIT(12)', ["b'101010101010'", "b'0'", 'NULL']],
  json: ['JSON', [`'{"k":[1,2,{"n":null}],"s":"v"}'`, "'[]'", 'NULL']],
};

const workload = [
  `SET time_zone='+00:00'`,
  ...Object.entries(typeCases).flatMap(([name, [type, vals]]) => [
    `DROP TABLE IF EXISTS t_${name}`,
    `CREATE TABLE t_${name} (id INT PRIMARY KEY, v ${type})`,
    `INSERT INTO t_${name} VALUES ${vals.map((v, i) => `(${i + 1}, ${v})`).join(',')}`,
  ]),
  `UPDATE t_varchar SET v='changed' WHERE id=1`,
  `DELETE FROM t_int WHERE id=2`,
  'DROP TABLE IF EXISTS t_small',
  'CREATE TABLE t_small (a INT, b VARCHAR(20))',
  `INSERT INTO t_small VALUES (1,'one'),(2,'two'),(3,'three')`,
  `UPDATE t_small SET b='uno' WHERE a=1`,
  `UPDATE t_small SET b=CONCAT(b,'!')`,
  `DELETE FROM t_small WHERE a=3`,
  'START TRANSACTION', `INSERT INTO t_small VALUES (10,'tx')`, `INSERT INTO t_small VALUES (11,NULL)`, 'COMMIT',
  'START TRANSACTION', `INSERT INTO t_small VALUES (99,'rolled back')`, 'ROLLBACK',
  `INSERT INTO t_small SELECT a+100, REPEAT('x',15) FROM t_small`,
  'ALTER TABLE t_small ADD COLUMN c INT DEFAULT 7',
  `INSERT INTO t_small VALUES (200,'after alter',8)`,
  'TRUNCATE TABLE t_small'
];

function norm(v, asTimeOfDay) {
  if (typeof v === 'bigint') return { $bigint: v.toString() };
  if (Buffer.isBuffer(v)) return { $buf: v.toString('hex') };
  if (v instanceof Date) {
    if (isNaN(v)) return { $date: 'invalid' };
    if (asTimeOfDay) return { $msSinceUtcMidnight: v - new Date().setUTCHours(0, 0, 0, 0) };
    return { $date: v.toISOString() };
  }
  if (v && typeof v === 'object' && typeof v.toString === 'function' && v.constructor && v.constructor.name === 'Long') return { $long: v.toString() };
  if (Array.isArray(v)) return v.map((x) => norm(x, asTimeOfDay));
  if (v && typeof v === 'object') {
    const o = {};
    for (const k of Object.keys(v)) o[k] = norm(v[k], asTimeOfDay);
    return o;
  }
  if (typeof v === 'number' && !Number.isFinite(v)) return { $num: String(v) };
  return v;
}

async function main() {
  const admin = mysql.createConnection(cfg).promise();
  await admin.query("SET GLOBAL binlog_row_metadata='" + mode + "'");
  await admin.end();
  const c = mysql.createConnection(cfg).promise();
  await c.query('DROP DATABASE IF EXISTS test; CREATE DATABASE test; USE test');
  await c.query('RESET MASTER');
  for (const sql of workload) await c.query(sql);
  const [[st]] = await c.query('SHOW MASTER STATUS');
  await c.end();

  const bc = mysql.createConnection({ ...cfg, database: undefined });
  const events = [];
  const stream = await bc.promise().createBinlogStream({
    serverId: 4242, filename: st.File, binlogPos: 4, flags: 1,
  });
  await new Promise((resolve, reject) => {
    stream.on('data', (e) => events.push(e));
    stream.on('end', resolve);
    stream.on('error', reject);
    setTimeout(() => reject(new Error('timeout; got ' + events.length + ' events')), 20000).unref();
  });
  try { bc.destroy(); } catch (e) {}

  // Fields that vary between runs or only expose raw wire bytes.
  // Decoders known to read past the row and return garbage; only their shape is stable.
  const UNSTABLE_TABLES = new Set(['t_decimal', 't_decimal_small']);
  // The driver returns TIME as a Date on the current day; keep only the offset from midnight.
  const TIME_TABLES = new Set(['t_time', 't_time3']);
  const VOLATILE = new Set(['packet', 'created', 'slaveProxyId', 'xid', 'ddlXid', 'tableId', 'table_id', 'timestamp', 'executionTime', 'tableMap']);
  const strip = (v) => {
    if (Array.isArray(v)) return v.map(strip);
    if (v && typeof v === 'object' && !v.$buf && !v.$bigint && !v.$date && !v.$long && v.$msSinceUtcMidnight === undefined) {
      const o = {};
      for (const k of Object.keys(v)) if (!VOLATILE.has(k)) o[k] = strip(v[k]);
      return o;
    }
    return v;
  };
  const dump = events.map((e) => {
    const o = { ctor: e.constructor && e.constructor.name };
    for (const k of Object.keys(e)) o[k] = norm(e[k]);
    // Row events decode lazily through a getter.
    if ('rows' in e) {
      try { o.rows = UNSTABLE_TABLES.has(e.table) ? `<${e.rows.length} rows, unstable>` : norm(e.rows, TIME_TABLES.has(e.table)); } catch (err) { o.rowsError = err.message; }
    }
    return strip(o);
  });
  fs.writeFileSync(out, JSON.stringify({ end: { file: st.File, pos: st.Position }, count: dump.length, events: dump }, null, 1));
  const tally = {};
  for (const d of dump) tally[d.ctor] = (tally[d.ctor] || 0) + 1;
  console.log(dump.length, 'events', tally);
}
function compare(actualPath, baselinePath) {
  const a = JSON.parse(fs.readFileSync(actualPath, 'utf8'));
  const b = JSON.parse(fs.readFileSync(baselinePath, 'utf8'));
  if (a.error || b.error) {
    const same = a.error === b.error;
    console.log(same ? 'same error as baseline:' : 'ERROR CHANGED:', 'now=', a.error, 'baseline=', b.error);
    return same;
  }
  let bad = 0;
  if (a.count !== b.count) { console.log(`event count ${a.count} != baseline ${b.count}`); bad++; }
  for (let i = 0; i < Math.min(a.count, b.count); i++) {
    if (JSON.stringify(a.events[i]) !== JSON.stringify(b.events[i])) {
      bad++;
      const e = a.events[i];
      console.log(`#${i} ${e.ctor}${e.table ? ' ' + e.table : ''} differs`);
    }
  }
  console.log(bad ? `${bad} difference(s) from baseline` : 'identical to baseline');
  return bad === 0;
}
main().then(() => process.exit(baselinePath && !compare(out, baselinePath) ? 1 : 0), (e) => { console.error('FAIL', e.message); fs.writeFileSync(out, JSON.stringify({ error: e.message }, null, 1)); process.exit(0); });
