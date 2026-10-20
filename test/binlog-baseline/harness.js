'use strict';
process.env.TZ = 'UTC';
// Dumps every binlog event the driver decodes for a set of independent
// scenarios and, with a baseline given, reports what differs from it.
// usage: node harness.js <repoRoot> <out.json> [minimal|full] [baseline.json]
// Needs a MySQL 8.0 server with the binlog enabled in ROW format, started with
// --restart=unless-stopped because the `stop` scenario issues SHUTDOWN.
// BINLOG_PORT (default 3399); BINLOG_SERVER=mariadb runs the MariaDB scenarios (the caller must SET @mariadb_slave_capability=4, as done here); BINLOG_SKIP_STOP=1 skips `stop`; QUIET=1 hides coverage.
const path = require('path');
const fs = require('fs');
const root = path.resolve(process.argv[2]);
const out = process.argv[3];
const baselinePath = process.argv[5];
const mode = (process.argv[4] || 'minimal').toUpperCase();
const { Readable } = require('stream');
const mysql = require(root + '/index.js');
const cfg = { host: '127.0.0.1', port: Number(process.env.BINLOG_PORT || 3399), user: 'root', database: 'test' };

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


const typeSteps = Object.entries(typeCases).flatMap(([name, [type, vals]]) => [
  `CREATE TABLE t_${name} (id INT PRIMARY KEY, v ${type})`,
  `INSERT INTO t_${name} VALUES ${vals.map((v, i) => `(${i + 1}, ${v})`).join(',')}`,
]);

const gtidUp = [
  'SET GLOBAL enforce_gtid_consistency=ON',
  "SET GLOBAL gtid_mode='OFF_PERMISSIVE'",
  "SET GLOBAL gtid_mode='ON_PERMISSIVE'",
  "SET GLOBAL gtid_mode='ON'",
];
const gtidDown = [
  "SET GLOBAL gtid_mode='ON_PERMISSIVE'",
  "SET GLOBAL gtid_mode='OFF_PERMISSIVE'",
  "SET GLOBAL gtid_mode='OFF'",
  'SET GLOBAL enforce_gtid_consistency=OFF',
];

// Each scenario gets a fresh binlog and its own stream, so a decoder crash in
// one cannot hide the others.
const mysqlScenarios = {
  types: [
    `SET time_zone='+00:00'`,
    ...typeSteps,
    `UPDATE t_varchar SET v='changed' WHERE id=1`,
    `DELETE FROM t_int WHERE id=2`,
  ],
  dml: [
    'CREATE TABLE t_small (a INT, b VARCHAR(20))',
    `INSERT INTO t_small VALUES (1,'one'),(2,'two'),(3,'three')`,
    `UPDATE t_small SET b='uno' WHERE a=1`,
    `UPDATE t_small SET b=CONCAT(b,'!')`,
    `DELETE FROM t_small WHERE a=3`,
    'START TRANSACTION',
    `INSERT INTO t_small VALUES (10,'tx')`,
    `INSERT INTO t_small VALUES (11,NULL)`,
    'COMMIT',
    'START TRANSACTION',
    `INSERT INTO t_small VALUES (99,'rolled back')`,
    'ROLLBACK',
    `INSERT INTO t_small SELECT a+100, REPEAT('x',15) FROM t_small`,
    'ALTER TABLE t_small ADD COLUMN c INT DEFAULT 7',
    `INSERT INTO t_small VALUES (200,'after alter',8)`,
    'TRUNCATE TABLE t_small',
  ],
  rows_query: [
    'SET SESSION binlog_rows_query_log_events=ON',
    'CREATE TABLE t_rq (id INT PRIMARY KEY, v INT)',
    'INSERT INTO t_rq VALUES (1,1),(2,2)',
    'UPDATE t_rq SET v=v+1',
    'DELETE FROM t_rq WHERE id=1',
  ],
  xa: [
    'CREATE TABLE t_xa (id INT PRIMARY KEY)',
    "XA START 'x1'",
    'INSERT INTO t_xa VALUES (1)',
    "XA END 'x1'",
    "XA PREPARE 'x1'",
    "XA COMMIT 'x1'",
    "XA START 'x2'",
    'INSERT INTO t_xa VALUES (2)',
    "XA END 'x2'",
    "XA PREPARE 'x2'",
    "XA ROLLBACK 'x2'",
  ],
  partial_json: [
    "SET SESSION binlog_row_value_options='PARTIAL_JSON'",
    'CREATE TABLE t_pj (id INT PRIMARY KEY, doc JSON)',
    `INSERT INTO t_pj VALUES (1, '{"a":1,"b":[1,2,3],"c":{"d":"x"}}')`,
    `UPDATE t_pj SET doc = JSON_SET(doc, '$.a', 2)`,
    `UPDATE t_pj SET doc = JSON_REMOVE(doc, '$.b[1]')`,
    `UPDATE t_pj SET doc = JSON_REPLACE(doc, '$.c.d', 'y')`,
  ],
  compression: [
    'SET SESSION binlog_transaction_compression=ON',
    'CREATE TABLE t_cmp (id INT PRIMARY KEY, v VARCHAR(100))',
    'START TRANSACTION',
    `INSERT INTO t_cmp VALUES (1,'a'),(2,'b')`,
    `UPDATE t_cmp SET v='c' WHERE id=1`,
    'COMMIT',
  ],
  sbr_intvar: [
    "SET SESSION binlog_format='STATEMENT'",
    'CREATE TABLE t_sbr (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(60))',
    `INSERT INTO t_sbr (v) VALUES ('plain')`,
    'INSERT INTO t_sbr (v) VALUES (LAST_INSERT_ID())',
    `INSERT INTO t_sbr (id, v) VALUES (NULL, 'a'), (NULL, 'b')`,
  ],
  sbr_rand: [
    "SET SESSION binlog_format='STATEMENT'",
    'CREATE TABLE t_sbr (id INT AUTO_INCREMENT PRIMARY KEY, d DOUBLE)',
    'INSERT INTO t_sbr (d) VALUES (RAND())',
  ],
  ...Object.fromEntries(
    Object.entries({ string: `'abc'`, int: '42', real: '1.5e0', decimal: '1.25', null: 'NULL' }).map(([kind, value]) => [
      `user_var_${kind}`,
      [
        "SET SESSION binlog_format='STATEMENT'",
        'CREATE TABLE t_uv (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(60))',
        `SET @u=${value}`,
        'INSERT INTO t_uv (v) VALUES (@u)',
      ],
    ])
  ),
  load_data: [
    "SET SESSION binlog_format='STATEMENT'",
    'CREATE TABLE t_load (id INT PRIMARY KEY, v VARCHAR(20))',
    { sql: "LOAD DATA LOCAL INFILE 'rows.tsv' INTO TABLE t_load (id, v)", infile: '1\tone\n2\ttwo\n3\tthree\n' },
  ],
  rotate: [
    'CREATE TABLE t_rot (id INT PRIMARY KEY)',
    'INSERT INTO t_rot VALUES (1)',
    'FLUSH BINARY LOGS',
    'INSERT INTO t_rot VALUES (2)',
    'FLUSH BINARY LOGS',
    'INSERT INTO t_rot VALUES (3)',
  ],
  gtid: {
    before: gtidUp,
    steps: [
      'CREATE TABLE t_gtid (id INT PRIMARY KEY)',
      'INSERT INTO t_gtid VALUES (1)',
      'START TRANSACTION',
      'INSERT INTO t_gtid VALUES (2)',
      'INSERT INTO t_gtid VALUES (3)',
      'COMMIT',
      'FLUSH BINARY LOGS',
      'INSERT INTO t_gtid VALUES (4)',
    ],
    after: gtidDown,
  },
  stop: {
    steps: ['CREATE TABLE t_stop (id INT PRIMARY KEY)', 'INSERT INTO t_stop VALUES (1)'],
    restart: true,
  },
  heartbeat: {
    steps: ['CREATE TABLE t_hb (id INT PRIMARY KEY)', 'INSERT INTO t_hb VALUES (1)'],
    untilHeartbeat: true,
  },
};


// MariaDB marks every logged DDL with a status variable the driver cannot decode
// (0x81, Q_XID), so tables are created unlogged to keep the DML scenarios usable.
const SETTLE = { settle: true };
const unloggedDdl = (...ddl) => ['SET SESSION sql_log_bin=0', ...ddl, 'SET SESSION sql_log_bin=1'];

const mariadbScenarios = {
  maria_dml: {
    steps: [
      ...unloggedDdl('CREATE TABLE t_m (id INT PRIMARY KEY, v VARCHAR(20))'),
      `INSERT INTO t_m VALUES (1,'one'),(2,'two')`,
      `UPDATE t_m SET v='uno' WHERE id=1`,
      'DELETE FROM t_m WHERE id=2',
      'START TRANSACTION',
      `INSERT INTO t_m VALUES (3,'tx')`,
      'COMMIT',
    ],
  },
  maria_types: {
    steps: [
      ...unloggedDdl('CREATE TABLE t_mt (id INT PRIMARY KEY, a TINYINT UNSIGNED, b BIGINT, c VARCHAR(30), d DATE, e DATETIME, f DECIMAL(10,2), g JSON, h BLOB)'),
      `INSERT INTO t_mt VALUES (1, 250, -9007199254740993, 'héllo', '2024-02-29', '2024-02-29 12:34:56', '12.50', '{"k":[1,2]}', 'blob')`,
    ],
  },
  maria_rotate: {
    steps: [
      ...unloggedDdl('CREATE TABLE t_mr (id INT PRIMARY KEY)'),
      'INSERT INTO t_mr VALUES (1)',
      SETTLE,
      'FLUSH BINARY LOGS',
      SETTLE,
      'INSERT INTO t_mr VALUES (2)',
      SETTLE,
      'FLUSH BINARY LOGS',
      SETTLE,
      'INSERT INTO t_mr VALUES (3)',
    ],
  },
  maria_ddl: {
    steps: ['CREATE TABLE t_md (id INT PRIMARY KEY)', 'INSERT INTO t_md VALUES (1)'],
  },
  maria_no_capability: {
    steps: [
      ...unloggedDdl('CREATE TABLE t_mn (id INT PRIMARY KEY)'),
      'INSERT INTO t_mn VALUES (1)',
    ],
    noCapability: true,
  },
};

const isMariaDb = process.env.BINLOG_SERVER === 'mariadb';
const scenarios = isMariaDb ? mariadbScenarios : mysqlScenarios;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function currentBinlogFile() {
  try {
    const c = await mysql.createConnection(cfg).promise();
    try {
      const [[st]] = await c.query('SHOW MASTER STATUS');
      return st.File;
    } finally {
      await c.end();
    }
  } catch (e) {
    return null;
  }
}

// A restarted server always opens a new binlog file; the dying one keeps
// accepting connections for a moment, so connectivity alone proves nothing.
async function restartServer(c) {
  const before = await currentBinlogFile();
  try { await c.query('SHUTDOWN'); } catch (e) { /* connection drops */ }
  try { c.destroy(); } catch (e) { /* already gone */ }
  for (let i = 0; i < 180; i++) {
    const now = await currentBinlogFile();
    if (now && now !== before) return;
    await sleep(500);
  }
  throw new Error('server did not restart');
}

const NON_BLOCK = 0x01;
const SEND_ANNOTATE_ROWS = 0x02;

function dumpFlags(def) {
  if (def.untilHeartbeat) return 0;
  return isMariaDb && !def.noCapability ? NON_BLOCK | SEND_ANNOTATE_ROWS : NON_BLOCK;
}

async function totalLoggedEvents(c) {
  const [logs] = await c.query('SHOW BINARY LOGS');
  let total = 0;
  for (const { Log_name } of logs) {
    const [events] = await c.query(`SHOW BINLOG EVENTS IN '${Log_name}'`);
    total += events.length;
  }
  return total;
}

// MariaDB writes binlog checkpoint events asynchronously, so the stream is only
// deterministic once the logs have stopped growing.
async function waitUntilLogsSettle(c) {
  let last = -1;
  let stable = 0;
  while (stable < 4) {
    const now = await totalLoggedEvents(c);
    stable = now === last ? stable + 1 : 0;
    last = now;
    await sleep(250);
  }
}


function runStep(c, step) {
  if (step === SETTLE) return waitUntilLogsSettle(c);
  if (typeof step === 'string') return c.query(step);
  return c.query({ sql: step.sql, infileStreamFactory: () => Readable.from([Buffer.from(step.infile)]) });
}

async function runScenario(spec) {
  const def = Array.isArray(spec) ? { steps: spec } : spec;
  if (def.restart && process.env.BINLOG_SKIP_STOP) return { skipped: true };

  const admin = mysql.createConnection(cfg).promise();
  if (!isMariaDb) {
    await admin.query(`SET GLOBAL binlog_row_metadata='${mode}'`);
    await admin.query('SET GLOBAL local_infile=1');
    for (const sql of gtidDown) {
      try { await admin.query(sql); } catch (e) { /* already off */ }
    }
  }
  await admin.end();

  const c = mysql.createConnection(cfg).promise();
  await c.query('DROP DATABASE IF EXISTS test');
  await c.query('CREATE DATABASE test');
  await c.query('USE test');
  await c.query('RESET MASTER');
  const [[st]] = await c.query('SHOW MASTER STATUS');
  const uuid = isMariaDb ? null : (await c.query('SELECT @@server_uuid AS uuid'))[0][0].uuid;
  for (const sql of def.before || []) await c.query(sql);
  for (const step of def.steps) await runStep(c, step);
  if (def.restart) {
    await restartServer(c);
  } else {
    if (isMariaDb) await waitUntilLogsSettle(c);
    await c.end();
  }

  const bc = mysql.createConnection({ ...cfg, database: undefined });
  const events = [];
  let error;
  try {
    if (def.untilHeartbeat) await bc.promise().query('SET @master_heartbeat_period=100000000');
    if (isMariaDb && !def.noCapability) await bc.promise().query('SET @mariadb_slave_capability=4');
    const stream = await bc.promise().createBinlogStream({
      serverId: 4242, filename: st.File, binlogPos: 4, flags: dumpFlags(def),
    });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timeout; got ${events.length} events`)), 20000);
      const finish = (fn, arg) => { clearTimeout(timer); fn(arg); };
      stream.on('data', (e) => {
        events.push(e);
        if (def.untilHeartbeat && e.eventType === 0x1b) finish(resolve);
      });
      stream.on('end', () => finish(resolve));
      stream.on('close', () => finish(resolve));
      stream.on('error', (e) => finish(reject, e));
    });
  } catch (e) {
    error = e.message;
  }
  try { bc.destroy(); } catch (e) { /* already gone */ }
  // The server refuses to dump GTID events once gtid_mode is OFF again.
  for (const sql of def.after || []) {
    const t = mysql.createConnection(cfg).promise();
    await t.query(sql);
    await t.end();
  }
  const dump = maskServerUuid(events.map(serialize), uuid);
  return error ? { count: dump.length, events: dump, error } : { count: dump.length, events: dump };
}

// Decoders known to read past the row and return garbage; only their shape is stable.
const UNSTABLE_TABLES = new Set(['t_decimal', 't_decimal_small']);
// The driver returns TIME as a Date on the current day; keep only the offset from midnight.
const TIME_TABLES = new Set(['t_time', 't_time3']);
// Fields that vary between runs or only expose raw wire bytes.
const VOLATILE = new Set(['packet', 'created', 'slaveProxyId', 'xid', 'ddlXid', 'tableId', 'table_id', 'timestamp', 'executionTime', 'tableMap', 'seed1', 'seed2']);

function strip(v) {
  if (Array.isArray(v)) return v.map(strip);
  if (v && typeof v === 'object' && !v.$buf && !v.$bigint && !v.$date && !v.$long && v.$msSinceUtcMidnight === undefined) {
    const o = {};
    for (const k of Object.keys(v)) if (!VOLATILE.has(k)) o[k] = strip(v[k]);
    return o;
  }
  return v;
}

function maskServerUuid(dump, uuid) {
  if (!uuid) return dump;
  const text = JSON.stringify(dump)
    .split(uuid).join('<server_uuid>')
    .split(uuid.replace(/-/g, '')).join('<server_uuid_hex>');
  return JSON.parse(text);
}

function serialize(e) {
  const o = { ctor: e.constructor && e.constructor.name };
  for (const k of Object.keys(e)) o[k] = norm(e[k]);
  // Row events decode lazily through a getter.
  if ('rows' in e) {
    try {
      o.rows = UNSTABLE_TABLES.has(e.table) ? `<${e.rows.length} rows, unstable>` : norm(e.rows, TIME_TABLES.has(e.table));
    } catch (err) {
      o.rowsError = err.message;
    }
  }
  const stripped = strip(o);
  // The compressed payload's size depends on the timestamps inside it.
  if (stripped.eventType === 0x28) {
    delete stripped.eventSize;
    delete stripped.eventHeader.eventSize;
    delete stripped.eventHeader.logPos;
  }
  return stripped;
}

const UNREACHABLE = {
  0x00: 'never sent', 0x01: 'pre-4.0 servers', 0x06: 'pre-5.0 servers', 0x07: 'internal',
  0x08: 'pre-5.0 servers', 0x09: 'pre-5.0 servers', 0x0a: 'pre-5.0 servers', 0x0b: 'pre-5.0 servers', 0x0c: 'pre-5.0 servers',
  0x14: 'pre-5.1 servers', 0x15: 'pre-5.1 servers', 0x16: 'pre-5.1 servers',
  0x17: 'MariaDB (BINLOG_SERVER=mariadb) or MySQL < 5.6.6', 0x18: 'MariaDB (BINLOG_SERVER=mariadb) or MySQL < 5.6.6', 0x19: 'MariaDB (BINLOG_SERVER=mariadb) or MySQL < 5.6.6',
  0x1a: 'needs a server fault (lost events)', 0x1c: 'only newer-server extensions',
  0x29: 'not sent by 8.0', 0x2a: 'MySQL 8.4+ tagged GTIDs', 0x2b: 'sentinel',
  0xa0: 'MariaDB only', 0xa1: 'MariaDB only', 0xa2: 'MariaDB only', 0xa3: 'MariaDB only', 0xa4: 'MariaDB only, needs the encryption plugin and a key file',
};

function printCoverage(result) {
  const T = require(`${root}/lib/constants/binlog_event_types.js`);
  const seen = {};
  for (const [name, sc] of Object.entries(result.scenarios)) {
    for (const e of sc.events || []) {
      const s = (seen[e.eventType] = seen[e.eventType] || { n: 0, by: new Set(), cls: new Set() });
      s.n++; s.by.add(name); s.cls.add(e.ctor);
    }
  }
  const hex = (v) => `0x${v.toString(16).padStart(2, '0')}`;
  const missing = [];
  const MARIADB_CODES = new Set([0x02, 0x04, 0x0e, 0x0f, 0x10, 0x13, 0x17, 0x18, 0x19, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4]);
  const known = Object.entries(T)
    .filter(([k, v]) => typeof v === 'number' && /_EVENT|_LOG/.test(k) && !/^INTVAR_.+_EVENT$/.test(k))
    .filter(([, v]) => !isMariaDb || MARIADB_CODES.has(v))
    .sort((a, b) => a[1] - b[1]);
  for (const [k, v] of known) {
    const s = seen[v];
    if (s) console.log(`${hex(v)} ${k.padEnd(30)} x${String(s.n).padEnd(4)} ${[...s.cls].join(',')}  [${[...s.by].join(',')}]`);
    else missing.push(`${hex(v)} ${k.padEnd(30)} ${UNREACHABLE[v] || 'NOT COVERED'}`);
  }
  console.log('--- not produced:');
  missing.forEach((m) => console.log(m));
}

async function main() {
  const result = { scenarios: {} };
  const only = process.env.ONLY && process.env.ONLY.split(',');
  for (const [name, spec] of Object.entries(scenarios)) {
    if (only && !only.includes(name)) continue;
    result.scenarios[name] = await runScenario(spec);
  }
  fs.writeFileSync(out, JSON.stringify(result, null, 1));
  for (const [name, sc] of Object.entries(result.scenarios)) {
    console.log(name.padEnd(13), sc.skipped ? 'skipped' : `${sc.count} events`, sc.error ? `ERROR: ${sc.error}` : '');
  }
  if (!process.env.QUIET) printCoverage(result);
}

function compare(actualPath, baseline) {
  const a = JSON.parse(fs.readFileSync(actualPath, 'utf8')).scenarios;
  const b = JSON.parse(fs.readFileSync(baseline, 'utf8')).scenarios;
  let bad = 0;
  for (const name of new Set([...Object.keys(a), ...Object.keys(b)])) {
    const x = a[name];
    const y = b[name];
    if (!x || !y) { console.log(`${name}: missing in ${x ? 'baseline' : 'actual'}`); bad++; continue; }
    if (x.error !== y.error) { console.log(`${name}: error now=${x.error} baseline=${y.error}`); bad++; }
    if (x.count !== y.count) { console.log(`${name}: event count ${x.count} != baseline ${y.count}`); bad++; }
    for (let i = 0; i < Math.min(x.count || 0, y.count || 0); i++) {
      if (JSON.stringify(x.events[i]) !== JSON.stringify(y.events[i])) {
        bad++;
        const e = x.events[i];
        console.log(`${name} #${i} ${e.ctor}${e.table ? ` ${e.table}` : ''} differs`);
      }
    }
  }
  console.log(bad ? `${bad} difference(s) from baseline` : 'identical to baseline');
  return bad === 0;
}

main().then(
  () => process.exit(baselinePath && !compare(out, baselinePath) ? 1 : 0),
  (e) => { console.error('FAIL', e.stack || e); process.exit(1); }
);
