'use strict';
// What the driver should decode for each harness scenario, written from the SQL
// the scenarios run, independent of what the driver currently does.
//
// Cell specs say which JS representations are acceptable. They follow what a
// regular query returns, except that nothing may lose information: a value that
// does not fit a double exactly must not come back as a plain number.

const spec = (kind, v, desc, extra) => ({ kind, v, desc, ...extra });
const nul = spec('null', null, 'null');
const num = (v) => spec('num', v, `number ${v}`);
const float = (v) => spec('num', v, `number ${v}`, { float: true });
const text = (v) => spec('text', v, JSON.stringify(v));
const bytes = (s) =>
  spec(
    'bytes',
    Buffer.from(s, 'latin1').toString('hex'),
    `bytes ${JSON.stringify(s)}`
  );
const hexBytes = (hex) => spec('bytes', hex, `bytes 0x${hex}`);
const decimal = (v) => spec('decimal', v, `decimal ${v}`);
const date = (v) => spec('dt', [v, `${v} 00:00:00`], `date ${v}`);
const dt = (...alts) => spec('dt', alts, `datetime ${alts[0]}`);
const time = (v) => spec('time', v, `time ${v}`);
const json = (v) => spec('json', v, `json ${JSON.stringify(v)}`);
const bits = (n) => spec('bit', String(n), `bit ${n}`);
const setOf = (v) => spec('set', v, `set ${JSON.stringify(v)}`);

const toHex = (a) => {
  if (a && typeof a === 'object' && a.$buf !== undefined) return a.$buf;
  if (typeof a === 'string') return Buffer.from(a, 'utf8').toString('hex');
  return undefined;
};

const dateForms = (iso) => {
  const full = iso.replace('T', ' ').replace('Z', '');
  const forms = [full, full.replace(/\.000$/, '')];
  if (/ 00:00:00\.000$/.test(full)) forms.push(full.slice(0, 10));
  return forms;
};

const deepEqual = (a, b) =>
  JSON.stringify(sortKeys(a)) === JSON.stringify(sortKeys(b));
const sortKeys = (v) => {
  if (Array.isArray(v)) return v.map(sortKeys);
  if (v && typeof v === 'object')
    return Object.fromEntries(
      Object.keys(v)
        .sort()
        .map((k) => [k, sortKeys(v[k])])
    );
  return v;
};

function matches(s, a) {
  switch (s.kind) {
    case 'null':
      return a === null;
    case 'num': {
      if (a === null || a === undefined || typeof a === 'boolean') return false;
      const str = typeof a === 'object' ? a.$bigint : String(a);
      if (str === undefined) return false;
      if (s.float) return Number(str) === Number(s.v);
      return /^-?\d+$/.test(str) && BigInt(str) === BigInt(s.v);
    }
    case 'text':
      return a === s.v;
    case 'set':
      return a === s.v || (Array.isArray(a) && a.join(',') === s.v);
    case 'bytes':
      return toHex(a) === s.v;
    case 'decimal':
      return a === s.v || (typeof a === 'number' && a === Number(s.v));
    case 'dt':
      if (typeof a === 'string') return s.v.includes(a);
      return (
        !!(a && a.$date) && dateForms(a.$date).some((f) => s.v.includes(f))
      );
    case 'time':
      return a === s.v;
    case 'json':
      try {
        return deepEqual(typeof a === 'string' ? JSON.parse(a) : a, s.v);
      } catch {
        return false;
      }
    case 'bit': {
      if (typeof a === 'number') return BigInt(a) === BigInt(s.v);
      if (a && a.$bigint !== undefined)
        return BigInt(a.$bigint) === BigInt(s.v);
      if (a && a.$buf !== undefined)
        return BigInt(`0x${a.$buf || '0'}`) === BigInt(s.v);
      return false;
    }
    default:
      throw new Error(`unknown spec ${s.kind}`);
  }
}

const show = (a) =>
  JSON.stringify(a, (k, v) => (typeof v === 'bigint' ? `${v}n` : v));

// Event selection works on the type code and table name, never the class name,
// so the checks survive renamed or replaced event classes.
const ROWS = {
  write: [0x17, 0x1e],
  update: [0x18, 0x1f, 0x27],
  delete: [0x19, 0x20],
};
const byType = (raw, ...codes) =>
  raw.events.filter((e) => codes.includes(e.eventType));
const present = (v) => Object.values(v);

function rowEvents(raw, kind, table) {
  const evs = byType(raw, ...ROWS[kind]).filter((e) => e.table === table);
  if (!evs.length)
    throw new Error(
      `no ${kind} rows event for ${table}${raw.error ? ` (stream failed: ${raw.error})` : ''}`
    );
  return evs;
}

function rowsOf(e) {
  if (e.rowsError) throw new Error(e.rowsError);
  if (!Array.isArray(e.rows)) throw new Error('rows not decoded');
  return e.rows;
}

const writeBatches = (raw, table) =>
  rowEvents(raw, 'write', table).map((e) =>
    rowsOf(e).map((r) => present(r.values))
  );
const deleteBatches = (raw, table) =>
  rowEvents(raw, 'delete', table).map((e) =>
    rowsOf(e).map((r) => present(r.values))
  );
const updateBatches = (raw, table) =>
  rowEvents(raw, 'update', table).map((e) =>
    rowsOf(e).map((r) => [present(r.before_values), present(r.after_values)])
  );

const fail = (detail) => ({ ok: false, detail });
const pass = { ok: true };

function compareCells(path, specs, cells) {
  if (cells.length !== specs.length)
    return `${path}: expected ${specs.length} columns, got ${cells.length}`;
  for (let i = 0; i < specs.length; i++) {
    const s =
      typeof specs[i] === 'object' && specs[i] !== null && specs[i].kind
        ? specs[i]
        : specs[i] === null
          ? nul
          : num(String(specs[i]));
    if (!matches(s, cells[i]))
      return `${path} col ${i}: expected ${s.desc}, got ${show(cells[i])}`;
  }
  return null;
}

// `expected` is an array of events, each an array of rows, each an array of cell specs.
function expectBatches(actual, expected) {
  if (actual.length !== expected.length)
    return fail(`expected ${expected.length} row events, got ${actual.length}`);
  for (let b = 0; b < expected.length; b++) {
    if (actual[b].length !== expected[b].length)
      return fail(
        `event ${b}: expected ${expected[b].length} rows, got ${actual[b].length}`
      );
    for (let r = 0; r < expected[b].length; r++) {
      const err = compareCells(
        `event ${b} row ${r}`,
        expected[b][r],
        actual[b][r]
      );
      if (err) return fail(err);
    }
  }
  return pass;
}

function expectUpdates(actual, expected) {
  const flat = (batches) =>
    batches.map((b) => b.flatMap(([before, after]) => [before, after]));
  const exp = expected.map((b) =>
    b.flatMap(([before, after]) => [before, after])
  );
  return expectBatches(flat(actual), exp);
}

function expectEqual(actual, expected, what) {
  return deepEqual(actual, expected)
    ? pass
    : fail(`${what}: expected ${show(expected)}, got ${show(actual)}`);
}

// Strings stored under any property, for events whose statement field has no fixed name.
const stringsOf = (e) =>
  Object.values(e).flatMap((v) => {
    if (typeof v === 'string') return [v];
    if (v && typeof v === 'object' && typeof v.$buf === 'string')
      return [Buffer.from(v.$buf, 'hex').toString('utf8')];
    return [];
  });

const queries = (raw) => byType(raw, 0x02).map((e) => e.query);

const checks = [];
const check = (id, scenario, fn) => checks.push({ id, scenario, fn });

// ---------------------------------------------------------------- types
const X400 = 'é'.repeat(400);
const typeExpectations = {
  tinyint: { code: 1, rows: [num('-5'), num('127')] },
  utinyint: { code: 1, unsigned: true, rows: [num('250'), num('0')] },
  smallint: { code: 2, rows: [num('-300'), num('32767')] },
  mediumint: { code: 9, rows: [num('-70000'), num('8388607')] },
  int: { code: 3, rows: [num('-2000000000'), num('2147483647')] },
  bigint: {
    code: 8,
    rows: [num('-9007199254740993'), num('9223372036854775807')],
  },
  ubigint: {
    code: 8,
    unsigned: true,
    rows: [num('18446744073709551615'), num('0')],
  },
  float: { code: 4, rows: [float('1.5'), float('-0.25')] },
  double: { code: 5, rows: [float('2.25'), float('-1e100')] },
  decimal: {
    code: 246,
    rows: [decimal('123456789012345.12345'), decimal('-0.00001')],
  },
  decimal_small: { code: 246, rows: [decimal('-12.50'), decimal('0.00')] },
  char: { code: 254, rows: [text('abc'), text('')] },
  varchar: { code: 15, rows: [text('héllo wörld'), text('')] },
  varchar_long: { code: 15, rows: [text(X400), text('x')] },
  text: { code: 252, rows: [text('long text'), text('')] },
  blob: { code: 252, rows: [bytes('blobdata'), bytes('')] },
  binary: { code: 254, rows: [hexBytes('61620000'), hexBytes('61626364')] },
  varbinary: { code: 15, rows: [bytes('vb'), bytes('')] },
  date: { code: 10, rows: [date('2024-02-29'), date('1000-01-01')] },
  time: { code: 19, rows: [time('-12:34:56'), time('838:59:59')] },
  time3: { code: 19, rows: [time('01:02:03.456'), time('00:00:00.000')] },
  datetime: {
    code: 18,
    rows: [dt('2024-02-29 12:34:56'), dt('1000-01-01 00:00:00')],
  },
  datetime6: {
    code: 18,
    rows: [
      dt('2024-02-29 12:34:56.123', '2024-02-29 12:34:56.123456'),
      dt('1000-01-01 00:00:00', '1000-01-01 00:00:00.000000'),
    ],
  },
  timestamp: {
    code: 17,
    rows: [dt('2024-02-29 12:34:56'), dt('1970-01-01 00:00:01')],
  },
  timestamp3: {
    code: 17,
    rows: [dt('2024-02-29 12:34:56.789'), dt('1970-01-01 00:00:01.000')],
  },
  year: { code: 13, rows: [num('2024'), num('1901')] },
  enum: { rows: [text('b'), text('c')] },
  set: { rows: [setOf('x,z'), setOf('')] },
  bit: { code: 16, rows: [bits(2730), bits(0)] },
  json: {
    code: 245,
    rows: [json({ k: [1, 2, { n: null }], s: 'v' }), json([])],
  },
};

for (const [name, e] of Object.entries(typeExpectations)) {
  const table = `t_${name}`;
  check(`types.${name}.values`, 'types', (raw) =>
    expectBatches(writeBatches(raw, table), [
      [
        [1, e.rows[0]],
        [2, e.rows[1]],
        [3, nul],
      ],
    ])
  );
  if (e.code !== undefined) {
    check(`types.${name}.column`, 'types', (raw) => {
      const map = byType(raw, 0x13).find((m) => m.table === table);
      if (!map) throw new Error(`no table map for ${table}`);
      const col = map.columns[1];
      if (col.type !== e.code)
        return fail(`expected column type ${e.code}, got ${col.type}`);
      if (Boolean(col.unsigned) !== Boolean(e.unsigned))
        return fail(
          `expected unsigned=${Boolean(e.unsigned)}, got ${col.unsigned}`
        );
      return pass;
    });
  }
}
check('types.update', 'types', (raw) =>
  expectUpdates(updateBatches(raw, 't_varchar'), [
    [
      [
        [1, text('héllo wörld')],
        [1, text('changed')],
      ],
    ],
  ])
);
check('types.delete', 'types', (raw) =>
  expectBatches(deleteBatches(raw, 't_int'), [[[2, num('2147483647')]]])
);

// ---------------------------------------------------------------- dml
const X15 = 'x'.repeat(15);
check('dml.inserts', 'dml', (raw) =>
  expectBatches(writeBatches(raw, 't_small'), [
    [
      [1, text('one')],
      [2, text('two')],
      [3, text('three')],
    ],
    [[10, text('tx')]],
    [[11, nul]],
    [
      [101, text(X15)],
      [102, text(X15)],
      [110, text(X15)],
      [111, text(X15)],
    ],
    [[200, text('after alter'), 8]],
  ])
);
check('dml.updates', 'dml', (raw) =>
  expectUpdates(updateBatches(raw, 't_small'), [
    [
      [
        [1, text('one')],
        [1, text('uno')],
      ],
    ],
    [
      [
        [1, text('uno')],
        [1, text('uno!')],
      ],
      [
        [2, text('two')],
        [2, text('two!')],
      ],
      [
        [3, text('three')],
        [3, text('three!')],
      ],
    ],
  ])
);
check('dml.deletes', 'dml', (raw) =>
  expectBatches(deleteBatches(raw, 't_small'), [[[3, text('three!')]]])
);
check('dml.statements', 'dml', (raw) => {
  const q = queries(raw);
  const want = [
    'CREATE TABLE t_small (a INT, b VARCHAR(20))',
    'ALTER TABLE t_small ADD COLUMN c INT DEFAULT 7',
    'TRUNCATE TABLE t_small',
  ];
  const missing = want.filter((w) => !q.includes(w));
  if (missing.length)
    return fail(`missing statements ${show(missing)} in ${show(q)}`);
  if (q.some((s) => /ROLLBACK/i.test(s)))
    return fail('rolled-back transaction was logged');
  return pass;
});
check('dml.commits', 'dml', (raw) =>
  expectEqual(byType(raw, 0x10).length, 7, 'XID events')
);
check('dml.table_maps', 'dml', (raw) => {
  const maps = byType(raw, 0x13).filter((m) => m.table === 't_small');
  const counts = maps.map((m) => m.columnCount);
  const e = expectEqual(counts, [2, 2, 2, 2, 2, 2, 2, 3], 'column counts');
  if (!e.ok) return e;
  return expectEqual(
    maps[0].columns.map((c) => c.type),
    [3, 15],
    'column types'
  );
});

// ---------------------------------------------------------------- statements and misc events
check('format_description', 'dml', (raw) => {
  const f = byType(raw, 0x0f)[0];
  if (!f) throw new Error('no FORMAT_DESCRIPTION event');
  if (f.binlogVersion !== 4) return fail(`binlogVersion ${f.binlogVersion}`);
  if (f.mysqlVersionStr === undefined || !/^8\.0\./.test(f.mysqlVersionStr))
    return fail(`mysqlVersionStr ${f.mysqlVersionStr}`);
  return pass;
});
check('anonymous_gtid.decoded', 'dml', (raw) => {
  const bad = byType(raw, 0x22).filter(
    (e) => e.ctor === 'NotImplementedEvent'
  ).length;
  return bad ? fail(`${bad} ANONYMOUS_GTID events not decoded`) : pass;
});

check('rows_query.rows', 'rows_query', (raw) => {
  const w = expectBatches(writeBatches(raw, 't_rq'), [
    [
      [1, num('1')],
      [2, num('2')],
    ],
  ]);
  if (!w.ok) return w;
  const u = expectUpdates(updateBatches(raw, 't_rq'), [
    [
      [
        [1, 1],
        [1, 2],
      ],
      [
        [2, 2],
        [2, 3],
      ],
    ],
  ]);
  if (!u.ok) return u;
  return expectBatches(deleteBatches(raw, 't_rq'), [[[1, num('2')]]]);
});
check('rows_query.text', 'rows_query', (raw) => {
  const got = byType(raw, 0x1d).map((e) =>
    stringsOf(e).find((s) => /^(INSERT|UPDATE|DELETE)/.test(s))
  );
  return expectEqual(
    got,
    [
      'INSERT INTO t_rq VALUES (1,1),(2,2)',
      'UPDATE t_rq SET v=v+1',
      'DELETE FROM t_rq WHERE id=1',
    ],
    'statements'
  );
});

check('xa.prepare', 'xa', (raw) => {
  const p = byType(raw, 0x26);
  if (p.length !== 2)
    return fail(`expected 2 XA_PREPARE events, got ${p.length}`);
  const got = p.map((e) => [
    e.onePhase,
    e.xidFormatId,
    toHex(e.xidGtrid),
    toHex(e.xidBqual),
  ]);
  return expectEqual(
    got,
    [
      [false, 1, '7831', ''],
      [false, 1, '7832', ''],
    ],
    'xa ids'
  );
});
check('xa.statements', 'xa', (raw) =>
  expectEqual(
    queries(raw),
    [
      'CREATE TABLE t_xa (id INT PRIMARY KEY)',
      "XA START X'7831',X'',1",
      "XA END X'7831',X'',1",
      "XA COMMIT X'7831',X'',1",
      "XA START X'7832',X'',1",
      "XA END X'7832',X'',1",
      "XA ROLLBACK X'7832',X'',1",
    ],
    'statements'
  )
);
check('xa.rows', 'xa', (raw) =>
  expectBatches(writeBatches(raw, 't_xa'), [[[1]], [[2]]])
);

check('partial_json.insert', 'partial_json', (raw) =>
  expectBatches(writeBatches(raw, 't_pj'), [
    [[1, json({ a: 1, b: [1, 2, 3], c: { d: 'x' } })]],
  ])
);
check('partial_json.updates_decode', 'partial_json', (raw) => {
  const evs = byType(raw, 0x27, 0x1f).filter((e) => e.table === 't_pj');
  if (evs.length !== 3)
    return fail(`expected 3 update events, got ${evs.length}`);
  for (const e of evs) rowsOf(e);
  return pass;
});

check('compression.inner_rows', 'compression', (raw) => {
  const w = expectBatches(writeBatches(raw, 't_cmp'), [
    [
      [1, text('a')],
      [2, text('b')],
    ],
  ]);
  if (!w.ok) return w;
  return expectUpdates(updateBatches(raw, 't_cmp'), [
    [
      [
        [1, text('a')],
        [1, text('c')],
      ],
    ],
  ]);
});

const intvars = (raw) => byType(raw, 0x05).map((e) => [e.type, e.value]);
check('sbr_intvar.intvar', 'sbr_intvar', (raw) =>
  expectEqual(
    intvars(raw),
    [
      [2, 1],
      [1, 1],
      [2, 2],
      [2, 3],
    ],
    '[type, value]'
  )
);
check('sbr_intvar.statements', 'sbr_intvar', (raw) =>
  expectEqual(
    queries(raw),
    [
      'CREATE TABLE t_sbr (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(60))',
      'BEGIN',
      "INSERT INTO t_sbr (v) VALUES ('plain')",
      'BEGIN',
      'INSERT INTO t_sbr (v) VALUES (LAST_INSERT_ID())',
      'BEGIN',
      "INSERT INTO t_sbr (id, v) VALUES (NULL, 'a'), (NULL, 'b')",
    ],
    'statements'
  )
);
check('sbr_rand.rand', 'sbr_rand', (raw) => {
  const r = byType(raw, 0x0d);
  if (r.length !== 1) return fail(`expected 1 RAND event, got ${r.length}`);
  return Number.isInteger(r[0].seed1) && Number.isInteger(r[0].seed2)
    ? pass
    : fail('seeds not integers');
});

const userVars = {
  string: [0, text('abc')],
  int: [2, num('42')],
  real: [1, float('1.5')],
  decimal: [4, decimal('1.25')],
};
for (const [kind, [type, value]] of Object.entries(userVars)) {
  check(`user_var.${kind}`, `user_var_${kind}`, (raw) => {
    const [e] = byType(raw, 0x0e);
    if (!e)
      throw new Error(
        `no USER_VAR event${raw.error ? ` (stream failed: ${raw.error})` : ''}`
      );
    if (e.name !== 'u') return fail(`name ${show(e.name)}`);
    if (e.isNull) return fail('isNull set');
    if (e.type !== type) return fail(`type ${e.type}, expected ${type}`);
    return matches(value, e.value)
      ? pass
      : fail(`expected ${value.desc}, got ${show(e.value)}`);
  });
}
check('user_var.null', 'user_var_null', (raw) => {
  const [e] = byType(raw, 0x0e);
  if (!e) throw new Error('no USER_VAR event');
  return e.name === 'u' && e.isNull && e.value === null
    ? pass
    : fail(`got ${show([e.name, e.isNull, e.value])}`);
});

check('load_data.begin', 'load_data', (raw) => {
  const [e] = byType(raw, 0x11);
  if (!e) throw new Error('no BEGIN_LOAD_QUERY event');
  return expectEqual(
    toHex(e.blockData),
    Buffer.from('1\tone\n2\ttwo\n3\tthree\n').toString('hex'),
    'block data'
  );
});
check('load_data.execute', 'load_data', (raw) => {
  const [e] = byType(raw, 0x12);
  if (!e) throw new Error('no EXECUTE_LOAD_QUERY event');
  const s = stringsOf(e).find((v) => /LOAD DATA/.test(v));
  return s && /t_load/.test(s)
    ? pass
    : fail(`no LOAD DATA statement on ${e.ctor}`);
});

check('rotate.sequence', 'rotate', (raw) =>
  expectEqual(
    byType(raw, 0x04).map((e) => [e.nextBinlog, e.position]),
    [
      ['mysql-bin.000001', 4],
      ['mysql-bin.000002', 4],
      ['mysql-bin.000002', 4],
      ['mysql-bin.000003', 4],
      ['mysql-bin.000003', 4],
    ],
    '[nextBinlog, position]'
  )
);
check('rotate.rows', 'rotate', (raw) =>
  expectBatches(writeBatches(raw, 't_rot'), [[[1]], [[2]], [[3]]])
);

check('gtid.sequence', 'gtid', (raw) => {
  const g = byType(raw, 0x21);
  const e = expectEqual(
    g.map((x) => x.gno),
    [1, 2, 3, 4],
    'gno'
  );
  if (!e.ok) return e;
  const sids = g.map((x) => toHex(x.sid));
  return sids.every((s) => s === '<server_uuid_hex>')
    ? pass
    : fail(`sid ${show(sids)}`);
});
check('gtid.previous', 'gtid', (raw) => {
  const p = byType(raw, 0x23).filter((e) => e.gtids && e.gtids.length);
  if (!p.length) throw new Error('no non-empty PREVIOUS_GTIDS event');
  return expectEqual(p[0].gtids, ['<server_uuid>:1-4'], 'gtids');
});

check('stop.event', 'stop', (raw) => {
  const events = raw.events;
  const stop = events.findIndex((e) => e.eventType === 0x03);
  if (stop < 0) throw new Error('no STOP event');
  return events.slice(stop + 1).some((e) => e.eventType === 0x0f)
    ? pass
    : fail('no FORMAT_DESCRIPTION after STOP');
});
check('heartbeat.event', 'heartbeat', (raw) => {
  const h = byType(raw, 0x1b);
  if (h.length !== 1) throw new Error('no HEARTBEAT event');
  return expectEqual(h[0].ident, 'mysql-bin.000001', 'ident');
});

for (const [code, name] of [
  [0x1d, 'ROWS_QUERY'],
  [0x28, 'TRANSACTION_PAYLOAD'],
  [0x12, 'EXECUTE_LOAD_QUERY'],
]) {
  const scenario = {
    0x1d: 'rows_query',
    0x28: 'compression',
    0x12: 'load_data',
  }[code];
  check(`decoded.${name}`, scenario, (raw) => {
    const evs = byType(raw, code);
    if (!evs.length) throw new Error(`no ${name} event`);
    return evs.every((e) => e.ctor !== 'NotImplementedEvent')
      ? pass
      : fail(`${name} is not decoded`);
  });
}

// ---------------------------------------------------------------- MariaDB
check('maria.dml.rows', 'maria_dml', (raw) => {
  const w = expectBatches(writeBatches(raw, 't_m'), [
    [
      [1, text('one')],
      [2, text('two')],
    ],
    [[3, text('tx')]],
  ]);
  if (!w.ok) return w;
  const u = expectUpdates(updateBatches(raw, 't_m'), [
    [
      [
        [1, text('one')],
        [1, text('uno')],
      ],
    ],
  ]);
  if (!u.ok) return u;
  return expectBatches(deleteBatches(raw, 't_m'), [[[2, text('two')]]]);
});
check('maria.dml.annotate', 'maria_dml', (raw) =>
  expectEqual(
    byType(raw, 0xa0).map((e) =>
      stringsOf(e).find((s) => /^(INSERT|UPDATE|DELETE)/.test(s))
    ),
    [
      "INSERT INTO t_m VALUES (1,'one'),(2,'two')",
      "UPDATE t_m SET v='uno' WHERE id=1",
      'DELETE FROM t_m WHERE id=2',
      "INSERT INTO t_m VALUES (3,'tx')",
    ],
    'annotated statements'
  )
);
check('maria.dml.gtid', 'maria_dml', (raw) =>
  expectEqual(
    byType(raw, 0xa2).map((e) => [e.domainId, e.serverId, e.gtidSeqNo, e.gtid]),
    [
      [0, 1, 1, '0-1-1'],
      [0, 1, 2, '0-1-2'],
      [0, 1, 3, '0-1-3'],
      [0, 1, 4, '0-1-4'],
    ],
    '[domain, server, seq, gtid]'
  )
);
check('maria.dml.checkpoint', 'maria_dml', (raw) =>
  expectEqual(
    byType(raw, 0xa1).map((e) => e.filename),
    ['mariadb-bin.000001'],
    'checkpoint file'
  )
);
check('maria.dml.gtid_list', 'maria_dml', (raw) =>
  expectEqual(
    byType(raw, 0xa3).map((e) => e.gtidList),
    [[]],
    'gtid list'
  )
);
check('maria.types.row', 'maria_types', (raw) =>
  expectBatches(writeBatches(raw, 't_mt'), [
    [
      [
        1,
        num('250'),
        num('-9007199254740993'),
        text('héllo'),
        date('2024-02-29'),
        dt('2024-02-29 12:34:56'),
        decimal('12.50'),
        json({ k: [1, 2] }),
        bytes('blob'),
      ],
    ],
  ])
);
check('maria.ddl.decodes', 'maria_ddl', (raw) => {
  if (raw.error) throw new Error(raw.error);
  return expectEqual(
    queries(raw).filter((q) => /^CREATE/.test(q)),
    ['CREATE TABLE t_md (id INT PRIMARY KEY)'],
    'statements'
  );
});
check('maria.rotate', 'maria_rotate', (raw) =>
  expectEqual(
    byType(raw, 0x04).map((e) => e.nextBinlog),
    [
      'mariadb-bin.000001',
      'mariadb-bin.000002',
      'mariadb-bin.000002',
      'mariadb-bin.000003',
      'mariadb-bin.000003',
    ],
    'nextBinlog'
  )
);
check('maria.rotate.rows', 'maria_rotate', (raw) =>
  expectBatches(writeBatches(raw, 't_mr'), [[[1]], [[2]], [[3]]])
);

module.exports = { checks, typeExpectations, matches };
