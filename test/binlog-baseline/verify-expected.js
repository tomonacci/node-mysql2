'use strict';
// Confirms the column-type expectations in expected.js against what a regular
// text-protocol query returns for the same rows, so the expectations are not
// just the author's belief. Needs the MySQL server from the README (BINLOG_PORT).
process.env.TZ = 'UTC';
const path = require('path');
const root = path.resolve(process.argv[2] || '.');
const mysql = require(`${root}/index.js`);
const typeCases = require('./type-cases.js');
const { typeExpectations, matches } = require('./expected.js');

const norm = (v) => {
  if (typeof v === 'bigint') return { $bigint: v.toString() };
  if (Buffer.isBuffer(v)) return { $buf: v.toString('hex') };
  if (v instanceof Date) return { $date: v.toISOString() };
  return v;
};

(async () => {
  const c = mysql.createConnection({
    host: '127.0.0.1', port: Number(process.env.BINLOG_PORT || 3399), user: 'root',
    supportBigNumbers: true, bigNumberStrings: true,
  }).promise();
  await c.query('DROP DATABASE IF EXISTS verify_expected');
  await c.query('CREATE DATABASE verify_expected');
  await c.query('USE verify_expected');
  await c.query("SET time_zone='+00:00'");
  let bad = 0;
  for (const [name, [type, vals]] of Object.entries(typeCases)) {
    await c.query(`CREATE TABLE t_${name} (id INT PRIMARY KEY, v ${type})`);
    await c.query(`INSERT INTO t_${name} VALUES ${vals.map((v, i) => `(${i + 1}, ${v})`).join(',')}`);
    const [rows] = await c.query(`SELECT v FROM t_${name} ORDER BY id`);
    const specs = typeExpectations[name].rows;
    specs.forEach((s, i) => {
      if (!matches(s, norm(rows[i].v))) {
        bad++;
        console.log(`t_${name} row ${i + 1}: expectation ${s.desc} rejects the server's ${JSON.stringify(norm(rows[i].v))}`);
      }
    });
  }
  await c.query('DROP DATABASE verify_expected');
  await c.end();
  console.log(bad ? `${bad} expectation(s) disagree with the server` : `all ${Object.keys(typeCases).length} type expectations agree with the server`);
  process.exit(bad ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
