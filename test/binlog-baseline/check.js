'use strict';
// Checks the decoded events of a harness run against expected.js.
// usage: node check.js <out.raw.json> [--update]
// Run it on a `minimal` run; `full` fails early on every table map.
// Failures listed in expected-failures.json are known defects: they do not fail
// the run, and are reported when they start passing. --update rewrites the list.
const fs = require('fs');
const path = require('path');
const { checks } = require('./expected.js');

const rawPath = process.argv[2];
if (!rawPath) {
  console.error('usage: node check.js <out.raw.json> [--update]');
  process.exit(2);
}
const update = process.argv.includes('--update');
const knownPath = path.join(__dirname, 'expected-failures.json');
const known = new Set(fs.existsSync(knownPath) ? JSON.parse(fs.readFileSync(knownPath, 'utf8')) : []);
const scenarios = JSON.parse(fs.readFileSync(rawPath, 'utf8')).scenarios;

const results = [];
for (const { id, scenario, fn } of checks) {
  const raw = scenarios[scenario];
  if (!raw || raw.skipped) continue;
  let outcome;
  try {
    outcome = fn(raw);
  } catch (e) {
    outcome = { ok: false, detail: `ERROR ${e.message}` };
  }
  results.push({ id, ...outcome });
}

const failing = results.filter((r) => !r.ok);
const regressions = failing.filter((r) => !known.has(r.id));
const fixed = results.filter((r) => r.ok && known.has(r.id));
const stillFailing = failing.filter((r) => known.has(r.id));
const unrun = [...known].filter((id) => !results.some((r) => r.id === id));

console.log(`${results.length} checks: ${results.length - failing.length} pass, ${stillFailing.length} known failures, ${regressions.length} new failures`);
if (process.env.VERBOSE) for (const r of stillFailing) console.log(`  known  ${r.id}: ${r.detail}`);
for (const r of regressions) console.log(`  FAIL   ${r.id}: ${r.detail}`);
for (const r of fixed) console.log(`  FIXED  ${r.id} (remove from expected-failures.json)`);
if (unrun.length) console.log(`  not run (scenario absent): ${unrun.length}`);

if (update) {
  const next = [...new Set([...failing.map((r) => r.id), ...unrun])].sort();
  fs.writeFileSync(knownPath, `${JSON.stringify(next, null, 2)}\n`);
  console.log(`wrote ${next.length} known failures`);
  process.exit(0);
}
process.exit(regressions.length ? 1 : 0);
