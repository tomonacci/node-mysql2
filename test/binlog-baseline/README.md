# Binlog regression baseline

Two independent layers:

- **Baseline** (`baseline-*.json`): what the driver decodes today, so a merge or rebase can be checked for behavior changes. A difference means "changed", not "wrong".
- **Expected values** (`expected.js`, `check.js`): what the driver _should_ decode, written from the SQL each scenario runs. Known defects are listed in `expected-failures.json`.

Each scenario gets a fresh binlog and its own stream, so a decoder crash in one is recorded for that scenario and does not hide the others. The events collected before a crash are kept.

## Running

Node >= 22.2 is needed (`zlib.crc32`), and a Node with `zlib.zstdDecompressSync` (22.15+, 23.8+) for the `compression` scenario.

```sh
docker run -d --name binlog-mysql --restart=unless-stopped -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -e MYSQL_DATABASE=test \
  -p 127.0.0.1:3399:3306 mysql:8.0 --server-id=1 --log-bin=mysql-bin \
  --binlog-format=ROW --binlog-row-image=FULL --binlog-checksum=CRC32 \
  --default-authentication-plugin=mysql_native_password

docker run -d --name binlog-mariadb --restart=unless-stopped -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 -e MARIADB_DATABASE=test \
  -p 127.0.0.1:3398:3306 mariadb:lts --server-id=1 --log-bin=mariadb-bin \
  --binlog-format=ROW --binlog-annotate-row-events=ON --binlog-checksum=CRC32

H=test/binlog-baseline
node $H/harness.js . /tmp/out.json minimal $H/baseline-mysql-minimal.json
node $H/harness.js . /tmp/out.json full    $H/baseline-mysql-full.json
BINLOG_SERVER=mariadb BINLOG_PORT=3398 node $H/harness.js . /tmp/out.json minimal $H/baseline-mariadb.json
```

```sh
# Correctness
node $H/harness.js . /tmp/out.json minimal            # also writes /tmp/out.raw.json
node $H/check.js /tmp/out.raw.json                     # MySQL checks
BINLOG_SERVER=mariadb BINLOG_PORT=3398 node $H/harness.js . /tmp/maria.json minimal
node $H/check.js /tmp/maria.raw.json                   # MariaDB checks
node $H/verify-expected.js .                           # sanity-check the expectations against a plain SELECT
```

`--restart=unless-stopped` is required: the `stop` scenario issues `SHUTDOWN` to make the server write a `STOP_EVENT`. `BINLOG_SKIP_STOP=1` skips it. `ONLY=a,b` runs selected scenarios, `QUIET=1` hides the coverage report.

The harness drops and recreates the `test` database, resets the binlog, and flips global server settings (`binlog_row_metadata`, `gtid_mode`, `local_infile`). Use a throwaway server. The `mysql:8.0` image pinned in the baseline reports `8.0.x`; other versions can legitimately differ.

## Expected values

`check.js` reads the harness's `*.raw.json` (the baseline without the volatile fields stripped) and evaluates the checks in `expected.js`: row values per column type with the table-map column type and signedness, update before/after images, deletes, statements, and the key fields of each other event type.

- Exit code 1 means a check failed that is not in `expected-failures.json`. Known failures are reported with `VERBOSE=1`. A known failure that starts passing is reported as `FIXED`; run `node check.js <raw> --update` to drop it from the list (run it once per server, since the list is merged). The list describes `minimal` runs; a `full` run passes every check.
- Cell specs say which JS representations are acceptable, following what a regular query returns, but nothing may lose information: a `BIGINT` that does not fit a double must not come back as a plain number, and `BIT` must be a number or Buffer, not a bit string.
- Events are selected by type code and table name, never by class name, so renamed or replaced event classes do not matter.
- `verify-expected.js` runs every column-type case through a regular `SELECT` and requires each expectation to accept the result. The non-column checks (statements, sequences, GTIDs) are written from the SQL and not verifiable that way; they agree with what the server logs.

Not covered by expected values: `Xid`, timestamps, thread ids and `executionTime` (stripped as volatile), and column names, which only `full` metadata carries.

## Event coverage

Every code in `lib/constants/binlog_event_types.js` that a current server can send is produced by a scenario. The coverage report at the end of a run lists the rest with the reason.

| Event                                                                                  | Scenario                                 |
| -------------------------------------------------------------------------------------- | ---------------------------------------- |
| STOP                                                                                   | `stop`                                   |
| ROTATE (real, not only the initial fake one)                                           | `rotate`                                 |
| INTVAR                                                                                 | `sbr_intvar`                             |
| RAND                                                                                   | `sbr_rand`                               |
| USER_VAR                                                                               | `user_var_*` (one per result type)       |
| BEGIN_LOAD_QUERY, EXECUTE_LOAD_QUERY                                                   | `load_data`                              |
| HEARTBEAT                                                                              | `heartbeat` (blocking dump, 0.1s period) |
| ROWS_QUERY                                                                             | `rows_query`                             |
| GTID, PREVIOUS_GTIDS (non-empty)                                                       | `gtid`                                   |
| XA_PREPARE                                                                             | `xa`                                     |
| PARTIAL_UPDATE_ROWS                                                                    | `partial_json`                           |
| TRANSACTION_PAYLOAD                                                                    | `compression`                            |
| WRITE/UPDATE/DELETE_ROWS_V1, ANNOTATE_ROWS, BINLOG_CHECKPOINT, MARIADB_GTID, GTID_LIST | `maria_*`                                |

Not produced: events only pre-5.x servers sent, `INCIDENT` (needs a server fault), `IGNORABLE`, `HEARTBEAT_V2`, `GTID_TAGGED` (MySQL 8.4+), and `MARIADB_START_ENCRYPTION` (needs the encryption plugin and a key file).

## Known limitations

- `ENUM`, `SET`: the member names only exist in `binlog_row_metadata=FULL`. With `minimal` the driver returns the stored index (`ENUM`) or bitmask (`SET`) instead of losing the value; `types.enum.values` and `types.set.values` stay in `expected-failures.json` for that reason.
- MariaDB only sends signedness with `binlog_row_metadata=MINIMAL` (its default is `NO_LOG`), which the harness sets, as it does for MySQL.
- Compressed transactions (`TRANSACTION_PAYLOAD`) are inflated into the stream, which needs a Node.js with zstd support; without it reading the stream fails with an error saying so.
- `DATETIME(n)` and `TIMESTAMP(n)` come back as a `Date`, so fractional seconds are cut to milliseconds.
