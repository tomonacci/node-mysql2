# Binlog regression baseline

Records what the driver decodes today, bugs included, so a merge or rebase can be checked for behavior changes. A difference means "changed", not "wrong".

Each scenario gets a fresh binlog and its own stream, so a decoder crash in one is recorded for that scenario and does not hide the others. The events collected before a crash are kept.

## Running

Node >= 22.2 is needed (`zlib.crc32`).

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

`--restart=unless-stopped` is required: the `stop` scenario issues `SHUTDOWN` to make the server write a `STOP_EVENT`. `BINLOG_SKIP_STOP=1` skips it. `ONLY=a,b` runs selected scenarios, `QUIET=1` hides the coverage report.

The harness drops and recreates the `test` database, resets the binlog, and flips global server settings (`binlog_row_metadata`, `gtid_mode`, `local_infile`). Use a throwaway server. The `mysql:8.0` image pinned in the baseline reports `8.0.x`; other versions can legitimately differ.

## Event coverage

Every code in `lib/constants/binlog_event_types.js` that a current server can send is produced by a scenario. The coverage report at the end of a run lists the rest with the reason.

| Event | Scenario |
| --- | --- |
| STOP | `stop` |
| ROTATE (real, not only the initial fake one) | `rotate` |
| INTVAR | `sbr_intvar` |
| RAND | `sbr_rand` |
| USER_VAR | `user_var_*` (one per result type) |
| BEGIN_LOAD_QUERY, EXECUTE_LOAD_QUERY | `load_data` |
| HEARTBEAT | `heartbeat` (blocking dump, 0.1s period) |
| ROWS_QUERY | `rows_query` |
| GTID, PREVIOUS_GTIDS (non-empty) | `gtid` |
| XA_PREPARE | `xa` |
| PARTIAL_UPDATE_ROWS | `partial_json` |
| TRANSACTION_PAYLOAD | `compression` |
| WRITE/UPDATE/DELETE_ROWS_V1, ANNOTATE_ROWS, BINLOG_CHECKPOINT, MARIADB_GTID, GTID_LIST | `maria_*` |

Not produced: events only pre-5.x servers sent, `INCIDENT` (needs a server fault), `IGNORABLE`, `HEARTBEAT_V2`, `GTID_TAGGED` (MySQL 8.4+), and `MARIADB_START_ENCRYPTION` (needs the encryption plugin and a key file).

## Known defects captured in the baselines

Decoding:

- `DECIMAL`: not parsed, reads past the row; rows masked as unstable
- `TINYINT/BIGINT UNSIGNED`: decoded as signed
- `TIME`: returned as a `Date` on the current day (baseline keeps the offset from midnight); negative values lose their sign
- `DATETIME`, `DATETIME(6)`: year 10058
- `ENUM`, `SET`: null
- `JSON`, including `PARTIAL_UPDATE_ROWS`: `packet.readBinaryJson` does not exist
- `binlog_row_metadata=FULL`: `packet.readVariableLengthString` does not exist, so every scenario with a table map fails (baseline `full`)
- `USER_VAR` with string, real or decimal values: `Buffer overrun`
- `ROWS_QUERY`, `TRANSACTION_PAYLOAD`, `EXECUTE_LOAD_QUERY`, anonymous GTID: `NotImplementedEvent`

Factory:

- `event_factory.js` indexes `Types.WRITE_ROWS_EVENT`, `UPDATE_ROWS_EVENT` and `DELETE_ROWS_EVENT`, which are not defined, so V1 row events fall through to `NotImplementedEvent`. MariaDB sends V1 row events, so no MariaDB row data is decoded.
- `Types.EXEC_LOAD_EVENT` (0x0a, obsolete) is mapped to `ExecuteLoadQueryEvent` instead of `EXECUTE_LOAD_QUERY_EVENT` (0x12).

MariaDB:

- Every logged DDL carries status variable `0x81` (`Q_XID`), which `QueryEvent` rejects (`maria_ddl`). The other scenarios create tables with `sql_log_bin=0` to get past it.
- The driver never sets `@mariadb_slave_capability`, so without it the server sends dummy events (`maria_no_capability`). The harness sets it to `4` for the other scenarios.
