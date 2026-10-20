# Binlog regression baseline

Records what the driver decodes today, bugs included, so a merge or rebase can be checked for behavior changes. A difference means "changed", not "wrong".

```sh
docker run -d --name binlog-mysql -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -e MYSQL_DATABASE=test \
  -p 127.0.0.1:3399:3306 mysql:8.0 --server-id=1 --log-bin=mysql-bin \
  --binlog-format=ROW --binlog-row-image=FULL --binlog-checksum=CRC32 \
  --default-authentication-plugin=mysql_native_password

# Node >= 22.2 (zlib.crc32)
node test/binlog-baseline/harness.js . /tmp/out.json minimal test/binlog-baseline/baseline-minimal.json
node test/binlog-baseline/harness.js . /tmp/out.json full    test/binlog-baseline/baseline-full.json
```

The harness pins `TZ=UTC`, drops and recreates the `test` database and resets the server's binlog.

## Known defects captured in the baseline (mode `minimal`)

- `DECIMAL`: not parsed, reads past the row; rows masked as unstable
- `TINYINT/BIGINT UNSIGNED`: decoded as signed
- `TIME`: returned as a `Date` on the current day (baseline keeps the offset from midnight); negative values lose their sign
- `DATETIME`, `DATETIME(6)`: year 10058
- `ENUM`, `SET`: null
- `JSON`: `packet.readBinaryJson` does not exist
- `binlog_row_metadata=FULL` (mode `full`): `packet.readVariableLengthString` does not exist
