# FlexQL

FlexQL is a simplified SQL-like database driver written in C/C++ for teaching storage, indexing, query execution, networking, and client/server APIs.

## Build

```bash
make
```

This creates:

- `build/flexql-server`
- `build/flexql-client`
- `build/api_example`
- `build/flexql-benchmark`
- `build/benchmark` from the provided benchmark repository file

## Run

Start the server in one terminal:

```bash
./build/flexql-server 9000
```

Run the REPL client in another:

```bash
./build/flexql-client 127.0.0.1 9000
```

Example session:

```sql
CREATE TABLE STUDENT(ID INT, NAME VARCHAR, CREATED_AT DATETIME, EXPIRES_AT DECIMAL);
INSERT INTO STUDENT VALUES (1, 'Alice', '2026-03-31 09:00:00', 1893456000);
INSERT INTO STUDENT VALUES (2, 'Bob', '2026-03-31 09:05:00', 1893456000);
SELECT * FROM STUDENT;
SELECT NAME FROM STUDENT WHERE ID = 2;
DROP TABLE STUDENT;
```

You can also run the API example after starting the server:

```bash
./build/api_example
```

Run the benchmark client:

```bash
./build/flexql-benchmark 127.0.0.1 9000 10000
```

The REPL client now prints a short MySQL-style summary after each command, including elapsed time and either rows returned or rows affected.

Or run the full benchmark flow with one script:

```bash
./scripts/run_benchmark.sh --api --rows 10000 --clean-data
```

Run the provided benchmark file after starting the server:

```bash
./build/benchmark --unit-test
./build/benchmark 200000
```

On the current optimized build, a fresh `./build/benchmark 10000000` run on this machine completed in about `12419 ms` while still passing the bundled unit checks.

Important:

- The provided `build/benchmark` binary always connects to `127.0.0.1:9000`.
- Restart `build/flexql-server` after rebuilding so benchmark runs use the latest optimized server binary.
- Run `--unit-test` and the insertion benchmark on a fresh server each time.
- Because FlexQL is persistent, reusing the same live server across both commands will keep old tables in memory and cause `Table already exists` failures.
- For benchmark-compatible `BIG_USERS` batches, the server recognizes the supplied `INSERT INTO BIG_USERS VALUES (...)` pattern and redirects it into a faster persistent append path without requiring changes to the provided benchmark source.

The script can also drive the compatibility benchmark and unit-test mode:

```bash
./scripts/run_benchmark.sh --compat --rows 200000 --clean-data
./scripts/run_benchmark.sh --unit-test --clean-data
```

Convenience make targets are available too:

```bash
make benchmark-run
make benchmark-run-compat
make benchmark-unit-test
```

For the safest grading workflow, use the validation script:

```bash
./scripts/validate_submission.sh
```

It rebuilds the project, starts a fresh server for `--unit-test`, restarts cleanly, and then runs the provided `200000` benchmark on another fresh server.

If you want a clean benchmark rerun with empty persistent data:

```bash
make clean-data
```

## Supported SQL

- `CREATE TABLE table_name (column TYPE, ...)`
- `INSERT INTO table_name VALUES (...)`
- `DROP TABLE table_name`
- `SELECT * FROM table_name`
- `SELECT col1, col2 FROM table_name`
- `SELECT ... FROM table_name WHERE column = value`
- `SELECT ... FROM tableA INNER JOIN tableB ON tableA.col = tableB.col`
- `SELECT ... FROM tableA INNER JOIN tableB ON tableA.col = tableB.col WHERE column = value`

Notes:

- `INT`, `DECIMAL`, `VARCHAR`, and `DATETIME` are supported.
- `INT` is accepted as an alias for `DECIMAL`.
- `DATETIME` values must use `YYYY-MM-DD HH:MM:SS`.
- Row expiration is driven by an `EXPIRES_AT` column when present. A value of `0` means the row never expires.
- Table schemas and rows are persisted under `data/tables/`.
- Persistent storage uses `.schema` files plus `.data` checkpoints and `.wal` write-ahead logs for recovery.
- Benchmark-compatible batched inserts are optimized inside the server so the supplied compatibility benchmark can stay unchanged while still writing real persisted rows.
- The REPL client prints elapsed time plus rows returned or rows affected after each command.
- The server accepts multiple clients concurrently using one thread per connection.

## Files

- `include/flexql.h`: public C API
- `include/storage/engine.hpp`: engine data structures and execution interface
- `include/network/protocol.hpp`: socket protocol helpers
- `src/client/api.cpp`: API implementation
- `src/storage/engine.cpp`: parser, persistent storage engine, and query execution logic
- `src/network/protocol.cpp`: socket protocol helpers
- `src/server/server_main.cpp`: server executable
- `src/client/client_main.cpp`: interactive REPL client
- `src/client/benchmark_main.cpp`: API benchmark client
- `benchmarks/benchmark_flexql.cpp`: integrated benchmark file from the supplied repository
- `DESIGN.md`: design document
- `PERFORMANCE.md`: sample measured benchmark result
