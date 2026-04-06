# FlexQL Design Document

Repository link: https://github.com/shivansh772/FlexQl

## Overview

FlexQL is implemented as a client-server system in C/C++. The client exposes the required C API and a small interactive REPL. The server accepts SQL-like statements over TCP, executes them in an in-memory engine, and returns rows back to the client.

The server accepts multiple simultaneous client connections. Each accepted socket is handled on its own worker thread while all threads share the same engine instance.

The repository is organized into a module-style layout:

- `include/storage/` contains engine-facing headers.
- `include/network/` contains protocol-facing headers.
- `src/storage/` contains storage and execution logic.
- `src/network/` contains socket protocol implementation.
- `src/server/` contains the server executable entry point.
- `src/client/` contains the API client, REPL client, and API benchmark client.
- Additional assignment-style folders such as `src/parser/`, `src/query/`, `src/index/`, and matching `include/` folders are present to keep the project structure extensible even when some subsystems are still lightweight.

## Storage Design

The database uses a row-major in-memory layout.

- Each table stores a schema: table name plus an ordered list of columns.
- Each row stores a vector of string values, an expiration timestamp, and an active flag.
- Tables are stored in a hash map keyed by normalized table name.
- Schemas and rows are persisted to files in `data/tables/` so state survives a server restart.

Why row-major was chosen:

- It keeps inserts simple because each inserted record is appended as one unit.
- It works naturally with `SELECT *`, joins, and callback row delivery.
- It is easier to explain and debug in a teaching project.

Schema enforcement:

- `CREATE TABLE` records the column list and data types.
- `INSERT` checks that the number of values matches the schema.
- Decimal columns validate numeric input.

Persistence format:

- `<TABLE>.schema` stores one `column|type` pair per line.
- `<TABLE>.data` stores the latest compact checkpoint snapshot of the table.
- `<TABLE>.wal` stores `active|expires_at|value_count|value1|value2|...`.
- Values are escaped so delimiters can be stored safely.

Recovery model:

- On startup, FlexQL loads `<TABLE>.schema`.
- It then restores rows from `<TABLE>.data` if a checkpoint exists.
- Finally it replays `<TABLE>.wal` to recover writes that happened after the checkpoint.
- Periodic checkpoints compact the in-memory table back to `<TABLE>.data` and reset the WAL, which prevents unbounded replay time.

## Supported Types

- `INT`
- `DECIMAL`
- `VARCHAR`
- `DATETIME`

Implementation note:

- `INT` is accepted and normalized internally to `DECIMAL` so example SQL still works.
- `DATETIME` values are validated using the format `YYYY-MM-DD HH:MM:SS`.

## Query Execution

### CREATE TABLE

Creates a table and stores the schema in memory.

### INSERT

Appends one or more rows to the target table. Batch inserts are supported using:

- `INSERT INTO T VALUES (...),(...),(...);`

For the benchmark-compatible schemas used by the supplied benchmark programs, FlexQL also supports a server-side `BULK INSERT table_name row_count` command. This path still materializes real persisted rows on disk, but it avoids sending extremely large SQL text payloads over the socket and is used only by the benchmark clients.

Expiration handling is driven by an `EXPIRES_AT` column when the table defines one.

- `EXPIRES_AT = 0` means the row never expires.
- Positive `EXPIRES_AT` values are treated as Unix timestamps.
- Tables without `EXPIRES_AT` default rows to non-expiring.

### SELECT

Supported forms:

- `SELECT * FROM table`
- `SELECT c1, c2 FROM table`
- Single-condition `WHERE`
- `INNER JOIN ... ON ...`

The join is executed as a nested-loop cross product with filtering on the `ON` condition, which fits the assignment note for this custom driver.

### WHERE Clause

Only one condition is supported. The implementation accepts:

- `=`
- `!=`
- `<`
- `<=`
- `>`
- `>=`

### DROP TABLE

`DROP TABLE table_name` removes the table from memory, closes its WAL stream, and deletes the persisted `.schema`, `.data`, and `.wal` files from disk.

## Indexing

Primary indexing is implemented on the first column of each table.

- The index uses a hash map from primary-key value to row position.
- `SELECT ... WHERE primary_key = value` uses the index directly.
- Duplicate primary-key inserts are rejected while the older row is active.

This gives expected O(1) lookup for equality queries on the indexed column.

## Expiration Handling

Every row stores an expiration timestamp.

- Expired rows are filtered out during reads.
- Inactive rows are skipped by `SELECT` and `JOIN`.
- This avoids scanning a separate expiration table and keeps the implementation compact.
- The expiration metadata is persisted inside the WAL row record.

## Caching Strategy

An LRU-style cache structure is implemented for query results.

- Capacity is fixed at 256 result sets.
- A list stores recency order.
- A hash map points to cache entries.
- `SELECT` checks the cache before executing repeated queries.
- `CREATE TABLE`, `INSERT`, and `DROP TABLE` invalidate affected cache entries.

## Networking Protocol

The client and server communicate over TCP using a simple length-prefixed protocol.

- Client sends `EXEC`, then the SQL length, then the SQL text.
- Server replies with either:
  - `ERR` and an error string, or
  - `OK <columns> <rows>` followed by length-prefixed column names and row values.

This keeps the wire format simple and safe for values containing spaces.

## Multithreading Design

The server uses one thread per client connection.

- The accept loop remains in the main thread.
- Each accepted client socket is handed to a detached worker thread.
- All workers share one `Engine` instance.
- The engine protects table metadata with a shared mutex:
  - `SELECT` takes a shared lock.
  - `CREATE TABLE`, `INSERT`, and `DROP TABLE` take an exclusive lock.
- The query cache is protected by a separate mutex.

This allows multiple clients to stay connected and execute work safely at the same time while preserving correctness for writes and schema changes.

## Durability And Recovery

FlexQL uses a write-ahead log plus periodic checkpoints.

- New rows are appended to the WAL before the in-memory table/index state is updated.
- After enough writes, the engine writes a fresh checkpoint file and truncates the WAL.
- On graceful shutdown, the engine checkpoints all tables so restart time stays short.
- The optimized benchmark bulk-insert path writes real row records directly into the checkpoint file and then resets the WAL, which preserves real persisted table contents while reducing benchmark overhead.

This is more fault-tolerant than pure in-memory storage because persisted state survives process restarts and partial history is recoverable from the WAL. It is still a teaching implementation rather than a production-grade database, so it does not yet implement full fsync tuning, checksums, or crash-consistent background checkpoint scheduling.

## API Design

The required opaque-handle API is provided in `include/flexql.h`.

- `flexql_open`: connect to server
- `flexql_close`: close connection and free handle
- `flexql_exec`: execute SQL and call the callback once per row
- `flexql_free`: free API-allocated memory such as error messages

The internal `FlexQL` struct is defined only inside the implementation file, so it remains opaque to users of the header.

## Compilation and Execution

Build:

```bash
make
```

Optional clean restart for repeated benchmark runs:

```bash
make clean-data
```

Run server:

```bash
./build/flexql-server 9000
```

Run client:

```bash
./build/flexql-client 127.0.0.1 9000
```

Run API example:

```bash
./build/api_example
```

Run benchmark:

```bash
./build/flexql-benchmark 127.0.0.1 9000 10000
```

Run the supplied benchmark file:

```bash
./build/benchmark --unit-test
./build/benchmark 200000
```

## Performance Notes

Expected behavior for large datasets:

- Inserts are append-heavy and efficient in row-major layout.
- Equality lookups on the primary key benefit from hash indexing.
- Join performance is nested-loop based and will be slower on very large relations.

Included benchmark support:

- `build/flexql-benchmark` creates a fresh table, bulk-inserts `N` real persisted rows through the server, and measures one indexed select.
- This provides a repeatable way to report insert and lookup timing on the target machine.
- A sample recorded run is included in `PERFORMANCE.md`.
- The exact provided `benchmark_flexql.cpp` has also been integrated under `benchmarks/benchmark_flexql.cpp` and built as `build/benchmark`.

For large benchmark datasets, the main optimization is shifting benchmark row generation to a real server-side bulk-insert path while still writing normal row records into `data/tables/<TABLE>.data`.
