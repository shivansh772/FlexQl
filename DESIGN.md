# FlexQL Design Document

Repository link: add your GitHub repository URL here before submission.

## Overview

FlexQL is implemented as a client-server system in C/C++. The client exposes the required C API and a small interactive REPL. The server accepts SQL-like statements over TCP, executes them in an in-memory engine, and returns rows back to the client.

This submission intentionally runs the server in single-threaded mode because that matches the current target machine constraints. The design still separates networking and execution cleanly so a threaded accept loop can be added later without changing the public API.

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
- `<TABLE>.rows` stores `active|expires_at|value_count|value1|value2|...`.
- Values are escaped so delimiters can be stored safely.

## Supported Types

- `DECIMAL`
- `VARCHAR`

Implementation note:

- `INT` is accepted and normalized internally to `DECIMAL` so example SQL still works.
- `DATETIME` is not implemented because it was optional.

## Query Execution

### CREATE TABLE

Creates a table and stores the schema in memory.

### INSERT

Appends a row to the target table. Each inserted row is automatically assigned:

- `expires_at = current_time + 24 hours`

The expiration field is metadata and is not exposed as a normal SQL column.

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

## Indexing

Primary indexing is implemented on the first column of each table.

- The index uses a hash map from primary-key value to row position.
- `SELECT ... WHERE primary_key = value` uses the index directly.
- Duplicate primary-key inserts are rejected while the older row is active.

This gives expected O(1) lookup for equality queries on the indexed column.

## Expiration Handling

Every row stores an expiration timestamp.

- Expired rows are lazily marked inactive before inserts and reads.
- Inactive rows are skipped by `SELECT` and `JOIN`.
- This avoids scanning a separate expiration table and keeps the implementation compact.
- The current row state is written back to disk after table creation and inserts.

## Caching Strategy

An LRU-style cache structure is implemented for query results.

- Capacity is fixed at 32 result sets.
- A list stores recency order.
- A hash map points to cache entries.

Per the assignment-specific instruction for this submission, the engine updates the cache after `SELECT` execution but does not consult the cache before executing a new query. That keeps the cache mechanism present and correctly maintained while preserving the required behavior.

## Networking Protocol

The client and server communicate over TCP using a simple length-prefixed protocol.

- Client sends `EXEC`, then the SQL length, then the SQL text.
- Server replies with either:
  - `ERR` and an error string, or
  - `OK <columns> <rows>` followed by length-prefixed column names and row values.

This keeps the wire format simple and safe for values containing spaces.

## Multithreading Design

The server in this submission is single-threaded.

- It accepts one connection at a time.
- A connected client can send multiple SQL statements over the same socket.
- Shared state is therefore accessed serially, so locking is not needed in the current build.

If multithreading is enabled later, the natural extension is:

- one thread per client or a worker pool
- mutex protection around table and index structures
- read/write locking for concurrent reads

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

- `build/flexql-benchmark` creates a fresh table, inserts `N` rows, and measures one indexed select.
- This provides a repeatable way to report insert and lookup timing on the target machine.
- A sample recorded run is included in `PERFORMANCE.md`.
- The exact provided `benchmark_flexql.cpp` has also been integrated under `benchmarks/benchmark_flexql.cpp` and built as `build/benchmark`.

For a 10 million row benchmark, the next improvements would be batched network requests, chunked storage allocation, and more efficient join processing.
