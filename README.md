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
CREATE TABLE STUDENT(ID DECIMAL, NAME VARCHAR);
INSERT INTO STUDENT VALUES (1, 'Alice');
INSERT INTO STUDENT VALUES (2, 'Bob');
SELECT * FROM STUDENT;
SELECT NAME FROM STUDENT WHERE ID = 2;
```

You can also run the API example after starting the server:

```bash
./build/api_example
```

Run the benchmark client:

```bash
./build/flexql-benchmark 127.0.0.1 9000 10000
```

Run the provided benchmark file after starting the server:

```bash
./build/benchmark --unit-test
./build/benchmark 200000
```

If you want a clean benchmark rerun with empty persistent data:

```bash
make clean-data
```

## Supported SQL

- `CREATE TABLE table_name (column TYPE, ...)`
- `INSERT INTO table_name VALUES (...)`
- `SELECT * FROM table_name`
- `SELECT col1, col2 FROM table_name`
- `SELECT ... FROM table_name WHERE column = value`
- `SELECT ... FROM tableA INNER JOIN tableB ON tableA.col = tableB.col`
- `SELECT ... FROM tableA INNER JOIN tableB ON tableA.col = tableB.col WHERE column = value`

Notes:

- `DECIMAL` and `VARCHAR` are supported.
- `INT` is accepted as an alias for `DECIMAL` so common sample queries still work.
- Rows receive an automatic expiration timestamp internally.
- Table schemas and rows are persisted under `data/tables/`.
- The server intentionally runs in single-threaded mode in this submission.

## Files

- `include/flexql.h`: public C API
- `src/api.cpp`: API implementation
- `src/engine.cpp`: parser and in-memory database engine
- `src/protocol.cpp`: socket protocol helpers
- `src/server_main.cpp`: server executable
- `src/client_main.cpp`: interactive REPL client
- `benchmarks/benchmark_flexql.cpp`: integrated benchmark file from the supplied repository
- `DESIGN.md`: design document
- `PERFORMANCE.md`: sample measured benchmark result
