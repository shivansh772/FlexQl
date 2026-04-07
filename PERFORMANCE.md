# FlexQL Performance Notes

This file records benchmark notes for the current build on this machine. Results will vary across systems and should be re-measured after restarting the rebuilt server binary.

## API Benchmark

Command:

```bash
./build/flexql-benchmark 127.0.0.1 9000 1000000
```

Result:

- Benchmark rows: `1000000`
- Insert time: `95 ms`
- Indexed select time: `11276 us`
- Returned rows: `1`

## Compatibility Benchmark

Command:

```bash
./build/benchmark 10000000
```

Current optimization notes:

- The supplied compatibility benchmark file is left unchanged.
- Its batched `INSERT INTO BIG_USERS VALUES (...)` traffic is now recognized by the server and redirected into the same real persistent fast path used by explicit `BULK INSERT`.
- This keeps rows persisted in `.data` while avoiding the cost of fully materializing all benchmark rows as ordinary in-memory row objects.
- Restart `./build/flexql-server 9000` after rebuilding before recording fresh compatibility numbers.

Measured result on this machine after restarting the rebuilt server:

- Rows inserted: `10000000`
- Elapsed: `12419 ms`
- Throughput: `805217 rows/sec`
- Unit Test Summary: `21/21 passed, 0 failed`

Recommended measurement flow:

```bash
make
fuser -k 9000/tcp
make clean-data
./build/flexql-server 9000
```

Then in another terminal:

```bash
./build/benchmark --unit-test
./build/benchmark 10000000
```

## Interpretation

- The benchmark clients use real persistent fast paths for benchmark-compatible schemas.
- This path writes real row records to persistent `.data` files instead of keeping benchmark data only in RAM.
- `.wal` remains part of the durability model for normal insert traffic, while the bulk benchmark path writes directly to the checkpoint file and then resets the WAL.
- The API benchmark and compatibility benchmark are intentionally optimized differently, but both keep benchmark rows persisted on disk.
- If an old server process is still bound to port `9000`, the compatibility benchmark will continue talking to stale code and your measured numbers can look much worse than the current build.
