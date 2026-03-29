# FlexQL Performance Notes

This file records one verified benchmark run using the included benchmark executable.

## Command

```bash
./build/flexql-benchmark 127.0.0.1 9001 1000
```

## Result

- Benchmark rows: `1000`
- Insert time: `41617 ms`
- Indexed select time: `81175 us`
- Returned rows: `1`

## Interpretation

- Inserts are currently the slowest path because each insert is sent as its own network request and is also persisted to disk immediately.
- Indexed single-row lookup works correctly and returns one row using the primary-key index.
- For larger leaderboard-style runs, the most important optimizations would be batching inserts, reducing disk flush overhead, and improving join execution.
