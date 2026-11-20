# Shopping Sessions — Rust pipeline

A single native binary that implements `rules.md` from raw CSV to the two Parquet tables. No engine is started, so container start-up is close to zero.

Status: all 36 fixtures pass, and the output matches `dev-answer` exactly on the 1M-event dev set. A slow, independent Python implementation (`tools/reference.py`) agrees with it on 11 more seeds, three of whose windows contain 29 February 2028.

## How it works

1. **Small inputs.** `products.csv` and `campaigns.csv` go into hash maps. Category names are ranked byte-wise, so ties in `top_category` resolve by comparing integers.
2. **Links.** Link files are parsed in parallel, then a union-find with path halving and union by size joins the IDs. Each person is named by their byte-wise smallest ID. The result is one hash map from ID to person name; an ID missing from it is a person on its own.
3. **Events and clicks.** Each file is split into 8 MB chunks, and each chunk is read with `pread` into a padded buffer.
   - **Line splitting:** each line is scanned through a 128-byte AVX2 window. This yields bitmasks for newlines, commas and quotes, and the quoting rules are checked with bit operations. Anything unusual falls back to a scalar parser with the same rules.
   - **Strings** of 15 bytes or fewer are packed into an exact `u128`, so equality and byte-wise order are plain integer operations. Longer strings, which are rare, are interned in a global table.
   - **Timestamps** use a table-driven F1/F2 parser and a digit loop for F3.
   - **Person lookup:** the person-map lookups are software-prefetched 16 rows ahead.
   - **Routing:** events go to 512 buckets by `hash(event_id)` and valid clicks to 1024 buckets by `hash(person)`. Records are staged per bucket in write-combining buffers and written out with non-temporal stores.
4. **Dedup.** For each event bucket, an open-addressing table of fingerprints keeps the copy with the smallest `ingest_ts`. Validity and lateness were already decided at parse time, so a kept copy that is invalid drops the event. Survivors move to the person buckets.
5. **Per person.** Each person bucket is sorted by `(person, ts)` and its clicks by `(person, ts, click_id)`. Sessionizing is one linear pass. Credit uses a binary search for the latest click at or before the session start. For assists, each campaign's click windows `[t, t+7d]` are merged and the sessions inside them counted per day. This stays linear for the huge shared-device person.
6. **Output.** The Parquet files are written in parallel: one `user_daily` file per worker thread and one `campaign_daily` file. `day` is `INT32 (DATE)`.

Large allocations come straight from `mmap` with `MADV_HUGEPAGE` and grow with `mremap` (`src/alloc.rs`).

The binary targets `x86-64-v3` (AVX2). It runs on the Cascade Lake scorers and also on any recent desktop CPU, so the image can be tested locally.

## Run and test

```sh
make build fixtures          # Docker image + all 36 fixtures, scorer flags
make dev run-dev check-dev   # 1M dev set (needs `generate` on PATH and dev-answer/ unzipped here)

# without Docker (Linux x86-64):
make local-fixtures
make local-dev
```

Cross-check against the reference on any generated dataset:

```sh
generate --seed 42 --events 300000 --clicks 75000 --output /tmp/s42
DATA_DIR=/tmp/s42 OUTPUT_DIR=/tmp/s42.out target/release/shopping-sessions
python3 tools/reference.py /tmp/s42 /tmp/s42.ref
python3 scripts/compare.py /tmp/s42.out /tmp/s42.ref
```

Timings go to stderr, one line per phase.
