# VidPG: PostgreSQL Video-Frame Performance Lab

Research target: PostgreSQL 18 (current supported release as of 13 July 2026). This is an experimental internals lab, not a production video-call design.

## Executive conclusion

The idea is technically sound and unusually good as a PostgreSQL learning project, provided the success criterion is **bounded latency with deliberate frame loss**, not reliable delivery of every frame.

My recommended v1 is:

1. Browser captures, downsizes, and asynchronously JPEG-encodes at 640×360 and 10–15 FPS.
2. Every stage has capacity one: if capture, WebSocket, insert, fetch, or render is busy, replace the pending frame with the newer one.
3. A relay owns a small, fixed PostgreSQL connection pool; browsers never connect to PostgreSQL.
4. Frames are sent as binary end to end and bound to `bytea` as binary parameters—never base64 or SQL literals.
5. PostgreSQL stores frames in a three-table ring of standalone `UNLOGGED` buckets. `frame bytea STORAGE EXTERNAL` avoids attempting to recompress JPEG data.
6. One non-unique B-tree per bucket on `(stream_id, seq DESC)` supports latest-frame lookup. The payload is not included in the index.
7. A short transaction inserts one frame and emits a metadata-only `NOTIFY`. The listener treats notification as “state is dirty,” drains notifications, and fetches only the greatest sequence number.
8. Rotate every five seconds and `TRUNCATE` only a safely inactive bucket, retaining roughly 5–10 seconds of readable current/previous data with three buckets.
9. Start with safe PostgreSQL durability settings plus `synchronous_commit=off` for the application role. Unsafe cluster-wide settings are a later, isolated experiment—not the first optimization.

The likely first limit is not PostgreSQL's 1 GB `bytea` ceiling. It is usually one of: browser encode/decode CPU, unbounded application queues, four payload-sized network/copy legs, per-frame transaction round trips, TOAST chunk/index churn, or `NOTIFY` fan-out. Which one wins depends on frame size, FPS, streams, host placement, and driver behavior; the benchmark must identify it rather than assume it.

## 1. End-to-end decomposition

| Stage | Work and likely bottleneck | Required backpressure | Measurement |
|---|---|---|---|
| Capture | camera cadence, color conversion | do not capture another frame while encoder slot is occupied | attempted/captured FPS |
| Resize/encode | canvas copy, JPEG CPU, allocation/GC | one encode in flight; overwrite one pending slot | encode p50/p95/p99, bytes/frame, browser CPU |
| Browser transport | WebSocket framing, kernel buffers | check `bufferedAmount`; keep newest unsent frame | sent FPS, buffered bytes, sender drops |
| Relay ingress | parsing metadata, memory copies, authentication | bounded per-stream slot, payload/size/rate limits | ingress FPS, event-loop lag, RSS |
| DB connection scheduling | checkout wait, one command stream per connection | fixed pool, fair per-stream scheduler, no browser-per-DB-connection | pool wait and active/idle counts |
| Insert/commit | protocol RTT, TOAST work, index update, WAL if LOGGED | bounded number of inserts; evict stale before SQL | queue wait separately from SQL and commit latency |
| Signal | `NOTIFY` queue and listener fan-out | metadata only; coalesce in relay | notification rate, queue usage, Notify SLRU stats |
| Latest fetch | index probe plus heap/TOAST fetch | at most one fetch per receiver/stream; dirty flag for one rerun | fetch latency and returned sequence |
| Relay egress | another payload copy and WebSocket queue | replace pending output with newest | egress queue and receiver drops |
| Decode/render | JPEG decode, canvas upload, paint | one decode/render in flight | decode/render latency and rendered FPS |
| Cleanup | lock acquisition and relation lifecycle | rotate first, truncate only inactive table, timeout/retry | truncation wait/runtime and relation size |

Capacity arithmetic must be explicit:

```text
source payload rate = streams × source FPS × mean encoded bytes/frame
retained payload     = source payload rate × retention seconds
application data movement is roughly four payload legs:
browser→relay, relay→DB, DB→relay, relay→receiver
```

Example: 30 FPS × 100 KiB is about 2.93 MiB/s per stream before protocol, row, TOAST, index, and allocator overhead. Sixteen streams are about 46.9 MiB/s of source payload and potentially about four times that amount moved across application boundaries. Compressed-frame size distribution matters more than nominal resolution.

## 2. PostgreSQL internals that matter

### WAL

A LOGGED insert records heap/TOAST/index changes in WAL. The first changed page after a checkpoint can also generate a full-page image when `full_page_writes=on`. WAL amplification is therefore workload- and checkpoint-dependent; it is not a fixed “2×” multiplier. Measure `wal_bytes/frame`, `wal_fpi`, and physical WAL write bytes.

`wal_level=minimal` is often misunderstood. It reduces replication-oriented information and enables special minimal-logging paths for operations such as `CREATE TABLE`, `TRUNCATE`, and rewrites in the right transaction. It does **not** make ordinary inserts into an existing LOGGED table WAL-free. The official WAL documentation describes the exact limited cases ([PostgreSQL WAL settings](https://www.postgresql.org/docs/current/runtime-config-wal.html)).

UNLOGGED tables remove WAL for their relation data and indexes, are cleared after crash/unclean shutdown, and are not replicated—an excellent match for disposable frames ([CREATE TABLE: UNLOGGED](https://www.postgresql.org/docs/current/sql-createtable.html)). They do not turn the entire cluster into a no-WAL system: catalogs, table lifecycle, and other logged relations still matter.

### TOAST and `bytea`

PostgreSQL pages are normally 8 KiB and tuples cannot span pages. Large values are moved to a per-table TOAST relation in chunks of roughly 2 KiB, each with a `(chunk_id, chunk_seq)` index entry. A 100 KiB JPEG therefore creates on the order of 50 TOAST rows, not one physical row. PostgreSQL documents the chunking and storage modes in [TOAST storage](https://www.postgresql.org/docs/current/storage-toast.html).

Default `EXTENDED` permits compression and out-of-line storage. `EXTERNAL` permits out-of-line storage without compression. JPEG/WebP/AVIF data is already compressed, so recompression usually buys little and can spend CPU. `STORAGE EXTERNAL` is therefore a strong hypothesis, not a universal truth: compare CPU, insert latency, and relation bytes with a real frame corpus. It permits rather than absolutely forces external storage, although normal frame sizes greatly exceed the TOAST target.

The `bytea` logical limit is 1 GB; the large-object API reaches 4 TB, but neither limit is practically relevant for live frames ([large objects versus TOAST](https://www.postgresql.org/docs/current/lo-intro.html)). The practical ceiling arrives first through copies, memory, TOAST row count, disk bandwidth, or latency.

### MVCC bloat

`DELETE` creates dead tuple versions because old snapshots might still need them. Deleting a row with an external value also deletes its many TOAST chunks using MVCC, producing dead TOAST heap/index entries that vacuum must later reclaim. Ordinary `VACUUM` generally makes space reusable inside the relation; it does not necessarily return it to the operating system. Long or idle transactions delay cleanup ([routine vacuuming](https://www.postgresql.org/docs/current/routine-vacuuming.html)).

`TRUNCATE` does not visit rows, reclaims space immediately, and includes the table's TOAST relation and indexes. It takes `ACCESS EXCLUSIVE`, is not MVCC-safe for old snapshots, and must therefore target an inactive bucket ([TRUNCATE](https://www.postgresql.org/docs/current/sql-truncate.html)). Rotation changes cleanup from work proportional to frames/chunks into relation-level work.

## 3. Schema alternatives

| Design | Strength | Failure/cost | Verdict |
|---|---|---|---|
| Single LOGGED append table + DELETE | simplest, exposes WAL and MVCC visibly | maximum WAL, TOAST bloat, vacuum and index churn | deliberately naive baseline |
| Single UNLOGGED table + DELETE | payload WAL removed | MVCC/TOAST bloat remains | useful isolation experiment |
| Single table + periodic TRUNCATE | instant reclaim | blocks ingestion and loses current frames | unsuitable without rotation |
| LOGGED declarative time partitions | clean SQL routing and fast detach/drop | WAL remains; DDL/parent locking and planning overhead | good LOGGED comparison |
| Standalone UNLOGGED bucket ring | no payload WAL, bounded storage, cheap cleanup | relay must route/query tables; rotation protocol required | recommended PG18 v1 |
| One-row-per-stream UPSERT | superficially “latest only” | every update creates a new heap version and replaces TOAST chunks; hot-row contention | reject |

Version trap: PostgreSQL 18 explicitly disallows UNLOGGED partitioned tables; the release notes explain that earlier support was non-functional ([PostgreSQL 18 release notes](https://www.postgresql.org/docs/18/release-18.html)). Consequently, do not describe an “UNLOGGED declarative partitioned parent” as the PG18 design. Use standalone UNLOGGED buckets or use LOGGED declarative partitions as a separate test.

The ready-to-run DDL is in `vidpg-schema.sql`. Key choices:

- Three global buckets are simpler than a table per stream.
- Keep the tiny bucket-state/control table LOGGED and update it only at rotation boundaries. Its WAL cost is negligible, and after a crash it can safely point at any now-empty UNLOGGED bucket.
- Sender/relay supplies `stream_id` and monotonically increasing `seq`; no global sequence is needed.
- A non-unique `(stream_id, seq DESC)` index avoids uniqueness checks. Add a unique index only if deduplication is a real requirement.
- No index includes `frame`; including it would duplicate or size-limit the payload in the index.
- `CHECK (octet_length(frame) <= 1048576)` provides a hard safety ceiling; use a lower application limit in normal operation.
- Prepare one INSERT and one latest-fetch statement per bucket because table identifiers cannot be bound as parameters.

## 4. Recommended rotation protocol

Use three buckets and a five-second period initially. For generation `g`, current
is `g mod 3`, previous is `(g-1) mod 3`, and next/reuse is `(g+1) mod 3`:

```text
writers continue using current bucket (g mod 3)
readers use only current and previous
TRUNCATE next/reuse bucket ((g+1) mod 3), which is neither
if truncate succeeds, publish generation g+1 in a short transaction
new writers use the empty new current; late old writers land in previous
```

The controller should use a PostgreSQL advisory lock so only one relay rotates. Set `lock_timeout='100ms'` in the cleanup session; if `TRUNCATE` cannot acquire its lock, log and retry while writers remain on the current bucket. Publish the new active bucket only after cleanup succeeds. The active write path must never dynamically concatenate an unvalidated table name: map bucket numbers 0–2 to fixed prepared statements.

Readers query the active and immediately previous buckets, take one candidate from each, then choose the greatest sequence. This closes the switch race without scanning old data. Three buckets give one active, one previous/readable, and one cleanup/reuse target. Choose period and count from measured retention needs, not folklore:

```text
bucket_count >= 3
readable retention ≈ (bucket_count - 1) × rotation_period
working-set bytes ≈ source payload rate × readable retention × measured storage amplification
```

## 5. Insert method comparison

| Method | Latency behavior | Throughput behavior | Use here |
|---|---|---|---|
| Prepared single-row INSERT with binary bind | one frame becomes visible per short commit; simple error attribution | parse overhead removed, but one RTT/commit per frame | v1 default |
| Multi-row/microbatch INSERT | queueing delay until batch closes | amortizes protocol and commit work | batch newest frame per stream for at most 1–5 ms; test later |
| `COPY BINARY` | rows are not useful to readers until the COPY transaction commits; chunk close adds latency | usually highest bulk throughput | throughput experiment, not v1 live path |
| Prepared INSERTs in libpq pipeline mode | removes wait-for-result RTT between commands | server still executes in order on one connection; bounded queue required | valuable when relay↔DB RTT is material |

PostgreSQL recommends COPY for bulk loading ([populating a database](https://www.postgresql.org/docs/current/populate.html)), but this workload optimizes freshness rather than completed rows/second. A long COPY stream is the wrong transaction boundary. Close COPY every small chunk and it becomes a latency/throughput trade, not a free win.

Pipeline mode reduces network round trips, not TOAST, index, WAL, or execution work. It is more complex, consumes queue memory, executes statements in send order, and cannot run COPY while in pipeline mode ([libpq pipeline mode](https://www.postgresql.org/docs/current/libpq-pipeline-mode.html)). Bound it to perhaps 2–8 outstanding newest frames and test; an unlimited pipeline merely moves the stale-frame backlog into libpq.

Use the extended protocol's binary parameter/result formats. PostgreSQL explicitly supports per-parameter and per-column binary format codes ([protocol formats](https://www.postgresql.org/docs/current/protocol-overview.html)). Text `bytea` hex uses two characters per byte, so a driver silently using text can roughly double payload bytes plus conversion work ([bytea formats](https://www.postgresql.org/docs/current/datatype-binary.html)). Verify the selected driver with packet sizes or driver tracing.

## 6. Notification and latest-frame fetch

`NOTIFY` payloads must be shorter than 8000 bytes in the default configuration. Send only compact metadata such as `stream-id,bucket,seq`, never a frame. Notifications are delivered at commit and only between listener transactions. The standard queue is up to 8 GB by default; if it fills, notifying transactions fail at commit. A listener sitting in a long transaction can prevent queue cleanup ([NOTIFY semantics](https://www.postgresql.org/docs/current/sql-notify.html)).

Correct listener startup is: execute and commit `LISTEN`, then inspect current database state in a new transaction, then rely on notifications. PostgreSQL documents this initial race explicitly ([LISTEN](https://www.postgresql.org/docs/current/sql-listen.html)). Use a dedicated, session-persistent listener connection; a transaction-pooled connection is unsuitable for session state.

Notification does not solve backpressure. Distinct transactions are not coalesced by PostgreSQL. The relay must implement:

```text
on notification(stream):
  latest_notified_seq[stream] = max(current, notified_seq)
  if no fetch running: start fetch_latest(stream)

on fetch complete(frame seq):
  publish only if seq > last_published_seq
  if latest_notified_seq > seq: run exactly one more fetch
```

Drain all available notifications before starting fetches. For multiple receivers of the same stream, fetch once into the relay and fan out the single result; do not make PostgreSQL fetch the same TOAST value once per browser.

`notify_buffers` caches `pg_notify` SLRU pages in shared memory; it does not enlarge the queue. `max_notify_queue_pages` sets the disk-backed queue cap. Raising either is not a cure for a stuck listener. Monitor `pg_notification_queue_usage()`, `pg_stat_slru WHERE name='Notify'`, and long transactions.

## 7. Configuration: what to tune and what not to claim

Start with the baseline profile in `vidpg-postgresql.conf.example`, sized there for a dedicated 16 GiB lab host.

| Parameter | Recommended interpretation |
|---|---|
| `fsync` | Keep `on` initially. `off` is a destructive cluster-corruption experiment after backups; ephemeral rows do not make catalogs ephemeral. |
| `synchronous_commit` | Set `off` for the application role first. It removes the commit flush wait for LOGGED tests without disabling cluster-wide fsync. It matters less to UNLOGGED payloads. |
| `full_page_writes` | Keep `on` unless running the disposable `fsync=off` profile. It is mostly irrelevant to UNLOGGED payload pages. |
| `wal_level` | Keep `replica` for a comparable baseline. Test `minimal` only with `max_wal_senders=0`; do not claim ordinary LOGGED INSERT becomes unlogged. |
| `shared_buffers` | Start near 25% of dedicated host RAM, leaving memory for OS cache, relay, and browsers. PostgreSQL notes that more than ~40% is unlikely to help ([resource settings](https://www.postgresql.org/docs/current/runtime-config-resource.html)). |
| `wal_buffers` | Leave `-1`; increase only if `wal_buffers_full` grows in a LOGGED run. |
| checkpoint settings | For LOGGED runs, `checkpoint_timeout=15min`, `checkpoint_completion_target=0.9`, `max_wal_size=4GB` are a reasonable 16 GiB-host starting point. Observe rather than assume. |
| `notify_buffers` | Leave the PG18 default 16 pages until Notify SLRU reads/misses justify a measured increase. |
| `max_notify_queue_pages` | Leave default and alert early. A bigger backlog is the opposite of latest-frame-wins. A deliberately smaller lab queue can expose failure sooner. |
| timeouts | Apply `idle_in_transaction_session_timeout=5s` to application roles. Use cleanup-session `lock_timeout=100ms`; use a modest role-level `statement_timeout`, not a risky global value. |
| timing stats | Enable `track_io_timing` and `track_wal_io_timing` during benchmark runs; measure their overhead once. |
| autovacuum | Keep it on globally for catalogs and XID safety. Rotating buckets should create little ordinary bloat; the DELETE baseline intentionally needs vacuum. |

The PostgreSQL docs warn that `fsync=off` can cause unrecoverable corruption after a crash and that `synchronous_commit=off` offers much of its latency benefit with less risk ([WAL settings](https://www.postgresql.org/docs/current/runtime-config-wal.html)). A RAM disk changes storage latency; it does not remove MVCC, TOAST chunking, indexes, locks, protocol RTT, or memory copies. It should be a named experiment, not presented as a database optimization.

## 8. Benchmark plan

### Workloads

Use both a fixed synthetic corpus and real JPEG frames. Zero-filled data gives misleading compression results. Test frame distributions around 32, 64, 128, and 256 KiB; rates 5, 15, and 30 FPS; streams 1, 4, 16, then increase until failure. For each run: 30 s warm-up, 180 s measurement, 30 s cooldown, at least five repetitions with randomized treatment order.

Use two load modes:

- Open-loop capture at target FPS with intentional drops. This reveals overload and freshness.
- Closed-loop “send next after commit.” This measures service time but can hide insufficient capacity.

### Controlled experiment sequence

Change one major variable at a time:

1. End-to-end client/relay loop without persistence, solely to bound encode/network/decode cost.
2. LOGGED single table + default `EXTENDED` + prepared binary INSERT + periodic DELETE.
3. LOGGED to UNLOGGED.
4. `EXTENDED` to `EXTERNAL` using the same frames.
5. DELETE to three-bucket TRUNCATE rotation.
6. Poll-latest versus NOTIFY-metadata-plus-fetch.
7. Prepared row INSERT versus 1–5 ms microbatch versus bounded pipeline versus chunked COPY BINARY.
8. Safe durability versus role-level async commit versus disposable unsafe profile.

Do not run the entire Cartesian product. Promote only settings that win a stage without unacceptable tail latency or freshness loss.

### Metrics and definitions

- attempted, captured, encoded, sent, relay-accepted, committed, fetched, and rendered FPS
- drop counts and reasons at every boundary
- queue wait, encode, relay→commit, insert/commit, notify→fetch, fetch, decode/render, and end-to-end p50/p95/p99
- `receiver_lag_frames = latest_committed_seq - rendered_seq`
- `WAL bytes/s` and `WAL bytes/committed frame`
- heap/index/TOAST bytes and growth slope
- live/dead main and TOAST tuples, vacuum counts/times
- relation/WAL read/write/extend bytes and times
- notification queue usage and Notify SLRU reads/writes
- bucket switch time, TRUNCATE lock wait, and TRUNCATE execution time
- PostgreSQL/relay/browser CPU, RSS, disk throughput, network throughput, and event-loop lag

End-to-end timestamps on different machines require clock synchronization or offset estimation. Always record server-side stage durations as well, so clock error cannot masquerade as database latency.

A run is sustainable only if p99 and receiver lag reach a steady distribution, application queues remain bounded, bucket sizes remain periodic rather than growing, notification queue usage remains near zero, and no resource is silently saturated. “High FPS” with a growing hidden queue is failure.

PG18 moves WAL write/fsync byte and timing detail into `pg_stat_io`; `pg_stat_wal` reports generated records, full-page images, bytes, and buffer-full events. The statistics documentation describes both views ([PG18 cumulative statistics](https://www.postgresql.org/docs/current/monitoring-stats.html)). Use deltas from before/after snapshots; cluster-wide views include unrelated activity.

Ready-to-run queries are in `vidpg-observability.sql`. Note that `pg_stat_user_tables` excludes TOAST tables; use `pg_stat_all_tables` joined through `pg_class.reltoastrelid` for dead TOAST chunks.

## 9. Risks and failure modes

| Symptom | Likely cause | Response |
|---|---|---|
| Latency climbs while FPS looks good | hidden unbounded queue/pipeline | cap every stage, expose queue time, drop stale |
| High relay CPU/network | base64/hex conversion or repeated receiver fetches | binary protocol and one fetch/fan-out |
| WAL roughly follows payload | LOGGED heap/TOAST/index data | expected baseline; compare UNLOGGED and `wal_bytes/frame` |
| WAL spikes after checkpoints | full-page images | correlate `wal_fpi`; lengthen checkpoints for experiment, do not mislabel |
| Main heap small, disk huge | external TOAST plus TOAST index | inspect TOAST relation explicitly |
| DELETE run never shrinks on disk | MVCC space reuse, insufficient vacuum, old snapshots | rotation/TRUNCATE; eliminate idle transactions |
| TRUNCATE pauses writers | bucket still active or reader transaction holds lock | switch first, short transactions, timeout/retry, third bucket |
| NOTIFY commit failures | queue full, stuck listener/transaction | monitor queue, kill/fix stale listener; do not enlarge blindly |
| Receiver renders old frames | one fetch per notification or FIFO delivery | dirty flag + latest query + overwrite output slot |
| More DB connections reduce throughput | index/TOAST/buffer/lock contention | benchmark 1/2/4/8 writers; keep smallest pool meeting latency |
| Unsafe run corrupts cluster | `fsync`/FPW disabled and crash | disposable cluster only, scripted re-init, never reuse valuable cluster |

## 10. What the portfolio presentation should emphasize

Title it **“PostgreSQL Internals Under an Ephemeral Binary Firehose.”** The video is a comprehensible workload generator and live visualization, not the product claim.

The strongest demo is a reproducible naive-versus-optimized sequence:

1. LOGGED + `EXTENDED` + individual text or binary INSERTs + DELETE.
2. Show WAL rate, TOAST size, dead tuples, latency, and receiver lag climbing.
3. Switch one factor at a time: binary binding, UNLOGGED, EXTERNAL, rotating TRUNCATE, latest-frame coalescing.
4. Show which graph each change affects—and which it does not.
5. Deliberately stall a listener to demonstrate notification queue behavior; deliberately hold a transaction to demonstrate cleanup/lock effects.

Use a four-panel screen: live sender/receiver video with sequence and age overlays; a stage-latency/drop waterfall; PostgreSQL internals (`wal_bytes/s`, WAL FPI, heap/index/TOAST bytes, dead tuples, Notify queue/SLRU, and wait events); and a controlled experiment selector showing the one variable changed. Keep a small architecture animation on the opening slide, but let synchronized graphs provide the proof.

The intellectually honest result can be “PostgreSQL sustained N streams at the chosen quality on this machine, and bottleneck X appeared first.” That is more impressive than an unsupported claim that PostgreSQL is a video transport.

## 11. Final v1 build order

1. Define a 640×360, 10–15 FPS, ≤150 KiB normal-frame target and a 1 MiB hard rejection limit.
2. Implement capture/encode/send with a single overwrite slot and stage counters.
3. Implement relay authentication, per-stream latest slots, size/rate limits, and fixed writer/listener/fetch pools.
4. Apply `vidpg-schema.sql`; use binary prepared inserts into bucket 0 without notifications.
5. Add indexed latest fetch and binary result retrieval.
6. Add metadata-only NOTIFY on the same short transaction and the dirty-flag listener algorithm.
7. Add three-bucket rotation with advisory lock, short lock timeout, and retries.
8. Add observability snapshots and application histograms before tuning.
9. Reproduce the naive LOGGED/DELETE baseline and then the optimized design.
10. Only then test batching, pipeline mode, COPY chunks, checkpoint tuning, and destructive durability settings.

## 12. Future experiments

- Compare JPEG quality/resolution as control-loop inputs that keep DB payload bandwidth under a target.
- Compare one writer connection per stream, shared writer pool, and consistent stream-to-writer hashing.
- Compare relay and PostgreSQL colocated over local socket/loopback versus realistic network RTT.
- Compare polling at a fixed display cadence with NOTIFY-triggered dirty fetch; notification overhead might exceed its freshness benefit at high FPS.
- Vary TOAST compression (`EXTENDED` with pglz/lz4 where available) versus `EXTERNAL` on real and synthetic data.
- Run crash tests: verify UNLOGGED buckets clear, state recovery selects bucket 0, and no stale frame is considered current.
- Use `pg_stat_statements` and `EXPLAIN (ANALYZE, BUFFERS, WAL)` in isolated DB microbenchmarks.

## Bottom line

Build it. The project becomes compelling when “latest wins” is treated as a first-class scheduling invariant and PostgreSQL is used to expose, not hide, the costs of WAL, TOAST, MVCC, notifications, and locks. The highest-value comparison is not prepared INSERT versus COPY on day one; it is **LOGGED/DELETE/unbounded consumption versus UNLOGGED rotating buckets/TRUNCATE/bounded latest-only consumption**. That produces both the largest practical improvement and the clearest PostgreSQL-internals story.
