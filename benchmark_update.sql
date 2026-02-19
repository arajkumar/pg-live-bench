-- ============================================================================
-- Benchmark: Batch UPDATE on Hypertable vs Logical Partition vs Plain Table
-- Table: metrics(id int, time timestamptz, name text, value float8)
-- Partition key: time (timestamptz), 200 chunks/partitions, 5000 rows each
--
-- Usage: psql -f benchmark_update_partitioning.sql
-- ============================================================================

\timing on
\pset pager off

-- ============================================================================
-- 1. SETUP: Hypertable
-- ============================================================================

DROP TABLE IF EXISTS metrics_ts CASCADE;

CREATE TABLE metrics_ts (
    id    int          NOT NULL,
    time  timestamptz  NOT NULL,
    name  text         NOT NULL,
    value float8       NOT NULL
);

SELECT create_hypertable('metrics_ts', by_range('time', INTERVAL '1 day'));

INSERT INTO metrics_ts (id, time, name, value)
SELECT
    g,
    '2025-01-01'::timestamptz + ((g - 1) / 5000) * INTERVAL '1 day'
                               + ((g - 1) % 5000) * INTERVAL '1 second',
    'sensor_' || (g % 500),
    random() * 100
FROM generate_series(1, 1000000) g;

SELECT count(*) AS ts_chunks
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_ts';

ANALYZE metrics_ts;

-- ============================================================================
-- 2. SETUP: Logical Partition
-- ============================================================================

DROP TABLE IF EXISTS metrics_pg CASCADE;

CREATE TABLE metrics_pg (
    id    int          NOT NULL,
    time  timestamptz  NOT NULL,
    name  text         NOT NULL,
    value float8       NOT NULL
) PARTITION BY RANGE (time);

DO $$
BEGIN
    FOR i IN 0..199 LOOP
        EXECUTE format(
            'CREATE TABLE metrics_pg_p%s PARTITION OF metrics_pg FOR VALUES FROM (%L) TO (%L)',
            i, ('2025-01-01'::date + i)::text, ('2025-01-01'::date + i + 1)::text
        );
    END LOOP;
END $$;

INSERT INTO metrics_pg SELECT * FROM metrics_ts;
ANALYZE metrics_pg;

-- ============================================================================
-- 3. SETUP: Plain Table
-- ============================================================================

DROP TABLE IF EXISTS metrics_plain CASCADE;

CREATE TABLE metrics_plain (
    id    int          NOT NULL,
    time  timestamptz  NOT NULL,
    name  text         NOT NULL,
    value float8       NOT NULL
);

CREATE INDEX ON metrics_plain (time);
CREATE INDEX ON metrics_plain (id);

INSERT INTO metrics_plain SELECT * FROM metrics_ts;
ANALYZE metrics_plain;

-- ============================================================================
-- 4. Build payload (50 rows spread across last 5 partitions)
-- ============================================================================

SELECT jsonb_agg(
    jsonb_build_object(
        'where', jsonb_build_object('time', time::text),
        'set',   jsonb_build_object('value', 99.9, 'name', 'updated')
    )
)::text AS p
FROM (
    SELECT m.time FROM generate_series(0, 4) d
    CROSS JOIN LATERAL (
        SELECT time FROM metrics_ts
        WHERE time >= '2025-07-15'::timestamptz + d * INTERVAL '1 day'
          AND time <  '2025-07-15'::timestamptz + (d + 1) * INTERVAL '1 day'
        ORDER BY time LIMIT 10
    ) m
) sub
\gset

SELECT array_agg(time)::text AS t
FROM (
    SELECT m.time FROM generate_series(0, 4) d
    CROSS JOIN LATERAL (
        SELECT time FROM metrics_ts
        WHERE time >= '2025-07-15'::timestamptz + d * INTERVAL '1 day'
          AND time <  '2025-07-15'::timestamptz + (d + 1) * INTERVAL '1 day'
        ORDER BY time LIMIT 10
    ) m
) sub
\gset

\echo ''
\echo '========================================='
\echo 'Payload: 50 rows spread across 5 partitions'
\echo '========================================='

-- ============================================================================
-- 4b. Build payload (50 rows from last chunk only)
-- ============================================================================

SELECT jsonb_agg(
    jsonb_build_object(
        'where', jsonb_build_object('time', time::text),
        'set',   jsonb_build_object('value', 99.9, 'name', 'updated')
    )
)::text AS p1
FROM (
    SELECT time FROM metrics_ts
    WHERE time >= '2025-07-19'::timestamptz
      AND time <  '2025-07-20'::timestamptz
    ORDER BY time LIMIT 50
) sub
\gset

SELECT array_agg(time)::text AS t1
FROM (
    SELECT time FROM metrics_ts
    WHERE time >= '2025-07-19'::timestamptz
      AND time <  '2025-07-20'::timestamptz
    ORDER BY time LIMIT 50
) sub
\gset

\echo ''
\echo '========================================='
\echo 'Payload: 50 rows from last chunk only'
\echo '========================================='

-- ============================================================================
-- 4c. Build payload (single row from last chunk)
-- ============================================================================

SELECT time::text AS single_time
FROM metrics_ts
WHERE time >= '2025-07-19'::timestamptz
  AND time <  '2025-07-20'::timestamptz
ORDER BY time LIMIT 1
\gset

SELECT jsonb_build_array(
    jsonb_build_object(
        'where', jsonb_build_object('time', single_time),
        'set',   jsonb_build_object('value', 99.9, 'name', 'updated')
    )
)::text AS p_single
FROM (SELECT :'single_time' AS single_time) x
\gset

SELECT ('{' || :'single_time' || '}')::text AS t_single
\gset

\echo ''
\echo '========================================='
\echo 'Payload: single row from last chunk'
\echo '========================================='

-- ============================================================================
-- 5. PREPARE statements
-- ============================================================================

PREPARE up_ts AS
UPDATE metrics_ts AS t
SET value = (cdc->'set'->>'value')::float8, name = cdc->'set'->>'name'
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_ts, cdc->'where') AS wc
WHERE t.time = wc.time AND t.time = ANY($2::timestamptz[]);

PREPARE up_ts_no_any AS
UPDATE metrics_ts AS t
SET value = (cdc->'set'->>'value')::float8, name = cdc->'set'->>'name'
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_ts, cdc->'where') AS wc
WHERE t.time = wc.time;

PREPARE up_pg AS
UPDATE metrics_pg AS t
SET value = (cdc->'set'->>'value')::float8, name = cdc->'set'->>'name'
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_pg, cdc->'where') AS wc
WHERE t.time = wc.time AND t.time = ANY($2::timestamptz[]);

PREPARE up_pg_no_any AS
UPDATE metrics_pg AS t
SET value = (cdc->'set'->>'value')::float8, name = cdc->'set'->>'name'
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_pg, cdc->'where') AS wc
WHERE t.time = wc.time;

PREPARE up_plain AS
UPDATE metrics_plain AS t
SET value = (cdc->'set'->>'value')::float8, name = cdc->'set'->>'name'
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_plain, cdc->'where') AS wc
WHERE t.time = wc.time AND t.time = ANY($2::timestamptz[]);

PREPARE up_plain_no_any AS
UPDATE metrics_plain AS t
SET value = (cdc->'set'->>'value')::float8, name = cdc->'set'->>'name'
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_plain, cdc->'where') AS wc
WHERE t.time = wc.time;

PREPARE up_ts_single AS
UPDATE metrics_ts SET value = 99.9, name = 'updated' WHERE time = $1::timestamptz;

PREPARE up_pg_single AS
UPDATE metrics_pg SET value = 99.9, name = 'updated' WHERE time = $1::timestamptz;

PREPARE up_plain_single AS
UPDATE metrics_plain SET value = 99.9, name = 'updated' WHERE time = $1::timestamptz;

SET plan_cache_mode = 'force_custom_plan';

-- ============================================================================
-- 6. BENCHMARKS (1 warmup + 3 measured runs each)
-- ============================================================================

\echo ''
\echo '=== 1. Hypertable + ANY (chunk pruning) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;

\echo ''
\echo '=== 2. Hypertable NO ANY (all chunks scanned) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts_no_any(:'p'::jsonb); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts_no_any(:'p'::jsonb); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts_no_any(:'p'::jsonb); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts_no_any(:'p'::jsonb); ROLLBACK;

\echo ''
\echo '=== 3. Logical Partition + ANY (plan-time pruning) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;

\echo ''
\echo '=== 4. Logical Partition NO ANY (runtime pruning) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg_no_any(:'p'::jsonb); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg_no_any(:'p'::jsonb); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg_no_any(:'p'::jsonb); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg_no_any(:'p'::jsonb); ROLLBACK;

\echo ''
\echo '=== 5. Plain Table + ANY ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;

\echo ''
\echo '=== 6. Plain Table NO ANY (baseline) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain_no_any(:'p'::jsonb); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain_no_any(:'p'::jsonb); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain_no_any(:'p'::jsonb); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain_no_any(:'p'::jsonb); ROLLBACK;

-- ============================================================================
-- 6b. SINGLE-CHUNK BENCHMARKS (last chunk only, 50 rows)
-- ============================================================================

\echo ''
\echo '=== 6a. Hypertable + ANY (single chunk) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;

\echo ''
\echo '=== 6b. Logical Partition + ANY (single partition) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;

\echo ''
\echo '=== 6c. Plain Table + ANY (single chunk baseline) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p1'::jsonb, :'t1'::timestamptz[]); ROLLBACK;

-- ============================================================================
-- 6d. BATCH UPDATE — 1 row, single chunk (jsonb batch with 1 element)
-- ============================================================================

\echo ''
\echo '=== 6d. Hypertable batch 1-row + ANY (single chunk) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;

\echo ''
\echo '=== 6e. Logical Partition batch 1-row + ANY (single partition) ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;

\echo ''
\echo '=== 6f. Plain Table batch 1-row + ANY ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain(:'p_single'::jsonb, :'t_single'::timestamptz[]); ROLLBACK;

-- ============================================================================
-- 6g. SINGLE-ROW UPDATE (no jsonb, plain WHERE time = $1)
-- ============================================================================

\echo ''
\echo '=== 6g. Hypertable single-row UPDATE ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts_single(:'single_time'); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts_single(:'single_time'); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts_single(:'single_time'); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts_single(:'single_time'); ROLLBACK;

\echo ''
\echo '=== 6h. Logical Partition single-row UPDATE ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg_single(:'single_time'); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg_single(:'single_time'); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg_single(:'single_time'); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg_single(:'single_time'); ROLLBACK;

\echo ''
\echo '=== 6i. Plain Table single-row UPDATE ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain_single(:'single_time'); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain_single(:'single_time'); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain_single(:'single_time'); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_plain_single(:'single_time'); ROLLBACK;

-- ============================================================================
-- 7. GENERIC PLAN tests
-- ============================================================================

SET plan_cache_mode = 'force_generic_plan';

\echo ''
\echo '=== 7. GENERIC PLAN: Hypertable + ANY ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_ts(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;

\echo ''
\echo '=== 8. GENERIC PLAN: Logical Partition + ANY ==='
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 1 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 2 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;
\echo '--- run 3 ---'
BEGIN; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE up_pg(:'p'::jsonb, :'t'::timestamptz[]); ROLLBACK;

SET plan_cache_mode = 'force_custom_plan';

-- ============================================================================
-- 8. CLEANUP
-- ============================================================================
-- DROP TABLE IF EXISTS metrics_ts CASCADE;
-- DROP TABLE IF EXISTS metrics_pg CASCADE;
-- DROP TABLE IF EXISTS metrics_plain CASCADE;

\echo ''
\echo 'Done. Compare Planning Time and Planning Buffers across runs.'
