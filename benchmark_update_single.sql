-- ============================================================================
-- Benchmark: Single-row UPDATE loop on Hypertable vs Logical Partition vs Plain
-- Executes 50 individual single-row UPDATEs in a loop (realistic CDC pattern).
-- Scenarios: 50 rows across 5 chunks, 50 rows within 1 chunk.
--
-- Usage:
--   psql $TARGET -v plan_cache_mode=force_custom_plan -v warmup=1 -f benchmark_update_single.sql
--   psql $TARGET -v plan_cache_mode=force_generic_plan -v warmup=1 -f benchmark_update_single.sql
--   psql $TARGET -v plan_cache_mode=auto -v warmup=5 -f benchmark_update_single.sql
-- ============================================================================

\timing on
\pset pager off

\if :{?plan_cache_mode}
\else
\set plan_cache_mode force_custom_plan
\endif
\if :{?warmup}
\else
\set warmup 1
\endif

-- ============================================================================
-- 0. SETUP: Source data
-- ============================================================================

DROP TABLE IF EXISTS _source_data;

CREATE TEMP TABLE _source_data AS
SELECT
    (g - 1) % 50                               AS id,
    '2025-01-01'::timestamptz
        + ((g - 1) / 5000) * INTERVAL '1 day'
        + ((g - 1) % 5000) * INTERVAL '1 second' AS time,
    'sensor_' || ((g - 1) % 50)                AS name,
    (50 + 10 * sin(g::float / 1000) + ((g - 1) % 50) * 0.5)::int::float8 AS value
FROM generate_series(1, 1000000) g;

-- ============================================================================
-- 1. SETUP: Hypertable
-- ============================================================================

DROP TABLE IF EXISTS metrics_ts CASCADE;

CREATE TABLE metrics_ts (
    id    int          NOT NULL,
    time  timestamptz  NOT NULL,
    name  text         NOT NULL,
    value float8       NOT NULL,
    PRIMARY KEY (id, time)
);

SELECT create_hypertable('metrics_ts', by_range('time', INTERVAL '1 day'));

INSERT INTO metrics_ts SELECT * FROM _source_data;

SELECT count(*) AS ts_chunks
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_ts';

ANALYZE metrics_ts;

-- ============================================================================
-- 1b. SETUP: Compressed Hypertable
-- ============================================================================

DROP TABLE IF EXISTS metrics_ts_comp CASCADE;

CREATE TABLE metrics_ts_comp (
    id    int          NOT NULL,
    time  timestamptz  NOT NULL,
    name  text         NOT NULL,
    value float8       NOT NULL,
    PRIMARY KEY (id, time)
);

SELECT create_hypertable('metrics_ts_comp', by_range('time', INTERVAL '1 day'));

INSERT INTO metrics_ts_comp SELECT * FROM _source_data;

SELECT count(*) AS ts_comp_chunks
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_ts_comp';

ANALYZE metrics_ts_comp;

ALTER TABLE metrics_ts_comp SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'id',
    timescaledb.compress_orderby = 'time'
);

SELECT compress_chunk(c.chunk_schema || '.' || c.chunk_name)
FROM timescaledb_information.chunks c
WHERE c.hypertable_name = 'metrics_ts_comp'
  AND c.range_start < '2025-07-15'::timestamptz
ORDER BY c.range_start;

SELECT count(*) AS compressed_chunks
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_ts_comp'
  AND is_compressed = true;

SELECT count(*) AS uncompressed_chunks
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_ts_comp'
  AND is_compressed = false;

-- ============================================================================
-- 2. SETUP: Logical Partition
-- ============================================================================

DROP TABLE IF EXISTS metrics_pg CASCADE;

CREATE TABLE metrics_pg (
    id    int          NOT NULL,
    time  timestamptz  NOT NULL,
    name  text         NOT NULL,
    value float8       NOT NULL,
    PRIMARY KEY (id, time)
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
    value float8       NOT NULL,
    PRIMARY KEY (id, time)
);

INSERT INTO metrics_plain SELECT * FROM metrics_ts;
ANALYZE metrics_plain;

DROP TABLE _source_data;

-- ============================================================================
-- 4. Build payloads as id[] and time[] arrays
-- ============================================================================

-- 50 rows spread across 5 chunks (10 per chunk)
SELECT array_agg(id)::text AS ids_5, array_agg(time)::text AS times_5
FROM (
    SELECT m.id, m.time FROM generate_series(0, 4) d
    CROSS JOIN LATERAL (
        SELECT id, time FROM metrics_ts
        WHERE time >= '2025-07-15'::timestamptz + d * INTERVAL '1 day'
          AND time <  '2025-07-15'::timestamptz + (d + 1) * INTERVAL '1 day'
        ORDER BY time LIMIT 10
    ) m
) sub
\gset

-- 50 rows from last chunk only
SELECT array_agg(id)::text AS ids_1, array_agg(time)::text AS times_1
FROM (
    SELECT id, time FROM metrics_ts
    WHERE time >= '2025-07-19'::timestamptz
      AND time <  '2025-07-20'::timestamptz
    ORDER BY time LIMIT 50
) sub
\gset

\echo ''
\echo '========================================='
\echo 'Payload: 50 rows x 5 chunks, 50 rows x 1 chunk'
\echo '========================================='

-- ============================================================================
-- 5. PREPARE single-row UPDATE statements
-- ============================================================================

PREPARE up_ts_single AS
UPDATE metrics_ts SET value = 99.9, name = 'updated' WHERE id = $1::int AND time = $2::timestamptz;

PREPARE up_ts_comp_single AS
UPDATE metrics_ts_comp SET value = 99.9, name = 'updated' WHERE id = $1::int AND time = $2::timestamptz;

PREPARE up_pg_single AS
UPDATE metrics_pg SET value = 99.9, name = 'updated' WHERE id = $1::int AND time = $2::timestamptz;

PREPARE up_plain_single AS
UPDATE metrics_plain SET value = 99.9, name = 'updated' WHERE id = $1::int AND time = $2::timestamptz;

-- ============================================================================
-- 6. run_bench_loop: loop single-row updates, EXPLAIN first + execute rest
-- ============================================================================

CREATE OR REPLACE FUNCTION run_bench_loop(
  label         text,
  stmt_name     text,
  ids           int[],
  times         timestamptz[],
  warmup_count  int DEFAULT 1,
  measured_runs int DEFAULT 3
) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
  i    int;
  j    int;
  n    int := array_length(ids, 1);
  line text;
  t0   timestamptz;
BEGIN
  RETURN NEXT '';
  RETURN NEXT format('=== %s (%s rows) ===', label, n);

  FOR i IN 1..warmup_count LOOP
    BEGIN
      FOR j IN 1..n LOOP
        EXECUTE format('EXECUTE %s(%s, %L)', stmt_name, ids[j], times[j]);
      END LOOP;
      RAISE EXCEPTION 'bench_rollback';
    EXCEPTION WHEN raise_exception THEN END;
  END LOOP;

  FOR i IN 1..measured_runs LOOP
    RETURN NEXT format('--- run %s ---', i);
    BEGIN
      t0 := clock_timestamp();
      FOR line IN EXECUTE format(
        'EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE %s(%s, %L)',
        stmt_name, ids[1], times[1]
      ) LOOP
        RETURN NEXT line;
      END LOOP;
      FOR j IN 2..n LOOP
        EXECUTE format('EXECUTE %s(%s, %L)', stmt_name, ids[j], times[j]);
      END LOOP;
      RETURN NEXT format('Total loop (%s rows): %s ms',
        n, round(extract(epoch from clock_timestamp() - t0) * 1000, 3));
      RAISE EXCEPTION 'bench_rollback';
    EXCEPTION WHEN raise_exception THEN END;
  END LOOP;
END;
$$;

SET plan_cache_mode = :'plan_cache_mode';

\echo ''
\echo '============================================================'
\echo 'plan_cache_mode = ' :plan_cache_mode ', warmup = ' :warmup
\echo '============================================================'

-- ============================================================================
-- 7. BENCHMARKS: 50 single-row UPDATEs across 5 chunks
-- ============================================================================

SELECT * FROM run_bench_loop('HT single-row x50 (5 chunks)',
  'up_ts_single', :'ids_5'::int[], :'times_5'::timestamptz[],
  :'warmup'::int);

SELECT * FROM run_bench_loop('Compressed HT single-row x50 (5 chunks)',
  'up_ts_comp_single', :'ids_5'::int[], :'times_5'::timestamptz[],
  :'warmup'::int);

SELECT * FROM run_bench_loop('Logical Partition single-row x50 (5 chunks)',
  'up_pg_single', :'ids_5'::int[], :'times_5'::timestamptz[],
  :'warmup'::int);

SELECT * FROM run_bench_loop('Plain Table single-row x50',
  'up_plain_single', :'ids_5'::int[], :'times_5'::timestamptz[],
  :'warmup'::int);

-- ============================================================================
-- 8. BENCHMARKS: 50 single-row UPDATEs within 1 chunk
-- ============================================================================

SELECT * FROM run_bench_loop('HT single-row x50 (1 chunk)',
  'up_ts_single', :'ids_1'::int[], :'times_1'::timestamptz[],
  :'warmup'::int);

SELECT * FROM run_bench_loop('Compressed HT single-row x50 (1 chunk)',
  'up_ts_comp_single', :'ids_1'::int[], :'times_1'::timestamptz[],
  :'warmup'::int);

SELECT * FROM run_bench_loop('Logical Partition single-row x50 (1 chunk)',
  'up_pg_single', :'ids_1'::int[], :'times_1'::timestamptz[],
  :'warmup'::int);

SELECT * FROM run_bench_loop('Plain Table single-row x50 (1 chunk)',
  'up_plain_single', :'ids_1'::int[], :'times_1'::timestamptz[],
  :'warmup'::int);

-- ============================================================================
-- 9. CLEANUP
-- ============================================================================

\echo ''
\echo 'Done. Compare per-row plan and total loop time across table types.'
