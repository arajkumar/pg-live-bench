-- ============================================================================
-- Benchmark: INSERT on Hypertable vs Logical Partition vs Plain Table
-- Table: metrics(id int, time timestamptz, name text, value float8)
-- Partition key: time (timestamptz), 200 chunks/partitions, 5000 rows each
--
-- Usage:
--   psql $TARGET -v plan_cache_mode=force_custom_plan -v warmup=1 -f benchmark_insert.sql
--   psql $TARGET -v plan_cache_mode=force_generic_plan -v warmup=1 -f benchmark_insert.sql
--   psql $TARGET -v plan_cache_mode=auto -v warmup=5 -f benchmark_insert.sql
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
-- 0. SETUP: Source data (realistic time-series: 50 sensors, slowly changing values)
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

-- Extra partitions for INSERT payloads (2026-01-01..2026-01-05, 2026-02-01, 2026-03-01)
DO $$
BEGIN
    FOR i IN 0..4 LOOP
        EXECUTE format(
            'CREATE TABLE metrics_pg_ins%s PARTITION OF metrics_pg FOR VALUES FROM (%L) TO (%L)',
            i, ('2026-01-01'::date + i)::text, ('2026-01-01'::date + i + 1)::text
        );
    END LOOP;
END $$;

CREATE TABLE metrics_pg_ins_feb PARTITION OF metrics_pg
    FOR VALUES FROM ('2026-02-01') TO ('2026-02-02');
CREATE TABLE metrics_pg_ins_mar PARTITION OF metrics_pg
    FOR VALUES FROM ('2026-03-01') TO ('2026-03-02');

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
-- 4. Build INSERT payloads (rows that don't conflict with existing PKs)
-- ============================================================================

-- Batch multi-chunk: 50 rows across 5 new day boundaries (2026-01-01..2026-01-05)
SELECT jsonb_agg(row_to_json(sub)::jsonb)::text AS ins_p
FROM (
    SELECT
        (g - 1) % 10 AS id,
        '2026-01-01'::timestamptz + ((g - 1) / 10) * INTERVAL '1 day' + ((g - 1) % 10) * INTERVAL '1 second' AS time,
        'sensor_' || ((g - 1) % 10) AS name,
        (50 + (g % 100) * 0.1)::float8 AS value
    FROM generate_series(1, 50) g
) sub
\gset

-- Batch single-chunk: 50 rows within a single day (2026-02-01)
SELECT jsonb_agg(row_to_json(sub)::jsonb)::text AS ins_p1
FROM (
    SELECT
        (g - 1) % 50 AS id,
        '2026-02-01'::timestamptz + (g - 1) * INTERVAL '1 second' AS time,
        'sensor_' || ((g - 1) % 50) AS name,
        (50 + (g % 100) * 0.1)::float8 AS value
    FROM generate_series(1, 50) g
) sub
\gset

-- Single-row scalar values
SELECT 0::text AS ins_single_id \gset
SELECT '2026-03-01 00:00:00+00'::text AS ins_single_time \gset
SELECT 'sensor_0'::text AS ins_single_name \gset
SELECT '55.5'::text AS ins_single_value \gset

\echo ''
\echo '========================================='
\echo 'Payload: 50 rows across 5 chunks (ins_p)'
\echo '========================================='

\echo ''
\echo '========================================='
\echo 'Payload: 50 rows single chunk (ins_p1)'
\echo '========================================='

-- ============================================================================
-- 5. PREPARE statements
-- ============================================================================

-- Batch INSERT from jsonb (single $1 param)
PREPARE ins_ts      AS INSERT INTO metrics_ts      SELECT * FROM jsonb_populate_recordset(NULL::metrics_ts, $1);
PREPARE ins_ts_comp AS INSERT INTO metrics_ts_comp SELECT * FROM jsonb_populate_recordset(NULL::metrics_ts_comp, $1);
PREPARE ins_pg      AS INSERT INTO metrics_pg      SELECT * FROM jsonb_populate_recordset(NULL::metrics_pg, $1);
PREPARE ins_plain   AS INSERT INTO metrics_plain   SELECT * FROM jsonb_populate_recordset(NULL::metrics_plain, $1);

-- Single-row INSERT
PREPARE ins_ts_single      AS INSERT INTO metrics_ts      VALUES ($1::int, $2::timestamptz, $3::text, $4::float8);
PREPARE ins_ts_comp_single AS INSERT INTO metrics_ts_comp VALUES ($1::int, $2::timestamptz, $3::text, $4::float8);
PREPARE ins_pg_single      AS INSERT INTO metrics_pg      VALUES ($1::int, $2::timestamptz, $3::text, $4::float8);
PREPARE ins_plain_single   AS INSERT INTO metrics_plain   VALUES ($1::int, $2::timestamptz, $3::text, $4::float8);

-- ============================================================================
-- 6. run_bench function + plan_cache_mode
-- ============================================================================

CREATE OR REPLACE FUNCTION run_bench(
  label         text,
  exec_sql      text,
  warmup_count  int DEFAULT 1,
  measured_runs int DEFAULT 3
) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
  i    int;
  line text;
BEGIN
  RETURN NEXT '';
  RETURN NEXT format('=== %s ===', label);

  FOR i IN 1..warmup_count LOOP
    BEGIN
      EXECUTE exec_sql;
      RAISE EXCEPTION 'bench_rollback';
    EXCEPTION WHEN raise_exception THEN END;
  END LOOP;

  FOR i IN 1..measured_runs LOOP
    RETURN NEXT format('--- run %s ---', i);
    BEGIN
      FOR line IN EXECUTE format('EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) %s', exec_sql) LOOP
        RETURN NEXT line;
      END LOOP;
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
-- 7. BENCHMARKS
-- ============================================================================

-- Batch multi-chunk
SELECT * FROM run_bench(
  '1. HT batch (5 chunks)',
  format('EXECUTE ins_ts(%L::jsonb)', :'ins_p'::jsonb),
  :'warmup'::int);

SELECT * FROM run_bench(
  '1b. Compressed HT batch (5 chunks)',
  format('EXECUTE ins_ts_comp(%L::jsonb)', :'ins_p'::jsonb),
  :'warmup'::int);

SELECT * FROM run_bench(
  '2. Logical Partition batch (5 parts)',
  format('EXECUTE ins_pg(%L::jsonb)', :'ins_p'::jsonb),
  :'warmup'::int);

SELECT * FROM run_bench(
  '3. Plain Table batch',
  format('EXECUTE ins_plain(%L::jsonb)', :'ins_p'::jsonb),
  :'warmup'::int);

-- Batch single-chunk
SELECT * FROM run_bench(
  '4. HT single-chunk batch',
  format('EXECUTE ins_ts(%L::jsonb)', :'ins_p1'::jsonb),
  :'warmup'::int);

SELECT * FROM run_bench(
  '4b. Compressed HT single-chunk batch',
  format('EXECUTE ins_ts_comp(%L::jsonb)', :'ins_p1'::jsonb),
  :'warmup'::int);

SELECT * FROM run_bench(
  '5. Logical Partition single-part batch',
  format('EXECUTE ins_pg(%L::jsonb)', :'ins_p1'::jsonb),
  :'warmup'::int);

SELECT * FROM run_bench(
  '6. Plain Table single-chunk batch',
  format('EXECUTE ins_plain(%L::jsonb)', :'ins_p1'::jsonb),
  :'warmup'::int);

-- Single-row
SELECT * FROM run_bench(
  '7. HT single-row',
  format('EXECUTE ins_ts_single(%L::int, %L::timestamptz, %L::text, %L::float8)',
    :'ins_single_id', :'ins_single_time', :'ins_single_name', :'ins_single_value'),
  :'warmup'::int);

SELECT * FROM run_bench(
  '7b. Compressed HT single-row',
  format('EXECUTE ins_ts_comp_single(%L::int, %L::timestamptz, %L::text, %L::float8)',
    :'ins_single_id', :'ins_single_time', :'ins_single_name', :'ins_single_value'),
  :'warmup'::int);

SELECT * FROM run_bench(
  '8. Logical Partition single-row',
  format('EXECUTE ins_pg_single(%L::int, %L::timestamptz, %L::text, %L::float8)',
    :'ins_single_id', :'ins_single_time', :'ins_single_name', :'ins_single_value'),
  :'warmup'::int);

SELECT * FROM run_bench(
  '9. Plain Table single-row',
  format('EXECUTE ins_plain_single(%L::int, %L::timestamptz, %L::text, %L::float8)',
    :'ins_single_id', :'ins_single_time', :'ins_single_name', :'ins_single_value'),
  :'warmup'::int);

-- ============================================================================
-- 8. CLEANUP
-- ============================================================================

\echo ''
\echo 'Done. Compare Planning Time and Planning Buffers across runs.'
