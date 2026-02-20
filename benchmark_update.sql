-- ============================================================================
-- Benchmark: Batch UPDATE on Hypertable vs Logical Partition vs Plain Table
-- Table: metrics(id int, time timestamptz, name text, value float8)
-- Partition key: time (timestamptz), 200 chunks/partitions, 5000 rows each
--
-- Usage:
--   psql $TARGET -v plan_cache_mode=force_custom_plan -v warmup=1 -f benchmark_update.sql
--   psql $TARGET -v plan_cache_mode=force_generic_plan -v warmup=1 -f benchmark_update.sql
--   psql $TARGET -v plan_cache_mode=auto -v warmup=5 -f benchmark_update.sql
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
-- 4. Build payload (50 rows spread across last 5 partitions)
-- ============================================================================

SELECT jsonb_agg(
    jsonb_build_object(
        'where', jsonb_build_object('id', id, 'time', time::text),
        'set',   jsonb_build_object('value', 99.9, 'name', 'updated')
    )
)::text AS p
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
        'where', jsonb_build_object('id', id, 'time', time::text),
        'set',   jsonb_build_object('value', 99.9, 'name', 'updated')
    )
)::text AS p1
FROM (
    SELECT id, time FROM metrics_ts
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

SELECT id::text AS single_id, time::text AS single_time
FROM metrics_ts
WHERE time >= '2025-07-19'::timestamptz
  AND time <  '2025-07-20'::timestamptz
ORDER BY time LIMIT 1
\gset

SELECT jsonb_build_array(
    jsonb_build_object(
        'where', jsonb_build_object('id', single_id::int, 'time', single_time),
        'set',   jsonb_build_object('value', 99.9, 'name', 'updated')
    )
)::text AS p_single
FROM (SELECT :'single_id' AS single_id, :'single_time' AS single_time) x
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
SET (value, name) = (SELECT p.value, p.name FROM jsonb_populate_record(t, cdc->'set') AS p)
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_ts, cdc->'where') AS wc
WHERE t.id = wc.id AND t.time = wc.time AND t.time = ANY($2::timestamptz[]);

PREPARE up_ts_comp AS
UPDATE metrics_ts_comp AS t
SET (value, name) = (SELECT p.value, p.name FROM jsonb_populate_record(t, cdc->'set') AS p)
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_ts_comp, cdc->'where') AS wc
WHERE t.id = wc.id AND t.time = wc.time AND t.time = ANY($2::timestamptz[]);

PREPARE up_ts_comp_no_any AS
UPDATE metrics_ts_comp AS t
SET (value, name) = (SELECT p.value, p.name FROM jsonb_populate_record(t, cdc->'set') AS p)
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_ts_comp, cdc->'where') AS wc
WHERE t.id = wc.id AND t.time = wc.time;

PREPARE up_ts_comp_single AS
UPDATE metrics_ts_comp SET value = 99.9, name = 'updated' WHERE id = $1::int AND time = $2::timestamptz;

PREPARE up_ts_no_any AS
UPDATE metrics_ts AS t
SET (value, name) = (SELECT p.value, p.name FROM jsonb_populate_record(t, cdc->'set') AS p)
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_ts, cdc->'where') AS wc
WHERE t.id = wc.id AND t.time = wc.time;

PREPARE up_pg AS
UPDATE metrics_pg AS t
SET (value, name) = (SELECT p.value, p.name FROM jsonb_populate_record(t, cdc->'set') AS p)
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_pg, cdc->'where') AS wc
WHERE t.id = wc.id AND t.time = wc.time AND t.time = ANY($2::timestamptz[]);

PREPARE up_pg_no_any AS
UPDATE metrics_pg AS t
SET (value, name) = (SELECT p.value, p.name FROM jsonb_populate_record(t, cdc->'set') AS p)
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_pg, cdc->'where') AS wc
WHERE t.id = wc.id AND t.time = wc.time;

PREPARE up_plain AS
UPDATE metrics_plain AS t
SET (value, name) = (SELECT p.value, p.name FROM jsonb_populate_record(t, cdc->'set') AS p)
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_plain, cdc->'where') AS wc
WHERE t.id = wc.id AND t.time = wc.time AND t.time = ANY($2::timestamptz[]);

PREPARE up_plain_no_any AS
UPDATE metrics_plain AS t
SET (value, name) = (SELECT p.value, p.name FROM jsonb_populate_record(t, cdc->'set') AS p)
FROM jsonb_array_elements($1) AS cdc
CROSS JOIN LATERAL jsonb_populate_record(NULL::metrics_plain, cdc->'where') AS wc
WHERE t.id = wc.id AND t.time = wc.time;

PREPARE up_ts_single AS
UPDATE metrics_ts SET value = 99.9, name = 'updated' WHERE id = $1::int AND time = $2::timestamptz;

PREPARE up_pg_single AS
UPDATE metrics_pg SET value = 99.9, name = 'updated' WHERE id = $1::int AND time = $2::timestamptz;

PREPARE up_plain_single AS
UPDATE metrics_plain SET value = 99.9, name = 'updated' WHERE id = $1::int AND time = $2::timestamptz;

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

SELECT * FROM run_bench('1. HT batch (5 chunks)',
  format('EXECUTE up_ts(%L::jsonb, %L::timestamptz[])', :'p'::jsonb, :'t'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('1b. Compressed HT batch (5 chunks)',
  format('EXECUTE up_ts_comp(%L::jsonb, %L::timestamptz[])', :'p'::jsonb, :'t'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('1c. Compressed HT NO ANY (all chunks)',
  format('EXECUTE up_ts_comp_no_any(%L::jsonb)', :'p'::jsonb),
  :'warmup'::int);

SELECT * FROM run_bench('2. HT NO ANY (all chunks)',
  format('EXECUTE up_ts_no_any(%L::jsonb)', :'p'::jsonb),
  :'warmup'::int);

SELECT * FROM run_bench('3. Logical Partition + ANY (5 parts)',
  format('EXECUTE up_pg(%L::jsonb, %L::timestamptz[])', :'p'::jsonb, :'t'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('4. Logical Partition NO ANY',
  format('EXECUTE up_pg_no_any(%L::jsonb)', :'p'::jsonb),
  :'warmup'::int);

SELECT * FROM run_bench('5. Plain Table + ANY',
  format('EXECUTE up_plain(%L::jsonb, %L::timestamptz[])', :'p'::jsonb, :'t'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('6. Plain Table NO ANY (baseline)',
  format('EXECUTE up_plain_no_any(%L::jsonb)', :'p'::jsonb),
  :'warmup'::int);

-- Single-chunk benchmarks (last chunk only, 50 rows)

SELECT * FROM run_bench('6a. HT + ANY (single chunk)',
  format('EXECUTE up_ts(%L::jsonb, %L::timestamptz[])', :'p1'::jsonb, :'t1'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('6a2. Compressed HT + ANY (single chunk)',
  format('EXECUTE up_ts_comp(%L::jsonb, %L::timestamptz[])', :'p1'::jsonb, :'t1'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('6b. Logical Partition + ANY (single part)',
  format('EXECUTE up_pg(%L::jsonb, %L::timestamptz[])', :'p1'::jsonb, :'t1'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('6c. Plain Table + ANY (single chunk baseline)',
  format('EXECUTE up_plain(%L::jsonb, %L::timestamptz[])', :'p1'::jsonb, :'t1'::timestamptz[]),
  :'warmup'::int);

-- Batch 1-row + ANY (single chunk)

SELECT * FROM run_bench('6d. HT batch 1-row + ANY (single chunk)',
  format('EXECUTE up_ts(%L::jsonb, %L::timestamptz[])', :'p_single'::jsonb, :'t_single'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('6d2. Compressed HT batch 1-row + ANY (single chunk)',
  format('EXECUTE up_ts_comp(%L::jsonb, %L::timestamptz[])', :'p_single'::jsonb, :'t_single'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('6e. Logical Partition batch 1-row + ANY (single part)',
  format('EXECUTE up_pg(%L::jsonb, %L::timestamptz[])', :'p_single'::jsonb, :'t_single'::timestamptz[]),
  :'warmup'::int);

SELECT * FROM run_bench('6f. Plain Table batch 1-row + ANY',
  format('EXECUTE up_plain(%L::jsonb, %L::timestamptz[])', :'p_single'::jsonb, :'t_single'::timestamptz[]),
  :'warmup'::int);

-- Single-row UPDATE (no jsonb, plain WHERE)

SELECT * FROM run_bench('6g. HT single-row UPDATE',
  format('EXECUTE up_ts_single(%L::int, %L::timestamptz)', :'single_id', :'single_time'),
  :'warmup'::int);

SELECT * FROM run_bench('6g2. Compressed HT single-row UPDATE',
  format('EXECUTE up_ts_comp_single(%L::int, %L::timestamptz)', :'single_id', :'single_time'),
  :'warmup'::int);

SELECT * FROM run_bench('6h. Logical Partition single-row UPDATE',
  format('EXECUTE up_pg_single(%L::int, %L::timestamptz)', :'single_id', :'single_time'),
  :'warmup'::int);

SELECT * FROM run_bench('6i. Plain Table single-row UPDATE',
  format('EXECUTE up_plain_single(%L::int, %L::timestamptz)', :'single_id', :'single_time'),
  :'warmup'::int);

-- ============================================================================
-- 8. CLEANUP
-- ============================================================================
-- DROP TABLE IF EXISTS metrics_ts CASCADE;
-- DROP TABLE IF EXISTS metrics_pg CASCADE;
-- DROP TABLE IF EXISTS metrics_plain CASCADE;

\echo ''
\echo 'Done. Compare Planning Time and Planning Buffers across runs.'
