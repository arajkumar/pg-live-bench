package main

import (
	"os/signal"
	"context"
	"flag"
	"strings"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	totalRecords = 10000
)

type config struct {
	seed          int64
	dbURL        string
	tableName    string
	schemaName  string
	scale         int
	planCacheMode string
	minUpdatePct float64
	maxUpdatePct float64
	mode          string
}

type executor interface {
	execute(ctx context.Context, conn *pgx.Conn)
}

func main() {
	var cfg config
	flag.Int64Var(&cfg.seed, "seed", 20672067, "Random seed for reproducibility")
	flag.StringVar(&cfg.dbURL, "db", "", "PostgreSQL connection URL")
	flag.StringVar(&cfg.schemaName, "schema", "public", "Schema name to use for the benchmark")
	flag.StringVar(&cfg.tableName, "table", "metrics", "Table name to use for the benchmark")
	flag.IntVar(&cfg.scale, "scale", 1, "Scale factor for the number of records (default is 1x, 10000 records)")
	flag.StringVar(&cfg.planCacheMode, "plan_cache_mode", "auto", "Plan cache mode to use for the connection")
	flag.Float64Var(&cfg.minUpdatePct, "min_update_pct", 0.1, "Minimum percentage of updates in the batch (default is 0.1)")
	flag.Float64Var(&cfg.maxUpdatePct, "max_update_pct", 0.3, "Maximum percentage of updates in the batch (default is 0.3)")
	flag.StringVar(&cfg.mode, "mode", "multi_value", "Execution mode: 'multi_value' or 'simple_pgx_batch'")

	flag.Parse()

	rand.Seed(cfg.seed)
	fmt.Println("Seed:", cfg.seed, "table:", cfg.tableName, "scale:", cfg.scale)

	ctx := context.Background()
	// set application name for the connection
	config, err := pgx.ParseConfig(cfg.dbURL)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse database URL: %v", err))
	}

	config.RuntimeParams["application_name"] = "live-sync-bench"

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect: %v", err))
	}
	defer conn.Close(ctx) //

	createTable(ctx, conn, cfg)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	ctx, cancel := context.WithCancel(ctx)
	// cancel on ctrl +c
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		cancel()
		fmt.Println("Shutting down...")
	}()

	var exec executor
	switch cfg.mode {
	case "multi_value":
		exec = multiValueExecutor{cfg: cfg}
	case "simple_pgx_batch":
		exec = simplePgxBatchExecutor{cfg: cfg}
	case "unnest":
		exec = unnestExecutor{cfg: cfg}
	case "temp_table":
		exec = tempTableExecutor{cfg: cfg}
	case "temp_json":
		exec = tempJsonExecutor{cfg: cfg}
	case "temp_json_record":
		exec = tempJsonRecordExecutor{cfg: cfg}
	default:
		panic(fmt.Sprintf("Unknown mode: %s", cfg.mode))
	}

	for {
		select {
		case <-ticker.C:
			exec.execute(ctx, conn)
		case <-ctx.Done():
			fmt.Println("Exiting due to context cancellation")
			return
		}
	}
}

func createTable(ctx context.Context, conn *pgx.Conn, cfg config) {
	ddl := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id integer,
			time timestamptz,
			name text,
			value numeric,
			seq_id bigserial,
			PRIMARY KEY (time, seq_id)
		);
		CREATE INDEX IF NOT EXISTS %[2]s ON %[1]s USING btree(time);`,
		pgx.Identifier{cfg.schemaName, cfg.tableName}.Sanitize(),
		pgx.Identifier{cfg.tableName + "_time_idx"}.Sanitize(),
	)

	_, err := conn.Exec(ctx, ddl)
	if err != nil {
		panic(fmt.Sprintf("Failed to create table: %v", err))
	}
}

func (c config) ops() (totalOps, numInserts, numUpdates int) {
	updatePct := c.minUpdatePct + rand.Float64()*(c.maxUpdatePct-c.minUpdatePct)

	totalRecords := c.scale * totalRecords

	numUpdates = int(float64(totalRecords) * updatePct)
	numInserts = totalRecords - numUpdates

	totalOps = numInserts + numUpdates
	return totalOps, numInserts, numUpdates
}

func (c config) relName() string {
	return pgx.Identifier{c.schemaName, c.tableName}.Sanitize()
}

// Get max seq_id to use for updates
type updateInfo struct {
	SeqID int64 `db:"seq_id"`
	Time time.Time `db:"time"`
}

func (c config) updateRecords(
	ctx context.Context, conn *pgx.Conn, numUpdates int,
) []updateInfo {
	queryUpdateTime := time.Now()

	// generic plan after inserting and updates is slow, so we force custom plan
	_, err := conn.Exec(ctx, "SET plan_cache_mode = 'force_custom_plan'")
	if err != nil {
		fmt.Printf("Failed to set plan cache mode: %v\n", err)
		return nil
	}

	relName := pgx.Identifier{c.schemaName, c.tableName}.Sanitize()
	maxQuery := fmt.Sprintf("SELECT seq_id, time FROM %s ORDER BY time DESC LIMIT $1", relName)
	rows, err := conn.Query(ctx, maxQuery, numUpdates)
	if err != nil {
		fmt.Printf("Failed to get max seq_id: %v\n", err)
		return nil
	}
	defer rows.Close()

	updates, err := pgx.CollectRows(rows, pgx.RowToStructByName[updateInfo])
	if err != nil {
		fmt.Printf("Failed to collect rows: %v\n", err)
		return nil
	}
	fmt.Printf("Collected %d for update in %v\n", len(updates), time.Since(queryUpdateTime))
	return updates
}

type tempJsonRecordExecutor struct {
	cfg config
}

func (e tempJsonRecordExecutor) execute(ctx context.Context, conn *pgx.Conn) {
	cfg := e.cfg

	batch := &pgx.Batch{}

	batch.Queue(
		fmt.Sprintf("SET plan_cache_mode = '%s'", cfg.planCacheMode),
	)

	totalOps, numInserts, numUpdates := cfg.ops()

	updates := cfg.updateRecords(ctx, conn, numUpdates)

	// Create a temporary table for the batch
	tempTableDDL := `
		CREATE TEMP TABLE IF NOT EXISTS cdc_stagging (
			id bigserial, -- this should be the primary key for operation ordering
			dest_table NAME,
			op varchar(1),
			payload jsonb,
			where_clause jsonb
		);
		TRUNCATE TABLE cdc_stagging;
		-- CREATE INDEX IF NOT EXISTS cdc_stagging_op_idx ON cdc_stagging (op);
		`

	_, err := conn.Exec(ctx, tempTableDDL)
	if err != nil {
		fmt.Printf("Failed to create temporary table: %v\n", err)
		return
	}


	// account copy time as well
	start := time.Now()

	rowsInserted := 0
	copyFromFunc := func() (row []any, err error) {
		if rowsInserted >= numInserts {
			return nil, nil
		}
		rowsInserted++
		return []any{
			cfg.relName(), // dest_table
			"I", // op
			map[string]any{
				"id":    rand.Intn(10000),
				"time":  time.Now(),
				"name":  fmt.Sprintf("metric_%d", rand.Intn(1000)),
				"value":  rand.Float64() * 100,
			}, // payload
		}, nil
	}

	_, err = conn.CopyFrom(ctx, pgx.Identifier{"cdc_stagging"}, []string{"dest_table", "op", "payload"}, pgx.CopyFromFunc(copyFromFunc))
	if err != nil {
		fmt.Printf("Failed to copy data into temporary table: %v\n", err)
		return
	}


	rowsUpdated := 0
	copyFromUpdateFunc := func() (row []any, err error) {
		if rowsUpdated >= len(updates) {
			return nil, nil
		}
		rowsUpdated++
		return []any {
			cfg.relName(), // dest_table
			"U", // op
			map[string]any{
				"value": rand.Float64() * 100000, // Random value for update
			}, // payload
			map[string]any{
				"time":  updates[rowsUpdated - 1].Time,
				"seq_id": updates[rowsUpdated - 1].SeqID,
			},
		}, nil
	}

	_, err = conn.CopyFrom(ctx, pgx.Identifier{"cdc_stagging"}, []string{"dest_table", "op", "payload", "where_clause"}, pgx.CopyFromFunc(copyFromUpdateFunc))
	if err != nil {
		fmt.Printf("Failed to copy data for updates into temporary table: %v\n", err)
		return
	}


	insertSQL := fmt.Sprintf(`
	INSERT INTO %[1]s (id, time, name, value) OVERRIDING SYSTEM VALUE
	SELECT p.id, p.time, p.name, p.value FROM cdc_stagging AS v
	CROSS JOIN LATERAL
  jsonb_populate_record(NULL::%[1]s, v.payload)      AS p
	WHERE v.op = 'I';
	`,cfg.relName(),
	)

	batch.Queue(
		insertSQL,
	)

	// updates
	updateSQL := fmt.Sprintf(`
	UPDATE %[1]s AS t
SET value = p.value
FROM cdc_stagging AS v
CROSS JOIN LATERAL
  jsonb_populate_record(NULL::%[1]s, v.payload)      AS p
CROSS JOIN LATERAL
  jsonb_populate_record(NULL::%[1]s, v.where_clause) AS w
WHERE v.op = 'U'
  AND t.seq_id   = w.seq_id
  AND t.time = w.time;
	`,
		cfg.relName(),
	)

	batch.Queue(
		updateSQL,
	)


	fmt.Printf("Queued batch with %d inserts and %d updates at %s\n", numInserts, len(updates), time.Now().Format(time.RFC3339))
	err = conn.SendBatch(ctx, batch).Close()
	if	err != nil {
		fmt.Printf("Failed to execute batch: %v\n", err)
		return
	}
	duration := time.Since(start).Seconds()
	if duration > 0 {
		rate := float64(totalOps) / duration
		fmt.Printf("Executed %d inserts and %d updates in %.2f seconds (%.2f records/sec)\n",numInserts, numUpdates, duration, rate)
	}
}

type tempJsonExecutor struct {
	cfg config
}

func (e tempJsonExecutor) execute(ctx context.Context, conn *pgx.Conn) {
	cfg := e.cfg

	batch := &pgx.Batch{}

	batch.Queue(
		fmt.Sprintf("SET plan_cache_mode = '%s'", cfg.planCacheMode),
	)

	totalOps, numInserts, numUpdates := cfg.ops()

	updates := cfg.updateRecords(ctx, conn, numUpdates)

	// Create a temporary table for the batch
	tempTableDDL := `
		CREATE TEMP TABLE IF NOT EXISTS cdc_stagging (
			id bigserial, -- this should be the primary key for operation ordering
			dest_table NAME,
			op varchar(1),
			payload jsonb,
			where_clause jsonb
		);
		TRUNCATE TABLE cdc_stagging;
		-- CREATE INDEX IF NOT EXISTS cdc_stagging_op_idx ON cdc_stagging (op);
		`

	_, err := conn.Exec(ctx, tempTableDDL)
	if err != nil {
		fmt.Printf("Failed to create temporary table: %v\n", err)
		return
	}


	// account copy time as well
	start := time.Now()

	rowsInserted := 0
	copyFromFunc := func() (row []any, err error) {
		if rowsInserted >= numInserts {
			return nil, nil
		}
		rowsInserted++
		return []any{
			cfg.relName(), // dest_table
			"I", // op
			map[string]any{
				"id":    rand.Intn(10000),
				"time":  time.Now(),
				"name":  fmt.Sprintf("metric_%d", rand.Intn(1000)),
				"value":  rand.Float64() * 100,
			}, // payload
		}, nil
	}

	_, err = conn.CopyFrom(ctx, pgx.Identifier{"cdc_stagging"}, []string{"dest_table", "op", "payload"}, pgx.CopyFromFunc(copyFromFunc))
	if err != nil {
		fmt.Printf("Failed to copy data into temporary table: %v\n", err)
		return
	}


	rowsUpdated := 0
	copyFromUpdateFunc := func() (row []any, err error) {
		if rowsUpdated >= len(updates) {
			return nil, nil
		}
		rowsUpdated++
		return []any {
			cfg.relName(), // dest_table
			"U", // op
			map[string]any{
				"value": rand.Float64() * 100000, // Random value for update
			}, // payload
			map[string]any{
				"time":  updates[rowsUpdated - 1].Time,
				"seq_id": updates[rowsUpdated - 1].SeqID,
			},
		}, nil
	}

	_, err = conn.CopyFrom(ctx, pgx.Identifier{"cdc_stagging"}, []string{"dest_table", "op", "payload", "where_clause"}, pgx.CopyFromFunc(copyFromUpdateFunc))
	if err != nil {
		fmt.Printf("Failed to copy data for updates into temporary table: %v\n", err)
		return
	}


	insertSQL := fmt.Sprintf(
		`INSERT INTO %s (id, time, name, value)
		SELECT (payload->>'id')::integer, (payload->>'time')::timestamptz, (payload->>'name')::text, (payload->>'value')::numeric
		FROM cdc_stagging
		WHERE op = 'I'`,
		cfg.relName(),
	)

	batch.Queue(
		insertSQL,
	)

	// updates
	updateSQL := fmt.Sprintf(
		`UPDATE %[1]s AS t SET value = (v.payload->>'value')::numeric
		FROM cdc_stagging AS v
		WHERE t.time = (v.where_clause->>'time')::timestamptz AND t.seq_id = (v.where_clause->>'seq_id')::bigint
		AND v.op = 'U'
	`,
		cfg.relName(),
	)

	batch.Queue(
		updateSQL,
	)


	fmt.Printf("Queued batch with %d inserts and %d updates at %s\n", numInserts, len(updates), time.Now().Format(time.RFC3339))
	err = conn.SendBatch(ctx, batch).Close()
	if	err != nil {
		fmt.Printf("Failed to execute batch: %v\n", err)
		return
	}
	duration := time.Since(start).Seconds()
	if duration > 0 {
		rate := float64(totalOps) / duration
		fmt.Printf("Executed %d inserts and %d updates in %.2f seconds (%.2f records/sec)\n",numInserts, numUpdates, duration, rate)
	}
}
type tempTableExecutor struct {
	cfg config
}

func (e tempTableExecutor) execute(ctx context.Context, conn *pgx.Conn) {
	cfg := e.cfg

	batch := &pgx.Batch{}
	totalOps, numInserts, numUpdates := cfg.ops()

	updates := cfg.updateRecords(ctx, conn, numUpdates)

	batch.Queue(
		fmt.Sprintf("SET plan_cache_mode = '%s'", cfg.planCacheMode),
	)

	// Create a temporary table for the batch
	tempTableName := "tmp_" + cfg.tableName
	tempTableDDL := fmt.Sprintf(`
		CREATE TEMP TABLE IF NOT EXISTS %[1]s (
			_op varchar(1),
			id integer,
			time timestamptz,
			name text,
			value numeric,
			seq_id bigint,
			old_time timestamptz,
			old_seq_id bigint
		);
		TRUNCATE TABLE %[1]s;
	`, pgx.Identifier{tempTableName}.Sanitize())

	_, err := conn.Exec(ctx, tempTableDDL)
	if err != nil {
		fmt.Printf("Failed to create temporary table: %v\n", err)
		return
	}


	// account copy time as well
	start := time.Now()

	rowsInserted := 0
	copyFromFunc := func() (row []any, err error) {
		if rowsInserted >= numInserts {
			return nil, nil
		}
		rowsInserted++
		return []any{
			"I", // op
			rand.Intn(10000),
			time.Now(),
			fmt.Sprintf("metric_%d", rand.Intn(1000)),
			rand.Float64() * 100,
		}, nil
	}

	_, err = conn.CopyFrom(ctx, pgx.Identifier{tempTableName}, []string{"_op", "id", "time", "name", "value"}, pgx.CopyFromFunc(copyFromFunc))
	if err != nil {
		fmt.Printf("Failed to copy data into temporary table: %v\n", err)
		return
	}

	rowsUpdated := 0
	copyFromUpdateFunc := func() (row []any, err error) {
		if rowsUpdated >= len(updates) {
			return nil, nil
		}
		rowsUpdated++
		return []any {
			"U", // op
			rand.Float64() * 100000, // Random value for update
			updates[rowsUpdated - 1].Time, // Use the time from the update info
			updates[rowsUpdated - 1].SeqID, // Use the max seq_id from the update info
		}, nil
	}

	_, err = conn.CopyFrom(ctx, pgx.Identifier{tempTableName}, []string{"_op", "value", "old_time", "old_seq_id"}, pgx.CopyFromFunc(copyFromUpdateFunc))
	if err != nil {
		fmt.Printf("Failed to copy data for updates into temporary table: %v\n", err)
		return
	}


	insertSQL := fmt.Sprintf(
		`INSERT INTO %s (id, time, name, value)
		SELECT id, time, name, value FROM %s WHERE _op = 'I'`,
		cfg.relName(),
		pgx.Identifier{tempTableName}.Sanitize(),
	)

	batch.Queue(
		insertSQL,
	)

	// updates
	updateSQL := fmt.Sprintf(`UPDATE %[1]s AS t SET value = v.value::numeric
		FROM %[2]s AS v
		WHERE t.time=v.old_time AND t.seq_id=v.old_seq_id AND v._op = 'U'
	`,
		cfg.relName(),
		pgx.Identifier{tempTableName}.Sanitize(),
	)

	batch.Queue(
		updateSQL,
	)


	fmt.Printf("Queued batch with %d inserts and %d updates at %s\n", numInserts, len(updates), time.Now().Format(time.RFC3339))
	err = conn.SendBatch(ctx, batch).Close()
	if	err != nil {
		fmt.Printf("Failed to execute batch: %v\n", err)
		return
	}
	duration := time.Since(start).Seconds()
	if duration > 0 {
		rate := float64(totalOps) / duration
		fmt.Printf("Executed %d inserts and %d updates in %.2f seconds (%.2f records/sec)\n",numInserts, numUpdates, duration, rate)
	}
}

type unnestExecutor struct {
	cfg config
}

func (e unnestExecutor) execute(ctx context.Context, conn *pgx.Conn) {
	cfg := e.cfg

	batch := &pgx.Batch{}
	totalOps, numInserts, numUpdates := cfg.ops()

	updates := cfg.updateRecords(ctx, conn, numUpdates)

	batch.Queue(
		fmt.Sprintf("SET plan_cache_mode = '%s'", cfg.planCacheMode),
	)

	insertSQL := fmt.Sprintf(
		`INSERT INTO %s (id, time, name, value)
		SELECT id, time, name, value FROM unnest(
			$1::integer[],
			$2::timestamptz[],
			$3::text[],
			$4::numeric[]
		) AS t(id, time, name, value)`,
		cfg.relName(),
	)

	values := make([][]any, 4)
	params := make([]any, len(values))
	for i := range values {
		values[i] = make([]any, numInserts)
		params[i] = values[i]
	}

	for i := 0; i < numInserts; i++ {
		values[0][i] = rand.Intn(10000) // id
		values[1][i] = time.Now() // time
		values[2][i] = fmt.Sprintf("metric_%d", rand.Intn(1000)) // name
		values[3][i] = rand.Float64() * 100 // value
	}

	batch.Queue(
		insertSQL,
		params...,
	)

	// updates
	updateSQL := `UPDATE %[1]s AS t SET value = v.value::numeric
		FROM (
			SELECT value, time, seq_id FROM unnest(
				$1::numeric[],
				$2::timestamptz[],
				$3::bigint[]
			) AS t(value, time, seq_id)
		) AS v
		WHERE t.time=v.time AND t.seq_id=v.seq_id
	`
	values = make([][]any, 3)
	params = make([]any, len(values))
	for i := range values {
		values[i] = make([]any, len(updates))
		params[i] = values[i]
	}
	for i, u := range updates {
		values[0][i] = rand.Float64() * 100000 // Random value for update
		values[1][i] = u.Time // Use the time from the update info
		values[2][i] = u.SeqID // Use the max seq_id from the update info
	}

	batch.Queue(
		fmt.Sprintf(updateSQL, cfg.relName()),
		params...,
	)

	start := time.Now()

	fmt.Printf("Queued batch with %d inserts and %d updates at %s\n", numInserts, len(updates), time.Now().Format(time.RFC3339))
	err := conn.SendBatch(ctx, batch).Close()
	if	err != nil {
		fmt.Printf("Failed to execute batch: %v\n", err)
		return
	}
	duration := time.Since(start).Seconds()
	if duration > 0 {
		rate := float64(totalOps) / duration
		fmt.Printf("Executed %d inserts and %d updates in %.2f seconds (%.2f records/sec)\n",numInserts, numUpdates, duration, rate)
	}
}

type multiValueExecutor struct {
	cfg config
}

func (e multiValueExecutor) execute(ctx context.Context, conn *pgx.Conn) {
	cfg := e.cfg

	batch := &pgx.Batch{}
	totalOps, numInserts, numUpdates := cfg.ops()

	updates := cfg.updateRecords(ctx, conn, numUpdates)

	batch.Queue(
		fmt.Sprintf("SET plan_cache_mode = '%s'", cfg.planCacheMode),
	)

	insertSQL := fmt.Sprintf("INSERT INTO %s (id, time, name, value) VALUES", cfg.relName())
	values := make([]any, 0, numInserts*4)
	placeholders := make([]string, 0, numInserts)
	for i := 0; i < numInserts; i++ {
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d)", i*4+1, i*4+2, i*4+3, i*4+4))
		values = append(values,
			rand.Intn(10000), time.Now(), fmt.Sprintf("metric_%d", rand.Intn(1000)), rand.Float64()*100,
		)
	}
	if len(placeholders) > 0 {
		insertSQL += strings.Join(placeholders, ", ")
		batch.Queue(
			insertSQL,
			values...,
		)
	} else {
		fmt.Println("No inserts to queue, skipping insert batch")
	}

	updateSQL := `UPDATE %[1]s AS t SET value = v.value::numeric
		FROM (
			VALUES %s
		) AS v(value, time, seq_id)
		WHERE t.time=v.time AND t.seq_id=v.seq_id
	`
	placeholders = make([]string, 0, len(updates))
	values = make([]any, 0, len(updates)*3)
	for i, u := range updates {
		value := rand.Float64() * 100000 // Random value for update
		placeholders = append(placeholders, fmt.Sprintf("($%d::numeric, $%d::timestamptz, $%d::bigint)", i*3+1, i*3+2, i*3+3))
		values = append(values, value, u.Time, u.SeqID)
	}
	if len(placeholders) > 0 {
		updateSQL = fmt.Sprintf(updateSQL, cfg.relName(), strings.Join(placeholders, ", "))
		batch.Queue(
			updateSQL,
			values...,
		)
	} else {
		fmt.Println("No updates to queue, skipping update batch")
	}

	start := time.Now()

	fmt.Printf("Queued batch with %d inserts and %d updates at %s\n", numInserts, len(updates), time.Now().Format(time.RFC3339))
	err := conn.SendBatch(ctx, batch).Close()
	if	err != nil {
		fmt.Printf("Failed to execute batch: %v\n", err)
		return
	}
	duration := time.Since(start).Seconds()
	if duration > 0 {
		rate := float64(totalOps) / duration
		fmt.Printf("Executed %d inserts and %d updates in %.2f seconds (%.2f records/sec)\n",numInserts, numUpdates, duration, rate)
	}
}

type simplePgxBatchExecutor struct {
	cfg config
}

func (e simplePgxBatchExecutor) execute(ctx context.Context, conn *pgx.Conn) {
	cfg := e.cfg

	batch := &pgx.Batch{}
	totalOps, numInserts, numUpdates := cfg.ops()

	updates := cfg.updateRecords(ctx, conn, numUpdates)

	batch.Queue(
		fmt.Sprintf("SET plan_cache_mode = '%s'", cfg.planCacheMode),
	)

	insertSQL := fmt.Sprintf("INSERT INTO %s (id, time, name, value) VALUES ($1, $2, $3, $4)", cfg.relName())
	for i := 0; i < numInserts; i++ {
		batch.Queue(
			insertSQL,
			rand.Intn(10000), time.Now(), fmt.Sprintf("metric_%d", rand.Intn(1000)), rand.Float64()*100,
		)
	}

	updateSQL := fmt.Sprintf("UPDATE %[1]s SET value = $1 WHERE time=$2 AND seq_id=$3", cfg.relName())
	for _, u := range updates {
		value := rand.Float64() * 100000 // Random value for update
		batch.Queue(
			updateSQL,
			value,
			u.Time, // Use the time from the update info
			u.SeqID, // Use the max seq_id from the update info
		)
	}

	start := time.Now()

	fmt.Printf("Queued batch with %d inserts and %d updates at %s\n", numInserts, len(updates), time.Now().Format(time.RFC3339))
	err := conn.SendBatch(ctx, batch).Close()
	if	err != nil {
		fmt.Printf("Failed to execute batch: %v\n", err)
		return
	}
	duration := time.Since(start).Seconds()
	if duration > 0 {
		rate := float64(totalOps) / duration
		fmt.Printf("Executed %d inserts and %d updates in %.2f seconds (%.2f records/sec)\n",numInserts, numUpdates, duration, rate)
	}
}

func dumpPreparedStatements(ctx context.Context, conn *pgx.Conn) {
	rows, err := conn.Query(ctx, "SELECT name, statement, generic_plans, custom_plans FROM pg_prepared_statements")
	if err != nil {
		fmt.Printf("Failed to query prepared statements: %v\n", err)
		return
	}
	defer rows.Close()

	fmt.Println("Prepared Statements:")
	for rows.Next() {
		var name, statement string
		var genericPlans, customPlans int64
		err := rows.Scan(&name, &statement, &genericPlans, &customPlans)
		if err != nil {
			fmt.Printf("Failed to scan prepared statement: %v\n", err)
			continue
		}
		fmt.Printf("Name: %s Statement: %s Generic Plans: %d Custom Plans: %d\n", name, statement, genericPlans, customPlans)
	}
}
