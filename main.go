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
	flag.StringVar(&cfg.planCacheMode, "plan_cache_mode", "force_generic_plan", "Plan cache mode to use for the connection")
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

	fmt.Println("Creating table with DDL:", ddl)
	_, err := conn.Exec(ctx, ddl)
	if err != nil {
		panic(fmt.Sprintf("Failed to create table: %v", err))
	}
}

type multiValueExecutor struct {
	cfg config
}

func (e multiValueExecutor) execute(ctx context.Context, conn *pgx.Conn) {
	cfg := e.cfg

	updatePct := cfg.minUpdatePct + rand.Float64()*(cfg.maxUpdatePct-cfg.minUpdatePct)

	totalRecords := cfg.scale * totalRecords

	numUpdates := int(float64(totalRecords) * updatePct)
	numInserts := totalRecords - numUpdates

	totalOps := numInserts + numUpdates
	batch := &pgx.Batch{}

	relName := pgx.Identifier{cfg.schemaName, cfg.tableName}.Sanitize()
	// Get max seq_id to use for updates
	type updateInfo struct {
		SeqID int64 `db:"seq_id"`
		Time time.Time `db:"time"`
	}


	queryUpdateTime := time.Now()

	// generic plan after inserting and updates is slow, so we force custom plan
	_, err := conn.Exec(ctx, "SET plan_cache_mode = 'force_custom_plan'")
	if err != nil {
		fmt.Printf("Failed to set plan cache mode: %v\n", err)
		return
	}

	maxQuery := fmt.Sprintf("SELECT seq_id, time FROM %s ORDER BY time DESC LIMIT $1", relName)
	rows, err := conn.Query(ctx, maxQuery, numUpdates)
	if err != nil {
		fmt.Printf("Failed to get max seq_id: %v\n", err)
		return
	}
	defer rows.Close()

	updates, err := pgx.CollectRows(rows, pgx.RowToStructByName[updateInfo])
	if err != nil {
		fmt.Printf("Failed to collect rows: %v\n", err)
		return
	}
	fmt.Printf("Collected %d for update in %v\n", len(updates), time.Since(queryUpdateTime))


	batch.Queue(
		fmt.Sprintf("SET plan_cache_mode = '%s'", cfg.planCacheMode),
	)

	insertSQL := fmt.Sprintf("INSERT INTO %s (id, time, name, value) VALUES", relName)
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


	updateSQL := fmt.Sprintf("UPDATE %[1]s SET value = $1 WHERE time=$2 AND seq_id=$3", relName)
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
	// dumpPreparedStatements(ctx, conn)
}

type simplePgxBatchExecutor struct {
	cfg config
}

func (e simplePgxBatchExecutor) execute(ctx context.Context, conn *pgx.Conn) {
	cfg := e.cfg

	updatePct := cfg.minUpdatePct + rand.Float64()*(cfg.maxUpdatePct-cfg.minUpdatePct)

	totalRecords := cfg.scale * totalRecords

	numUpdates := int(float64(totalRecords) * updatePct)
	numInserts := totalRecords - numUpdates

	totalOps := numInserts + numUpdates
	batch := &pgx.Batch{}

	relName := pgx.Identifier{cfg.schemaName, cfg.tableName}.Sanitize()
	// Get max seq_id to use for updates
	type updateInfo struct {
		SeqID int64 `db:"seq_id"`
		Time time.Time `db:"time"`
	}


	queryUpdateTime := time.Now()

	// generic plan after inserting and updates is slow, so we force custom plan
	_, err := conn.Exec(ctx, "SET plan_cache_mode = 'force_custom_plan'")
	if err != nil {
		fmt.Printf("Failed to set plan cache mode: %v\n", err)
		return
	}

	maxQuery := fmt.Sprintf("SELECT seq_id, time FROM %s ORDER BY time DESC LIMIT $1", relName)
	rows, err := conn.Query(ctx, maxQuery, numUpdates)
	if err != nil {
		fmt.Printf("Failed to get max seq_id: %v\n", err)
		return
	}
	defer rows.Close()

	updates, err := pgx.CollectRows(rows, pgx.RowToStructByName[updateInfo])
	if err != nil {
		fmt.Printf("Failed to collect rows: %v\n", err)
		return
	}
	fmt.Printf("Collected %d for update in %v\n", len(updates), time.Since(queryUpdateTime))


	batch.Queue(
		fmt.Sprintf("SET plan_cache_mode = '%s'", cfg.planCacheMode),
	)

	insertSQL := fmt.Sprintf("INSERT INTO %s (id, time, name, value) VALUES ($1, $2, $3, $4)", relName)
	for i := 0; i < numInserts; i++ {
		batch.Queue(
			insertSQL,
			rand.Intn(10000), time.Now(), fmt.Sprintf("metric_%d", rand.Intn(1000)), rand.Float64()*100,
		)
	}

	updateSQL := fmt.Sprintf("UPDATE %[1]s SET value = $1 WHERE time=$2 AND seq_id=$3", relName)
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
	// dumpPreparedStatements(ctx, conn)
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
