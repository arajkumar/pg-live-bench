package main

import (
	"os/signal"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	totalRecords = 10000
)

const(
	minUpdatePct = 0.1
	maxUpdatePct = 0.4
)

func main() {
	seed := flag.Int64("seed", 20672067, "Random seed for reproducibility")
	dbURL := flag.String("db", "", "PostgreSQL connection URL")
	tableName := flag.String("table", "metrics", "Table name to use for the benchmark")
	scale := flag.Int("scale", 1, "Scale factor for the number of records (default is 1x, 10000 records)")
	planCacheMode := flag.String("plan_cache_mode", "force_generic_plan", "Plan cache mode to use for the connection")

	flag.Parse()
	rand.Seed(*seed)

	if *dbURL == "" {
		fmt.Println("DATABASE_URL is required")
		os.Exit(1)
	}
	if *tableName == "" {
		fmt.Println("Table name is required")
		os.Exit(1)
	}

	totalRecords = *scale * totalRecords

	fmt.Println("Seed:", *seed, "table:", *tableName, "scale:", *scale)

	ctx := context.Background()
	// set application name for the connection
	config, err := pgx.ParseConfig(*dbURL)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse database URL: %v", err))
	}

	config.RuntimeParams["application_name"] = "live-sync-bench"

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect: %v", err))
	}
	defer conn.Close(ctx)

	createTable(ctx, conn, *tableName)

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

	for {
		select {
		case <-ticker.C:
			executeBatch(ctx, conn, *tableName, *planCacheMode)
		case <-ctx.Done():
			fmt.Println("Exiting due to context cancellation")
			return
		}
	}
}

func createTable(ctx context.Context, conn *pgx.Conn, tableName string) {
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id integer,
		time timestamptz,
		name text,
		value numeric,
		seq_id bigint PRIMARY KEY DEFAULT nextval('metrics1_seq_id_seq')
	);`, pgx.Identifier{tableName}.Sanitize())

	_, err := conn.Exec(ctx, ddl)
	if err != nil {
		panic(fmt.Sprintf("Failed to create table: %v", err))
	}
}

func executeBatch(ctx context.Context, conn *pgx.Conn, tableName string, plan string) {
	updatePct := minUpdatePct + rand.Float64()*(maxUpdatePct-minUpdatePct)

	numUpdates := int(float64(totalRecords) * updatePct)
	numInserts := totalRecords - numUpdates

	totalOps := numInserts + numUpdates
	batch := &pgx.Batch{}

	insertSQL := fmt.Sprintf("INSERT INTO %s (id, time, name, value) VALUES ($1, $2, $3, $4)", pgx.Identifier{tableName}.Sanitize())
	updateSQL := fmt.Sprintf("UPDATE %[1]s SET value = $1 WHERE time=$2 AND seq_id=$3", pgx.Identifier{tableName}.Sanitize())

	// Get max seq_id to use for updates
	var maxSeqID int64
	var maxtTime time.Time

	maxQuery := fmt.Sprintf("SELECT seq_id, time FROM %s ORDER BY time desc LIMIT 1", pgx.Identifier{tableName}.Sanitize())
	err := conn.QueryRow(ctx, maxQuery).Scan(&maxSeqID, &maxtTime)

	batch.Queue(
		fmt.Sprintf("SET plan_cache_mode = '%s'", plan),
	)

	for i := 0; i < numInserts; i++ {
		batch.Queue(
			insertSQL,
			rand.Intn(10000), time.Now(), fmt.Sprintf("metric_%d", rand.Intn(1000)), rand.Float64()*100,
		)
	}

	for i := 0; i < numUpdates; i++ {
		value := rand.Float64() * 100
		timevalue := time.Now().Add(-time.Duration(rand.Intn(1000)) * time.Second)
		seqID := maxSeqID - int64(rand.Intn(10000)) // Randomly select a seq_id for update
		batch.Queue(
			updateSQL,
			value, timevalue,
			seqID,
		)
	}

	start := time.Now()

	fmt.Printf("Queued batch with %d inserts and %d updates at %s\n", numInserts, numUpdates, time.Now().Format(time.RFC3339))
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
