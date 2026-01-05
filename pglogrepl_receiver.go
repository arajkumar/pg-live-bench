package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/felixge/fgprof"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// CreateProfiler returns serve and cancel functions for profiling
func CreateProfiler(listenAddr string) (func() error, func(error)) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Panic("Failed to create profiler listener", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/debug/fgprof", fgprof.Handler())

	// Register standard pprof endpoints
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	serve := func() error {
		log.Println("Serving profiler")
		return http.Serve(ln, mux)
	}

	cancel := func(error) {
		_ = ln.Close()
		log.Println("Profiler listener closed")
	}

	return serve, cancel
}

func main() {
	// Parse CLI flags
	connString := flag.String("conn", os.Getenv("PGLOGREPL_DEMO_CONN_STRING"), "PostgreSQL connection string")
	slotName := flag.String("slot", "foo", "Replication slot name")
	protoVersion := flag.Int("proto", 1, "pgoutput protocol version (1 or 2)")
	tempSlot := flag.Bool("temp-slot", true, "Use temporary replication slot")

	flag.Parse()

	if *connString == "" {
		log.Fatalln("connection string required (use -conn flag or PGLOGREPL_DEMO_CONN_STRING env var)")
	}

	serveProfiler, cancelProfiler := CreateProfiler(":6060")
	go func() {
		if err := serveProfiler(); err != nil {
			log.Println("Profiler server error:", err)
		}
	}()
	defer cancelProfiler(nil)

	// Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutdown signal received, stopping replication...")
		cancel()
	}()

	// Parse connection string and ensure replication parameter is set
	config, err := pgconn.ParseConfig(*connString)
	if err != nil {
		log.Fatalln("failed to parse connection string:", err)
	}
	config.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer conn.Close(context.Background())

	// Build plugin arguments based on protocol version
	var pluginArguments []string
	if *protoVersion == 2 {
		// Protocol version 2 supports streaming of large transactions (PG 14+)
		pluginArguments = []string{
			"proto_version '2'",
			"publication_names 'load'",
			"messages 'true'",
			"streaming 'true'",
		}
	} else {
		// Protocol version 1
		pluginArguments = []string{
			"proto_version '1'",
			"publication_names 'load'",
			"messages 'true'",
		}
	}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, *slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: *tempSlot})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}

	err = pglogrepl.StartReplication(context.Background(), conn, *slotName, 0, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", *slotName)

	var (
		clientXLogPos pglogrepl.LSN
		keepAliveXLog pglogrepl.LSN
		msgEndXLog    pglogrepl.LSN
		walStart      pglogrepl.LSN
	)

	// Message rate measurement
	messageCount := 0
	lastStatsTime := time.Now()
	statsPrintInterval := 10 * time.Second

	// Time-based standby updates
	lastStandbyUpdate := time.Now()
	standbyUpdateInterval := 10 * time.Second

	for ctx.Err() == nil {
		// Send standby status update every 10 seconds
		if time.Since(lastStandbyUpdate) >= standbyUpdateInterval {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: clientXLogPos,
					WALFlushPosition: clientXLogPos,
					WALApplyPosition: clientXLogPos,
				})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			lastStandbyUpdate = time.Now()
		}

		receiveCtx, receiveCancel := context.WithTimeout(ctx, standbyUpdateInterval)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		receiveCancel()
		if err != nil {
			if pgconn.Timeout(err) || err == context.DeadlineExceeded {
				// Timeout is expected, continue loop to send standby update
				continue
			}
			if ctx.Err() != nil {
				// Context cancelled, shutdown in progress
				break
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Fatalf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		// Count message and print rate every 10 seconds
		messageCount++
		if time.Since(lastStatsTime) >= statsPrintInterval {
			elapsed := time.Since(lastStatsTime).Seconds()
			rate := float64(messageCount) / elapsed
			log.Printf(
				"Message rate: %.2f messages/sec (%d messages in %.1f seconds) xlog %s keepalive %s msgstart %s msgend %s",
				rate, messageCount, elapsed, clientXLogPos,
				keepAliveXLog, walStart,
				msgEndXLog,
			)
			messageCount = 0
			lastStatsTime = time.Now()
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
			}
			if pkm.ReplyRequested {
				// Send immediate reply when server requests it
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn,
					pglogrepl.StandbyStatusUpdate{
						WALWritePosition: clientXLogPos,
						WALFlushPosition: clientXLogPos,
						WALApplyPosition: clientXLogPos,
					})
				if err != nil {
					log.Fatalln("SendStandbyStatusUpdate failed:", err)
				}
				lastStandbyUpdate = time.Now()
			}
			keepAliveXLog = pkm.ServerWALEnd

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParseXLogData failed:", err)
			}

			processMessage(xld.WALData, &clientXLogPos)
			walStart = xld.WALStart
			msgEndXLog = xld.ServerWALEnd
		}
	}
	log.Println("Replication stopped")
}

func processMessage(walData []byte, txnEndLSN *pglogrepl.LSN) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		log.Fatalf("Parse logical replication message: %s", err)
	}

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		//

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	case *pglogrepl.CommitMessage:
		*txnEndLSN = logicalMsg.TransactionEndLSN

	case *pglogrepl.StreamCommitMessageV2:
		*txnEndLSN = logicalMsg.TransactionEndLSN
		log.Printf("Stream commit message: xid %d, LSN %s", logicalMsg.Xid, logicalMsg.TransactionEndLSN)

	case *pglogrepl.InsertMessage:

	case *pglogrepl.UpdateMessage:
		// ...
	case *pglogrepl.DeleteMessage:
		// ...
	case *pglogrepl.TruncateMessage:
		// ...

	case *pglogrepl.TypeMessage:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessage:
		log.Printf("Logical decoding message: %q, %q", logicalMsg.Prefix, logicalMsg.Content)

	case *pglogrepl.StreamStartMessageV2:
		log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		log.Printf("Stream stop message")
	case *pglogrepl.StreamAbortMessageV2:
		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}
}
