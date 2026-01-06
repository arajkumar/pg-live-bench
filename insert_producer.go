package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"sync"
	"time"
	"flag"
)

const tableSchema = `
CREATE TABLE IF NOT EXISTS public.load (
	id       bigserial PRIMARY KEY,
	time     integer NOT NULL,
	device   double precision,
	device2  text,
	device3  text,
	device4  text,
	device5  text,
	device6  text,
	device7  text,
	device8  text,
	device9  text
)
`


func main() {
	var (
		connString string
		insertsSecond int
		batchSize     int
	)
	flag.StringVar(&connString, "conn", "", "PostgreSQL connection URL")
	flag.IntVar(&insertsSecond, "inserts", 100_000, "Number of inserts per second")
	flag.IntVar(&batchSize, "batch", 5_000, "Batch size for inserts")
	flag.Parse()

	// insertsSecond should be divisible by batch size
	insertRecordsSecond(connString, insertsSecond, batchSize)
}

func insertRecordsSecond(connString string, targetSize, batchSize int) {
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	_, err = pool.Exec(context.Background(), tableSchema)
	if err != nil {
		log.Fatal(err)
	}

	for {
		start := time.Now()
		target := start.Add(1 * time.Second)

		inserted := 0

		var wg sync.WaitGroup
		for inserted < targetSize {

			wg.Add(1)
			go func() {

				_, err = pool.Exec(context.Background(), `
									WITH t AS (SELECT generate_series(1, $1) as time,
																			  random() *10,
																			  gen_random_uuid(),
																			  gen_random_uuid(),
																			  gen_random_uuid(),
																			  gen_random_uuid(),
																			  gen_random_uuid(),
																			  gen_random_uuid(),
																			  gen_random_uuid(),
																			  gen_random_uuid())
															INSERT
															INTO public.load (time, device, device2, device3, device4, device5, device6, device7, device8, device9)
															SELECT *
															FROM t;
		`, batchSize)
				if err != nil {
					fmt.Printf("err inserting records, retrying %+v", err)
					pool.Reset()
				}
				wg.Done()
			}()
			inserted += batchSize
		}
		wg.Wait()

		if time.Now().Before(target) {
			time.Sleep(target.Sub(time.Now()))
		}
		fmt.Printf("inserted %d record in duration %s \n", targetSize, time.Now().Sub(start))
	}
}
