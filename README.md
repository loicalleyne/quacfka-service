Quacfka-Service 🏹🦆
===================
High throughput streaming of Protobuf data from Kafka into DuckDB
Read about its initial development on my guest post on the Apache Arrow [blog](https://arrow.apache.org/blog/2025/03/10/fast-streaming-inserts-in-duckdb-with-adbc/).

Uses generics. Use your protobuf message as a type parameter to autogenerate an Arrow schema, provide a protobuf unmarshaling func, and stream data into DuckDB with a very high throughput.
If using file rotation should be run alongside [Quacfka-Runner](https://github.com/loicalleyne/quacfka-runner).

See [quacfka] documentation for API usage details.
[![Go Reference](https://pkg.go.dev/badge/github.com/loicalleyne/quacfka.svg)](https://pkg.go.dev/github.com/loicalleyne/quacfka)

## Usage
### Code generation for Protobuf
Place your Kafka message schema proto file in `proto/` and run `buf generate`.

### Initialize a quacfka.Orchestrator
Modify/comment out the Normalizer queries
Put the message type as the type parameter for the Quacfka Orchestrator.
```go
	// Defining normalizer fields and aliases - must match
	normFields := []string{"id", "site.id", "site.publisher.id", "timestamp.seconds", "imp[0].banner.w", "imp[0].banner.h", "imp[0].pmp.deals.id"}
	normAliases := []string{"bidreq_id", "device_id", "pub_id", "event_time", "width", "height", "deal"}
	o, err = q.NewOrchestrator[*rr.Bidrequest](q.WithFileRotateThresholdMB(int64(*duckSize)), q.WithNormalizer(normFields, normAliases, false), q.WithDuckPathsChan(3))
```

### Configure Kafka client with environment variables or in `.env`
```go
	k := o.NewKafkaConfig()
	k.ClientCount.Store(int32(*kafkaRoutines))
	k.MsgChanCap = 122880 * *kCap
	k.ConsumerGroup = os.Getenv("CONSUMER_GROUP")
	k.Seeds = append(k.Seeds, os.Getenv("KAFKA_SEED"))
	k.User = os.Getenv("KAFKA_USER")
	k.Password = os.Getenv("KAFKA_PW")
	// Use this if consuming from topic produced by Confluence client as it prefixes messages with 6 magic bytes
	k.Munger = q.WithMessageCutConfluencePrefix
	k.Topic = os.Getenv("KAFKA_TOPIC")
```

### Configure Processor
```go
	// Protobuf processor configuration
	err = o.ConfigureProcessor(*arrowQueueSize, *batchMultiplier, *routines, customProtoUnmarshal)
	if err != nil {
		log.Println(err)
		panic(err)
	}
```

### Configure DuckDB
```go
	var driverPath string
	switch runtime.GOOS {
	case "darwin":
		driverPath = "/usr/local/lib/libduckdb.so.dylib"
	case "linux":
		driverPath = "/usr/local/lib/libduckdb.so"
	case "windows":
		h, _ := os.UserHomeDir()
		driverPath = h + "\\Downloads\\libduckdb-windows-amd64\\duckdb.dll"
	default:
	}
    err = o.ConfigureDuck(q.WithPathPrefix("bidreq"), q.WithDriverPath(driverPath), q.WithDestinationTable("bidreq"), q.WithDuckConnections(*duckRoutines))
```

### Setup RPC
### RPC Usage
```go
	gorpc.RegisterType(rpc.Request{})
	gorpc.RegisterType(rpc.Response{})
    addr = "./gorpc-sock.unix"
	client := gorpc.NewUnixClient(addr)
	client.Start()

	// partition query
	partitionQuery := `select
			datepart('year', epoch_ms(timestamp.seconds * 1000))::STRING as year,
			datepart('month', epoch_ms(timestamp.seconds * 1000))::STRING as month,
			datepart('day', epoch_ms(timestamp.seconds * 1000))::STRING as day,
			datepart('hour', epoch_ms(timestamp.seconds * 1000))::STRING as hour
		from bidreq
		group by all
		ORDER BY 1,2,3,4`
	// export_raw.sql
	rawQuery := `COPY (
		SELECT *
		FROM bidreq
		WHERE
		datepart('year', epoch_ms(((timestamp.seconds * 1000) + (timestamp.nanos/1000000))::BIGINT)) = {{year}}
		and datepart('month', epoch_ms(((timestamp.seconds * 1000) + (timestamp.nanos/1000000))::BIGINT)) = {{month}}
		and datepart('day', epoch_ms(((timestamp.seconds * 1000) + (timestamp.nanos/1000000))::BIGINT)) = {{day}}
		and datepart('hour', epoch_ms(((timestamp.seconds * 1000) + (timestamp.nanos/1000000))::BIGINT)) = {{hour}} ) TO '{{exportpath}}/{{logname}}/{{queryname}}/year={{year}}/month={{month}}/day={{day}}/hour={{hour}}/bidreq_raw_{{rand}}.parquet' (format PARQUET, compression zstd, ROW_GROUP_SIZE_BYTES 100_000_000, OVERWRITE_OR_IGNORE)`
	hourlyRequestsAggQuery := `COPY (
			select
			datetrunc('day', epoch_ms(event_time*1000))::DATE date,
			extract('hour' FROM epoch_ms(event_time*1000)) as hour,
			bidreq_norm.pub_id,
			bidreq_norm.device_id,
			CONCAT(width::string, 'x', height::string) resolution,
			deal,
			count(distinct bidreq_id) requests,
			from bidreq_norm
			where
			datepart('year', epoch_ms(event_time * 1000)) = {{year}}
			and datepart('month', epoch_ms(event_time * 1000)) = {{month}}
			and datepart('day', epoch_ms(event_time * 1000)) = {{day}}
			and datepart('hour', epoch_ms(event_time * 1000)) = {{hour}}
			group by all)
			TO '{{exportpath}}/{{logname}}/{{queryname}}/year={{year}}/month={{month}}/day={{day}}/hour={{hour}}/bidreq_hourly_requests_agg_{{rand}}.parquet' (format PARQUET, compression zstd, ROW_GROUP_SIZE_BYTES 100_000_000, OVERWRITE_OR_IGNORE)`
	logName := "ortb.bid-requests"
	queries := []string{rawQuery, hourlyRequestsAggQuery}
	queriesNames := []string{"raw", "hourly_requests_agg"}
	execQueries := []string{"SET threads = 32", "SET allocator_background_threads = true"}
	execQueriesNames := []string{"", ""}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for dPath := range o.DuckPaths() {
			path, err := filepath.Abs(dPath)
			if err != nil {
				log.Printf("dPath error: %v\n", err)
			}
            // Requests should be validated first with Call, then the request in the validated Response sent with Send.
			resp, err := client.Call(rpc.Request{
				Type:             rpc.REQUEST_VALIDATE,
				Path:             path,
				ExportPath:       *parquetPath,
				LogName:          logName,
				ExecQueries:      execQueries,
				ExecQueriesNames: execQueriesNames,
				PartitionQuery:   partitionQuery,
				Queries:          queries,
				QueriesNames:     queriesNames,
			})
			if err != nil {
				log.Printf(" runner request issue: %v\n", err)
			} else {
				req := resp.(rpc.Response).Request
				req.Type = rpc.REQUEST_RUN
				err = client.Send(req)
				if err != nil {
					log.Printf("rpc send error: %v\n", err)
				}
			}
            // Backpressure 
			d := 0
			for i := 0; i <= 60; i++ {
				c, _ := dbFileCount("./")
				if c > 3 {
					delay := time.NewTimer(1 * time.Second)
					<-delay.C
					d++
				} else {
					if d > 0 {
						log.Printf("delayed by %d sec\n", d)
					}
					break
				}
			}
		}
	}()
```

### Run it 🚀
```go
	var wg sync.WaitGroup
	ctx, ctxCancelFunc := context.WithCancel(context.Background())

	wg.Add(1)
	go o.Run(ctx, &wg)
    wg.Wait()
```

## 💫 Show your support

Give a ⭐️ if this project helped you!
Feedback and PRs welcome.

## Licence

Quacfka-Service is released under the Apache 2.0 license. See [LICENCE](LICENCE)