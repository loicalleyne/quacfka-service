package main

import (
	"flag"
	"path/filepath"

	"runtime"
	"runtime/pprof"
	"strings"
	"sync"

	"github.com/loicalleyne/quacfka-runner/rpc"

	rr "github.com/loicalleyne/quacfka-service/gen"
	"github.com/yassinebenaid/godump"

	"github.com/apache/arrow-go/v18/arrow/array"
	q "github.com/loicalleyne/quacfka"

	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/valyala/gorpc"

	"github.com/joho/godotenv"
	"github.com/loicalleyne/bufarrow"
)

var (
	err  error
	addr string
)

var (
	cpuprofile      = flag.String("cpuprofile", "default.pgo", "write cpu profile to `file`")
	batchMultiplier = flag.Int("bs", 1, "122880 * {bs} limit")
	kafkaRoutines   = flag.Int("kr", 5, "kafka client goroutine count")
	kCap            = flag.Int("kc", 8, "msg buffer 122880 * *kCap")
	routines        = flag.Int("gr", 1, "deserializing goroutine count")
	duckRoutines    = flag.Int("acr", 1, "adbc connections")
	arrowQueueSize  = flag.Int("q", 5, "arrow queue size")
	duckSize        = flag.Int("s", 4200, "duckdb file size threshold")
	maxProcs        = flag.Int("mp", runtime.NumCPU(), "GOMAXPROCS")
	verbose         = flag.Bool("verbose", false, "verbose logging")
	parquetPath     = flag.String("pp", "../parquet", "path to parquet folder, without trailing slash")
)

func main() {
	flag.Parse()
	log.Printf("GOMAXPROCS=%d\n", *maxProcs)
	runtime.GOMAXPROCS(*maxProcs)
	godotenv.Load()
	numCPUs := runtime.NumCPU()
	log.Printf("CPU count: %d\n", numCPUs)

	metricsFile, err := os.OpenFile("metrics.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	var o *q.Orchestrator[*rr.Bidrequest]
	defer func() {
		_, err = metricsFile.WriteString(o.ReportJSONL())
		if err != nil {
			log.Println(err)
		}
		metricsFile.Sync()
		metricsFile.Close()
	}()
	if *cpuprofile != "" {
		log.Printf("profiling to %s\n", strings.TrimSuffix(filepath.Base(*cpuprofile), filepath.Ext(*cpuprofile))+"_"+filepath.Ext(*cpuprofile))
		f, err := os.Create(strings.TrimSuffix(filepath.Base(*cpuprofile), filepath.Ext(*cpuprofile)) + filepath.Ext(*cpuprofile))
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
		defer log.Printf("program ended\nto view profile run 'go tool pprof -http localhost:8080 %s'\n", strings.TrimSuffix(filepath.Base(*cpuprofile), filepath.Ext(*cpuprofile))+filepath.Ext(*cpuprofile))
	}

	// Defining normalizer fields and aliases - must match
	normFields := []string{"id", "site.id", "site.publisher.id", "timestamp.seconds", "imp[0].banner.w", "imp[0].banner.h", "imp[0].pmp.deals.id"}
	normAliases := []string{"bidreq_id", "device_id", "pub_id", "event_time", "width", "height", "deal"}
	o, err = q.NewOrchestrator[*rr.Bidrequest](q.WithFileRotateThresholdMB(int64(*duckSize)), q.WithNormalizer(normFields, normAliases, false), q.WithDuckPathsChan(3))

	if err != nil {
		panic(err)
	}

	defer o.Close()
	fmt.Println(o.Schema().Schema().String())
	if *verbose {
		q.SetDebugLogger(log.Printf)
		q.SetErrorLogger(log.Printf)
		q.SetFatalLogger(log.Fatalf)
	}
	q.SetBenchmarkLogger(log.Printf)
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

	// Protobuf processor configuration
	err = o.ConfigureProcessor(*arrowQueueSize, *batchMultiplier, *routines, customProtoUnmarshal)
	if err != nil {
		log.Println(err)
		panic(err)
	}

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
	godump.Dump(driverPath)

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
	gorpc.RegisterType(rpc.Request{})
	gorpc.RegisterType(rpc.Response{})

	addr = "./gorpc-sock.unix"
	client := gorpc.NewUnixClient(addr)
	client.Start()

	err = o.ConfigureDuck(q.WithPathPrefix("bidreq"), q.WithDriverPath(driverPath), q.WithDestinationTable("bidreq"), q.WithDuckConnections(*duckRoutines))

	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	ctx_, ctxCancelFunc := context.WithCancel(context.Background())
	ctrlC(ctxCancelFunc, o, metricsFile)

	wg.Add(1)
	go o.Run(ctx_, &wg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for dPath := range o.DuckPaths() {
			path, err := filepath.Abs(dPath)
			if err != nil {
				log.Printf("dPath error: %v\n", err)
			}
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

	wg.Wait()
	if o.Error() != nil {
		log.Println(err)
	}

	log.Printf("%v\n", o.Report())

	o.Close()
}

func customProtoUnmarshal(m []byte, s any) error {
	newMessage := rr.BidrequestFromVTPool()
	err := newMessage.UnmarshalVTUnsafe(m)
	if err != nil {
		return err
	}
	rb := s.(*bufarrow.Schema[*rr.Bidrequest]).NormalizerBuilder()
	if rb != nil {
		b := rb.Fields()
		if b != nil {
			// messageDescriptor := newMessage.ProtoReflect().Descriptor()
			// rootFields := messageDescriptor.Fields()
			id := newMessage.GetId()
			deviceID := coalesceStringFunc(newMessage.GetUser().GetId, newMessage.GetSite().GetId, newMessage.GetDevice().GetIfa)
			publisherID := newMessage.GetSite().GetPublisher().GetId()
			timestampSeconds := newMessage.GetTimestamp().GetSeconds()
			timestampNanos := newMessage.GetTimestamp().GetNanos()
			var width, height int32
			if newMessage.GetImp()[0].GetBanner() != nil {
				width = newMessage.GetImp()[0].GetBanner().GetW()
			} else {
				width = newMessage.GetImp()[0].GetVideo().GetW()
			}
			if newMessage.GetImp()[0].GetBanner() != nil {
				height = newMessage.GetImp()[0].GetBanner().GetH()
			} else {
				height = newMessage.GetImp()[0].GetVideo().GetH()
			}
			if len(newMessage.GetImp()[0].GetPmp().GetDeals()) == 0 {
				b[0].(*array.StringBuilder).Append(id)
				b[1].(*array.StringBuilder).Append(deviceID)
				b[2].(*array.StringBuilder).Append(publisherID)
				b[4].(*array.Int64Builder).Append(timestampSeconds + int64(timestampNanos/1000000000))
				b[5].(*array.Uint32Builder).Append(uint32(width))
				b[6].(*array.Uint32Builder).Append(uint32(height))
				b[7].(*array.StringBuilder).AppendNull()
			}
			// var deals []string = make([]string, len(newMessage.GetImp()[0].GetPmp().GetDeals()))
			for i := 0; i < len(newMessage.GetImp()[0].GetPmp().GetDeals()); i++ {
				b[0].(*array.StringBuilder).Append(id)
				b[1].(*array.StringBuilder).Append(deviceID)
				b[2].(*array.StringBuilder).Append(publisherID)
				b[4].(*array.Int64Builder).Append(timestampSeconds + int64(timestampNanos/1000000000))
				b[5].(*array.Uint32Builder).Append(uint32(width))
				b[6].(*array.Uint32Builder).Append(uint32(height))
				b[7].(*array.StringBuilder).Append(newMessage.GetImp()[0].GetPmp().GetDeals()[i].GetId())
			}
		}
	}

	// Assert s to `*bufarrow.Schema[*your.CustomProtoMessageType]`
	s.(*bufarrow.Schema[*rr.Bidrequest]).Append(newMessage)
	newMessage.ReturnToVTPool()
	return nil
}

func dbFileCount(path string) (int, error) {
	i := 0
	files, err := os.ReadDir(path)
	if err != nil {
		return 0, err
	}
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".db" {
			i++
		}
	}
	return i, nil
}

// CtrlC intercepts any Ctrl+C keyboard input and exits to the shell.
func ctrlC(f context.CancelFunc, o *q.Orchestrator[*rr.Bidrequest], metricsFile *os.File) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		f()
		o.UpdateMetrics()
		log.Printf("%v\n", o.Report())
		_, err = metricsFile.WriteString(o.ReportJSONL())
		if err != nil {
			log.Println(err)
		}
		metricsFile.Sync()
		metricsFile.Close()
		pprof.StopCPUProfile()
		log.Printf("Closing")
		fmt.Fprintf(os.Stdout, "Bye👋\n")
		os.Exit(2)
	}()
}

func coalesceStringFunc(args ...func() string) string {
	for _, arg := range args {
		if arg() != "" {
			return arg()
		}
	}
	return ""
}
