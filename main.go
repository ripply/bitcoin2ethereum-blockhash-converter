package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/db"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/dispatcher"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	providers  = kingpin.Flag("providers", "qtum rpc providers").Default("https://janus.qiswap.com").Short('p').URLList()
	numWorkers = kingpin.Flag("workers", "Number of workers. Defaults to system's number of CPUs.").Default(strconv.Itoa(runtime.NumCPU())).Short('w').Int()
	debug      = kingpin.Flag("debug", "debug mode").Short('d').Default("false").Bool()
	blockFrom  = kingpin.Flag("from", "block number to start scanning from (default: 'Latest'").Short('f').Default("0").Int64()
	blockTo    = kingpin.Flag("to", "block number to stop scanning (default: 1)").Short('t').Default("0").Int64()

	host     = kingpin.Flag("host", "database hostname").Default("127.0.0.1").String()
	port     = kingpin.Flag("port", "database port").Default("5432").String()
	user     = kingpin.Flag("host", "database username").Default("dbuser").String()
	password = kingpin.Flag("password", "database password").Default("dbpass").String()
	dbname   = kingpin.Flag("dbname", "database name").Default("qtum").String()

	dbConnectionString = kingpin.Flag("dbstring", "database connection string").String()
)
var logger *logrus.Logger
var start time.Time

func init() {
	kingpin.Version("0.0.1")
	kingpin.Parse()
	mainLogger, err := log.GetLogger(
		log.WithDebugLevel(*debug),
		log.WithWriter(os.Stdout),
	)
	if err != nil {
		logrus.Panic(err)
	}
	logger = mainLogger
}

func checkError(e error) {
	if e != nil {
		logger.Fatal(e)
	}
}

func main() {

	var wg sync.WaitGroup

	logger.Info("Number of workers: ", *numWorkers)
	// channel to receive errors from goroutines
	errChan := make(chan error, *numWorkers+1)
	// channel to pass blocks to workers
	blockChan := make(chan int64, *numWorkers)
	completedBlockChan := make(chan int64, *numWorkers)
	// channel to pass results from workers to DB
	resultChan := make(chan jsonrpc.HashPair, *numWorkers)

	connectionString := db.DbConfig{
		Host:     *host,
		Port:     *port,
		User:     *user,
		Password: *password,
		DBName:   *dbname,
	}.String()

	if dbConnectionString != nil {
		connectionString = *dbConnectionString
	}

	qdb, err := db.NewQtumDB(connectionString, resultChan, errChan)
	checkError(err)
	dbCloseChan := make(chan error)
	qdb.Start(dbCloseChan)
	// channel to signal  work completion to main from dispatcher
	done := make(chan struct{})
	// channel to receive os signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// dispatch blocks to block channel
	ctx, cancelFunc := context.WithCancel(context.Background())

	d := dispatcher.NewDispatcher(blockChan, completedBlockChan, *providers, *blockFrom, *blockTo, done, errChan)
	d.Start(ctx, *numWorkers, *providers)
	// start workers
	// wg.Add(*numWorkers)
	// workers.StartWorkers(ctx, *numWorkers, blockChan, resultChan, *providers, &wg, errChan)
	start = time.Now()

	var status int
	select {
	case <-done:
		logger.Info("Dispatcher finished")
		status = 0
	case <-sigs:
		logger.Warn("Received ^C ... exiting")
		logger.Warn("Canceling block dispatcher and stopping workers")
		cancelFunc()
		status = 1
	case err := <-errChan:
		logger.Warn("Received fatal error: ", err)
		logger.Warn("Canceling block dispatcher and stopping workers")
		cancelFunc()
		status = 1
	}
	logger.Debug("Waiting for all workers to exit")
	wg.Wait()
	logger.Info("All workers stopped. Waiting for DB to finish")
	close(resultChan)
	if status != 0 {
		logger.Info("Dropping table")
		qdb.DropTable()
	}
	select {
	case err = <-dbCloseChan:
		if err != nil {
			logger.Fatal("Error closing DB:", err)
		}
	case <-time.After(time.Second * 2):
		logger.Fatal("Error waiting for DB to close")
	}

	logger.WithFields(logrus.Fields{
		"workers":             *numWorkers,
		" successBlocks":      qdb.GetRecords(),
		" totalScannedBlocks": d.GetDispatchedBlocks(),
		" duration":           time.Since(start).Truncate(time.Second),
	}).Info()
	logger.Print("Program finished")
	os.Exit(status)
}
