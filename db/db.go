package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	_ "github.com/lib/pq"
	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
)

type DbConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSL      bool
}

func (config DbConfig) String() string {
	ssl := "disable"
	if config.SSL {
		ssl = "enable"
	}
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", config.Host, config.Port, config.User, config.Password, config.DBName, ssl)
}

type QtumDB struct {
	db         *sql.DB
	logger     *logrus.Entry
	records    int64
	resultChan chan jsonrpc.HashPair
	errChan    chan error
}

func NewQtumDB(connectionString string, resultChan chan jsonrpc.HashPair, errChan chan error) (*QtumDB, error) {
	dbLogger, _ := log.GetLogger()
	logger := dbLogger.WithField("module", "db")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	logger.Debug("Database Connected!")
	createStmt := `CREATE TABLE IF NOT EXISTS "Hashes" ("BlockNum" int, "Eth" text PRIMARY KEY, "Qtum" text NOT NULL)`
	_, err = db.Exec(createStmt)

	if err != nil {
		return nil, err
	}

	return &QtumDB{db: db, logger: logger, resultChan: resultChan, errChan: errChan}, nil
}

func (q *QtumDB) insert(blockNum int, eth, qtum string) (sql.Result, error) {
	insertDynStmt := `INSERT INTO "Hashes"("BlockNum", "Eth", "Qtum") VALUES($1, $2, $3) ON CONFLICT ON CONSTRAINT "Hashes_pkey" DO UPDATE SET "Eth" = $2, "Qtum" = $3`
	return q.db.Exec(insertDynStmt, blockNum, eth, qtum)
}

func (q *QtumDB) getMissing(latestBlock int) (sql.Result, error) {
	// TODO: Compute missing rows, work sql magic, there will be like 2million records
	insertDynStmt := `INSERT INTO "Hashes"("BlockNum", "Eth", "Qtum") VALUES($1, $2, $3) ON CONFLICT ON CONSTRAINT "Hashes_pkey" DO UPDATE SET "Eth" = $2, "Qtum" = $3`
	return q.db.Exec(insertDynStmt, latestBlock)
}

func (q *QtumDB) getQtumHash(ctx context.Context, ethHash string) (*sql.Rows, error) {
	// TODO: Compute missing rows, work sql magic, there will be like 2million records
	selectStatement := `SELECCT Qtum FROM "Hashes" WHERE "Hashes"."Eth" = $1`
	if ctx == nil {
		return q.db.Query(selectStatement, ethHash)
	} else {
		return q.db.QueryContext(ctx, selectStatement, ethHash)
	}
}

func (q *QtumDB) GetQtumHash(ethHash string) (string, error) {
	return q.GetQtumHashContext(nil, ethHash)
}

func (q *QtumDB) GetQtumHashContext(ctx context.Context, ethHash string) (string, error) {
	var qtumHash string
	rows, err := q.getQtumHash(ctx, ethHash)
	if err != nil {
		return qtumHash, err
	}

	if rows == nil {
		panic("no rows")
	}

	err = rows.Scan(&qtumHash)
	return qtumHash, err
}

func (q *QtumDB) DropTable() (sql.Result, error) {
	dropStmt := `DROP TABLE IF EXISTS "Hashes"`
	return q.db.Exec(dropStmt)
}

func (q *QtumDB) Start(dbCloseChan chan error) {
	const PROGRESS_LEVEL_THRESHOLD = 10000
	go func() {
		progBar := getBar(PROGRESS_LEVEL_THRESHOLD)
		start := time.Now()
		for {
			pair, ok := <-q.resultChan
			if !ok {
				q.logger.Info("QtumDB -> channel closed")
				err := q.db.Close()
				dbCloseChan <- err
				return
			}
			q.logger = q.logger.WithFields(logrus.Fields{
				"blockNum": pair.BlockNumber,
			})
			q.logger.Debug(" Received new pair of hashes")
			if q.logger.Level != logrus.DebugLevel {
				progBar.Add(1)
			}

			if pair.BlockNumber%PROGRESS_LEVEL_THRESHOLD == 0 {
				duration := time.Since(start)
				fmt.Println()
				q.logger.WithFields(logrus.Fields{
					"elapsedTime":     duration,
					"blocksProcessed": PROGRESS_LEVEL_THRESHOLD,
				}).Info("progress checkpoint")
				start = time.Now()
				progBar = getBar(PROGRESS_LEVEL_THRESHOLD)
			}
			_, err := q.insert(pair.BlockNumber, pair.EthHash, pair.QtumHash)
			if err != nil {
				q.logger.Error("error writing to db: ", err, " for block: ", pair.BlockNumber)
				q.errChan <- err
				q.logger.Debug("database exiting")
				return
			}
			q.records += 1
		}
	}()

}

func (q *QtumDB) GetRecords() int64 {
	return q.records
}

// creates a progress bar used to display progress when only 1 alien is left
func getBar(I int) *progressbar.ProgressBar {
	bar := progressbar.NewOptions(I,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSetDescription("[cyan][reset]==>Processing 10K Qtum blocks..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	return bar
}
