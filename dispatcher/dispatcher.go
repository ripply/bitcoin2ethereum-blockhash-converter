package dispatcher

import (
	"context"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/workers"
	"github.com/sirupsen/logrus"
)

type dispatcher struct {
	blockChan          chan int64
	completedBlockChan chan int64
	done               chan struct{}
	errChan            chan error
	resultChan         chan jsonrpc.HashPair
	latestBlock        int64
	firstBlock         int64
	urls               []*url.URL
	logger             *logrus.Entry
	dispatchedBlocks   int64
	workers            *workers.Workers
}

type EthJSONRPC func(ctx context.Context, method string, params ...interface{})

func NewDispatcher(
	blockChan chan int64,
	completedBlockChan chan int64,
	urls []*url.URL,
	blockFrom int64,
	blockTo int64,
	done chan struct{},
	errChan chan error,
) *dispatcher {
	dispatchLogger, _ := log.GetLogger()
	return &dispatcher{
		blockChan:          blockChan,
		completedBlockChan: completedBlockChan,
		done:               done,
		errChan:            errChan,
		urls:               urls,
		logger:             dispatchLogger.WithField("module", "dispatcher"),
		latestBlock:        blockFrom,
		firstBlock:         blockTo,
		workers:            workers.NewWorkers(),
	}
}

func (d *dispatcher) findLatestBlock() int64 {
	rpcClient := jsonrpc.NewClient(d.urls[0].String(), 0)
	rpcResponse, err := rpcClient.Call(context.Background(), "eth_getBlockByNumber", "latest", false)
	if err != nil {
		d.logger.Error("Invalid endpoint: ", err)
		d.errChan <- err
		return 0
	}
	if rpcResponse.Error != nil {
		d.logger.Error("rpc response error: ", rpcResponse.Error)
		d.errChan <- err
		return 0
	}
	var qtumBlock jsonrpc.GetBlockByNumberResponse
	err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &qtumBlock)
	if err != nil {
		d.logger.Error("could not convert result to qtum.GetBlockByNumberResponse", err)
		d.errChan <- err
		return 0
	}
	latest, _ := strconv.ParseInt(qtumBlock.Number, 0, 64)
	d.logger.Debug("LatestBlock: ", latest)
	return latest
}

func (d *dispatcher) Start(ctx context.Context, numWorkers int, providers []*url.URL) {
	// logger.Info("Number of workers: ", numWorkers)
	// channel to receive errors from goroutines
	d.errChan = make(chan error, numWorkers+1)
	// channel to pass blocks to workers
	d.blockChan = make(chan int64, numWorkers*2)
	// channel to get completed blocks from workers
	d.completedBlockChan = make(chan int64, numWorkers*4)
	// channel to pass results from workers to DB
	d.resultChan = make(chan jsonrpc.HashPair, numWorkers)

	var wg sync.WaitGroup
	var blocksProcessingWaitGroup sync.WaitGroup
	blocksProcessingFinished := make(chan struct{})

	completedBlocks := 0
	var completedBlockMutex sync.Mutex

	completedBlockChanCtx, completedBlockChanCancel := context.WithCancel(ctx)

	go func() {
		for {
			t := time.After(1 * time.Second)
			select {
			case <-t:
				d.logger.Info("Completed: %d", completedBlocks)
			case <-d.completedBlockChan:
				completedBlockMutex.Lock()
				completedBlocks++
				completedBlockMutex.Unlock()
				blocksProcessingWaitGroup.Done()
			case <-completedBlockChanCtx.Done():
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	workerState := workers.StartWorkers(
		ctx,
		numWorkers,
		d.blockChan,
		d.completedBlockChan,
		d.resultChan,
		providers,
		&wg,
		d.errChan,
	)

	go func() {
		defer completedBlockChanCancel()
		defer func() {
			/*
				close(d.errChan)
				close(d.blockChan)
				close(d.completedBlockChan)
				close(d.resultChan)
			*/
		}()
		if d.latestBlock == 0 {
			d.logger.Info("Latest block not specified, getting latest")
			d.latestBlock = d.findLatestBlock()
		}
		d.logger.Info("Starting dispatcher from block ", d.latestBlock, " to ", d.firstBlock)
		for i := d.latestBlock; i > d.firstBlock; i-- {
			select {
			case <-ctx.Done():
				return
			default:
				d.logger.Info("Adding 1")
				blocksProcessingWaitGroup.Add(1)
				d.blockChan <- i
				d.dispatchedBlocks++
				// time.Sleep(time.Second * 10)
			}
		}

		go func() {
			blocksProcessingWaitGroup.Wait()
			blocksProcessingFinished <- struct{}{}
		}()

		d.logger.Info("Checking for failed blocks")
		attempts := 0
		for {
			failedBlocks := workerState.GetAndResetFailedBlocks()
			if len(failedBlocks) > 0 {
				attempts++
				d.logger.WithFields(logrus.Fields{
					"failed blocks": len(failedBlocks),
					"attempts":      attempts,
				}).Warn("retrying...")
				for _, fb := range failedBlocks {
					select {
					case <-ctx.Done():
						return

					default:
						d.blockChan <- fb
					}
				}
				d.logger.Info("waiting 10 seconds before retrying again...")
				time.Sleep(time.Second * 10)
			} else {
				d.logger.Warn("No more failed blocks found")
				break
			}
		}

		// need to wait for workers to finish processing blocks
		d.logger.Info("Waiting for blocks to finish processing")
		select {
		case <-blocksProcessingFinished:
			d.logger.Info("blocks finished processing")
		case <-ctx.Done():
		}

		d.logger.Info("closing block channel")
		close(d.blockChan)
		d.logger.Debug("finished dispatching blocks")
		d.done <- struct{}{}
	}()
}

// func dispatch(ctx context.Context, blockChan chan string, b int64) {
// 	select {
// 	case <-ctx.Done():
// 		return

// 	default:
// 		block := fmt.Sprintf("0x%x", b)
// 		blockChan <- block
// 	}

// }

func (d *dispatcher) GetDispatchedBlocks() int64 {
	return d.dispatchedBlocks
}
