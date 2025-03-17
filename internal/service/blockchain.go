package service

import (
	"context"

	"github.com/akshaysangma/blockchain-analytics-go/internal/config"
	"github.com/akshaysangma/blockchain-analytics-go/internal/connector"
	"github.com/akshaysangma/blockchain-analytics-go/internal/model"
	"github.com/akshaysangma/blockchain-analytics-go/internal/publisher"
	"go.uber.org/zap"
)

type BlockchainService struct {
	connector  connector.BlockchainConnector
	publisher  publisher.Publisher
	config     *config.EthereumConfig
	logger     *zap.Logger
	cancelFunc context.CancelFunc
}

func NewBlockchainService(
	connector connector.BlockchainConnector,
	publisher publisher.Publisher,
	cfg *config.EthereumConfig,
	logger *zap.Logger,
) *BlockchainService {
	return &BlockchainService{
		connector: connector,
		publisher: publisher,
		config:    cfg,
		logger:    logger,
	}
}

func (s *BlockchainService) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel

	var startBlock uint64
	if s.config.StartBlock > 0 {
		startBlock = s.config.StartBlock
	} else {
		latestBlock, err := s.connector.GetLatestBlockNumber(ctx)
		if err != nil {
			return err
		}
		startBlock = latestBlock
	}

	s.logger.Info("Starting block processing",
		zap.Uint64("start_block", startBlock))

	newBlocks, err := s.connector.SubscribeToNewBlock(ctx)
	if err != nil {
		return err
	}

	s.logger.Debug("Subscribe to new Head successful")

	go func() {
		s.processBlockchain(ctx, startBlock, newBlocks)
	}()

	return nil
}

func (s *BlockchainService) Stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

func (s *BlockchainService) processBlockchain(
	ctx context.Context,
	startBlock uint64,
	newBlocks <-chan uint64,
) {
	currentBlock := startBlock
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping processing block")
			return

		case blockNumber, ok := <-newBlocks:
			if !ok {
				s.logger.Warn("New blocks channel closed")
				return
			}
			s.logger.Debug("new block head received",
				zap.Uint64("block", blockNumber))
			if blockNumber > currentBlock {
				s.logger.Info("Processing New blocks",
					zap.Uint64("from", currentBlock+1),
					zap.Uint64("to", blockNumber))

				for start := currentBlock + 1; start <= blockNumber; start += s.config.MaxBlockRange {
					end := min(start+s.config.MaxBlockRange-1, blockNumber)

					err := s.processBlockRange(ctx, start, end)
					if err != nil {
						s.logger.Error("Failed to process block range",
							zap.Uint64("from", start),
							zap.Uint64("to", end))
					}
				}
				currentBlock = blockNumber
			}
		}
	}
}

func (s *BlockchainService) processBlockRange(
	ctx context.Context,
	start, end uint64,
) error {
	s.logger.Debug("Processing block range",
		zap.Uint64("start", start),
		zap.Uint64("end", end))

	for blockNumber := start; blockNumber <= end; blockNumber++ {

		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
		}

		block, err := s.connector.GetBlockByNumber(ctx, blockNumber)
		if err != nil {
			s.logger.Error("Failed to get block",
				zap.Uint64("block_number", blockNumber),
				zap.Error(err))
			continue
		}

		s.logger.Debug("Processing block transactions",
			zap.Uint64("block", blockNumber),
			zap.Int("tx_count", len(block.Transactions)))

		transactions := make([]*model.Transaction, 0, len(block.Transactions))
		for _, txHash := range block.Transactions {

			s.logger.Debug("fetching for details for transaction",
				zap.String("hash", txHash))

			tx, err := s.connector.GetTransactionByHash(ctx, txHash)

			s.logger.Debug("fetch completed for transaction",
				zap.String("hash", txHash))

			if err != nil {
				s.logger.Error("Failed to get transaction",
					zap.String("hash", txHash),
					zap.Error(err))
				continue
			}

			transactions = append(transactions, tx)
		}

		if len(transactions) > 0 {
			s.logger.Debug("Publishing Transactions batch to message Broker",
				zap.Uint64("block", blockNumber),
				zap.Int("count", len(transactions)))

			err := s.publisher.PublishTransactions(ctx, transactions)
			if err != nil {
				s.logger.Error("Failed to publish transactions",
					zap.Uint64("block", blockNumber),
					zap.Int("count", len(transactions)),
					zap.Error(err))
			} else {
				s.logger.Info("Published transactions from block",
					zap.Uint64("block", blockNumber),
					zap.Int("count", len(transactions)))
			}
		} else {
			s.logger.Debug("No transactions in block", zap.Uint64("block", blockNumber))
		}
	}

	return nil
}
