package connector

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/akshaysangma/blockchain-analytics-go/internal/config"
	"github.com/akshaysangma/blockchain-analytics-go/internal/model"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
)

// EthereumConnector implements the BlockchainConnector interface for Ethereum
type EthereumConnector struct {
	config     *config.EthereumConfig
	client     *ethclient.Client
	wsclient   *ethclient.Client
	logger     *zap.Logger
	cancelFunc context.CancelFunc
}

// NewEthereumConfig craete a new Ethereum Connector
func NewEthereumConfig(config *config.EthereumConfig, logger *zap.Logger) *EthereumConnector {
	return &EthereumConnector{
		config: config,
		logger: logger,
	}
}

// Connect establishes connection to Ethereum node
func (e *EthereumConnector) Connect(ctx context.Context) error {
	var err error

	e.client, err = ethclient.DialContext(ctx, e.config.NodeURL)
	if err != nil {
		return fmt.Errorf("failed to connect to Ethereum node: %w", err)
	}

	e.wsclient, err = ethclient.DialContext(ctx, e.config.WebsocketURL)
	if err != nil {
		return fmt.Errorf("failed to connect to Ethereum WebSocket: %w", err)
	}

	e.logger.Info("Connected to Ethereum node",
		zap.String("node_url", e.config.NodeURL),
		zap.String("websocket_url", e.config.WebsocketURL))

	return nil
}

// Close closes the connection to Ethereum node
func (e *EthereumConnector) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}

	if e.client != nil {
		e.client.Close()
	}

	if e.wsclient != nil {
		e.wsclient.Close()
	}

	e.logger.Info("Disconnected from Ethereum Node")
	return nil
}

// GetLatestBlockNumber retrieves the latest block Number
func (e *EthereumConnector) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	blockNumber, err := e.client.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch latest block number: %w", err)
	}

	return blockNumber, nil
}

// GetBlockByNumber retrieves a block by number and converts to simplified model.Block
func (e *EthereumConnector) GetBlockByNumber(ctx context.Context, number uint64) (*model.Block, error) {
	ethBlock, err := e.client.BlockByNumber(ctx, big.NewInt(int64(number)))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch block %d: %w", number, err)
	}

	txHashes := make([]string, len(ethBlock.Transactions()))
	for i, tx := range ethBlock.Transactions() {
		txHashes[i] = tx.Hash().Hex()
	}

	return &model.Block{
		Number:       ethBlock.NumberU64(),
		Hash:         ethBlock.Hash().Hex(),
		ParentHash:   ethBlock.ParentHash().Hex(),
		Timestamp:    time.Unix(int64(ethBlock.Time()), 0),
		Transactions: txHashes,
	}, nil
}

// getTxSender is a helper function to get Sender for a transaction
func getTxSender(ctx context.Context, client *ethclient.Client, tx *types.Transaction) (string, error) {
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch chainID: %w", err)
	}

	signer := types.NewLondonSigner(chainID)

	from, error := types.Sender(signer, tx)
	if error != nil {
		return "", err
	}

	return from.Hex(), nil
}

// GetTransactionByHash retrieves a transaction for the given hash and coverts to simplified model.Transaction
func (e *EthereumConnector) GetTransactionByHash(ctx context.Context, hash string) (*model.Transaction, error) {
	txHash := common.HexToHash(hash)
	tx, isPending, err := e.client.TransactionByHash(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction %s: %w", hash, err)
	}

	if isPending {
		return nil, fmt.Errorf("transaction %s is still pending", hash)
	}

	receipt, err := e.client.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve receipt for transaction %s: %w", hash, err)
	}

	ethBlock, err := e.client.BlockByHash(ctx, receipt.BlockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve block for transaction %s: %w", hash, err)
	}

	from, err := getTxSender(ctx, e.client, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender for transaction %s: %w", hash, err)
	}

	transaction := &model.Transaction{
		Hash:        tx.Hash().Hex(),
		BlockNumber: ethBlock.NumberU64(),
		BlockHash:   ethBlock.Hash().Hex(),
		From:        from,
		Value:       tx.Value(),
		GasPrice:    tx.GasPrice(),
		GasUsed:     receipt.GasUsed,
		Timestamp:   time.Unix(int64(ethBlock.Time()), 0),
		Status:      receipt.Status == 1,
		InputData:   common.Bytes2Hex(tx.Data()),
	}

	if tx.To() != nil {
		transaction.To = tx.To().Hex()
	} else {
		transaction.To = ""
	}

	return transaction, nil
}

// SubscribeToNewBlock subscribes to new blocks and returns channel for notifications
func (e *EthereumConnector) SubscribeToNewBlock(ctx context.Context) (<-chan uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel

	headers := make(chan *types.Header)
	sub, err := e.wsclient.SubscribeNewHead(ctx, headers)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to subscribe to new blocks: %w", err)
	}

	blockNumbers := make(chan uint64)

	go func() {
		defer close(blockNumbers)

		for {
			select {
			case err := <-sub.Err():
				e.logger.Error("Subscription error", zap.Error(err))
				return

			case header := <-headers:
				blockNumber := header.Number.Uint64()
				e.logger.Debug("New block detected", zap.Uint64("block_number", blockNumber))

				if e.config.ConfirmationBlocks > 0 {
					time.Sleep(time.Second * time.Duration(12) * time.Duration(e.config.ConfirmationBlocks))
				}

				select {
				case blockNumbers <- blockNumber:

				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return blockNumbers, nil
}
