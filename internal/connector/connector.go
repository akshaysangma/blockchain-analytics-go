package connector

import (
	"context"

	"github.com/akshaysangma/blockchain-analytics-go/internal/model"
)

// BlockchainConnector defines the interface for extracting data from a blockchain
type BlockchainConnector interface {
	// Connect establishes a connection to a blockchain node
	Connect(ctx context.Context) error

	// Close closes the connetion to blockchain node
	Close() error

	// GetLatestBlockNumber retrieves the latest block number from the blockchain
	GetLatestBlockNumber(ctx context.Context) (uint64, error)

	// GetBlockByNumber retrieves specific block by its number
	GetBlockByNumber(ctx context.Context, number uint64) (*model.Block, error)

	// GetTransactionByHash retrieves specific Transaction by its hash
	GetTransactionByHash(ctx context.Context, hash string) (*model.Transaction, error)

	// SubscribeToNewBlock starts a subscription for new blocks
	// It returns a channel that recieves new block numbers
	SubscribeToNewBlock(ctx context.Context) (<-chan uint64, error)
}
