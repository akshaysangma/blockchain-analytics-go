package publisher

import (
	"context"

	"github.com/akshaysangma/blockchain-analytics-go/internal/model"
)

// Publisher define the interface to publishing blockchain data
type Publisher interface {
	// Connect establishes a connection with the message broker
	Connect(ctx context.Context) error

	// Close closes the connection to the message broker
	Close() error

	// PublishTransaction publishes a transaction to message broker
	PublishTransaction(ctx context.Context, transaction *model.Transaction) error

	// PublishTransactions publishes multiple transactions in batch to message broker
	PublishTransactions(ctx context.Context, transactions []*model.Transaction) error
}
