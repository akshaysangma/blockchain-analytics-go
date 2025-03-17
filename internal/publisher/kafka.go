package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/akshaysangma/blockchain-analytics-go/internal/config"
	"github.com/akshaysangma/blockchain-analytics-go/internal/model"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// KafkaPublisher implements Publisher interface for Kafka
type KafkaPublisher struct {
	config *config.KafkaConfig
	writer *kafka.Writer
	logger *zap.Logger
}

// NewKafkaPublisher creates a new KafkaPublisher
func NewKafkaPublisher(cfg *config.KafkaConfig, logger *zap.Logger) *KafkaPublisher {
	return &KafkaPublisher{
		config: cfg,
		logger: logger,
	}
}

func (k *KafkaPublisher) Connect(ctx context.Context) error {
	k.writer = &kafka.Writer{
		Addr:         kafka.TCP(k.config.Brokers...),
		Topic:        k.config.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    k.config.BatchSize,
		BatchTimeout: time.Duration(k.config.BatchTimeout) * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	pingMsg := struct {
		Type    string    `json:"type"`
		Message string    `json:"message"`
		Time    time.Time `json:"time"`
	}{
		Type:    "ping",
		Message: "Blockchain Platform startup",
		Time:    time.Now(),
	}

	pingMsgBytes, err := json.Marshal(pingMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal ping message: %w", err)
	}

	err = k.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("ping"),
		Value: pingMsgBytes,
	})
	if err != nil {
		return fmt.Errorf("failed to publish to kafka topic %s: %w", k.config.Topic, err)
	}

	k.logger.Info("Connected to Kafka",
		zap.Strings("brokers", k.config.Brokers),
		zap.String("topic", k.config.Topic))

	return nil
}

func (k *KafkaPublisher) Close() error {
	if k.writer != nil {
		err := k.writer.Close()
		if err != nil {
			return fmt.Errorf("failed to close Kafka connection: %w", err)
		}
	}

	k.logger.Info("Disconnected from Kafka")
	return nil
}

func (k *KafkaPublisher) PublishTransaction(ctx context.Context, transaction *model.Transaction) error {
	if transaction == nil {
		return fmt.Errorf("cannot publish nil transaction")
	}

	message := struct {
		Type string             `json:"type"`
		Data *model.Transaction `json:"data"`
		Time time.Time          `json:"time"`
	}{
		Type: "transaction",
		Data: transaction,
		Time: time.Now(),
	}

	msgByte, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction message: %w", err)
	}

	err = k.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(transaction.Hash),
		Value: msgByte,
	})
	if err != nil {
		return fmt.Errorf("failed to publish transaction message: %w", err)
	}

	k.logger.Info("Published transaction",
		zap.String("hash", transaction.Hash),
		zap.Uint64("block", transaction.BlockNumber))

	return nil
}

func (k *KafkaPublisher) PublishTransactions(ctx context.Context, transactions []*model.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	messages := make([]kafka.Message, len(transactions))

	for i, tx := range transactions {
		if tx == nil {
			continue
		}

		message := struct {
			Type string             `json:"type"`
			Data *model.Transaction `json:"data"`
			Time time.Time          `json:"time"`
		}{
			Type: "transaction",
			Data: tx,
			Time: time.Now(),
		}

		msgByte, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to marshal transaction message: %w", err)
		}

		messages[i] = kafka.Message{
			Key:   []byte(tx.Hash),
			Value: msgByte,
		}

	}

	err := k.writer.WriteMessages(ctx, messages...)
	if err != nil {
		return fmt.Errorf("failed to publish batch of transaction message: %w", err)
	}

	k.logger.Info("Published transaction batch",
		zap.Int("count", len(transactions)),
		zap.String("first_hash", transactions[0].Hash),
		zap.String("last_hast", transactions[len(transactions)-1].Hash))

	return nil
}
