package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akshaysangma/blockchain-analytics-go/internal/config"
	"github.com/akshaysangma/blockchain-analytics-go/internal/connector"
	"github.com/akshaysangma/blockchain-analytics-go/internal/publisher"
	"github.com/akshaysangma/blockchain-analytics-go/internal/service"
	"go.uber.org/zap"
)

func main() {
	cfg, err := config.LoadConfig(".")
	if err != nil {
		panic("Failed to load configuration: " + err.Error())
	}

	logger, err := config.NewLogger(&cfg.Log)
	if err != nil {
		panic("Failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	logger.Info("Starting Blockchain connector")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ethereumConnector := connector.NewEthereumConfig(&cfg.Ethereum, logger)
	err = ethereumConnector.Connect(ctx)
	if err != nil {
		logger.Fatal("Failed to connect to Ethereum node", zap.Error(err))
	}
	defer ethereumConnector.Close()

	kafkaPublisher := publisher.NewKafkaPublisher(&cfg.Kafka, logger)
	err = kafkaPublisher.Connect(ctx)
	if err != nil {
		logger.Fatal("Failed to connect to Kafka", zap.Error(err))
	}
	defer kafkaPublisher.Close()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	blockchainService := service.NewBlockchainService(
		ethereumConnector,
		kafkaPublisher,
		&cfg.Ethereum,
		logger)

	err = blockchainService.Start(ctx)
	if err != nil {
		logger.Fatal("Failed to start blockchain service", zap.Error(err))
	}
	defer blockchainService.Stop()

	<-stop
	logger.Info("Shutdown signal received, gracefully shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	select {
	case <-time.After(100 * time.Millisecond):
		logger.Info("Graceful shutdown completed")
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timed out, forcing exit")
	}
}
