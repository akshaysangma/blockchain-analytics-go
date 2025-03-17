package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Ethereum EthereumConfig `mapstructure:"ethereum"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
	Log      LogConfig      `mapstructure:"log"`
}

type EthereumConfig struct {
	NodeURL            string `mapstructure:"node_url"`
	WebsocketURL       string `mapstructure:"websocket_url"`
	StartBlock         uint64 `mapstructure:"start_block"`
	MaxBlockRange      uint64 `mapstructure:"max_block_range"`
	ConfirmationBlocks uint64 `mapstructure:"confirmation_blocks"`
}

type KafkaConfig struct {
	Brokers      []string `mapstructure:"brokers"`
	Topic        string   `mapstructure:"topic"`
	BatchSize    int      `mapstructure:"batch_size"`
	BatchTimeout int      `mapstructure:"batch_timeout"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func LoadConfig(path string) (*Config, error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config Config
	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}
