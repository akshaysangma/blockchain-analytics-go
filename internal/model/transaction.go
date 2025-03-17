package model

import (
	"math/big"
	"time"
)

// Transaction represents a simplified model of Ethereum transaction
type Transaction struct {
	Hash        string    `json:"hash"`
	BlockNumber uint64    `json:"block_number"`
	BlockHash   string    `json:"block_hash"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	Value       *big.Int  `json:"value"`
	GasPrice    *big.Int  `json:"gas_price"`
	GasUsed     uint64    `json:"gas_used"`
	Timestamp   time.Time `json:"timestamp"`
	Status      bool      `json:"status"`
	InputData   string    `json:"input_data"`
}

// Block represents an Ethereum Block
type Block struct {
	Number       uint64    `json:"number"`
	Hash         string    `json:"hash"`
	ParentHash   string    `json:"parent_hash"`
	Timestamp    time.Time `json:"timestamp"`
	Transactions []string  `json:"transactions"`
}
