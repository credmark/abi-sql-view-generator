package internal

import (
	"encoding/json"
	"fmt"
)

type QueueMessage struct {
	ContractAddress    string `json:"contract_address"`
	SQLStatements      string `json:"sql_statements"`
	NumberOfStatements int    `json:"number_of_statements"`
}

func SerializeMessage(message *QueueMessage) (string, error) {
	bytes, err := json.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("error JSON serializing message: %w", err)
	}

	return string(bytes), nil
}

func DeserializeMessage(body string) (*QueueMessage, error) {
	message := QueueMessage{}

	if err := json.Unmarshal([]byte(body), &message); err != nil {
		return nil, fmt.Errorf("error deserializing SQS message body: %w", err)
	}

	return &message, nil
}

func NewMessage(contractAddress string, sql string, numberOfStatements int) *QueueMessage {
	return &QueueMessage{
		ContractAddress:    contractAddress,
		SQLStatements:      sql,
		NumberOfStatements: numberOfStatements,
	}
}
