package aws

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/credmark/abi-sql-view-generator/internal"
	sf "github.com/snowflakedb/gosnowflake"
)

func SendSQSMessage(cfg Config, queueURL string, body string) error {
	config := aws.Config(cfg)
	client := sqs.NewFromConfig(config)

	_, err := client.SendMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: aws.String(body),
		QueueUrl:    aws.String(queueURL),
	})
	if err != nil {
		return err
	}

	return nil
}

func HandleSQSMessage(ctx context.Context, event events.SQSMessage, db *sql.DB, wg sync.WaitGroup, successChan chan string, errorChan chan error) {
	defer wg.Done()

	message, err := internal.DeserializeMessage(event.Body)
	if err != nil {
		errorChan <- fmt.Errorf("error deserializing SQS message body: %w", err)
	}

	multiStatementCtx, _ := sf.WithMultiStatement(ctx, message.NumberOfStatements)

	_, err = db.ExecContext(multiStatementCtx, message.SQLStatements)
	if err != nil {
		errorChan <- fmt.Errorf("error with multistatement query for contract address: %s: %w", message.ContractAddress, err)
	}

	successChan <- event.ReceiptHandle
}

func DeleteSQSMessage(cfg Config, queueURL string, receiptHandle string) error {
	config := aws.Config(cfg)
	client := sqs.NewFromConfig(config)

	_, err := client.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("error deleting SQS message: %w", err)
	}

	return nil
}
