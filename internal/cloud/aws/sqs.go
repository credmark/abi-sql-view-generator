package aws

import (
	"context"
	"database/sql"
	"fmt"
	"log"

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

func HandleSQSMessage(ctx context.Context, client *sqs.Client, event events.SQSMessage, queueName string, db *sql.DB) error {

	message, err := internal.DeserializeMessage(event.Body)
	if err != nil {
		return fmt.Errorf("error deserializing SQS message body: %w", err)
	}

	if message.NumberOfStatements == 0 {
		log.Println("message has 0 sql statements to process. Deleting message...")
		return DeleteSQSMessage(ctx, client, queueName, event.ReceiptHandle)
	}

	uuid := sf.NewUUID()
	ctxWithId := sf.WithRequestID(ctx, uuid)
	multiStatementCtx, _ := sf.WithMultiStatement(ctxWithId, message.NumberOfStatements)

	log.Printf("submitting query with query ID: %s\n", uuid.String())

	_, err = db.ExecContext(multiStatementCtx, message.SQLStatements)
	if err != nil {
		return fmt.Errorf("error with multistatement query for contract address: %s: %w", message.ContractAddress, err)
	}

	log.Printf("query ID %s completed. Deleting SQS message receipt handle %s\n", uuid.String(), event.ReceiptHandle)

	return DeleteSQSMessage(ctx, client, queueName, event.ReceiptHandle)
}

func DeleteSQSMessage(ctx context.Context, client *sqs.Client, queueName string, receiptHandle string) error {

	queueURL, err := getQueueURL(ctx, client, queueName)
	if err != nil {
		return fmt.Errorf("error running getQueueURL: %w", err)
	}

	log.Printf("deleting SQS message from queue URL %s\n", queueURL)

	_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("error deleting SQS message: %w", err)
	}

	log.Printf("successfully deleted SQS message with receipt handle %s\n", receiptHandle)

	return nil
}

func getQueueURL(ctx context.Context, client *sqs.Client, queueName string) (string, error) {
	log.Printf("getting queue URL for queue %s\n", queueName)
	result, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", fmt.Errorf("error getting queue URL from queue name: %w", err)
	}

	return *result.QueueUrl, nil
}
