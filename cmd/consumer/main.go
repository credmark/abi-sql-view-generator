package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	internal "github.com/credmark/abi-sql-view-generator/internal/cloud/aws"
	sf "github.com/snowflakedb/gosnowflake"
)

var (
	account   = os.Getenv("SF_ACCOUNT")
	user      = os.Getenv("SF_USER")
	password  = os.Getenv("SF_PASSWORD")
	database  = os.Getenv("SF_DATABASE")
	schema    = os.Getenv("SF_SCHEMA")
	warehouse = os.Getenv("SF_WAREHOUSE")
	role      = os.Getenv("SF_ROLE")
	key       = os.Getenv("LAMBDA_ACCESS_KEY_ID")
	secret    = os.Getenv("LAMBDA_SECRET_ACCESS_KEY")
	region    = os.Getenv("LAMBDA_REGION")
	queueURL  = os.Getenv("SQS_QUEUE_URL")
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func GetQueueName(url string) string {
	split := strings.Split(url, "/")

	return split[len(split)-1]
}

func Handler(ctx context.Context, event events.SQSEvent) error {
	config, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)
	if err != nil {
		return fmt.Errorf("error loading config: %w", err)
	}
	config.Region = region
	client := sqs.NewFromConfig(config)

	queueName := GetQueueName(queueURL)

	// Processing channels
	errorChan := make(chan error)

	cfg := sf.Config{
		User:      user,
		Password:  password,
		Account:   account,
		Database:  database,
		Schema:    schema,
		Warehouse: warehouse,
		Role:      role,
	}

	dsn, err := sf.DSN(&cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Open snowflake connection
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	wg := new(sync.WaitGroup)

	for _, record := range event.Records {
		wg.Add(1)
		go func(ctx context.Context, client *sqs.Client, queueName string, record events.SQSMessage, db *sql.DB, wg *sync.WaitGroup, errorChan chan error) {
			defer wg.Done()

			if err := internal.HandleSQSMessage(ctx, client, record, queueName, db); err != nil {
				errorChan <- err
				return
			}
		}(ctx, client, queueName, record, db, wg, errorChan)
	}

	log.Println("finished processing SQS records")

	wg.Wait()
	close(errorChan)

	errors := make([]error, 0)

	for err := range errorChan {
		log.Println("ERROR:", err)
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		log.Fatal("errors encountered in processing")
	}

	return nil
}

func main() {
	lambda.Start(Handler)
}
