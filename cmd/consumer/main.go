package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
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

func Handler(event events.SQSEvent) {
	SQSConfig := internal.NewConfig(key, secret, region)
	ctx := context.Background()

	// Processing channels
	deleteChan := make(chan string)
	deleteDoneChan := make(chan int)
	errorChan := make(chan error)
	errorDoneChan := make(chan int)
	errors := make([]error, 0)

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

	wg := sync.WaitGroup{}

	go func() {
		for {
			select {
			case receiptHandle := <-deleteChan:
				log.Printf("received receiptHandle on delete channel: %s\n", receiptHandle)
				internal.DeleteSQSMessage(ctx, SQSConfig, queueURL, receiptHandle, errorChan)
			case <-deleteDoneChan:
				close(deleteChan)
				close(deleteDoneChan)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case err := <-errorChan:
				log.Printf("received err on error channel: %s\n", err.Error())
				errors = append(errors, err)
			case <-errorDoneChan:
				close(errorChan)
				close(errorDoneChan)
			}
		}
	}()

	for _, record := range event.Records {
		wg.Add(1)
		go internal.HandleSQSMessage(ctx, record, db, wg, deleteChan, errorChan)
	}

	wg.Wait()

	deleteDoneChan <- 0
	errorDoneChan <- 0

	if len(errors) > 0 {
		log.Println("errors handling SQS message")

		for _, err := range errors {
			log.Println("ERROR:", err)
		}

		os.Exit(1)
	}
}

func main() {
	lambda.Start(Handler)
}
