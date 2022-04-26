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
	key       = os.Getenv("AWS_ACCESS_KEY_ID")
	secret    = os.Getenv("AWS_SECRET_ACCESS_KEY")
	region    = os.Getenv("AWS_REGION")
	queueURL  = os.Getenv("SQS_QUEUE_URL")
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func Handler(event events.SQSEvent) {
	SQSConfig := internal.NewConfig(key, secret, region)

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
				err := internal.DeleteSQSMessage(SQSConfig, queueURL, receiptHandle)
				if err != nil {
					errorChan <- err
				}
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
				errors = append(errors, err)
			case <-errorDoneChan:
				close(errorChan)
				close(errorDoneChan)
			}
		}
	}()

	for _, record := range event.Records {
		wg.Add(1)
		go internal.HandleSQSMessage(context.TODO(), record, db, wg, deleteChan, errorChan)
	}

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
