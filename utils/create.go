package utils

import (
	"bytes"
	"context"
	"database/sql"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"text/template"

	"github.com/credmark/abi-sql-view-generator/internal"
	"github.com/credmark/abi-sql-view-generator/internal/cloud/aws"
	"github.com/ethereum/go-ethereum/accounts/abi"
)

type SnowflakeError struct {
	ContractAddress string
	Error           error
}

func NewSnowflakeError(contractAddress string, err error) *SnowflakeError {
	return &SnowflakeError{
		ContractAddress: contractAddress,
		Error:           err,
	}
}

func getCreateQuery(options *Options) string {
	fpath, err := filepath.Abs("templates/create.sql")
	if err != nil {
		log.Fatal(err)
	}

	t, err := template.New("create.sql").ParseFiles(fpath)
	if err != nil {
		log.Fatal(err)
	}

	buffer := bytes.Buffer{}
	err = t.Execute(&buffer, options)
	if err != nil {
		log.Fatal(err)
	}

	return buffer.String()
}

func CreateViews(ctx context.Context, options *Options) {

	if options.DryRun {
		log.Println("running in dry-run mode. View create statements will not be submitted to snowflake")
	}

	cfg := aws.NewConfig(options.Key, options.Secret, options.Region)

	// Open snowflake connection
	db, err := sql.Open("snowflake", options.DSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	query := getCreateQuery(options)
	log.Println("getting contracts to process with query:\n", query)

	// Get ABIs and contract addresses
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Create channels for communicating with goroutines
	processingErrorChan := make(chan SnowflakeError)
	processingDoneChan := make(chan int)
	processingErrors := make([]SnowflakeError, 0)
	viewCountDoneChan := make(chan int)
	viewCountChan := make(chan int)
	viewCount := 0

	// Add any errors received in the processingErrorChan to the processingErr slice
	// and close the below goroutine when a done message is received
	go func() {
		for {
			select {
			case err := <-processingErrorChan:
				processingErrors = append(processingErrors, err)
			case <-processingDoneChan:
				close(processingDoneChan)
				close(processingErrorChan)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case count := <-viewCountChan:
				viewCount += count
			case <-viewCountDoneChan:
				close(viewCountDoneChan)
				close(viewCountChan)
				return
			}
		}
	}()

	counter := 0
	var contractProcessingGroup sync.WaitGroup

	// Iterate through results, create and submit SQL statements for each ABI and contract address
	for rows.Next() {
		var contractAddress string
		bs := []byte{}
		err := rows.Scan(&contractAddress, &bs)
		if err != nil {
			log.Fatal(err)
		}

		abiVal, err := abi.JSON(strings.NewReader(string(bs)))
		if err != nil {
			log.Fatal(err)
		}

		contractProcessingGroup.Add(1)

		go func(ctx context.Context, contractAddress string, abi abi.ABI, options *Options, cfg aws.Config, wg *sync.WaitGroup) {
			defer wg.Done()

			contractAbi := NewAbiContract(contractAddress, abi, options.Namespace)
			contractAbi.ValidateNames()
			if contractAbi.Skip {
				log.Println("skipping contract due to long event or method name")
				return
			}
			multiStatementBuffer := contractAbi.GenerateSql()
			numStatements := contractAbi.GetNumberOfStatements()

			viewCountChan <- numStatements

			message := internal.NewMessage(contractAddress, multiStatementBuffer.String(), numStatements)

			if !options.DryRun {
				body, err := internal.SerializeMessage(message)
				if err != nil {
					snowflakeError := NewSnowflakeError(contractAddress, err)
					processingErrorChan <- *snowflakeError
					return
				}

				if err = aws.SendSQSMessage(cfg, options.QueueUrl, body); err != nil {
					snowflakeError := NewSnowflakeError(contractAddress, err)
					processingErrorChan <- *snowflakeError
				}
			}

		}(ctx, contractAddress, abiVal, options, cfg, &contractProcessingGroup)

		counter += 1
		if counter%100 == 0 {
			log.Printf("%d contract addresses processed so far...\n", counter)
		}
	}

	log.Println("waiting for all submitted queries to finish processing...")
	contractProcessingGroup.Wait()

	processingDoneChan <- 0
	viewCountDoneChan <- 0

	if len(processingErrors) > 0 {
		log.Printf("processing finished with %d errors\n", len(processingErrors))

		for _, err := range processingErrors {
			log.Printf("ERROR: contractAddress=%s error=%s", err.ContractAddress, err.Error.Error())
		}
	}

	log.Printf("%d create view statements submitted", viewCount)
}
