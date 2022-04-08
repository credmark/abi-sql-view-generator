package utils

import (
	"context"
	"database/sql"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	sf "github.com/snowflakedb/gosnowflake"
)

func getCreateQuery() string {
	path, _ := filepath.Abs("sql/create_dev.sql")
	fb, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	return string(fb)
}

func CreateViews(ctx context.Context, dsn string, namespace string) {

	// Open snowflake connection
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	query := getCreateQuery()

	// Get ABIs and contract addresses
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Create channels for communicating with goroutines
	processingErrorChan := make(chan error)
	processingDoneChan := make(chan int)
	processingErrors := make([]error, 0)

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

		counter += 1
		log.Printf("counter=%d\n", counter)
		contractProcessingGroup.Add(counter)

		log.Println("submitting multi statement query for contract address:", contractAddress)
		go func(ctx context.Context, contractAddress string, abi abi.ABI, namespace string) {
			defer contractProcessingGroup.Done()

			contractAbi := NewAbiContract(contractAddress, abi, namespace)
			multiStatementBuffer := contractAbi.GenerateSql()
			numStatements := contractAbi.GetNumberOfStatements()
			multiStatementCtx, _ := sf.WithMultiStatement(ctx, numStatements)

			log.Printf("contractAddress='%s' statementsToProcess=%d\n", contractAddress, numStatements)

			// Since query statements just create views there is no need to catch the result object
			_, err = db.ExecContext(multiStatementCtx, multiStatementBuffer.String())
			if err != nil {
				processingErrorChan <- err
			}
		}(ctx, contractAddress, abiVal, namespace)

		if counter%100 == 0 {
			log.Printf("%d ABIs processed so far...\n", counter)
		}
	}

	contractProcessingGroup.Wait()

	if len(processingErrors) > 0 {
		log.Printf("processing finished with %d errors\n", len(processingErrors))
	}
}
