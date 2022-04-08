package main

import (
	"database/sql"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/credmark/abi-sql-view-generator/utils"
	"github.com/ethereum/go-ethereum/accounts/abi"
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
	namespace = os.Getenv("NAMESPACE")
)

func getQuery() string {
	path, _ := filepath.Abs("templates/query_dev.sql")
	fb, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	return string(fb)
}

func main() {

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

	query := getQuery()
	log.Println("query:", query)
	log.Printf("connecting to database...\n\tuser=%s\n\trole=%s\n\tdatabase=%s\n\tschema=%s\n\twarehouse=%s\n", user, role, database, schema, warehouse)

	// Open snowflake connection
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Get ABIs and contract addresses
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	counter := 0

	// Iterate through results, create and submit SQL statements for each ABI and contract address
	for rows.Next() {
		var contractAddress string
		bs := []byte{}
		err := rows.Scan(&contractAddress, &bs)
		if err != nil {
			log.Fatal(err)
		}

		abi, err := abi.JSON(strings.NewReader(string(bs)))
		if err != nil {
			log.Fatal(err)
		}

		contractAbi := utils.NewAbiContract(contractAddress, abi, namespace)
		buffer := contractAbi.GenerateSql()
		log.Println(buffer.String())

		counter += 1

		if counter%100 == 0 {
			log.Printf("%d ABIs processed so far...\n", counter)
		}
	}
}
