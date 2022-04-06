package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	abi "github.com/credmark/abi-sql-view-generator/pkg"
	_ "github.com/snowflakedb/gosnowflake"
)

// const query string = "SELECT DISTINCT CONTRACT_ADDRESS, CONTRACT_NAME, ABI FROM DEPLOYED_CONTRACT_METADATA;"
const query string = `
with verified_contracts as (
    select distinct contract_address, contract_name, abi
    from deployed_contract_metadata
)

select
    l.address as contract_address,
    c.contract_name as contract_name,
    c.abi as abi,
    count(*) as n_logs
from logs l
join verified_contracts c on l.address = c.contract_address
group by 1, 2, 3
order by 4 desc
limit 10;
`

func main() {
	account := os.Getenv("DBT_SF_ACCOUNT")
	user := os.Getenv("DBT_SF_USER")
	password := os.Getenv("DBT_SF_PASSWORD")
	database := os.Getenv("DBT_SF_DATABASE")
	schema := os.Getenv("DBT_SF_SCHEMA")
	warehouse := os.Getenv("DBT_SF_WAREHOUSE")
	role := os.Getenv("DBT_SF_ROLE")

	connectionString := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s", user, password, account, database, schema, warehouse, role)
	db, err := sql.Open("snowflake", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	log.Println("Connection successful!")

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	counter := 0
	for rows.Next() {
		var contractAddress string
		var contractName string
		var abis []abi.Abi
		var numLogs int
		bs := []byte{}
		err := rows.Scan(&contractAddress, &contractName, &bs, &numLogs)
		if err != nil {
			log.Fatal(err)
		}

		err = json.Unmarshal(bs, &abis)
		if err != nil {
			log.Fatal(err)
		}

		for _, v := range abis {
			a := abi.NewAbi(v, contractAddress)
			a.CreateSQLFile()
		}
		counter += 1

		if counter%100 == 0 {
			log.Printf("%d ABIs processed so far...\n", counter)
		}
	}
}
