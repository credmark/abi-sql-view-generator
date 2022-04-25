package utils

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"

	sf "github.com/snowflakedb/gosnowflake"
)

func getDropQuery() string {
	path, _ := filepath.Abs("sql/drop.sql")
	fb, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	return string(fb)
}

// TODO: break up into batches to avoid buffer too big error if trying to delete all views later
func generateDropStatements(rows *sql.Rows) (bytes.Buffer, int) {
	buffer := bytes.Buffer{}
	rowCount := 0
	for rows.Next() {
		var viewName string
		err := rows.Scan(&viewName)
		if err != nil {
			log.Fatal(err)
		}

		statement := fmt.Sprintf("DROP VIEW IF EXISTS %s;\n", viewName)
		buffer.WriteString(statement)
		rowCount += 1
	}

	return buffer, rowCount
}

func dropViews(ctx context.Context, db *sql.DB, buffer bytes.Buffer, rowCount int) {
	multiStatementCtx, _ := sf.WithMultiStatement(ctx, rowCount)

	_, err := db.ExecContext(multiStatementCtx, buffer.String())
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("view deletion complete")
}

func DropViews(ctx context.Context, options *Options) {

	if options.DryRun {
		log.Println("running in dry-run mode. Views will not be dropped")
	} else {
		log.Println("preparing to drop all views...")
	}

	query := getDropQuery()
	log.Println("connecting to database...")

	db, err := sql.Open("snowflake", options.DSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}

	buffer, rowCount := generateDropStatements(rows)
	log.Printf("dropping %d views\n", rowCount)

	if !options.DryRun {
		dropViews(ctx, db, buffer, rowCount)
	}
}
