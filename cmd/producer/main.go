package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/credmark/abi-sql-view-generator/utils"
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
	key       = os.Getenv("AWS_ACCESS_KEY_ID")
	secret    = os.Getenv("AWS_SECRET_ACCESS_KEY")
	region    = os.Getenv("AWS_REGION")
	queueURL  = os.Getenv("SQS_QUEUE_URL")
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {

	var drop bool
	var dryRun bool
	var limit int
	var count int
	var flagQueueURL string
	var flagRegion string
	flag.BoolVar(&drop, "drop", false, "drop all existing views")
	flag.BoolVar(&dryRun, "dry-run", false, "run without submitting/creating queries")
	flag.IntVar(&limit, "limit", 0, "limit number of verified contracts returned for processing")
	flag.IntVar(&count, "count", 5, "number of minimum logs a contract should have")
	flag.StringVar(&flagQueueURL, "queue-url", queueURL, "URL of the SQS queue")
	flag.StringVar(&flagRegion, "region", region, "AWS Region of the SQS queue")
	flag.Parse()

	ctx := context.Background()
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

	options := utils.NewOptions(dsn, namespace, key, secret, flagRegion, flagQueueURL, dryRun, drop, limit, count)

	if drop {
		utils.DropViews(ctx, options)
		os.Exit(0)
	} else {
		utils.CreateViews(ctx, options)
		os.Exit(0)
	}
}
