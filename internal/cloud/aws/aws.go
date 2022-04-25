package aws

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type Config aws.Config

func NewConfig(key, secret, region string) Config {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)
	if err != nil {
		log.Fatal("error creating AWS config:", err)
	}

	return Config(cfg)
}
