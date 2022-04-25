package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func SendSQSMessage(cfg Config, queueURL string, body string) error {
	config := aws.Config(cfg)
	client := sqs.NewFromConfig(config)

	_, err := client.SendMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: aws.String(body),
		QueueUrl:    aws.String(queueURL),
	})
	if err != nil {
		return err
	}

	return nil
}
