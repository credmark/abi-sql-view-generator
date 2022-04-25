package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func SendSQSMessage(cfg aws.Config, queueURL string, body string) error {
	client := sqs.NewFromConfig(cfg)

	_, err := client.SendMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: aws.String(body),
		QueueUrl:    aws.String(queueURL),
	})
	if err != nil {
		return err
	}

	return nil
}
