//go:build e2e

// Package e2e contains a minimal Go test that verifies S3 content after
// chainsaw has run the CRD lifecycle tests. Run chainsaw first, then this.
package e2e

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	minioEndpoint  = "http://localhost:9000"
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
)

var (
	s3Client *s3.Client
	ctx      context.Context
)

func TestMain(m *testing.M) {
	ctx = context.Background()

	s3Client = s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(minioAccessKey, minioSecretKey, ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint)
		o.UsePathStyle = true
	})

	os.Exit(m.Run())
}
