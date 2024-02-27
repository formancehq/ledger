package iam

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	AWSRegionFlag          = "aws-region"
	AWSAccessKeyIDFlag     = "aws-access-key-id"
	AWSSecretAccessKeyFlag = "aws-secret-access-key"
	AWSSessionTokenFlag    = "aws-session-token"
	AWSProfileFlag         = "aws-profile"
)

func InitFlags(flags *pflag.FlagSet) {
	flags.String(AWSRegionFlag, "", "Specify AWS region")
	flags.String(AWSAccessKeyIDFlag, "", "AWS access key id")
	flags.String(AWSSecretAccessKeyFlag, "", "AWS secret access key")
	flags.String(AWSSessionTokenFlag, "", "AWS session token")
	flags.String(AWSProfileFlag, "", "AWS profile")
}

func LoadOptionFromViper() func(opts *config.LoadOptions) error {
	return func(opts *config.LoadOptions) error {
		if viper.GetString(AWSAccessKeyIDFlag) != "" {
			opts.Credentials = aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     viper.GetString(AWSAccessKeyIDFlag),
					SecretAccessKey: viper.GetString(AWSSecretAccessKeyFlag),
					SessionToken:    viper.GetString(AWSSessionTokenFlag),
					Source:          "flags",
				}, nil
			})
		}
		opts.Region = viper.GetString(AWSRegionFlag)
		opts.SharedConfigProfile = viper.GetString(AWSProfileFlag)

		return nil
	}
}
