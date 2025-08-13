package iam

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/pflag"
)

const (
	AWSRegionFlag          = "aws-region"
	AWSAccessKeyIDFlag     = "aws-access-key-id"
	AWSSecretAccessKeyFlag = "aws-secret-access-key"
	AWSSessionTokenFlag    = "aws-session-token"
	AWSProfileFlag         = "aws-profile"
	AWSRoleArnFlag         = "aws-role-arn"
)

func AddFlags(flags *pflag.FlagSet) {
	flags.String(AWSRegionFlag, "", "Specify AWS region")
	flags.String(AWSAccessKeyIDFlag, "", "AWS access key id")
	flags.String(AWSSecretAccessKeyFlag, "", "AWS secret access key")
	flags.String(AWSSessionTokenFlag, "", "AWS session token")
	flags.String(AWSProfileFlag, "", "AWS profile")
	flags.String(AWSRoleArnFlag, "", "AWS Role ARN")
}

func LoadOptionFromCommand(cmd *cobra.Command) func(opts *config.LoadOptions) error {
	return func(opts *config.LoadOptions) error {
		awsRegion, _ := cmd.Flags().GetString(AWSRegionFlag)
		awsAccessKeyID, _ := cmd.Flags().GetString(AWSAccessKeyIDFlag)
		awsSecretAccessKey, _ := cmd.Flags().GetString(AWSSecretAccessKeyFlag)
		awsSessionToken, _ := cmd.Flags().GetString(AWSSessionTokenFlag)
		awsProfile, _ := cmd.Flags().GetString(AWSProfileFlag)

		if awsAccessKeyID != "" {
			opts.Credentials = aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     awsAccessKeyID,
					SecretAccessKey: awsSecretAccessKey,
					SessionToken:    awsSessionToken,
					Source:          "flags",
				}, nil
			})
		}
		opts.Region = awsRegion
		opts.SharedConfigProfile = awsProfile

		return nil
	}
}
