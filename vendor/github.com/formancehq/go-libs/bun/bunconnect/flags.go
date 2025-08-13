package bunconnect

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/lib/pq"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/formancehq/go-libs/aws/iam"
	"github.com/formancehq/go-libs/logging"
	"github.com/spf13/pflag"
)

const (
	PostgresURIFlag             = "postgres-uri"
	PostgresAWSEnableIAMFlag    = "postgres-aws-enable-iam"
	PostgresMaxIdleConnsFlag    = "postgres-max-idle-conns"
	PostgresMaxOpenConnsFlag    = "postgres-max-open-conns"
	PostgresConnMaxIdleTimeFlag = "postgres-conn-max-idle-time"
)

func AddFlags(flags *pflag.FlagSet) {
	flags.String(PostgresURIFlag, "", "Postgres URI")
	flags.Bool(PostgresAWSEnableIAMFlag, false, "Enable AWS IAM authentication")
	flags.Int(PostgresMaxIdleConnsFlag, 0, "Max Idle connections")
	flags.Duration(PostgresConnMaxIdleTimeFlag, time.Minute, "Max Idle time for connections")
	flags.Int(PostgresMaxOpenConnsFlag, 20, "Max opened connections")
}

func ConnectionOptionsFromFlags(cmd *cobra.Command) (*ConnectionOptions, error) {
	var connector func(string) (driver.Connector, error)

	awsEnable, _ := cmd.Flags().GetBool(PostgresAWSEnableIAMFlag)
	if awsEnable {
		cfg, err := config.LoadDefaultConfig(context.Background(), iam.LoadOptionFromCommand(cmd))
		if err != nil {
			return nil, err
		}

		connector = func(s string) (driver.Connector, error) {
			return &iamConnector{
				dsn: s,
				driver: &iamDriver{
					awsConfig: cfg,
				},
				logger: logging.FromContext(cmd.Context()),
			}, nil
		}
	} else {
		connector = func(dsn string) (driver.Connector, error) {
			return pq.NewConnector(dsn)
		}
	}

	postgresUri, _ := cmd.Flags().GetString(PostgresURIFlag)
	if postgresUri == "" {
		return nil, errors.New("missing postgres uri")
	}
	maxIdleConns, _ := cmd.Flags().GetInt(PostgresMaxIdleConnsFlag)
	connMaxIdleConns, _ := cmd.Flags().GetDuration(PostgresConnMaxIdleTimeFlag)
	maxOpenConns, _ := cmd.Flags().GetInt(PostgresMaxOpenConnsFlag)

	return &ConnectionOptions{
		DatabaseSourceName: postgresUri,
		MaxIdleConns:       maxIdleConns,
		ConnMaxIdleTime:    connMaxIdleConns,
		MaxOpenConns:       maxOpenConns,
		Connector:          connector,
	}, nil
}
