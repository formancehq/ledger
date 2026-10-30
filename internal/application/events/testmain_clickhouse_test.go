//go:build clickhouse

package events_test

import (
	"context"
	"fmt"
	"net/url"

	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

var sharedClickHouseDSN string
var sharedClickHouseHTTPURL string

func init() {
	var chContainer *chmodule.ClickHouseContainer

	registerTestSetup(
		func(ctx context.Context) error {
			var err error

			chContainer, err = chmodule.Run(ctx, "clickhouse/clickhouse-server:24-alpine")
			if err != nil {
				return fmt.Errorf("failed to start ClickHouse container: %w", err)
			}

			sharedClickHouseDSN, err = chContainer.ConnectionString(ctx)
			if err != nil {
				_ = chContainer.Terminate(ctx)

				return fmt.Errorf("failed to get ClickHouse DSN: %w", err)
			}
			endpoint, err := chContainer.PortEndpoint(ctx, "8123/tcp", "http")
			if err != nil {
				_ = chContainer.Terminate(ctx) // Best effort cleanup after setup failure.

				return fmt.Errorf("failed to get ClickHouse HTTP endpoint: %w", err)
			}
			httpURL, err := url.Parse(endpoint)
			if err != nil {
				_ = chContainer.Terminate(ctx) // Best effort cleanup after setup failure.

				return fmt.Errorf("parsing ClickHouse HTTP endpoint: %w", err)
			}
			httpURL.User = url.UserPassword(chContainer.User, chContainer.Password)
			values := httpURL.Query()
			values.Set("database", chContainer.DbName)
			httpURL.RawQuery = values.Encode()
			sharedClickHouseHTTPURL = httpURL.String()

			return nil
		},
		func(ctx context.Context) {
			if chContainer != nil {
				_ = chContainer.Terminate(ctx)
			}
		},
	)
}
