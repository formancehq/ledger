//go:build clickhouse

package events_test

import (
	"context"
	"fmt"

	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

var sharedClickHouseDSN string

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

			return nil
		},
		func(ctx context.Context) {
			if chContainer != nil {
				_ = chContainer.Terminate(ctx)
			}
		},
	)
}
