package connectionconfig

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func init() {
	registerSinkNormalizer(sinkNormalizers, "clickhouse", normalizeClickhouseSink)
}

func normalizeClickhouseSink(input *commonpb.ClickHouseSinkConfigInput) (*commonpb.ClickHouseSinkConfig, error) {
	if input == nil {
		return nil, errors.New("ClickHouse configuration is required")
	}
	connection, err := parseDatabase(input.GetDsn(), false)
	if err != nil {
		return nil, err
	}

	return &commonpb.ClickHouseSinkConfig{Connection: connection, Table: input.GetTable()}, nil
}
