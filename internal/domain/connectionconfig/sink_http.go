package connectionconfig

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func init() {
	registerSinkNormalizer(sinkNormalizers, "http", normalizeHttpSink)
}

func normalizeHttpSink(input *commonpb.HttpSinkConfigInput) (*commonpb.HttpSinkConfig, error) {
	if input == nil {
		return nil, errors.New("HTTP configuration is required")
	}
	endpoint, err := parseURL(input.GetEndpoint(), "http")
	if err != nil {
		return nil, err
	}

	return &commonpb.HttpSinkConfig{Endpoint: endpoint, Secret: input.GetSecret()}, nil
}
