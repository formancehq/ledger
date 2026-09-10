package connectionconfig

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func init() {
	registerSinkNormalizer(sinkNormalizers, "kafka", normalizeKafkaSink)
}

func normalizeKafkaSink(input *commonpb.KafkaSinkConfig) (*commonpb.KafkaSinkConfig, error) {
	if input == nil {
		return nil, errors.New("kafka configuration is required")
	}

	return input.CloneVT(), nil
}
