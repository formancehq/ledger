package connectionconfig

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func init() {
	registerSinkNormalizer(sinkNormalizers, "databricks", normalizeDatabricksSink)
}

func normalizeDatabricksSink(input *commonpb.DatabricksSinkConfig) (*commonpb.DatabricksSinkConfig, error) {
	if input == nil {
		return nil, errors.New("databricks configuration is required")
	}

	return input.CloneVT(), nil
}
