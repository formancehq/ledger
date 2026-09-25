package controller

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// managedNATSSink is the normalized form used to compare the declarative CRD
// configuration with `ledgerctl events list --json` output.
type managedNATSSink struct {
	name         string
	url          string
	topic        string
	format       string
	batchSize    int32
	batchDelayMS int64
	eventTypes   []string
}

type ledgerctlSinkExec func(args ...string) (string, error)

type actualEventSink struct {
	kind          string
	controllerID  string
	nats          managedNATSSink
	cursor        uint64
	deliveryError string
	hasStatus     bool
}

func desiredEventSink(resource *ledgerv1alpha1.EventSink) managedNATSSink {
	spec := resource.Spec
	format := spec.Format
	if format == "" {
		format = "json"
	}

	var batchSize int32
	if spec.BatchSize != nil {
		batchSize = *spec.BatchSize
	}

	var batchDelayMS int64
	if spec.BatchDelayMs != nil {
		batchDelayMS = *spec.BatchDelayMs
	}

	eventTypes := slices.Clone(spec.EventTypes)
	slices.Sort(eventTypes)

	return managedNATSSink{
		name:         resource.Name,
		url:          spec.NATS.URL,
		topic:        spec.NATS.Topic,
		format:       format,
		batchSize:    batchSize,
		batchDelayMS: batchDelayMS,
		eventTypes:   eventTypes,
	}
}

// listedEventSinksResponse mirrors only the stable, non-secret fields emitted
// by `ledgerctl events list --json`. Other sink variants are retained as raw
// JSON solely so name conflicts are detected and never overwritten.
type listedEventSinksResponse struct {
	Sinks        []listedEventSink  `json:"sinks"`
	SinkStatuses []listedSinkStatus `json:"sinkStatuses"`
}

type listedSinkStatus struct {
	SinkName string          `json:"sinkName"`
	Cursor   protoJSONUint64 `json:"cursor"`
	Error    *struct {
		Message string `json:"message"`
	} `json:"error"`
}

type protoJSONUint64 uint64

func (v *protoJSONUint64) UnmarshalJSON(data []byte) error {
	value := strings.Trim(string(data), `"`)
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing protobuf JSON uint64 %q: %w", value, err)
	}
	*v = protoJSONUint64(parsed)

	return nil
}

type listedEventSink struct {
	Name         string          `json:"name"`
	ControllerID string          `json:"controllerId"`
	Format       string          `json:"format"`
	BatchSize    int32           `json:"batchSize"`
	BatchDelayMS protoJSONInt64  `json:"batchDelayMs"`
	EventTypes   []string        `json:"eventTypes"`
	NATS         *listedNATSSink `json:"nats"`
	ClickHouse   json.RawMessage `json:"clickhouse"`
	Kafka        json.RawMessage `json:"kafka"`
	HTTP         json.RawMessage `json:"http"`
	Databricks   json.RawMessage `json:"databricks"`
}

type listedNATSSink struct {
	URL   string `json:"url"`
	Topic string `json:"topic"`
}

// protoJSONInt64 accepts both protobuf JSON's quoted int64 representation and
// an unquoted JSON number so the parser remains compatible with either CLI
// encoder without weakening the surrounding response shape.
type protoJSONInt64 int64

func (v *protoJSONInt64) UnmarshalJSON(data []byte) error {
	value := strings.Trim(string(data), `"`)
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing protobuf JSON int64 %q: %w", value, err)
	}

	*v = protoJSONInt64(parsed)

	return nil
}

func parseActualEventSinks(stdout string) (map[string]actualEventSink, error) {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return map[string]actualEventSink{}, nil
	}

	var response listedEventSinksResponse
	if err := json.Unmarshal([]byte(trimmed), &response); err != nil {
		return nil, fmt.Errorf("parsing events list output: %w", err)
	}

	actual := make(map[string]actualEventSink, len(response.Sinks))
	statuses := make(map[string]listedSinkStatus, len(response.SinkStatuses))
	for _, status := range response.SinkStatuses {
		statuses[status.SinkName] = status
	}
	for _, sink := range response.Sinks {
		format := sink.Format
		if format == "" {
			format = "json"
		}

		eventTypes := slices.Clone(sink.EventTypes)
		slices.Sort(eventTypes)

		entry := actualEventSink{kind: listedSinkKind(sink), controllerID: sink.ControllerID}
		if status, ok := statuses[sink.Name]; ok {
			entry.hasStatus = true
			entry.cursor = uint64(status.Cursor)
			if status.Error != nil {
				entry.deliveryError = status.Error.Message
			}
		}
		if sink.NATS != nil {
			entry.nats = managedNATSSink{
				name:         sink.Name,
				url:          sink.NATS.URL,
				topic:        sink.NATS.Topic,
				format:       format,
				batchSize:    sink.BatchSize,
				batchDelayMS: int64(sink.BatchDelayMS),
				eventTypes:   eventTypes,
			}
		}

		actual[sink.Name] = entry
	}

	return actual, nil
}

func listedSinkKind(sink listedEventSink) string {
	switch {
	case sink.NATS != nil:
		return "nats"
	case len(sink.ClickHouse) > 0 && string(sink.ClickHouse) != "null":
		return "clickhouse"
	case len(sink.Kafka) > 0 && string(sink.Kafka) != "null":
		return "kafka"
	case len(sink.HTTP) > 0 && string(sink.HTTP) != "null":
		return "http"
	case len(sink.Databricks) > 0 && string(sink.Databricks) != "null":
		return "databricks"
	default:
		return "unknown"
	}
}

func eventSinksEqual(desired managedNATSSink, actual actualEventSink) bool {
	return actual.kind == "nats" &&
		desired.name == actual.nats.name &&
		desired.url == actual.nats.url &&
		desired.topic == actual.nats.topic &&
		desired.format == actual.nats.format &&
		desired.batchSize == actual.nats.batchSize &&
		desired.batchDelayMS == actual.nats.batchDelayMS &&
		slices.Equal(desired.eventTypes, actual.nats.eventTypes)
}

func addNATSSinkArgs(sink managedNATSSink) []string {
	args := []string{
		"events", "add-sink",
		"--name", sink.name,
		"--nats-url", sink.url,
		"--nats-topic", sink.topic,
		"--format", sink.format,
		"--batch-size", strconv.FormatInt(int64(sink.batchSize), 10),
		"--batch-delay-ms", strconv.FormatInt(sink.batchDelayMS, 10),
	}
	if len(sink.eventTypes) > 0 {
		args = append(args, "--event-types", strings.Join(sink.eventTypes, ","))
	}

	return args
}
