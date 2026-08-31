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

type actualEventSink struct {
	kind string
	nats managedNATSSink
}

type eventSinkDiff struct {
	toCreate []managedNATSSink
	toDrop   []string
	conflict []string
}

func desiredEventSinks(spec *ledgerv1alpha1.EventSinksSpec) []managedNATSSink {
	if spec == nil {
		return nil
	}

	desired := make([]managedNATSSink, 0, len(spec.NATS))
	for _, sink := range spec.NATS {
		format := sink.Format
		if format == "" {
			format = "json"
		}

		var batchSize int32
		if sink.BatchSize != nil {
			batchSize = *sink.BatchSize
		}

		var batchDelayMS int64
		if sink.BatchDelayMs != nil {
			batchDelayMS = *sink.BatchDelayMs
		}

		eventTypes := slices.Clone(sink.EventTypes)
		slices.Sort(eventTypes)

		desired = append(desired, managedNATSSink{
			name:         sink.Name,
			url:          sink.URL,
			topic:        sink.Topic,
			format:       format,
			batchSize:    batchSize,
			batchDelayMS: batchDelayMS,
			eventTypes:   eventTypes,
		})
	}

	slices.SortFunc(desired, func(a, b managedNATSSink) int {
		return strings.Compare(a.name, b.name)
	})

	return desired
}

// listedEventSinksResponse mirrors only the stable, non-secret fields emitted
// by `ledgerctl events list --json`. Other sink variants are retained as raw
// JSON solely so name conflicts are detected and never overwritten.
type listedEventSinksResponse struct {
	Sinks []listedEventSink `json:"sinks"`
}

type listedEventSink struct {
	Name         string          `json:"name"`
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
	for _, sink := range response.Sinks {
		format := sink.Format
		if format == "" {
			format = "json"
		}

		eventTypes := slices.Clone(sink.EventTypes)
		slices.Sort(eventTypes)

		entry := actualEventSink{kind: listedSinkKind(sink)}
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

// diffEventSinks computes an ownership-scoped reconciliation plan. An
// externally-owned sink is always a conflict, even if its configuration is an
// exact match: only names durably reserved in status.appliedSinks may be
// mutated or removed. Mismatched sinks owned by this operator are removed
// first and recreated on the next pass; Ledger retains the per-name cursor, so
// the two-pass update delays delivery without losing committed events.
func diffEventSinks(desired []managedNATSSink, actual map[string]actualEventSink, applied []string) eventSinkDiff {
	desiredByName := make(map[string]managedNATSSink, len(desired))
	owned := make(map[string]struct{}, len(applied))
	for _, name := range applied {
		owned[name] = struct{}{}
	}

	var diff eventSinkDiff
	for _, sink := range desired {
		desiredByName[sink.name] = sink
		actualSink, exists := actual[sink.name]
		if !exists {
			diff.toCreate = append(diff.toCreate, sink)

			continue
		}
		if eventSinksEqual(sink, actualSink) {
			if _, operatorOwned := owned[sink.name]; !operatorOwned {
				diff.conflict = append(diff.conflict, sink.name)
			}

			continue
		}
		if _, operatorOwned := owned[sink.name]; operatorOwned {
			diff.toDrop = append(diff.toDrop, sink.name)
		} else {
			diff.conflict = append(diff.conflict, sink.name)
		}
	}

	for _, name := range applied {
		if _, stillDesired := desiredByName[name]; stillDesired {
			continue
		}
		// Include absent sinks too: the caller must relinquish stale ownership.
		diff.toDrop = append(diff.toDrop, name)
	}

	slices.Sort(diff.toDrop)
	diff.toDrop = slices.Compact(diff.toDrop)
	slices.Sort(diff.conflict)

	return diff
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

func removeEventSinkArgs(name string) []string {
	return []string{"events", "remove-sink", "--name", name}
}

func nextAppliedSinks(current, created, dropped []string) []string {
	owned := make(map[string]struct{}, len(current)+len(created))
	for _, name := range current {
		owned[name] = struct{}{}
	}
	for _, name := range created {
		owned[name] = struct{}{}
	}
	for _, name := range dropped {
		delete(owned, name)
	}

	if len(owned) == 0 {
		return nil
	}

	next := make([]string, 0, len(owned))
	for name := range owned {
		next = append(next, name)
	}
	slices.Sort(next)

	return next
}
