package ginkgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/publish"
	"github.com/formancehq/go-libs/v3/testing/deferred"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/testserver"
	"github.com/google/go-cmp/cmp"
	"github.com/invopop/jsonschema"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/xeipuuv/gojsonschema"
	"math/big"
	"reflect"
)

type HaveCoherentStateMatcher struct{}

func (h HaveCoherentStateMatcher) Match(actual interface{}) (success bool, err error) {
	d, ok := actual.(*deferred.Deferred[*testservice.Service])
	if !ok {
		return false, fmt.Errorf("expect type %T", new(testservice.Service))
	}
	ctx := context.Background()

	testServer, err := d.Wait(context.Background())
	if err != nil {
		return false, err
	}
	client := testserver.Client(testServer).Ledger.V2

	ledgers, err := client.ListLedgers(ctx, operations.V2ListLedgersRequest{
		PageSize: pointer.For(int64(100)),
	})
	if err != nil {
		return false, err
	}

	for _, ledger := range ledgers.V2LedgerListResponse.Cursor.Data {
		aggregatedBalances, err := client.GetBalancesAggregated(ctx, operations.V2GetBalancesAggregatedRequest{
			Ledger:           ledger.Name,
			UseInsertionDate: pointer.For(true),
		})
		Expect(err).To(BeNil())
		if len(aggregatedBalances.V2AggregateBalancesResponse.Data) == 0 { // it's random, a ledger could not have been targeted
			// just in case, check if the ledger has transactions
			txs, err := client.ListTransactions(ctx, operations.V2ListTransactionsRequest{
				Ledger: ledger.Name,
			})
			Expect(err).To(BeNil())
			Expect(txs.V2TransactionsCursorResponse.Cursor.Data).To(HaveLen(0))
		} else {
			Expect(aggregatedBalances.V2AggregateBalancesResponse.Data).To(HaveLen(1))
			Expect(aggregatedBalances.V2AggregateBalancesResponse.Data["USD"]).To(Equal(big.NewInt(0)))
		}
	}

	return true, nil
}

func (h HaveCoherentStateMatcher) FailureMessage(_ interface{}) (message string) {
	return "server should has coherent state"
}

func (h HaveCoherentStateMatcher) NegatedFailureMessage(_ interface{}) (message string) {
	return "server should not has coherent state but has"
}

var _ types.GomegaMatcher = (*HaveCoherentStateMatcher)(nil)

func HaveCoherentState() *HaveCoherentStateMatcher {
	return &HaveCoherentStateMatcher{}
}

type PayloadMatcher interface {
	Match(actual interface{}) error
}

type NoOpPayloadMatcher struct{}

func (n NoOpPayloadMatcher) Match(interface{}) error {
	return nil
}

var _ PayloadMatcher = (*NoOpPayloadMatcher)(nil)

type StructPayloadMatcher struct {
	expected any
}

func (e StructPayloadMatcher) Match(payload interface{}) error {
	rawSchema := jsonschema.Reflect(e.expected)
	data, err := json.Marshal(rawSchema)
	if err != nil {
		return fmt.Errorf("unable to marshal schema: %s", err)
	}

	schemaJSONLoader := gojsonschema.NewStringLoader(string(data))
	schema, err := gojsonschema.NewSchema(schemaJSONLoader)
	if err != nil {
		return fmt.Errorf("unable to load json schema: %s", err)
	}

	dataJsonLoader := gojsonschema.NewRawLoader(payload)

	validate, err := schema.Validate(dataJsonLoader)
	if err != nil {
		return err
	}

	if !validate.Valid() {
		return fmt.Errorf("%s", validate.Errors())
	}

	marshaledPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("unable to marshal payload: %s", err)
	}

	unmarshalledPayload := reflect.New(reflect.TypeOf(e.expected)).Interface()
	if err := json.Unmarshal(marshaledPayload, unmarshalledPayload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %s", err)
	}

	// unmarshalledPayload is actually a pointer
	// as it is seen as "any" by the code, we use reflection to get the targeted valud
	unmarshalledPayload = reflect.ValueOf(unmarshalledPayload).Elem().Interface()

	diff := cmp.Diff(unmarshalledPayload, e.expected, cmp.Comparer(func(v1 *big.Int, v2 *big.Int) bool {
		return v1.String() == v2.String()
	}))
	if diff != "" {
		return errors.New(diff)
	}

	return nil
}

func WithPayload(v any) StructPayloadMatcher {
	return StructPayloadMatcher{
		expected: v,
	}
}

var _ PayloadMatcher = (*StructPayloadMatcher)(nil)

// todo(libs): move in shared libs
type EventMatcher struct {
	eventName string
	matchers  []PayloadMatcher
	err       error
}

func (e *EventMatcher) Match(actual any) (success bool, err error) {
	msg, ok := actual.(*nats.Msg)
	if !ok {
		return false, fmt.Errorf("expected type %t", actual)
	}

	ev := publish.EventMessage{}
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		return false, fmt.Errorf("unable to unmarshal msg: %s", err)
	}

	if ev.Type != e.eventName {
		return false, nil
	}

	for _, matcher := range e.matchers {
		if e.err = matcher.Match(ev.Payload); e.err != nil {
			return false, nil
		}
	}

	return true, nil
}

func (e *EventMatcher) FailureMessage(_ any) (message string) {
	return fmt.Sprintf("event does not match expectations: %s", e.err)
}

func (e *EventMatcher) NegatedFailureMessage(_ any) (message string) {
	return "event should not match"
}

var _ types.GomegaMatcher = (*EventMatcher)(nil)

func Event(eventName string, matchers ...PayloadMatcher) types.GomegaMatcher {
	return &EventMatcher{
		matchers:  matchers,
		eventName: eventName,
	}
}
