package testserver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/publish"
	"github.com/formancehq/stack/ledger/client/models/operations"
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
	srv, ok := actual.(*Server)
	if !ok {
		return false, fmt.Errorf("expect type %T", new(Server))
	}
	ctx := context.Background()

	ledgers, err := ListLedgers(ctx, srv, operations.V2ListLedgersRequest{
		PageSize: pointer.For(int64(100)),
	})
	if err != nil {
		return false, err
	}

	for _, ledger := range ledgers.Data {
		aggregatedBalances, err := GetAggregatedBalances(ctx, srv, operations.V2GetBalancesAggregatedRequest{
			Ledger:           ledger.Name,
			UseInsertionDate: pointer.For(true),
		})
		Expect(err).To(BeNil())
		if len(aggregatedBalances) == 0 { // it's random, a ledger could not have been targeted
			// just in case, check if the ledger has transactions
			txs, err := ListTransactions(ctx, srv, operations.V2ListTransactionsRequest{
				Ledger: ledger.Name,
			})
			Expect(err).To(BeNil())
			Expect(txs.Data).To(HaveLen(0))
		} else {
			Expect(aggregatedBalances).To(HaveLen(1))
			Expect(aggregatedBalances["USD"]).To(Equal(big.NewInt(0)))
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

// todo(libs): move in shared libs
type EventMatcher struct {
	eventName        string
	expected         any
	validationErrors []gojsonschema.ResultError
	diff             string
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

	Expect(ev.Type).To(Equal(e.eventName))

	rawSchema := jsonschema.Reflect(e.expected)
	data, err := json.Marshal(rawSchema)
	if err != nil {
		return false, fmt.Errorf("unable to marshal schema: %s", err)
	}

	schemaJsonLoader := gojsonschema.NewStringLoader(string(data))
	schema, err := gojsonschema.NewSchema(schemaJsonLoader)
	if err != nil {
		return false, fmt.Errorf("unable to load json schema: %s", err)
	}

	dataJsonLoader := gojsonschema.NewRawLoader(ev.Payload)

	validate, err := schema.Validate(dataJsonLoader)
	if err != nil {
		return false, err
	}

	if !validate.Valid() {
		e.validationErrors = validate.Errors()
		return false, fmt.Errorf("%s", validate.Errors())
	}

	marshaledPayload, err := json.Marshal(ev.Payload)
	if err != nil {
		return false, fmt.Errorf("unable to marshal payload: %s", err)
	}

	unmarshalledPayload := reflect.New(reflect.TypeOf(e.expected)).Interface()
	if err := json.Unmarshal(marshaledPayload, unmarshalledPayload); err != nil {
		return false, fmt.Errorf("unable to unmarshal payload: %s", err)
	}

	// unmarshalledPayload is actually a pointer
	// as it is seen as "any" by the code, we use reflection to get the targeted valud
	unmarshalledPayload = reflect.ValueOf(unmarshalledPayload).Elem().Interface()

	diff := cmp.Diff(unmarshalledPayload, e.expected, cmp.Comparer(func(v1 *big.Int, v2 *big.Int) bool {
		return v1.String() == v2.String()
	}))
	if diff != "" {
		e.diff = diff
		return false, nil
	}

	return true, nil
}

func (e *EventMatcher) FailureMessage(_ any) (message string) {
	ret := "event does not match expectations"
	for _, validationError := range e.validationErrors {
		ret += validationError.String() + "\n"
	}
	if e.diff != "" {
		ret += e.diff
		ret += "\n"
	}
	return ret
}

func (e *EventMatcher) NegatedFailureMessage(_ any) (message string) {
	return fmt.Sprintf("event should not match expected type %T", e.expected)
}

var _ types.GomegaMatcher = (*EventMatcher)(nil)

func Event(eventName string, expected any) types.GomegaMatcher {
	return &EventMatcher{
		expected: expected,
		eventName: eventName,
	}
}
