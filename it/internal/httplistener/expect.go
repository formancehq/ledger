package httplistener

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kr/pretty"
	"github.com/numary/ledger/pkg/bus"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

type haveTriggeredEvent[T bus.Payload] struct {
	event      T
	foundEvent *T
}

func (a *haveTriggeredEvent[T]) Match(actual interface{}) (success bool, err error) {
	ledger, ok := actual.(string)
	if !ok {
		return false, errors.New("have trace expect an object of type *string")
	}

	a.foundEvent, err = PickEvent[T](func(ledgerEvent string, payload T) bool {
		return ledgerEvent == ledger
	})
	if err != nil {
		return false, err
	}

	foundEventAsMap := map[string]any{}
	foundEventAsJson, err := json.Marshal(*a.foundEvent)
	if err != nil {
		panic(err)
	}
	if err = json.Unmarshal(foundEventAsJson, &foundEventAsMap); err != nil {
		panic(err)
	}

	eventAsMap := map[string]any{}
	eventAsJson, err := json.Marshal(a.event)
	if err != nil {
		panic(err)
	}
	if err = json.Unmarshal(eventAsJson, &eventAsMap); err != nil {
		panic(err)
	}

	fmt.Println(pretty.Diff(eventAsMap, foundEventAsMap))

	return Equal(eventAsMap).Match(foundEventAsMap)
}

func (a *haveTriggeredEvent[T]) FailureMessage(actual interface{}) (message string) {
	return Equal(a.event).FailureMessage(*a.foundEvent)
}

func (a *haveTriggeredEvent[T]) NegatedFailureMessage(actual interface{}) (message string) {
	return Equal(a.event).NegatedFailureMessage(*a.foundEvent)
}

var _ types.GomegaMatcher = &haveTriggeredEvent[bus.CommittedTransactions]{}

func HaveTriggeredEvent[T bus.Payload](event T) *haveTriggeredEvent[T] {
	return &haveTriggeredEvent[T]{
		event: event,
	}
}
