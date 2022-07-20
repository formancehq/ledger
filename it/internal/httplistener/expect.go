package httplistener

import (
	"errors"

	"github.com/numary/ledger/pkg/bus"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

type haveTriggeredEvent[T bus.Payload] struct {
	event      T
	foundEvent *T
}

func (a *haveTriggeredEvent[T]) Match(actual interface{}) (success bool, err error) {
	ledger, ok := actual.(*string)
	if !ok {
		return false, errors.New("have trace expect an object of type *string")
	}

	a.foundEvent, err = PickEvent[T](func(ledgerEvent string, payload T) bool {
		return ledgerEvent == *ledger
	})
	if err != nil {
		return false, err
	}

	return Equal(a.event).Match(*a.foundEvent)
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
