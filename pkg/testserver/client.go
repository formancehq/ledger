package testserver

import (
	"github.com/formancehq/go-libs/v2/testing/deferred"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
)

func Client(srv *testservice.Service) *ledgerclient.Formance {
	return ledgerclient.New(
		ledgerclient.WithServerURL(testservice.GetServerURL(srv).String()),
	)
}

func DeferClient(srv *deferred.Deferred[*testservice.Service]) *deferred.Deferred[*ledgerclient.Formance] {
	return deferred.Map(srv, Client)
}
