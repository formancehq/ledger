package grpc

import (
	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/pkg/sensitive"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// ledgerInfoReadStream projects only public read responses, after current or
// checkpoint selection and pagination. Controller/worker values stay intact.
type ledgerInfoReadStream struct {
	servicepb.BucketService_ListLedgersServer
}

func (s ledgerInfoReadStream) Send(info *ledgerpb.LedgerInfo) error {
	return s.BucketService_ListLedgersServer.Send(sensitive.Redact(info))
}
