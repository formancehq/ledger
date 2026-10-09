package protohelpers

import ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

// NewPosting constructs a server-side uncolored posting.
var NewPosting = ledgerpb.NewPosting

// NewColoredPosting constructs a server-side posting with an explicit color.
var NewColoredPosting = ledgerpb.NewColoredPosting
