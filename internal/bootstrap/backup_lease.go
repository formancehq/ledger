package bootstrap

import (
	"context"

	backupapp "github.com/formancehq/ledger/v3/internal/application/backup"
	"github.com/formancehq/ledger/v3/internal/infra/node"
)

type backupLeaseProvider struct{ node *node.Node }

func (p backupLeaseProvider) CurrentLeadershipLease() backupapp.LeadershipLease {
	lease := p.node.CurrentLeadershipLease()
	if lease == nil {
		return nil
	}

	return backupLease{lease}
}

type backupLease struct{ *node.LeadershipLease }

func (l backupLease) ProposalContext(ctx context.Context) context.Context {
	return node.WithLeadershipLease(ctx, l.LeadershipLease)
}
