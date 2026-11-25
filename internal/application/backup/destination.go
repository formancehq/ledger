package backup

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func admissionDestinationKey(destination *raftcmdpb.BackupDestination) string {
	normalized := destination.CloneVT()
	switch target := normalized.GetTarget().(type) {
	case *raftcmdpb.BackupDestination_S3:
		target.S3.Endpoint = admissionEndpoint(target.S3.GetEndpoint())
	case *raftcmdpb.BackupDestination_Azure:
		target.Azure.AccountName = strings.ToLower(target.Azure.GetAccountName())
		endpoint := target.Azure.GetEndpoint()
		if endpoint == "" {
			endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", target.Azure.GetAccountName())
		}
		target.Azure.Endpoint = admissionEndpoint(endpoint)
	}

	return string(state.CanonicalDestinationKey(normalized))
}

func admissionEndpoint(endpoint string) string {
	trimmed := strings.TrimRight(endpoint, "/")
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return trimmed
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if strings.HasPrefix(parsed.Host, "[") && strings.Contains(parsed.Host, "%") {
		address, zone, _ := strings.Cut(parsed.Host, "%")
		parsed.Host = strings.ToLower(address) + "%" + zone
	} else {
		parsed.Host = strings.ToLower(parsed.Host)
	}

	return parsed.String()
}
