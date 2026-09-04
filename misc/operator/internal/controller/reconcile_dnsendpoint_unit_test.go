package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestDesiredDNSEndpointsPreservesLegacyObjectName(t *testing.T) {
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "main"},
		Spec: ledgerv1alpha1.ClusterSpec{
			DNSEndpoint: &ledgerv1alpha1.LegacyDNSEndpointSpec{
				Enabled: true,
				Annotations: map[string]string{
					"external-dns.alpha.kubernetes.io/provider": "private",
				},
				Endpoints: []ledgerv1alpha1.DNSEndpointEntry{{
					DNSName: "ledger.example.com",
					Targets: []string{"ledger.example.net"},
				}},
			},
		},
	}

	desired := desiredDNSEndpoints(cluster)
	require.Len(t, desired, 1)
	assert.Equal(t, "ledger-main", desired[0].name)
	assert.Equal(t, "private", desired[0].annotations["external-dns.alpha.kubernetes.io/provider"])
	require.Len(t, desired[0].endpoints, 1)
}

func TestDesiredDNSEndpointsPrefersNewList(t *testing.T) {
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "main"},
		Spec: ledgerv1alpha1.ClusterSpec{
			DNSEndpoint: &ledgerv1alpha1.LegacyDNSEndpointSpec{
				Enabled:   true,
				Endpoints: []ledgerv1alpha1.DNSEndpointEntry{{DNSName: "legacy.example.com"}},
			},
			DNSEndpoints: []ledgerv1alpha1.DNSEndpointSpec{{
				Name:    "public",
				Enabled: true,
				Endpoints: []ledgerv1alpha1.DNSEndpointEntry{{
					DNSName: "ledger.example.com",
				}},
			}},
		},
	}

	desired := desiredDNSEndpoints(cluster)
	require.Len(t, desired, 1)
	assert.Equal(t, "ledger-main-public", desired[0].name)
}

func TestDesiredDNSEndpointsOmitsDisabledLegacyConfig(t *testing.T) {
	cluster := &ledgerv1alpha1.Cluster{
		Spec: ledgerv1alpha1.ClusterSpec{
			DNSEndpoint: &ledgerv1alpha1.LegacyDNSEndpointSpec{},
		},
	}

	assert.Empty(t, desiredDNSEndpoints(cluster))
}
