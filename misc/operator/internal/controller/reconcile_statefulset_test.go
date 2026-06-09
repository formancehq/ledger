package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestBuildStatefulSetSpec_ExplicitRollingUpdate(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.LedgerService{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default"},
		Spec:       ledgerv1alpha1.LedgerServiceSpec{},
	}

	spec := buildStatefulSetSpec(ls, "hash", nil, "disabled")

	assert.Equal(t, appsv1.RollingUpdateStatefulSetStrategyType, spec.UpdateStrategy.Type)
	require.NotNil(t, spec.UpdateStrategy.RollingUpdate)
	require.NotNil(t, spec.UpdateStrategy.RollingUpdate.Partition)
	assert.Equal(t, int32(0), *spec.UpdateStrategy.RollingUpdate.Partition,
		"Partition=0 ensures all pods are rolled when the template hash changes")
}
