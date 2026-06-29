package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// TestDeletionProtectionPolicyInstalled verifies the controller decides whether
// deletion protection is active by probing the actual cluster-scoped
// ValidatingAdmissionPolicyBinding, not a release-local flag — so it stays correct
// in a multi-release setup where a sibling release relies on the owning release's
// singleton policy.
func TestDeletionProtectionPolicyInstalled(t *testing.T) {
	t.Parallel()

	t.Run("binding present", func(t *testing.T) {
		t.Parallel()

		binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
			ObjectMeta: metav1.ObjectMeta{Name: volumeProtectionPVCBindingName},
		}
		r := &LedgerServiceReconciler{Clientset: fake.NewClientset(binding)}

		installed, err := r.deletionProtectionPolicyInstalled(context.Background())
		require.NoError(t, err)
		assert.True(t, installed, "binding exists, so protection is active")
	})

	t.Run("binding absent", func(t *testing.T) {
		t.Parallel()

		r := &LedgerServiceReconciler{Clientset: fake.NewClientset()}

		installed, err := r.deletionProtectionPolicyInstalled(context.Background())
		require.NoError(t, err)
		assert.False(t, installed, "no binding, so protection is inactive")
	})
}
