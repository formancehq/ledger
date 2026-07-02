package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
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
		r := &ClusterReconciler{Clientset: fake.NewClientset(binding)}

		installed, err := r.deletionProtectionPolicyInstalled(context.Background())
		require.NoError(t, err)
		assert.True(t, installed, "binding exists, so protection is active")
	})

	t.Run("binding absent", func(t *testing.T) {
		t.Parallel()

		r := &ClusterReconciler{Clientset: fake.NewClientset()}

		installed, err := r.deletionProtectionPolicyInstalled(context.Background())
		require.NoError(t, err)
		assert.False(t, installed, "no binding, so protection is inactive")
	})

	// On Kubernetes < 1.30 the ValidatingAdmissionPolicy API is unregistered, so the
	// GET is answered with a bare 404 whose Reason is not the canonical NotFound. The
	// probe must still treat that as "not installed" (relying on IsNotFound's unknown
	// reason + code 404 fallback) rather than erroring, because deletionProtection now
	// defaults on and this probe runs on every reconcile.
	t.Run("api unregistered (old cluster) reports not installed", func(t *testing.T) {
		t.Parallel()

		cs := fake.NewClientset()
		cs.PrependReactor("get", "validatingadmissionpolicybindings",
			func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, &apierrors.StatusError{ErrStatus: metav1.Status{
					Status:  metav1.StatusFailure,
					Code:    404,
					Reason:  "", // no canonical reason, as an unrecognized resource path yields
					Message: "the server could not find the requested resource",
				}}
			})
		r := &LedgerServiceReconciler{Clientset: cs}

		installed, err := r.deletionProtectionPolicyInstalled(context.Background())
		require.NoError(t, err, "a 404 for the unregistered API must not error the reconcile")
		assert.False(t, installed)
	})

	// Any other API error must propagate: swallowing it would make a ledger silently
	// look unprotected on a transient failure instead of retrying.
	t.Run("unexpected API error propagates", func(t *testing.T) {
		t.Parallel()

		cs := fake.NewClientset()
		cs.PrependReactor("get", "validatingadmissionpolicybindings",
			func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewInternalError(assert.AnError)
			})
		r := &LedgerServiceReconciler{Clientset: cs}

		installed, err := r.deletionProtectionPolicyInstalled(context.Background())
		require.Error(t, err, "a non-NotFound error must surface, not be treated as not-installed")
		assert.False(t, installed)
	})
}
