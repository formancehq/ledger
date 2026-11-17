package cmdutil

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// TestClusterPodName verifies pod names route through the resource prefix,
// matching the operator's "ledger-<cr>-<ordinal>" StatefulSet pod naming (EN-1319).
func TestClusterPodName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		crName   string
		ordinal  int
		expected string
	}{
		{name: "ordinal 0", crName: "foo", ordinal: 0, expected: "ledger-foo-0"},
		{name: "ordinal 2", crName: "foo", ordinal: 2, expected: "ledger-foo-2"},
		{name: "name with dashes", crName: "my-cluster", ordinal: 1, expected: "ledger-my-cluster-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.expected, ClusterPodName(tt.crName, tt.ordinal))
		})
	}
}

func resourceCluster() *ledgerv1alpha1.Cluster {
	return &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ledger", UID: "cluster-uid"}}
}

func resourceStatefulSet(ledger *ledgerv1alpha1.Cluster) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ledger-" + ledger.Name, Namespace: ledger.Namespace, UID: "sts-uid",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ledger, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))},
		},
		Spec: appsv1.StatefulSetSpec{
			Selector:             &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": "ledger", "app.kubernetes.io/instance": ledger.Name}},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
		},
	}
}

func TestClusterPods(t *testing.T) {
	t.Parallel()
	for _, customLabels := range []bool{false, true} {
		t.Run(map[bool]string{false: "defaults", true: "custom labels and rejected desired drift"}[customLabels], func(t *testing.T) {
			t.Parallel()
			ledger := resourceCluster()
			sts := resourceStatefulSet(ledger)
			if customLabels {
				sts.Spec.Selector.MatchLabels = map[string]string{"app.kubernetes.io/name": "custom", "app.kubernetes.io/instance": "custom-instance", "extra": "value"}
				ledger.Spec.AdditionalLabels = map[string]string{"app.kubernetes.io/name": "rejected-desired-name"}
			}
			sts.Spec.Selector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: "zone", Operator: metav1.LabelSelectorOpIn, Values: []string{"west"}}}
			podLabels := make(map[string]string)
			for key, value := range sts.Spec.Selector.MatchLabels {
				podLabels[key] = value
			}
			podLabels["zone"] = "west"
			owned := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "ledger-test-0", Namespace: ledger.Namespace, Labels: podLabels,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
			}}
			foreign := owned.DeepCopy()
			foreign.Name = "foreign"
			foreign.OwnerReferences[0].UID = "foreign-sts"
			unowned := owned.DeepCopy()
			unowned.Name = "unowned"
			unowned.OwnerReferences = nil
			wrongExpression := owned.DeepCopy()
			wrongExpression.Name = "wrong-expression"
			wrongExpression.Labels["zone"] = "east"
			otherNamespace := owned.DeepCopy()
			otherNamespace.Namespace = "other"
			cs := fake.NewClientset(sts, owned, foreign, unowned, wrongExpression, otherNamespace)
			pods, err := ClusterPods(t.Context(), cs, ledger)
			require.NoError(t, err)
			require.Len(t, pods.Items, 1)
			require.Equal(t, owned.Name, pods.Items[0].Name)
			require.Equal(t, "ledger", cs.Actions()[1].GetNamespace())
		})
	}
}

func TestClusterServices(t *testing.T) {
	t.Parallel()
	ledger := resourceCluster()
	owned := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: "ledger-test", Namespace: ledger.Namespace,
		Labels:          map[string]string{"app.kubernetes.io/name": "custom", "app.kubernetes.io/instance": "custom-instance", "extra": "value"},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ledger, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))},
	}}
	foreign := owned.DeepCopy()
	foreign.Name = "foreign"
	foreign.OwnerReferences[0].UID = "other-cluster"
	unowned := owned.DeepCopy()
	unowned.Name = "unowned"
	unowned.OwnerReferences = nil
	otherNamespace := owned.DeepCopy()
	otherNamespace.Namespace = "other"
	cs := fake.NewClientset(owned, foreign, unowned, otherNamespace)
	services, err := ClusterServices(t.Context(), cs, ledger)
	require.NoError(t, err)
	require.Len(t, services.Items, 1)
	require.Equal(t, owned.Name, services.Items[0].Name)
}

func TestClusterPVCs(t *testing.T) {
	t.Parallel()
	for _, missingStatefulSet := range []bool{false, true} {
		t.Run(map[bool]string{false: "live templates", true: "missing StatefulSet retained claims"}[missingStatefulSet], func(t *testing.T) {
			t.Parallel()
			ledger := resourceCluster()
			ledger.Spec.AdditionalLabels = map[string]string{"app.kubernetes.io/name": "custom", "app.kubernetes.io/instance": "custom-instance", "extra": "value"}
			objects := []runtime.Object{}
			if !missingStatefulSet {
				sts := resourceStatefulSet(ledger)
				sts.Spec.VolumeClaimTemplates = append(sts.Spec.VolumeClaimTemplates, corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "legacy"}})
				objects = append(objects, sts)
			}
			for _, name := range []string{"wal-ledger-test-0", "data-ledger-test-8", "wal-ledger-test-01", "wal-ledger-test--1", "wal-ledger-test-1-extra", "data-ledger-other-0", "legacy-ledger-test-2"} {
				objects = append(objects, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ledger.Namespace}})
			}
			objects = append(objects, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "wal-ledger-test-3", Namespace: "other"}})
			cs := fake.NewClientset(objects...)
			pvcs, err := ClusterPVCs(t.Context(), cs, ledger)
			require.NoError(t, err)
			expected := []string{"wal-ledger-test-0", "data-ledger-test-8"}
			if !missingStatefulSet {
				expected = append(expected, "legacy-ledger-test-2")
			}
			names := make([]string, 0, len(pvcs.Items))
			for _, pvc := range pvcs.Items {
				names = append(names, pvc.Name)
			}
			require.ElementsMatch(t, expected, names)
		})
	}
}

func TestClusterResourcesRejectForeignStatefulSet(t *testing.T) {
	t.Parallel()
	ledger := resourceCluster()
	sts := resourceStatefulSet(ledger)
	sts.OwnerReferences[0].UID = "previous-cluster"
	cs := fake.NewClientset(sts)
	_, err := ClusterPods(t.Context(), cs, ledger)
	require.ErrorContains(t, err, "is not controlled by Cluster")
	_, err = ClusterPVCs(t.Context(), cs, ledger)
	require.ErrorContains(t, err, "is not controlled by Cluster")
	for _, action := range cs.Actions() {
		require.Equal(t, "get", action.GetVerb())
	}
}

func TestClusterPodsMissingStatefulSet(t *testing.T) {
	t.Parallel()
	pods, err := ClusterPods(t.Context(), fake.NewClientset(), resourceCluster())
	require.NoError(t, err)
	require.Empty(t, pods.Items)
}

func TestClusterResourceAPIErrors(t *testing.T) {
	t.Parallel()
	for _, resource := range []string{"statefulsets", "pods", "persistentvolumeclaims", "services"} {
		t.Run(resource, func(t *testing.T) {
			t.Parallel()
			ledger := resourceCluster()
			cs := fake.NewClientset(resourceStatefulSet(ledger))
			apiErr := errors.New("API unavailable")
			cs.PrependReactor("*", resource, func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, apiErr })
			var err error
			switch resource {
			case "statefulsets", "pods":
				_, err = ClusterPods(t.Context(), cs, ledger)
			case "persistentvolumeclaims":
				_, err = ClusterPVCs(t.Context(), cs, ledger)
			case "services":
				_, err = ClusterServices(t.Context(), cs, ledger)
			}
			require.ErrorIs(t, err, apiErr)
			if resource == "statefulsets" {
				_, err = ClusterPVCs(t.Context(), cs, ledger)
				require.ErrorIs(t, err, apiErr)
			}
		})
	}
}

func TestClusterPodsInvalidSelector(t *testing.T) {
	t.Parallel()
	ledger := resourceCluster()
	sts := resourceStatefulSet(ledger)
	sts.Spec.Selector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: "zone", Operator: "invalid"}}
	_, err := ClusterPods(t.Context(), fake.NewClientset(sts), ledger)
	require.ErrorContains(t, err, "selector")
}
