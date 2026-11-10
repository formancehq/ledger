package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// Stop at the actual journal-create boundary, after the production code has
// durably prepared its intent. The returned bytes were never stored in the API.
func newStorageJournalPreparingFixture(t *testing.T) (*claimRecoveryFixture, *corev1.ConfigMap) {
	t.Helper()
	f := newClaimRecoveryFixture(t)
	stop := errors.New("journal create stopped after durable intent")
	var attempted *corev1.ConfigMap
	c := interceptor.NewClient(f.client, interceptor.Funcs{
		Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if cm, ok := obj.(*corev1.ConfigMap); ok {
				attempted = cm.DeepCopy()
				pointer := f.latestLedger(t).Status.ClaimTemplateRecovery
				require.NotNil(t, pointer, "durable intent must precede the first journal write")
				require.Empty(t, pointer.ConfigMapUID)
				require.False(t, pointer.Completed)
				require.Equal(t, string(f.source.UID), pointer.SourceUID)
				require.Equal(t, cm.Name, pointer.ConfigMapName)
				require.Equal(t, claimTemplateRecoveryDigest(f.ledger, []byte(cm.Data[claimTemplateRecoveryKey])), pointer.PayloadSHA256)

				return stop
			}

			return inner.Create(ctx, obj, opts...)
		},
	})
	_, err := f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.ErrorIs(t, err, stop)
	_, fenced := errors.AsType[*claimTemplateJournalWriteError](err)
	require.True(t, fenced)
	require.NotNil(t, attempted)

	return f, attempted
}

// The fake client supplies deterministic ordering observations only. In
// particular, these tests do not assert API-server RBAC or immutable-CM rules.
func storageJournalMutationRecorder(c client.WithWatch, mutations *[]string) client.WithWatch {
	record := func(verb string, obj client.Object) {
		*mutations = append(*mutations, fmt.Sprintf("%s %T/%s", verb, obj, obj.GetName()))
	}

	return interceptor.NewClient(c, interceptor.Funcs{
		Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			record("create", obj)

			return inner.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			record("update", obj)

			return inner.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, inner client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			record("patch", obj)

			return inner.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			record("delete", obj)

			return inner.Delete(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, inner client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			record("patch "+sub, obj)

			return inner.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}

func TestStorageJournalTrustDigestBindsExactPayloadAndCluster(t *testing.T) {
	t.Parallel()
	f, journal := newStorageJournalPreparingFixture(t)
	payload := []byte(journal.Data[claimTemplateRecoveryKey])
	domain := "ledger.formance.com/claim-template-recovery/v1\x00" + string(f.ledger.UID) + "\x00" + f.ledger.Namespace + "\x00" + resourceName(f.ledger.Name) + "-storage-recovery\x00"
	digest := sha256.Sum256(append([]byte(domain), payload...))
	want := hex.EncodeToString(digest[:])
	require.Equal(t, want, f.latestLedger(t).Status.ClaimTemplateRecovery.PayloadSHA256)
	require.Equal(t, resourceName(f.ledger.Name)+"-storage-recovery-"+want[:32], journal.Name)
	for _, change := range []string{"cluster UID", "namespace", "workload name", "payload bytes"} {
		t.Run(change, func(t *testing.T) {
			ledger := f.ledger.DeepCopy()
			data := append([]byte(nil), payload...)
			switch change {
			case "cluster UID":
				ledger.UID = "other-cluster"
			case "namespace":
				ledger.Namespace = "other-namespace"
			case "workload name":
				ledger.Name = "other-workload"
			case "payload bytes":
				data = append(data, '\n') // Even equivalent JSON has different committed bytes.
			}
			require.NotEqual(t, want, claimTemplateRecoveryDigest(ledger, data))
		})
	}
}

func TestStorageJournalTrustPrunedIntentPreventsJournalCreate(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"sourceUID", "payloadSHA256", "configMapName"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			creates, statusWrites, deletes := 0, 0, 0
			wrapped := interceptor.NewClient(f.client, interceptor.Funcs{
				SubResourcePatch: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
					statusWrites++
					pointer := obj.(*ledgerv1alpha1.Cluster).Status.ClaimTemplateRecovery
					switch field {
					case "sourceUID":
						pointer.SourceUID = ""
					case "payloadSHA256":
						pointer.PayloadSHA256 = ""
					case "configMapName":
						pointer.ConfigMapName = ""
					}

					return nil // Model a successful response that pruned one new field.
				},
				Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*corev1.ConfigMap); ok {
						creates++
					}

					return inner.Create(ctx, obj, opts...)
				},
				Delete: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					deletes++

					return inner.Delete(ctx, obj, opts...)
				},
			})
			_, err := f.reconciler(wrapped).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.ErrorContains(t, err, "install the updated Cluster CRD")
			_, fenced := errors.AsType[*claimTemplateJournalWriteError](err)
			require.True(t, fenced)
			require.Equal(t, 1, statusWrites)
			require.Zero(t, creates)
			require.Zero(t, deletes)
			f.requireStorageUnchanged(t)
		})
	}
}

func TestStorageJournalTrustStandaloneJournalHasNoAuthority(t *testing.T) {
	t.Parallel()
	f, journal := newStorageJournalPreparingFixture(t)
	ledger := f.latestLedger(t)
	require.NoError(t, f.reconciler(f.client).persistClaimTemplateRecovery(t.Context(), ledger, ledger.Status.ClaimTemplateRecovery, nil))
	// A plausible owned journal remains only an inert fixture without status.
	journal.Name = resourceName(ledger.Name) + "-storage-recovery-inert-unbound-fixture"
	var saved claimTemplateRecovery
	require.NoError(t, json.Unmarshal([]byte(journal.Data[claimTemplateRecoveryKey]), &saved))
	saved.StatefulSet.Spec.Template.Annotations["fixture-note"] = "unbound"
	data, err := json.Marshal(saved)
	require.NoError(t, err)
	journal.Data[claimTemplateRecoveryKey] = string(data)
	require.NoError(t, f.client.Create(t.Context(), journal))
	var mutations []string
	r := f.reconciler(storageJournalMutationRecorder(f.client, &mutations))
	handled, _, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.False(t, handled)
	require.Empty(t, mutations)
	require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
	safe, err := r.claimTemplatePreparationUncommitted(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.True(t, safe, "standalone ConfigMaps must not block the direct pre-attempt continuation fence")
	f.requireStorageUnchanged(t)
}

func TestStorageJournalTrustRejectsUnauthenticatedBytesBeforeDependencies(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"mutable", "workload annotation", "retained claim", "digest suffix", "source UID"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			f := newStorageRecoveryDependenciesFixture(t)
			journal, snapshot := f.journal(t)
			pointer := *f.latestLedger(t).Status.ClaimTemplateRecovery
			if change == "digest suffix" || change == "source UID" {
				ledger := f.latestLedger(t)
				if change == "digest suffix" {
					replacement := "0"
					if strings.HasSuffix(pointer.PayloadSHA256, replacement) {
						replacement = "1"
					}
					ledger.Status.ClaimTemplateRecovery.PayloadSHA256 = pointer.PayloadSHA256[:63] + replacement
				} else {
					ledger.Status.ClaimTemplateRecovery.SourceUID = "different-source"
				}
				require.NoError(t, f.client.Status().Update(t.Context(), ledger))
			} else {
				switch change {
				case "mutable":
					journal.Immutable = new(false)
				case "workload annotation":
					snapshot.StatefulSet.Spec.Template.Annotations["fixture-note"] = "changed"
				case "retained claim":
					snapshot.Claims[0].PVUID = "different-retained-volume"
				}
				if change != "mutable" {
					data, err := json.Marshal(snapshot)
					require.NoError(t, err)
					journal.Data[claimTemplateRecoveryKey] = string(data)
				}
			}
			var mutations []string
			wrapped := storageJournalMutationRecorder(f.client, &mutations)
			r := f.reconciler(wrapped)
			// Model a divergent API observation without asking the fake client to
			// enforce immutability. Only harmless fixture metadata/bindings differ.
			r.APIReader = interceptor.NewClient(f.client, interceptor.Funcs{
				Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := inner.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if cm, ok := obj.(*corev1.ConfigMap); ok && key == client.ObjectKeyFromObject(journal) {
						*cm = *journal.DeepCopy()
					}

					return nil
				},
			})
			typed := storageRecoveryTypedClient(f, false)
			r.Clientset = typed
			handled, _, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.True(t, handled)
			require.Error(t, err)
			switch change {
			case "mutable":
				require.ErrorContains(t, err, "immutable")
			case "source UID":
				require.ErrorContains(t, err, "source UID differs from authorized pointer")
			default:
				require.ErrorContains(t, err, "digest")
			}
			require.Empty(t, mutations, "authentication must precede dependency, workload and status mutations")
			require.Empty(t, typed.Actions(), "no volume-protection reads or writes before journal authentication")
			require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{})))
			f.requireStorageUnchanged(t)
		})
	}
}

func TestStorageJournalTrustIntentAttemptFencesSamePassWithoutCommit(t *testing.T) {
	t.Parallel()
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("intentCommitted=%t", commit), func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			f.ledger.Spec.Persistence.DeletionProtection = new(false)
			hash := computeSpecHash(&f.ledger.Spec)
			f.source.CreationTimestamp = metav1.Now()
			f.source.Spec = buildStatefulSetSpec(f.ledger, hash, nil, tlsModeDisabled)
			recordPodConfigHash(f.source, hash)
			require.NoError(t, f.client.Update(t.Context(), f.source))
			original := f.source.DeepCopy()
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			f.ledger.Spec.Replicas = new(int32(5))
			f.ledger.Spec.Persistence.DeletionProtection = new(true)
			require.NoError(t, f.client.Update(t.Context(), f.ledger))
			lost := errors.New("first intent response unavailable")
			attempts, journalCreates, normalWrites := 0, 0, 0
			wrapped := interceptor.NewClient(f.client, interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, inner client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					attempts++
					if commit {
						if err := inner.SubResource(sub).Patch(ctx, obj, patch, opts...); err != nil {
							return err
						}
					}

					return lost
				},
				Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*corev1.ConfigMap); ok {
						journalCreates++
					}

					return inner.Create(ctx, obj, opts...)
				},
				Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					options := (&client.UpdateOptions{}).ApplyOptions(opts)
					if _, ok := obj.(*appsv1.StatefulSet); ok && len(options.DryRun) == 0 {
						normalWrites++
					}

					return inner.Update(ctx, obj, opts...)
				},
			})
			r := f.reconciler(wrapped)
			typed := storageRecoveryTypedClient(f, false)
			r.Clientset = typed
			_, err := r.reconcileStatefulSet(t.Context(), f.ledger, computeSpecHash(&f.ledger.Spec), nil)
			require.ErrorIs(t, err, lost)
			_, fenced := errors.AsType[*claimTemplateJournalWriteError](err)
			require.True(t, fenced)
			require.Equal(t, 1, attempts)
			require.Zero(t, journalCreates)
			require.Zero(t, normalWrites)
			require.Empty(t, typed.Actions())
			var actual appsv1.StatefulSet
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &actual))
			require.Equal(t, original.Spec, actual.Spec)
			if commit {
				require.NotNil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
			} else {
				require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
			}
		})
	}
}

func TestStorageJournalTrustLostResponsesResumeFromDurableState(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"intent", "create", "bind"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			lost := errors.New("one committed response lost")
			intentWrites, createWrites, bindWrites, deletes := 0, 0, 0, 0
			var committed *corev1.ConfigMap
			wrapped := interceptor.NewClient(f.client, interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, inner client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					if err := inner.SubResource(sub).Patch(ctx, obj, patch, opts...); err != nil {
						return err
					}
					pointer := obj.(*ledgerv1alpha1.Cluster).Status.ClaimTemplateRecovery
					if pointer.ConfigMapUID == "" {
						intentWrites++
						if boundary == "intent" {
							return lost
						}
					} else {
						bindWrites++
						if boundary == "bind" {
							return lost
						}
					}

					return nil
				},
				Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if err := inner.Create(ctx, obj, opts...); err != nil {
						return err
					}
					if cm, ok := obj.(*corev1.ConfigMap); ok {
						createWrites++
						committed = cm.DeepCopy()
						if boundary == "create" {
							return lost
						}
					}

					return nil
				},
				Delete: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					deletes++

					return inner.Delete(ctx, obj, opts...)
				},
			})
			_, err := f.reconciler(wrapped).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.ErrorIs(t, err, lost)
			require.Equal(t, 1, intentWrites)
			require.Zero(t, deletes)
			ledger := f.latestLedger(t)
			require.NotNil(t, ledger.Status.ClaimTemplateRecovery)
			ledger.Spec.Replicas = new(int32(2))
			ledger.Spec.Health = &ledgerv1alpha1.HealthConfig{Interval: "invalid"}
			require.Error(t, validateSpec(ledger))
			require.NoError(t, f.client.Update(t.Context(), ledger))
			fresh := f.reconciler(f.client)
			handled, result, err := fresh.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			require.True(t, handled)
			require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
			if boundary == "intent" {
				require.Zero(t, createWrites)
				require.Zero(t, bindWrites)
				require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery, "missing journal permits only safe intent abandonment")
				var source appsv1.StatefulSet
				require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &source))
				require.Equal(t, f.source.Spec, source.Spec)
			} else {
				require.Equal(t, 1, createWrites)
				wantBinds := 0
				if boundary == "bind" {
					wantBinds = 1
				}
				require.Equal(t, wantBinds, bindWrites)
				require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{})))
				handled, _, err = f.reconciler(f.client).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
				require.NoError(t, err)
				require.True(t, handled)
				journal, snapshot := f.journal(t)
				require.Equal(t, committed.UID, journal.UID)
				require.Equal(t, committed.Data, journal.Data)
				require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec)
				require.EqualValues(t, 3, *f.replacement(t).Spec.Replicas)
			}
			f.requireStorageUnchanged(t)
		})
	}
}

func TestStorageJournalTrustRechecksJournalAfterBindingUID(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"missing", "different UID", "different payload"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			f, journal := newStorageJournalPreparingFixture(t)
			require.NoError(t, f.client.Create(t.Context(), journal))
			var mutations []string
			r := f.reconciler(storageJournalMutationRecorder(f.client, &mutations))
			journalReads := 0
			r.APIReader = interceptor.NewClient(f.client, interceptor.Funcs{
				Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if cm, ok := obj.(*corev1.ConfigMap); ok && key == client.ObjectKeyFromObject(journal) {
						journalReads++
						if journalReads == 2 && change == "missing" {
							return apierrors.NewNotFound(corev1.Resource("configmaps"), key.Name)
						}
						if err := inner.Get(ctx, key, cm, opts...); err != nil {
							return err
						}
						if journalReads == 2 {
							switch change {
							case "different UID":
								cm.UID = "another-journal"
							case "different payload":
								cm.Data[claimTemplateRecoveryKey] += "\n"
							}
						}

						return nil
					}

					return inner.Get(ctx, key, obj, opts...)
				},
			})
			typed := storageRecoveryTypedClient(f, false)
			r.Clientset = typed
			handled, _, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.True(t, handled)
			require.Error(t, err)
			require.Equal(t, 2, journalReads)
			switch change {
			case "missing":
				require.True(t, apierrors.IsNotFound(err))
			case "different UID":
				require.ErrorContains(t, err, "differs from committed pointer")
			default:
				require.ErrorContains(t, err, "digest")
			}
			require.Equal(t, []string{fmt.Sprintf("patch status %T/%s", f.ledger, f.ledger.Name)}, mutations,
				"only the UID binding may commit before reauthentication")
			require.Equal(t, string(journal.UID), f.latestLedger(t).Status.ClaimTemplateRecovery.ConfigMapUID)
			require.Empty(t, typed.Actions())
			var source appsv1.StatefulSet
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &source))
			require.Equal(t, f.source.Spec, source.Spec)
			f.requireStorageUnchanged(t)
		})
	}
}

func TestStorageJournalTrustPreparingAbsenceAbandonsOnlySameLiveSource(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"unready", "source generation", "invalid desired spec"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			f, lateJournal := newStorageJournalPreparingFixture(t)
			if change == "invalid desired spec" {
				ledger := f.latestLedger(t)
				ledger.Spec.Replicas = new(int32(2))
				ledger.Spec.Health = &ledgerv1alpha1.HealthConfig{Interval: "invalid"}
				require.NoError(t, f.client.Update(t.Context(), ledger))
			} else {
				source := f.source.DeepCopy()
				if change == "unready" {
					source.Status.ReadyReplicas = 0
					require.NoError(t, f.client.Status().Update(t.Context(), source))
				} else {
					source.Generation++
					source.Spec.Template.Annotations["fixture-note"] = "new-generation"
					require.NoError(t, f.client.Update(t.Context(), source))
				}
			}
			var mutations []string
			r := f.reconciler(storageJournalMutationRecorder(f.client, &mutations))
			r.APIReader = interceptor.NewClient(f.client, interceptor.Funcs{
				Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					switch obj.(type) {
					case *ledgerv1alpha1.Cluster, *appsv1.StatefulSet, *corev1.ConfigMap:
					default:
						t.Fatalf("abandonment must not read dependencies or retained storage: %T", obj)
					}

					return inner.Get(ctx, key, obj, opts...)
				},
				List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
					t.Fatal("abandonment must not list dependencies")

					return errors.New("unexpected list")
				},
			})
			handled, result, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			require.True(t, handled)
			require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
			require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
			require.Equal(t, []string{fmt.Sprintf("patch status %T/%s", f.ledger, f.ledger.Name)}, mutations)
			// A late successful create after the clear has no authority in a new pass.
			require.NoError(t, f.client.Create(t.Context(), lateJournal))
			mutations = nil
			handled, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			require.False(t, handled)
			require.Empty(t, mutations)
			f.requireStorageUnchanged(t)
		})
	}
}

func TestStorageJournalTrustPreparingAbsenceRejectsUncertainSource(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"missing", "deleting", "replaced", "foreign", "read failure"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			f, _ := newStorageJournalPreparingFixture(t)
			pointer := *f.latestLedger(t).Status.ClaimTemplateRecovery
			var mutations []string
			r := f.reconciler(storageJournalMutationRecorder(f.client, &mutations))
			r.APIReader = interceptor.NewClient(f.client, interceptor.Funcs{
				Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if sts, ok := obj.(*appsv1.StatefulSet); ok {
						if change == "missing" {
							return apierrors.NewNotFound(appsv1.Resource("statefulsets"), key.Name)
						}
						if change == "read failure" {
							return errors.New("direct source observation unavailable")
						}
						if err := inner.Get(ctx, key, sts, opts...); err != nil {
							return err
						}
						switch change {
						case "deleting":
							sts.DeletionTimestamp = new(metav1.Now())
						case "replaced":
							sts.UID = "another-statefulset"
						case "foreign":
							sts.OwnerReferences[0].UID = "another-cluster"
						}

						return nil
					}

					return inner.Get(ctx, key, obj, opts...)
				},
			})
			handled, _, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.True(t, handled)
			require.Error(t, err)
			require.Empty(t, mutations)
			require.Equal(t, &pointer, f.latestLedger(t).Status.ClaimTemplateRecovery)
			f.requireStorageUnchanged(t)
		})
	}
}

func TestStorageJournalTrustPreparingAbortRequiresDirectReader(t *testing.T) {
	t.Parallel()
	f, _ := newStorageJournalPreparingFixture(t)
	pointer := *f.latestLedger(t).Status.ClaimTemplateRecovery
	var mutations []string
	r := f.reconciler(storageJournalMutationRecorder(f.client, &mutations))
	r.APIReader = nil
	handled, _, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.True(t, handled)
	require.ErrorContains(t, err, "uncached API reader")
	require.Empty(t, mutations)
	require.Equal(t, &pointer, f.latestLedger(t).Status.ClaimTemplateRecovery)
}

func TestStorageJournalTrustLateJournalRequiresFreshMatchingPreparation(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("differentClaimFloor=%t", changed), func(t *testing.T) {
			t.Parallel()
			f, lateJournal := newStorageJournalPreparingFixture(t)
			handled, _, err := f.reconciler(f.client).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			require.True(t, handled)
			require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
			require.NoError(t, f.client.Create(t.Context(), lateJournal))
			if changed {
				f.claims[1].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("175Gi")
			}
			attempts := 0
			wrapped := interceptor.NewClient(f.client, interceptor.Funcs{
				Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if cm, ok := obj.(*corev1.ConfigMap); ok {
						attempts++
						intent := f.latestLedger(t).Status.ClaimTemplateRecovery
						require.NotNil(t, intent)
						require.Empty(t, intent.ConfigMapUID, "fresh durable authorization precedes reuse too")
						require.Equal(t, claimTemplateRecoveryDigest(f.ledger, []byte(cm.Data[claimTemplateRecoveryKey])), intent.PayloadSHA256)
					}

					return inner.Create(ctx, obj, opts...)
				},
			})
			_, err = f.reconciler(wrapped).startClaimTemplateRecovery(t.Context(), f.latestLedger(t), f.source, f.claims)
			require.NoError(t, err)
			require.Equal(t, 1, attempts)
			active, _ := f.journal(t)
			if changed {
				require.NotEqual(t, lateJournal.Name, active.Name)
				require.NotEqual(t, lateJournal.Data, active.Data)
			} else {
				require.Equal(t, lateJournal.Name, active.Name)
				require.Equal(t, lateJournal.UID, active.UID)
				require.Equal(t, lateJournal.Data, active.Data)
			}
			storedLate := &corev1.ConfigMap{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(lateJournal), storedLate))
			require.Equal(t, lateJournal.UID, storedLate.UID)
			require.Equal(t, lateJournal.Data, storedLate.Data)
			f.requireStorageUnchanged(t)
		})
	}
}

func TestStorageJournalTrustExpectedPointerPreventsStaleTransitions(t *testing.T) {
	t.Parallel()
	for _, winner := range []string{"bind", "abandon", "new operation after completion"} {
		t.Run(winner, func(t *testing.T) {
			t.Parallel()
			f, journal := newStorageJournalPreparingFixture(t)
			stale := f.latestLedger(t)
			expected := *stale.Status.ClaimTemplateRecovery
			r := f.reconciler(f.client)
			bound := expected
			bound.ConfigMapUID = "journal-uid"
			switch winner {
			case "bind":
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), f.latestLedger(t), &expected, &bound))
				err := r.persistClaimTemplateRecovery(t.Context(), stale, &expected, nil)
				require.ErrorContains(t, err, "pointer changed before status transition")
				require.Equal(t, &bound, f.latestLedger(t).Status.ClaimTemplateRecovery)
			case "abandon":
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), f.latestLedger(t), &expected, nil))
				err := r.persistClaimTemplateRecovery(t.Context(), stale, &expected, &bound)
				require.ErrorContains(t, err, "pointer changed before status transition")
				require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
			case "new operation after completion":
				completed := bound
				completed.Completed = true
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), stale, &expected, &completed))
				stale = f.latestLedger(t)
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), f.latestLedger(t), &completed, nil))
				next := expected
				next.PayloadSHA256 = claimTemplateRecoveryDigest(f.ledger, append([]byte(journal.Data[claimTemplateRecoveryKey]), '\n'))
				next.ConfigMapName = claimTemplateRecoveryDigestName(f.ledger, next.PayloadSHA256)
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), f.latestLedger(t), nil, &next))
				err := r.persistClaimTemplateRecovery(t.Context(), stale, &completed, nil)
				require.ErrorContains(t, err, "pointer changed before status transition")
				require.Equal(t, &next, f.latestLedger(t).Status.ClaimTemplateRecovery)
			}
		})
	}
}

func TestStorageJournalTrustRequiresDirectContinuationAuthorization(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	r := f.reconciler(f.client)
	r.APIReader = nil
	safe, err := r.claimTemplatePreparationUncommitted(t.Context(), f.ledger)
	require.ErrorContains(t, err, "uncached API reader")
	require.False(t, safe)
	for _, change := range []string{"new operation", "different cluster", "deleting cluster", "uncertain read"} {
		t.Run(change, func(t *testing.T) {
			r := f.reconciler(f.client)
			r.APIReader = interceptor.NewClient(f.client, interceptor.Funcs{
				Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if change == "uncertain read" {
						return errors.New("authoritative API unavailable")
					}
					if err := inner.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if ledger, ok := obj.(*ledgerv1alpha1.Cluster); ok {
						switch change {
						case "new operation":
							ledger.Status.ClaimTemplateRecovery = &ledgerv1alpha1.ClaimTemplateRecoveryStatus{ConfigMapName: "pending"}
						case "different cluster":
							ledger.UID = "different-cluster"
						case "deleting cluster":
							ledger.DeletionTimestamp = new(metav1.Now())
						}
					}

					return nil
				},
			})
			safe, _ := r.claimTemplatePreparationUncommitted(t.Context(), f.ledger)
			require.False(t, safe, "cached nil status cannot override a direct API observation")
		})
	}
}

func TestStorageJournalTrustCompletedCleanupNeedsOnlyBoundIdentity(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"corrupt payload", "different UID", "lost delete response"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			_, err := f.reconciler(f.client).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.NoError(t, err)
			journal, _ := f.journal(t)
			ledger := f.latestLedger(t)
			expected := *ledger.Status.ClaimTemplateRecovery
			completed := expected
			completed.Completed = true
			require.NoError(t, f.reconciler(f.client).persistClaimTemplateRecovery(t.Context(), ledger, &expected, &completed))
			lost := errors.New("journal deletion committed but response lost")
			deletes := 0
			wrapped := interceptor.NewClient(f.client, interceptor.Funcs{
				Delete: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					deletes++
					options := (&client.DeleteOptions{}).ApplyOptions(opts)
					require.Equal(t, &metav1.Preconditions{UID: &journal.UID, ResourceVersion: &journal.ResourceVersion}, options.Preconditions)
					if err := inner.Delete(ctx, obj, opts...); err != nil {
						return err
					}
					if change == "lost delete response" {
						return lost
					}

					return nil
				},
			})
			r := f.reconciler(wrapped)
			r.APIReader = interceptor.NewClient(f.client, interceptor.Funcs{
				Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					switch obj.(type) {
					case *ledgerv1alpha1.Cluster, *corev1.ConfigMap:
					default:
						t.Fatalf("completed cleanup must not inspect workload, Pods or storage: %T", obj)
					}
					if err := inner.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if cm, ok := obj.(*corev1.ConfigMap); ok {
						cm.Data = map[string]string{claimTemplateRecoveryKey: "invalid JSON"}
						cm.Immutable = nil
						if change == "different UID" {
							cm.UID = types.UID("replacement-journal")
						}
					}

					return nil
				},
			})
			handled, result, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.True(t, handled)
			require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
			switch change {
			case "different UID":
				require.Error(t, err)
				require.Zero(t, deletes)
				require.Equal(t, &completed, f.latestLedger(t).Status.ClaimTemplateRecovery)
			case "lost delete response":
				require.ErrorIs(t, err, lost)
				require.Equal(t, 1, deletes)
				require.Equal(t, &completed, f.latestLedger(t).Status.ClaimTemplateRecovery)
				handled, _, err = f.reconciler(f.client).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
				require.NoError(t, err)
				require.True(t, handled)
				require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
			default:
				require.NoError(t, err)
				require.Equal(t, 1, deletes)
				require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
			}
			f.requireStorageUnchanged(t)
		})
	}
}
