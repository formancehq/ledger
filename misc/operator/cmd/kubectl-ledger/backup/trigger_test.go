package backup

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestWaitForTerminalProgress(t *testing.T) {
	for _, scenario := range []string{"succeeded", "failed", "get error", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var output bytes.Buffer
				infoWriter, errorWriter, successWriter := pterm.Info.Writer, pterm.Error.Writer, pterm.Success.Writer
				pterm.Info.Writer, pterm.Error.Writer, pterm.Success.Writer = &output, &output, &output
				t.Cleanup(func() {
					pterm.Info.Writer, pterm.Error.Writer, pterm.Success.Writer = infoWriter, errorWriter, successWriter
				})
				scheme := runtime.NewScheme()
				require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
				run := &ledgerv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "manual", Namespace: "default"}}
				getFailure := errors.New("backup status unavailable")
				var attempts int
				crdClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, underlying client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
						attempts++
						if scenario == "get error" {
							return getFailure
						}
						if err := underlying.Get(ctx, key, object, options...); err != nil {
							return err
						}
						if attempts > 1 {
							final := object.(*ledgerv1alpha1.BackupRun)
							switch scenario {
							case "succeeded":
								final.Status.Phase = ledgerv1alpha1.BackupRunPhaseSucceeded
							case "failed":
								final.Status.Phase = ledgerv1alpha1.BackupRunPhaseFailed
								final.Status.Message = "backup upload failed"
							}
						}

						return nil
					},
				}).Build()
				timeout := 3 * time.Second
				if scenario == "timeout" {
					timeout = time.Second
				}
				err := waitForTerminal(t.Context(), crdClient, run, timeout)
				require.Contains(t, output.String(), "Waiting for backup run to complete...")
				switch scenario {
				case "succeeded":
					require.NoError(t, err)
					require.Equal(t, 2, attempts)
					require.Contains(t, output.String(), "succeeded")
					require.NotContains(t, output.String(), "failed")
				case "failed":
					require.ErrorContains(t, err, "backup run manual failed")
					require.Equal(t, 2, attempts)
					require.Contains(t, output.String(), "backup upload failed")
				case "get error":
					require.ErrorIs(t, err, getFailure)
					require.Equal(t, 1, attempts)
					require.Contains(t, output.String(), "Wait failed or timed out")
				case "timeout":
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.Equal(t, 1, attempts)
					require.Contains(t, output.String(), "Wait failed or timed out")
				}
			})
		})
	}
}
