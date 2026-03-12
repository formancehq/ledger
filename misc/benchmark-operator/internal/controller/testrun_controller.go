package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const testRunFinalizer = "benchmark.formance.com/finalizer"

// TestRunReconciler watches standalone k6 TestRuns (not owned by a Benchmark)
// and creates Grafana snapshots when they finish.
type TestRunReconciler struct {
	Dynamic        dynamic.Interface
	Grafana        *GrafanaClient
	WatchNamespace string
	k8sClient      client.Client
}

func (r *TestRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	obj, err := r.Dynamic.Resource(testRunGVR).Namespace(req.Namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Skip TestRuns owned by a Benchmark — the BenchmarkReconciler handles those.
	if _, owned := isOwnedByBenchmark(obj); owned {
		return ctrl.Result{}, nil
	}

	// Handle deletion.
	if obj.GetDeletionTimestamp() != nil {
		return r.handleDelete(ctx, log, obj)
	}

	// Ensure finalizer.
	if err := r.ensureFinalizer(ctx, obj); err != nil {
		return ctrl.Result{}, err
	}

	stage := getString(obj.Object, "status", "stage")
	if stage != stageFinished && stage != stageError {
		return ctrl.Result{}, nil
	}

	if isProcessed(obj) {
		return ctrl.Result{}, nil
	}

	log.Info("processing standalone testrun", "stage", stage)

	result, err := r.Grafana.ProcessTestRun(ctx, obj)
	if err != nil {
		return ctrl.Result{}, err
	}

	if result.Report != "" {
		reportName := reportConfigMapName(obj.GetName())
		log.Info("writing report", "configmap", reportName)
		if cmErr := r.ensureReportConfigMap(ctx, obj.GetNamespace(), reportName, result.Report); cmErr != nil {
			log.Error(cmErr, "failed to write report configmap")
		}
	}

	log.Info("processed standalone testrun")

	return ctrl.Result{}, r.markProcessed(ctx, obj)
}

func (r *TestRunReconciler) ensureFinalizer(ctx context.Context, obj *unstructured.Unstructured) error {
	if hasTestRunFinalizer(obj) {
		return nil
	}

	patch := fmt.Appendf(nil, `{"metadata":{"finalizers":["%s"]}}`, testRunFinalizer)
	_, err := r.Dynamic.Resource(testRunGVR).Namespace(obj.GetNamespace()).Patch(ctx, obj.GetName(), types.MergePatchType, patch, metav1.PatchOptions{})

	return err
}

func (r *TestRunReconciler) handleDelete(ctx context.Context, log logr.Logger, obj *unstructured.Unstructured) (ctrl.Result, error) {
	if !hasTestRunFinalizer(obj) {
		return ctrl.Result{}, nil
	}

	key := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	log.Info("cleaning resources for testrun", "key", key)

	reportName := reportConfigMapName(obj.GetName())
	if err := r.cleanupReport(ctx, log, obj.GetNamespace(), reportName); err != nil {
		log.Error(err, "cleanup failed", "key", key)
	}

	patch := []byte(`{"metadata":{"finalizers":[]}}`)
	_, err := r.Dynamic.Resource(testRunGVR).Namespace(obj.GetNamespace()).Patch(ctx, obj.GetName(), types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return ctrl.Result{}, err
	}

	log.Info("cleanup completed", "key", key)

	return ctrl.Result{}, nil
}

func (r *TestRunReconciler) cleanupReport(ctx context.Context, log logr.Logger, namespace, name string) error {
	cm := &corev1.ConfigMap{}
	if err := r.k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cm); err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	reportData := cm.Data["report.json"]
	if reportData != "" {
		if err := r.deleteSnapshots(ctx, reportData); err != nil {
			log.Error(err, "snapshot cleanup failed")
		}
	}

	return r.k8sClient.Delete(ctx, cm)
}

func (r *TestRunReconciler) deleteSnapshots(ctx context.Context, report string) error {
	var parsed struct {
		Entries []struct {
			DeleteURL   string `json:"deleteUrl"`
			DeleteKey   string `json:"deleteKey"`
			SnapshotKey string `json:"snapshotKey"`
		} `json:"entries"`
	}

	if err := json.Unmarshal([]byte(report), &parsed); err != nil {
		return err
	}

	for _, entry := range parsed.Entries {
		_ = r.Grafana.DeleteSnapshot(ctx, entry.DeleteURL, entry.DeleteKey, entry.SnapshotKey) //nolint:errcheck // best-effort
	}

	return nil
}

func (r *TestRunReconciler) markProcessed(ctx context.Context, obj *unstructured.Unstructured) error {
	patch := fmt.Appendf(nil, `{"metadata":{"annotations":{"%s":"%s"}}}`, processedAnnotation, time.Now().UTC().Format(time.RFC3339))
	_, err := r.Dynamic.Resource(testRunGVR).Namespace(obj.GetNamespace()).Patch(ctx, obj.GetName(), types.MergePatchType, patch, metav1.PatchOptions{})

	return err
}

func (r *TestRunReconciler) ensureReportConfigMap(ctx context.Context, namespace, name, report string) error {
	cm := &corev1.ConfigMap{}
	cm.Namespace = namespace
	cm.Name = name

	_, err := controllerutil.CreateOrUpdate(ctx, r.k8sClient, cm, func() error {
		cm.Data = map[string]string{
			"report.json": report,
		}
		return nil
	})

	return err
}

func hasTestRunFinalizer(obj *unstructured.Unstructured) bool {
	for _, f := range obj.GetFinalizers() {
		if f == testRunFinalizer {
			return true
		}
	}

	return false
}

// SetupWithManager registers the TestRunReconciler with a dynamic informer
// as the event source (since k6 TestRun is not in our scheme).
func (r *TestRunReconciler) SetupWithManager(mgr manager.Manager) error {
	r.k8sClient = mgr.GetClient()

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(r.Dynamic, 30*time.Second, r.WatchNamespace, nil)
	informer := factory.ForResource(testRunGVR).Informer()

	ch := make(chan event.TypedGenericEvent[reconcile.Request], 256)

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			enqueueTestRun(obj, ch)
		},
		UpdateFunc: func(_, newObj any) {
			enqueueTestRun(newObj, ch)
		},
		DeleteFunc: func(obj any) {
			enqueueTestRun(obj, ch)
		},
	})
	if err != nil {
		return err
	}

	// Start the informer when the manager starts.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		informer.Run(ctx.Done())
		return nil
	})); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("testrun").
		WatchesRawSource(source.Channel(ch, &testRunEventHandler{})).
		Complete(r)
}

func enqueueTestRun(obj any, ch chan<- event.TypedGenericEvent[reconcile.Request]) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		u, ok = tombstone.Obj.(*unstructured.Unstructured)
		if !ok {
			return
		}
	}

	name := u.GetName()
	ns := u.GetNamespace()
	if strings.TrimSpace(name) == "" {
		return
	}

	ch <- event.TypedGenericEvent[reconcile.Request]{
		Object: reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: ns,
				Name:      name,
			},
		},
	}
}

// testRunEventHandler passes generic events from the channel directly as reconcile requests.
type testRunEventHandler struct{}

func (h *testRunEventHandler) Create(_ context.Context, _ event.TypedCreateEvent[reconcile.Request], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *testRunEventHandler) Update(_ context.Context, _ event.TypedUpdateEvent[reconcile.Request], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *testRunEventHandler) Delete(_ context.Context, _ event.TypedDeleteEvent[reconcile.Request], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *testRunEventHandler) Generic(_ context.Context, evt event.TypedGenericEvent[reconcile.Request], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	q.Add(evt.Object)
}
