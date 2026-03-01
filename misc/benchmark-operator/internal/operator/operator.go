package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	processedAnnotation = "benchmark.formance.com/processed"
	benchmarkFinalizer  = "benchmark.formance.com/finalizer"
	stageFinished       = "finished"
	stageError          = "error"
)

var testRunGVR = schema.GroupVersionResource{
	Group:    "k6.io",
	Version:  "v1alpha1",
	Resource: "testruns",
}

type Operator struct {
	config      Config
	clientset   kubernetes.Interface
	dynamic     dynamic.Interface
	queue       workqueue.TypedRateLimitingInterface[any]
	informer    cache.SharedIndexInformer
	workerOnce  sync.Once
	grafana     *GrafanaClient
	statusMu    sync.Mutex
	statusByKey map[string]string
}

func New(restConfig *rest.Config, clientset kubernetes.Interface, cfg Config) (*Operator, error) {
	if cfg.GrafanaURL == "" {
		return nil, errors.New("GRAFANA_URL is required")
	}

	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}

	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[any]())

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynamicClient, 30*time.Second, cfg.WatchNamespace, nil)
	informer := factory.ForResource(testRunGVR).Informer()

	op := &Operator{
		config:      cfg,
		clientset:   clientset,
		dynamic:     dynamicClient,
		queue:       queue,
		informer:    informer,
		grafana:     NewGrafanaClient(cfg),
		statusByKey: make(map[string]string),
	}

	_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    op.enqueue,
		UpdateFunc: func(_, newObj interface{}) { op.enqueue(newObj) },
	})
	if err != nil {
		return nil, err
	}

	return op, nil
}

func (o *Operator) Run(stopCh <-chan struct{}) error {
	defer o.queue.ShutDown()

	log.Printf("watching testruns in namespace=%q", o.config.WatchNamespace)

	go o.informer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, o.informer.HasSynced) {
		return errors.New("cache sync failed")
	}

	go o.runWorker(stopCh)

	<-stopCh
	return nil
}

func (o *Operator) runWorker(stopCh <-chan struct{}) {
	o.workerOnce.Do(func() {
		for o.processNextItem() {
			select {
			case <-stopCh:
				return
			default:
			}
		}
	})
}

func (o *Operator) processNextItem() bool {
	key, quit := o.queue.Get()
	if quit {
		return false
	}
	defer o.queue.Done(key)

	obj, exists, err := o.informer.GetIndexer().GetByKey(key.(string))
	if err != nil {
		log.Printf("failed to get object %q: %v", key, err)
		o.queue.AddRateLimited(key)
		return true
	}
	if !exists {
		return true
	}

	if err := o.reconcile(obj.(*unstructured.Unstructured)); err != nil {
		log.Printf("reconcile failed for %q: %v", key, err)
		o.queue.AddRateLimited(key)
		return true
	}

	o.queue.Forget(key)
	return true
}

func (o *Operator) enqueue(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Printf("failed to get key for object: %v", err)
		return
	}
	if strings.TrimSpace(key) == "" {
		return
	}
	o.queue.Add(key)
}

func (o *Operator) reconcile(obj *unstructured.Unstructured) error {
	stage := getString(obj.Object, "status", "stage")
	phase := getString(obj.Object, "status", "phase")
	state := getString(obj.Object, "status", "state")
	status := firstNonEmpty(stage, phase, state)
	o.logStatus(obj, status)

	if obj.GetDeletionTimestamp() != nil {
		return o.handleDelete(context.Background(), obj)
	}

	if err := o.ensureFinalizer(context.Background(), obj); err != nil {
		return err
	}

	if stage != stageFinished && stage != stageError {
		return nil
	}

	if isProcessed(obj) {
		return nil
	}

	log.Printf("processing testrun %s/%s (stage=%s)", obj.GetNamespace(), obj.GetName(), stage)

	ctx := context.Background()
	result, err := o.grafana.ProcessTestRun(ctx, obj)
	if err != nil {
		return err
	}

	if result.Report != "" {
		reportName := reportConfigMapName(obj.GetName())
		log.Printf("writing report for testrun %s/%s to configmap %s", obj.GetNamespace(), obj.GetName(), reportName)
		err = o.ensureReportConfigMap(ctx, obj.GetNamespace(), reportName, result.Report)
		if err != nil {
			log.Printf("failed to write report configmap: %v", err)
		}
	}

	log.Printf("processed testrun %s/%s", obj.GetNamespace(), obj.GetName())
	return o.markProcessed(ctx, obj)
}

func (o *Operator) ensureFinalizer(ctx context.Context, obj *unstructured.Unstructured) error {
	for _, value := range obj.GetFinalizers() {
		if value == benchmarkFinalizer {
			return nil
		}
	}

	patch := []byte(fmt.Sprintf(`{"metadata":{"finalizers":["%s"]}}`, benchmarkFinalizer))
	_, err := o.dynamic.Resource(testRunGVR).Namespace(obj.GetNamespace()).Patch(ctx, obj.GetName(), types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

func (o *Operator) handleDelete(ctx context.Context, obj *unstructured.Unstructured) error {
	key := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	if !hasFinalizer(obj, benchmarkFinalizer) {
		log.Printf("testrun delete observed without finalizer %s", key)
		return nil
	}

	log.Printf("cleaning resources for testrun %s", key)
	reportName := reportConfigMapName(obj.GetName())
	if err := o.cleanupReport(ctx, obj.GetNamespace(), reportName); err != nil {
		log.Printf("cleanup failed for %s: %v", key, err)
	}

	patch := []byte(`{"metadata":{"finalizers":[]}}`)
	_, err := o.dynamic.Resource(testRunGVR).Namespace(obj.GetNamespace()).Patch(ctx, obj.GetName(), types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}

	log.Printf("cleanup completed for testrun %s", key)
	return nil
}

func (o *Operator) cleanupReport(ctx context.Context, namespace, name string) error {
	configMap, err := o.clientset.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	reportData := configMap.Data["report.json"]
	if reportData != "" {
		if err := o.deleteSnapshots(ctx, reportData); err != nil {
			log.Printf("snapshot cleanup failed: %v", err)
		}
	}

	return o.clientset.CoreV1().ConfigMaps(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func (o *Operator) deleteSnapshots(ctx context.Context, report string) error {
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
		if err := o.grafana.DeleteSnapshot(ctx, entry.DeleteURL, entry.DeleteKey, entry.SnapshotKey); err != nil {
			log.Printf("failed to delete snapshot: %v", err)
		}
	}

	return nil
}

func (o *Operator) logStatus(obj *unstructured.Unstructured, status string) {
	key := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	if strings.TrimSpace(status) == "" {
		status = "unknown"
	}

	o.statusMu.Lock()
	defer o.statusMu.Unlock()

	prev, ok := o.statusByKey[key]
	if !ok {
		o.statusByKey[key] = status
		log.Printf("testrun detected %s (status=%s)", key, status)
		return
	}

	if prev != status {
		o.statusByKey[key] = status
		log.Printf("testrun status change %s: %s -> %s", key, prev, status)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func reportConfigMapName(testRunName string) string {
	return fmt.Sprintf("k6-report-%s", testRunName)
}

func hasFinalizer(obj *unstructured.Unstructured, value string) bool {
	for _, finalizer := range obj.GetFinalizers() {
		if finalizer == value {
			return true
		}
	}
	return false
}

func (o *Operator) markProcessed(ctx context.Context, obj *unstructured.Unstructured) error {
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{"%s":"%s"}}}`, processedAnnotation, time.Now().UTC().Format(time.RFC3339)))
	_, err := o.dynamic.Resource(testRunGVR).Namespace(obj.GetNamespace()).Patch(ctx, obj.GetName(), types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

func (o *Operator) ensureReportConfigMap(ctx context.Context, namespace, name, report string) error {
	configMap := &corev1.ConfigMap{}
	configMap.Namespace = namespace
	configMap.Name = name
	configMap.Data = map[string]string{
		"report.json": report,
	}

	client := o.clientset.CoreV1().ConfigMaps(namespace)
	_, err := client.Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		_, err = client.Update(ctx, configMap, metav1.UpdateOptions{})
		return err
	}

	_, err = client.Create(ctx, configMap, metav1.CreateOptions{})
	return err
}

func isProcessed(obj *unstructured.Unstructured) bool {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return false
	}

	value := strings.TrimSpace(annotations[processedAnnotation])
	return value != ""
}

func getString(obj map[string]interface{}, path ...string) string {
	value := obj
	for i, key := range path {
		if i == len(path)-1 {
			if raw, ok := value[key]; ok {
				if str, ok := raw.(string); ok {
					return str
				}
			}
			return ""
		}

		next, ok := value[key].(map[string]interface{})
		if !ok {
			return ""
		}
		value = next
	}

	return ""
}
