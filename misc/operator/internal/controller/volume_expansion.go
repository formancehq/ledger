package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	controllermetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	volumeExpansionRequeueInterval = 5 * time.Minute
	volumeExpansionRetryInterval   = time.Minute
	maximumDiskUsageSampleAge      = time.Minute

	annotationLastExpansionAt     = "ledger.formance.com/last-expansion-at"
	annotationLastExpansionTarget = "ledger.formance.com/last-expansion-target"
)

var (
	volumeUsageRatioMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ledger_operator_volume_usage_ratio",
		Help: "Filesystem usage ratio observed by the Ledger volume expansion reconciler.",
	}, []string{"namespace", "cluster", "pod", "volume"})
	volumeRequestedBytesMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ledger_operator_volume_requested_bytes",
		Help: "Largest requested PVC capacity for a Ledger volume group.",
	}, []string{"namespace", "cluster", "volume"})
	volumeExpansionsMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_operator_volume_expansions_total",
		Help: "Volume expansion reconciliation outcomes.",
	}, []string{"namespace", "cluster", "volume", "result"})
	volumeExpansionErrorsMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_operator_volume_expansion_errors_total",
		Help: "Volume expansion errors by stage and volume kind.",
	}, []string{"stage", "volume"})
)

func init() {
	controllermetrics.Registry.MustRegister(
		volumeUsageRatioMetric,
		volumeRequestedBytesMetric,
		volumeExpansionsMetric,
		volumeExpansionErrorsMetric,
	)
}

type podDiskUsage struct {
	WAL  measuredVolume
	Data measuredVolume
}

type measuredVolume struct {
	UsedBytes  uint64
	TotalBytes uint64
	ObservedAt time.Time
	SampleAge  time.Duration
	Valid      bool
	Error      string
}

type readPodDiskUsageFunc func(ctx context.Context, ledger *ledgerv1alpha1.Cluster, pod, tlsMode string) (podDiskUsage, error)

// VolumeExpansionReconciler periodically grows the live PVCs owned by one
// Cluster. It owns neither Cluster status nor StatefulSet templates; its entire
// mutation surface is the storage request and bookkeeping annotations on PVCs.
type VolumeExpansionReconciler struct {
	client.Client

	APIReader     client.Reader
	Config        *rest.Config
	Clientset     kubernetes.Interface
	Recorder      record.EventRecorder
	ReadDiskUsage readPodDiskUsageFunc
	Now           func() time.Time
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get

func (r *VolumeExpansionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ledger ledgerv1alpha1.Cluster
	if err := r.Get(ctx, req.NamespacedName, &ledger); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !ledger.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	definitions := enabledVolumeExpansionDefinitions(&ledger)
	if len(definitions) == 0 {
		return ctrl.Result{}, nil
	}
	applyDefaults(&ledger)

	var statefulSet appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: resourceName(ledger.Name)}, &statefulSet); err != nil {
		for _, definition := range definitions {
			r.recordWarningf(&ledger, "VolumeExpansionPending", "%s auto-expansion is waiting for the live StatefulSet: %v", definition.Name, err)
		}
		volumeExpansionErrorsMetric.WithLabelValues("statefulset", "cluster").Inc()

		return ctrl.Result{RequeueAfter: volumeExpansionRetryInterval}, nil
	}
	liveReplicas := int32(1)
	if statefulSet.Spec.Replicas != nil {
		liveReplicas = *statefulSet.Spec.Replicas
	}
	if liveReplicas < 1 {
		volumeExpansionErrorsMetric.WithLabelValues("statefulset", "cluster").Inc()

		return ctrl.Result{RequeueAfter: volumeExpansionRetryInterval}, fmt.Errorf("invariant: live StatefulSet %s/%s has %d replicas", statefulSet.Namespace, statefulSet.Name, liveReplicas)
	}
	tlsMode := currentTLSModeFromStatefulSet(&statefulSet)

	nextRequeue := volumeExpansionRequeueInterval
	var reconcileErrors []error
	for _, definition := range definitions {
		retrySoon, err := r.reconcileVolume(ctx, &ledger, definition, tlsMode, liveReplicas)
		if retrySoon {
			nextRequeue = volumeExpansionRetryInterval
		}
		if err != nil {
			reconcileErrors = append(reconcileErrors, err)
		}
	}
	if len(reconcileErrors) > 0 {
		return ctrl.Result{RequeueAfter: nextRequeue}, errors.Join(reconcileErrors...)
	}

	return ctrl.Result{RequeueAfter: nextRequeue}, nil
}

func enabledVolumeExpansionDefinitions(ledger *ledgerv1alpha1.Cluster) []persistenceVolumeDefinition {
	var enabled []persistenceVolumeDefinition
	for _, definition := range persistenceVolumeDefinitions(ledger) {
		auto := definition.Spec.AutoExpansion
		if definition.AutoExpansionAllowed && auto != nil && auto.Enabled {
			enabled = append(enabled, definition)
		}
	}

	return enabled
}

func (r *VolumeExpansionReconciler) reconcileVolume(
	ctx context.Context,
	ledger *ledgerv1alpha1.Cluster,
	definition persistenceVolumeDefinition,
	tlsMode string,
	replicas int32,
) (bool, error) {
	logger := ctrl.LoggerFrom(ctx).WithValues("volume", definition.Name)
	if err := validateVolumeSpec(
		definition.Field,
		definition.Spec,
		definition.DefaultSize,
		definition.AutoExpansionAllowed,
	); err != nil {
		r.recordWarningf(ledger, "VolumeExpansionUnsupported", "%s auto-expansion policy is invalid: %v", definition.Name, err)
		volumeExpansionErrorsMetric.WithLabelValues("policy", definition.Name).Inc()

		return false, nil
	}

	policy, err := resolveVolumeExpansionPolicy(definition.Spec.AutoExpansion)
	if err != nil {
		r.recordWarningf(ledger, "VolumeExpansionUnsupported", "%s auto-expansion policy is invalid: %v", definition.Name, err)
		volumeExpansionErrorsMetric.WithLabelValues("policy", definition.Name).Inc()

		return false, nil
	}

	pvcs, states, err := r.loadVolumePVCs(ctx, ledger, definition.Name, replicas)
	if err != nil {
		volumeExpansionErrorsMetric.WithLabelValues("pvc", definition.Name).Inc()
		r.recordWarningf(ledger, "VolumeExpansionPending", "%s PVC group is not ready: %v", definition.Name, err)

		return true, nil
	}
	if err := r.validateExpandableStorageClasses(ctx, pvcs); err != nil {
		volumeExpansionErrorsMetric.WithLabelValues("storage-class", definition.Name).Inc()
		r.recordWarningf(ledger, "VolumeExpansionUnsupported", "%s PVC group cannot expand: %v", definition.Name, err)

		return true, nil
	}

	// Let the pure policy resolve convergence, pending resize and cooldown first.
	// Those states do not need a pod exec and should not turn an unrelated
	// ledgerctl failure into measurement noise.
	decision := decideVolumeExpansion(policy, states, nil, r.now())
	var measurements []podVolumeMeasurement
	if decision.Kind == volumeExpansionDecisionIncomplete {
		measurements = r.collectMeasurements(ctx, ledger, definition.Name, tlsMode, replicas)
		decision = decideVolumeExpansion(policy, states, measurements, r.now())
	}
	volumeRequestedBytesMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name).
		Set(float64(decision.LargestRequestBytes))
	for _, measurement := range measurements {
		if measurement.Err == nil && measurement.TotalBytes > 0 {
			volumeUsageRatioMetric.WithLabelValues(ledger.Namespace, ledger.Name, measurement.Pod, definition.Name).
				Set(float64(measurement.UsedBytes) / float64(measurement.TotalBytes))
		}
	}

	logger = logger.WithValues(
		"decision", decision.Kind,
		"currentBytes", decision.LargestRequestBytes,
		"targetBytes", decision.TargetBytes,
		"maximumBytes", policy.MaximumSize.Value(),
		"maxUsageRatio", decision.MaxUsageRatio,
		"failedMeasurements", decision.FailedMeasurements,
	)
	if decision.FailedMeasurements > 0 &&
		(decision.Kind == volumeExpansionDecisionExpand || decision.Kind == volumeExpansionDecisionLimit) {
		r.recordWarningf(ledger, "VolumeExpansionMeasurementFailed", "%s usage failed on %d replica(s); continuing because another replica crossed the threshold", definition.Name, decision.FailedMeasurements)
	}

	switch decision.Kind {
	case volumeExpansionDecisionConverge:
		if err := r.patchPVCGroup(ctx, pvcs, decision.TargetBytes, decision.LastExpansionAt); err != nil {
			volumeExpansionErrorsMetric.WithLabelValues("patch", definition.Name).Inc()

			return true, fmt.Errorf("converging %s PVCs: %w", definition.Name, err)
		}
		logger.Info("converged partially-expanded PVC group")
		volumeExpansionsMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name, "converged").Inc()

		return true, nil
	case volumeExpansionDecisionPending:
		logger.Info("volume expansion is pending")
		r.recordNormalf(ledger, "VolumeExpansionPending", "%s PVC expansion is still pending", definition.Name)
		volumeExpansionsMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name, "pending").Inc()

		return true, nil
	case volumeExpansionDecisionCooldown:
		logger.Info("volume expansion is in cooldown", "cooldownUntil", decision.LastExpansionAt.Add(policy.Cooldown))
		volumeExpansionsMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name, "cooldown").Inc()

		return false, nil
	case volumeExpansionDecisionIncomplete:
		logger.Info("volume usage measurement is incomplete")
		r.recordWarningf(ledger, "VolumeExpansionMeasurementFailed", "%s usage could not be measured on every replica; no threshold crossing was observed", definition.Name)
		volumeExpansionsMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name, "measurement-incomplete").Inc()

		return true, nil
	case volumeExpansionDecisionLimit:
		logger.Info("volume expansion maximum reached")
		r.recordWarningf(ledger, "VolumeExpansionLimitReached", "%s PVCs reached maximumSize %s at %.1f%% usage", definition.Name, policy.MaximumSize.String(), decision.MaxUsageRatio*100)
		volumeExpansionsMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name, "limit-reached").Inc()

		return false, nil
	case volumeExpansionDecisionAboveMax:
		logger.Info("volume request exceeds the configured maximum")
		r.recordWarningf(
			ledger,
			"VolumeExpansionUnsupported",
			"%s PVC request %s exceeds maximumSize %s; refusing to propagate it to the remaining replicas",
			definition.Name,
			formatBytesAsQuantity(decision.LargestRequestBytes),
			policy.MaximumSize.String(),
		)
		volumeExpansionsMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name, "above-maximum").Inc()

		return false, nil
	case volumeExpansionDecisionExpand:
		now := r.now()
		if err := r.patchPVCGroup(ctx, pvcs, decision.TargetBytes, now); err != nil {
			volumeExpansionErrorsMetric.WithLabelValues("patch", definition.Name).Inc()

			return true, fmt.Errorf("expanding %s PVCs: %w", definition.Name, err)
		}
		target := *resource.NewQuantity(decision.TargetBytes, resource.BinarySI)
		logger.Info("requested PVC expansion", "target", target.String())
		r.recordNormalf(ledger, "VolumeExpansionRequested", "%s PVCs expanding from %s to %s after reaching %.1f%% usage", definition.Name, formatBytesAsQuantity(decision.LargestRequestBytes), target.String(), decision.MaxUsageRatio*100)
		volumeRequestedBytesMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name).Set(float64(decision.TargetBytes))
		volumeExpansionsMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name, "requested").Inc()

		return true, nil
	case volumeExpansionDecisionNone:
		logger.Info("volume expansion not required")
		volumeExpansionsMetric.WithLabelValues(ledger.Namespace, ledger.Name, definition.Name, "unchanged").Inc()

		return false, nil
	default:
		return false, fmt.Errorf("invariant: unknown volume expansion decision %q", decision.Kind)
	}
}

func (r *VolumeExpansionReconciler) loadVolumePVCs(
	ctx context.Context,
	ledger *ledgerv1alpha1.Cluster,
	volume string,
	replicas int32,
) ([]*corev1.PersistentVolumeClaim, []volumePVCState, error) {
	if r.APIReader == nil {
		return nil, nil, errors.New("invariant: APIReader is required for direct PVC reads")
	}

	pvcs := make([]*corev1.PersistentVolumeClaim, 0, replicas)
	states := make([]volumePVCState, 0, replicas)
	for ordinal := range replicas {
		name := fmt.Sprintf("%s-%s-%d", volume, resourceName(ledger.Name), ordinal)
		var pvc corev1.PersistentVolumeClaim
		// PVCs are intentionally read through the uncached APIReader. The
		// controller only has get/list/patch RBAC for PVCs (no watch), so the
		// manager cache cannot establish an informer for this resource.
		if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: name}, &pvc); err != nil {
			return nil, nil, fmt.Errorf("getting PVC %s: %w", name, err)
		}

		requested := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		capacity := pvc.Status.Capacity[corev1.ResourceStorage]
		lastExpansion, err := parseExpansionTime(pvc.Annotations[annotationLastExpansionAt])
		if err != nil {
			return nil, nil, fmt.Errorf("PVC %s has invalid %s annotation: %w", name, annotationLastExpansionAt, err)
		}
		_, err = parseExpansionTarget(pvc.Annotations[annotationLastExpansionTarget])
		if err != nil {
			return nil, nil, fmt.Errorf("PVC %s has invalid %s annotation: %w", name, annotationLastExpansionTarget, err)
		}

		pvcs = append(pvcs, pvc.DeepCopy())
		states = append(states, volumePVCState{
			Name:            name,
			RequestedBytes:  requested.Value(),
			CapacityBytes:   capacity.Value(),
			Bound:           pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "",
			ResizePending:   pvcResizePending(&pvc),
			LastExpansionAt: lastExpansion,
		})
	}

	return pvcs, states, nil
}

func (r *VolumeExpansionReconciler) validateExpandableStorageClasses(ctx context.Context, pvcs []*corev1.PersistentVolumeClaim) error {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	checked := map[string]struct{}{}
	for _, pvc := range pvcs {
		if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
			return fmt.Errorf("PVC %s has no StorageClass", pvc.Name)
		}
		name := *pvc.Spec.StorageClassName
		if _, ok := checked[name]; ok {
			continue
		}
		var storageClass storagev1.StorageClass
		if err := reader.Get(ctx, types.NamespacedName{Name: name}, &storageClass); err != nil {
			return fmt.Errorf("getting StorageClass %s: %w", name, err)
		}
		if storageClass.AllowVolumeExpansion == nil || !*storageClass.AllowVolumeExpansion {
			return fmt.Errorf("StorageClass %s does not set allowVolumeExpansion: true", name)
		}
		checked[name] = struct{}{}
	}

	return nil
}

func (r *VolumeExpansionReconciler) collectMeasurements(
	ctx context.Context,
	ledger *ledgerv1alpha1.Cluster,
	volume, tlsMode string,
	replicas int32,
) []podVolumeMeasurement {
	read := r.ReadDiskUsage
	if read == nil {
		read = r.readPodDiskUsage
	}

	measurements := make([]podVolumeMeasurement, 0, replicas)
	for ordinal := range replicas {
		pod := podName(ledger.Name, int(ordinal))
		usage, err := read(ctx, ledger, pod, tlsMode)
		measurement := podVolumeMeasurement{Pod: pod, Err: err}
		if err == nil {
			var selected measuredVolume
			switch volume {
			case "wal":
				selected = usage.WAL
			case "data":
				selected = usage.Data
			default:
				measurement.Err = fmt.Errorf("invariant: unsupported volume kind %q", volume)
			}
			if measurement.Err == nil {
				measurement.Err = validateMeasuredVolume(selected)
				measurement.UsedBytes = selected.UsedBytes
				measurement.TotalBytes = selected.TotalBytes
			}
		}
		if measurement.Err != nil {
			volumeExpansionErrorsMetric.WithLabelValues("measurement", volume).Inc()
		}
		measurements = append(measurements, measurement)
	}

	return measurements
}

func (r *VolumeExpansionReconciler) readPodDiskUsage(
	ctx context.Context,
	ledger *ledgerv1alpha1.Cluster,
	pod, tlsMode string,
) (podDiskUsage, error) {
	serverAddr := podSelfServerAddr(headlessServiceName(ledger.Name), ledger.Spec.GrpcPort)
	result, err := podExecWithTimeout(ctx, r.Config, r.Clientset, ledger.Namespace, pod, ledgerContainer,
		ledgerctlCommand(serverAddr, tlsMode, "cluster", "disk-usage", "--json"),
	)
	if err != nil {
		return podDiskUsage{}, fmt.Errorf("reading disk usage from %s: %w", pod, err)
	}

	usage, err := parsePodDiskUsage([]byte(result.Stdout))
	if err != nil {
		return podDiskUsage{}, fmt.Errorf("parsing disk usage from %s: %w", pod, err)
	}

	return usage, nil
}

func (r *VolumeExpansionReconciler) patchPVCGroup(
	ctx context.Context,
	pvcs []*corev1.PersistentVolumeClaim,
	targetBytes int64,
	expansionTime time.Time,
) error {
	if expansionTime.IsZero() {
		expansionTime = r.now()
	}
	target := *resource.NewQuantity(targetBytes, resource.BinarySI)
	for _, pvc := range pvcs {
		current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if current.Value() >= targetBytes {
			continue
		}
		base := pvc.DeepCopy()
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1.ResourceList{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = target
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		pvc.Annotations[annotationLastExpansionAt] = expansionTime.UTC().Format(time.RFC3339)
		pvc.Annotations[annotationLastExpansionTarget] = target.String()
		if err := r.Patch(ctx, pvc, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("patching PVC %s to %s: %w", pvc.Name, target.String(), err)
		}
	}

	return nil
}

func (r *VolumeExpansionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.Cluster{}).
		Named("volume-expansion").
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Complete(r)
}

func (r *VolumeExpansionReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

func (r *VolumeExpansionReconciler) recordNormalf(object runtime.Object, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(object, corev1.EventTypeNormal, reason, format, args...)
	}
}

func (r *VolumeExpansionReconciler) recordWarningf(object runtime.Object, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(object, corev1.EventTypeWarning, reason, format, args...)
	}
}

func pvcResizePending(pvc *corev1.PersistentVolumeClaim) bool {
	for _, condition := range pvc.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		if condition.Type == corev1.PersistentVolumeClaimResizing ||
			condition.Type == corev1.PersistentVolumeClaimFileSystemResizePending {
			return true
		}
	}

	return false
}

func parseExpansionTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}

	return time.Parse(time.RFC3339, value)
}

func parseExpansionTarget(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	target, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, err
	}

	return target.Value(), nil
}

func formatBytesAsQuantity(value int64) string {
	return resource.NewQuantity(value, resource.BinarySI).String()
}

type protoJSONUint64 uint64

func (value *protoJSONUint64) UnmarshalJSON(data []byte) error {
	raw := strings.Trim(string(data), `"`)
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing uint64 %q: %w", raw, err)
	}
	*value = protoJSONUint64(parsed)

	return nil
}

type podDiskUsageVolumeJSON struct {
	UsedBytes   protoJSONUint64 `json:"usedBytes"`
	TotalBytes  protoJSONUint64 `json:"totalBytes"`
	ObservedAt  protoJSONUint64 `json:"observedAtUs"`
	SampleAgeMS protoJSONUint64 `json:"sampleAgeMs"`
	Valid       bool            `json:"valid"`
	Error       string          `json:"error"`
}

func measuredVolumeFromJSON(volume podDiskUsageVolumeJSON) (measuredVolume, error) {
	if uint64(volume.ObservedAt) > math.MaxInt64 {
		return measuredVolume{}, errors.New("reported observedAtUs exceeds int64")
	}
	if uint64(volume.SampleAgeMS) > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return measuredVolume{}, errors.New("reported sampleAgeMs exceeds time.Duration")
	}

	var observedAt time.Time
	if volume.ObservedAt > 0 {
		observedAt = time.UnixMicro(int64(volume.ObservedAt)).UTC()
	}

	return measuredVolume{
		UsedBytes:  uint64(volume.UsedBytes),
		TotalBytes: uint64(volume.TotalBytes),
		ObservedAt: observedAt,
		SampleAge:  time.Duration(volume.SampleAgeMS) * time.Millisecond,
		Valid:      volume.Valid,
		Error:      volume.Error,
	}, nil
}

func validateMeasuredVolume(volume measuredVolume) error {
	if !volume.Valid {
		if volume.Error != "" {
			return fmt.Errorf("latest collection attempt failed: %s", volume.Error)
		}

		return errors.New("latest collection attempt failed")
	}
	if volume.TotalBytes == 0 {
		return errors.New("reported totalBytes is zero")
	}
	if volume.ObservedAt.IsZero() {
		return errors.New("reported observedAtUs is zero")
	}
	if volume.SampleAge > maximumDiskUsageSampleAge {
		return fmt.Errorf("disk usage sample is stale: age %s exceeds %s", volume.SampleAge, maximumDiskUsageSampleAge)
	}

	return nil
}

func parsePodDiskUsage(data []byte) (podDiskUsage, error) {
	var payload struct {
		WALVolume  podDiskUsageVolumeJSON `json:"walVolume"`
		DataVolume podDiskUsageVolumeJSON `json:"dataVolume"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return podDiskUsage{}, err
	}
	wal, err := measuredVolumeFromJSON(payload.WALVolume)
	if err != nil {
		return podDiskUsage{}, fmt.Errorf("parsing WAL volume: %w", err)
	}
	dataVolume, err := measuredVolumeFromJSON(payload.DataVolume)
	if err != nil {
		return podDiskUsage{}, fmt.Errorf("parsing data volume: %w", err)
	}

	return podDiskUsage{
		WAL:  wal,
		Data: dataVolume,
	}, nil
}
