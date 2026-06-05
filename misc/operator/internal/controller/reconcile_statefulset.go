package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func (r *LedgerServiceReconciler) reconcileStatefulSet(ctx context.Context, ledger *ledgerv1alpha1.LedgerService, specHash string, agents []agentKeyInfo) error {
	logger := log.FromContext(ctx)

	// Determine the TLS_MODE to apply this pass. The operator walks the
	// StatefulSet through "optional" during a toggle so peers on either side
	// of a rolling update can still talk to each other; the user-facing CR
	// only exposes a boolean.
	existingForTLS, err := r.fetchExistingStatefulSet(ctx, ledger)
	if err != nil {
		return fmt.Errorf("fetching StatefulSet for TLS state: %w", err)
	}

	targetTLSMode := computeTargetTLSMode(
		desiredTLSMode(ledger),
		currentTLSModeFromStatefulSet(existingForTLS),
		rolloutConverged(existingForTLS),
	)
	ledger.Status.TLSMigrationPhase = tlsMigrationPhase(desiredTLSMode(ledger), targetTLSMode)

	desiredReplicas := int32(3)
	if ledger.Spec.Replicas != nil {
		desiredReplicas = *ledger.Spec.Replicas
	}

	// Check for scale-down: if the existing StatefulSet has more replicas than
	// desired, we need to remove Raft nodes before reducing replicas.
	// First, update the StatefulSet spec (image, env, etc.) while keeping
	// the current replica count so pods can start with the correct config.
	var previousReplicas int32
	scalingDown := false
	if r.Config != nil && r.Clientset != nil {
		existing := &appsv1.StatefulSet{}
		err := r.Get(ctx, types.NamespacedName{
			Name:      ledger.Name,
			Namespace: ledger.Namespace,
		}, existing)
		if err == nil && existing.Spec.Replicas != nil && *existing.Spec.Replicas > desiredReplicas {
			previousReplicas = *existing.Spec.Replicas
			scalingDown = true

			// Update the StatefulSet with the current replica count first
			// so that the image, env vars, etc. are applied. This allows
			// pods to start (e.g. after an image change) before we attempt
			// the Raft scale-down which requires running containers.
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ledger.Name,
					Namespace: ledger.Namespace,
				},
			}
			savedReplicas := ledger.Spec.Replicas
			ledger.Spec.Replicas = &previousReplicas
			_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
				sts.Labels = commonLabels(ledger)
				desired := buildStatefulSetSpec(ledger, specHash, agents, targetTLSMode)

				if sts.CreationTimestamp.IsZero() {
					sts.Spec = desired
				} else {
					sts.Spec.Replicas = desired.Replicas
					sts.Spec.Template = desired.Template
					sts.Spec.UpdateStrategy = desired.UpdateStrategy
					sts.Spec.PersistentVolumeClaimRetentionPolicy = desired.PersistentVolumeClaimRetentionPolicy
					sts.Spec.MinReadySeconds = desired.MinReadySeconds
				}

				return controllerutil.SetControllerReference(ledger, sts, r.Scheme)
			})
			ledger.Spec.Replicas = savedReplicas
			if err != nil {
				return fmt.Errorf("updating StatefulSet spec before scale-down: %w", err)
			}

			logger.Info("scale-down detected, removing Raft nodes",
				"currentReplicas", previousReplicas,
				"desiredReplicas", desiredReplicas,
			)
			// Use the TLS mode of the *previous* StatefulSet (existingForTLS, snapshot
			// taken before the CreateOrUpdate above): the rolling update has not yet
			// started, so pod-0's gRPC server is still on the old TLS_MODE.
			runningTLSMode := currentTLSModeFromStatefulSet(existingForTLS)
			if err := raftScaleDown(ctx, r.Config, r.Clientset, ledger, previousReplicas, desiredReplicas, runningTLSMode); err != nil {
				return fmt.Errorf("removing Raft nodes before scale-down: %w", err)
			}
		}
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ledger.Name,
			Namespace: ledger.Namespace,
		},
	}

	desired := buildStatefulSetSpec(ledger, specHash, agents, targetTLSMode)

	// Check if VolumeClaimTemplates changed on an existing StatefulSet.
	// VCTs are immutable — we must delete-recreate with orphan propagation.
	existing := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: ledger.Name, Namespace: ledger.Namespace}, existing); err == nil {
		if volumeClaimTemplatesChanged(existing.Spec.VolumeClaimTemplates, desired.VolumeClaimTemplates) {
			logger.Info("VolumeClaimTemplates changed, recreating StatefulSet with orphan propagation")
			orphan := metav1.DeletePropagationOrphan
			if err := r.Delete(ctx, existing, &client.DeleteOptions{
				PropagationPolicy: &orphan,
			}); err != nil {
				return fmt.Errorf("deleting StatefulSet for VolumeClaimTemplate change: %w", err)
			}
			// Return nil to requeue — next reconciliation will create the new StatefulSet
			// and the orphaned pods will be adopted.
			return nil
		}
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Labels = commonLabels(ledger)

		if sts.CreationTimestamp.IsZero() {
			// New StatefulSet — set the full spec.
			sts.Spec = desired
		} else {
			// Existing StatefulSet — only update mutable fields.
			// ServiceName, Selector, PodManagementPolicy and
			// VolumeClaimTemplates are immutable after creation.
			sts.Spec.Replicas = desired.Replicas
			sts.Spec.Template = desired.Template
			sts.Spec.UpdateStrategy = desired.UpdateStrategy
			sts.Spec.PersistentVolumeClaimRetentionPolicy = desired.PersistentVolumeClaimRetentionPolicy
			sts.Spec.MinReadySeconds = desired.MinReadySeconds
		}

		return controllerutil.SetControllerReference(ledger, sts, r.Scheme)
	})
	if err != nil {
		return err
	}

	// After the StatefulSet is updated (pods terminated), delete orphaned PVCs.
	if scalingDown && r.Clientset != nil {
		volNames := pvcVolumeNames(&ledger.Spec.Persistence)
		if err := deleteScaledDownPVCs(ctx, r.Clientset, ledger.Namespace, ledger.Name, previousReplicas, desiredReplicas, volNames); err != nil {
			return fmt.Errorf("deleting PVCs after scale-down: %w", err)
		}
	}

	return nil
}

func buildStatefulSetSpec(ledger *ledgerv1alpha1.LedgerService, specHash string, agents []agentKeyInfo, targetTLSMode string) appsv1.StatefulSetSpec {
	replicas := int32(3)
	if ledger.Spec.Replicas != nil {
		replicas = *ledger.Spec.Replicas
	}

	// OrderedReady ensures pods start sequentially. This is critical for Raft
	// clusters: etcd/raft only processes one ConfChange at a time and silently
	// drops concurrent proposals, so nodes must join one at a time.
	spec := appsv1.StatefulSetSpec{
		ServiceName:         headlessServiceName(ledger),
		Replicas:            &replicas,
		PodManagementPolicy: appsv1.OrderedReadyPodManagement,
		Selector: &metav1.LabelSelector{
			MatchLabels: selectorLabels(ledger),
		},
		PersistentVolumeClaimRetentionPolicy: buildRetentionPolicy(ledger),
		Template:                             buildPodTemplate(ledger, specHash, agents, targetTLSMode),
		VolumeClaimTemplates:                 buildVolumeClaimTemplates(ledger),
	}

	return spec
}

func buildRetentionPolicy(ledger *ledgerv1alpha1.LedgerService) *appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy {
	whenScaled := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	whenDeleted := appsv1.RetainPersistentVolumeClaimRetentionPolicyType

	if ledger.Spec.Persistence.RetentionPolicy != nil {
		rp := ledger.Spec.Persistence.RetentionPolicy
		if rp.WhenScaled == "Delete" {
			whenScaled = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
		}
		if rp.WhenDeleted == "Delete" {
			whenDeleted = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
		}
	}

	return &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenScaled:  whenScaled,
		WhenDeleted: whenDeleted,
	}
}

func buildPodTemplate(ledger *ledgerv1alpha1.LedgerService, specHash string, agents []agentKeyInfo, targetTLSMode string) corev1.PodTemplateSpec {
	spec := &ledger.Spec

	// Pod annotations with spec hash for rolling updates
	podAnnotations := make(map[string]string)
	maps.Copy(podAnnotations, ledger.Spec.PodAnnotations)
	podAnnotations[annotationSpecHash] = specHash
	if len(agents) > 0 {
		podAnnotations[annotationAuthKeysHash] = computeAuthKeysHash(agents)
	}

	// Container ports
	ports := []corev1.ContainerPort{
		{Name: "http", ContainerPort: spec.HttpPort, Protocol: corev1.ProtocolTCP},
		{Name: "grpc", ContainerPort: spec.GrpcPort, Protocol: corev1.ProtocolTCP},
		{Name: "raft", ContainerPort: raftPortFromBindAddr(spec.BindAddr), Protocol: corev1.ProtocolTCP},
	}

	// Image pull policy
	pullPolicy := ledger.Spec.Image.PullPolicy
	if ledger.Spec.Image.Tag == "latest" {
		pullPolicy = corev1.PullAlways
	}

	// Volume mounts — PVC-backed volumes reference VolumeClaimTemplates by name;
	// hostPath-backed volumes are added as inline pod volumes with SubPathExpr
	// so each pod gets an isolated subdirectory (<hostPath>/<pod-ordinal>).
	type volumeDef struct {
		name      string
		mountPath string
		spec      *ledgerv1alpha1.VolumeSpec
	}
	volumeDefs := []volumeDef{
		{"wal", spec.WalDir, &ledger.Spec.Persistence.WAL},
		{"data", spec.DataDir, &ledger.Spec.Persistence.Data},
		{"cold-cache", "/data/cold-cache", &ledger.Spec.Persistence.ColdCache},
	}

	var volumeMounts []corev1.VolumeMount
	var volumes []corev1.Volume

	for _, vd := range volumeDefs {
		if vd.spec.HostPath != nil {
			hp := vd.spec.HostPath
			hostPathType := corev1.HostPathDirectoryOrCreate
			if hp.Type == "Directory" {
				hostPathType = corev1.HostPathDirectory
			}
			volumes = append(volumes, corev1.Volume{
				Name: vd.name,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: hp.Path,
						Type: &hostPathType,
					},
				},
			})
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:        vd.name,
				MountPath:   vd.mountPath,
				SubPathExpr: "$(POD_INDEX)",
			})
		} else {
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      vd.name,
				MountPath: vd.mountPath,
			})
		}
	}

	if spec.ResponseSigning != nil && spec.ResponseSigning.Enabled {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "response-signing",
			MountPath: "/response-signing",
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "response-signing",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: spec.ResponseSigning.SecretName,
				},
			},
		})
	}

	// Mount the TLS secret whenever TLS is at least partially active
	// (targetTLSMode != "disabled"), so pods in the optional phase have
	// certs available even before the user-facing flip is complete.
	if targetTLSMode != tlsModeDisabled && spec.TLS != nil {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "tls-certs",
			MountPath: "/tls",
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "tls-certs",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: spec.TLS.SecretName,
				},
			},
		})
	}

	if len(agents) > 0 {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "auth-keys",
			MountPath: "/auth-keys",
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "auth-keys",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: authKeysConfigMapName(ledger),
					},
				},
			},
		})
	}

	envVars := buildEnvVars(ledger, targetTLSMode, agents)
	// Inject CLUSTER_SECRET only when TLS is at least partially active:
	// the secret is a static bearer token and must never travel in
	// plaintext. During a TLS toggle, the secret appears together with the
	// optional mode (rolling update phase 1), so pods on either side of
	// the rollout see consistent behavior.
	if shouldInjectClusterSecret(targetTLSMode) {
		envVars = append(envVars, corev1.EnvVar{
			Name: "CLUSTER_SECRET",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: clusterSecretName(ledger),
					},
					Key: clusterSecretKey,
				},
			},
		})
	}

	container := corev1.Container{
		Name:            "ledger",
		Image:           fmt.Sprintf("%s:%s", ledger.Spec.Image.Repository, ledger.Spec.Image.Tag),
		ImagePullPolicy: pullPolicy,
		Ports:           ports,
		Env:             envVars,
		Command:         buildCommand(ledger),
		VolumeMounts:    volumeMounts,
		Resources:       ledger.Spec.Resources,
	}

	if ledger.Spec.SecurityContext != nil {
		container.SecurityContext = ledger.Spec.SecurityContext
	}
	// Probes: start from defaults, then overlay any user-provided fields.
	container.LivenessProbe = mergeProbe(defaultLivenessProbe(), ledger.Spec.LivenessProbe)
	container.ReadinessProbe = mergeProbe(defaultReadinessProbe(), ledger.Spec.ReadinessProbe)
	container.StartupProbe = mergeProbe(defaultStartupProbe(), ledger.Spec.StartupProbe)

	podSpec := corev1.PodSpec{
		ServiceAccountName: serviceAccountName(ledger),
		Containers:         []corev1.Container{container},
		ImagePullSecrets:   ledger.Spec.ImagePullSecrets,
		NodeSelector:       ledger.Spec.NodeSelector,
		Tolerations:        ledger.Spec.Tolerations,
	}

	if ledger.Spec.PodSecurityContext != nil {
		podSpec.SecurityContext = ledger.Spec.PodSecurityContext
	}

	if len(volumes) > 0 {
		podSpec.Volumes = volumes
	}

	// Affinity
	affinity := buildAffinity(ledger)
	if affinity != nil {
		podSpec.Affinity = affinity
	}

	if len(ledger.Spec.TopologySpreadConstraints) > 0 {
		podSpec.TopologySpreadConstraints = buildTopologySpreadConstraints(ledger)
	}

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      selectorLabels(ledger),
			Annotations: podAnnotations,
		},
		Spec: podSpec,
	}
}

func buildAffinity(ledger *ledgerv1alpha1.LedgerService) *corev1.Affinity {
	var affinity *corev1.Affinity

	if ledger.Spec.Affinity != nil {
		affinity = ledger.Spec.Affinity.DeepCopy()
	}

	if ledger.Spec.PodAntiAffinity == nil || !ledger.Spec.PodAntiAffinity.Enabled {
		return affinity
	}

	paa := ledger.Spec.PodAntiAffinity

	if affinity == nil {
		affinity = &corev1.Affinity{}
	}

	topologyKey := paa.TopologyKey
	if topologyKey == "" {
		topologyKey = "kubernetes.io/hostname"
	}

	selector := selectorLabels(ledger)

	if paa.Type == "hard" {
		if affinity.PodAntiAffinity == nil {
			affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
		}
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
			affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			corev1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{MatchLabels: selector},
				TopologyKey:   topologyKey,
			},
		)
	} else {
		weight := paa.Weight
		if weight == 0 {
			weight = 100
		}
		if affinity.PodAntiAffinity == nil {
			affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
		}
		affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			corev1.WeightedPodAffinityTerm{
				Weight: weight,
				PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: &metav1.LabelSelector{MatchLabels: selector},
					TopologyKey:   topologyKey,
				},
			},
		)
	}

	return affinity
}

// buildTopologySpreadConstraints returns a deep-copied list of the user-provided
// topologySpreadConstraints with a default LabelSelector pointing to the
// LedgerService selector when the user did not supply one.
func buildTopologySpreadConstraints(ledger *ledgerv1alpha1.LedgerService) []corev1.TopologySpreadConstraint {
	in := ledger.Spec.TopologySpreadConstraints
	out := make([]corev1.TopologySpreadConstraint, len(in))
	selector := selectorLabels(ledger)
	for i := range in {
		out[i] = *in[i].DeepCopy()
		if out[i].LabelSelector == nil {
			out[i].LabelSelector = &metav1.LabelSelector{MatchLabels: selector}
		}
	}

	return out
}

// buildCommand emits the shell entrypoint for the ledger container. It is
// intentionally minimal: the only logic that lives here is what depends on
// POD_INDEX (a Kubernetes concept that must not leak into the server) —
// deriving the Raft NODE_ID and choosing between bootstrap / join / restore
// for the cluster startup flag. Everything else is plain configuration and
// is passed through env vars built by buildEnvVars.
func buildCommand(ledger *ledgerv1alpha1.LedgerService) []string {
	spec := &ledger.Spec

	var clusterLogic string
	if spec.Restore {
		clusterLogic = `CLUSTER_FLAG="--restore"`
	} else {
		bootstrap0 := fmt.Sprintf("%s-0.%s.${POD_NAMESPACE}.svc.cluster.local", ledger.Name, headlessServiceName(ledger))
		clusterLogic = fmt.Sprintf(`if [ "$POD_INDEX" = "0" ]; then
  if [ -d "%s/checkpoints" ] && [ "$(ls -A %s/checkpoints 2>/dev/null)" ]; then
    CLUSTER_FLAG=""
  else
    CLUSTER_FLAG="--bootstrap"
  fi
else
  CLUSTER_FLAG="--join %s:${GRPC_PORT}"
fi`, spec.DataDir, spec.DataDir, bootstrap0)
	}

	script := fmt.Sprintf(`NODE_ID=$((POD_INDEX + 1))
%s
exec ./ledger-server run --node-id $NODE_ID $CLUSTER_FLAG`, clusterLogic)

	return []string{"/bin/sh", "-c", script}
}

func buildVolumeClaimTemplates(ledger *ledgerv1alpha1.LedgerService) []corev1.PersistentVolumeClaim {
	type vctDef struct {
		name string
		spec *ledgerv1alpha1.VolumeSpec
		dflt string // default size
	}
	defs := []vctDef{
		{"wal", &ledger.Spec.Persistence.WAL, "5Gi"},
		{"data", &ledger.Spec.Persistence.Data, "10Gi"},
		{"cold-cache", &ledger.Spec.Persistence.ColdCache, "10Gi"},
	}

	var templates []corev1.PersistentVolumeClaim
	for _, d := range defs {
		if d.spec.HostPath != nil {
			continue // hostPath volumes are handled as inline pod volumes
		}

		accessMode := corev1.ReadWriteOnce
		if d.spec.AccessMode != "" {
			accessMode = corev1.PersistentVolumeAccessMode(d.spec.AccessMode)
		}
		size := d.spec.Size
		if size.IsZero() {
			size = resource.MustParse(d.dflt)
		}

		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: d.name},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{accessMode},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: size,
					},
				},
			},
		}
		if d.spec.StorageClass != "" {
			pvc.Spec.StorageClassName = &d.spec.StorageClass
		}
		if d.spec.VolumeAttributesClassName != "" {
			pvc.Spec.VolumeAttributesClassName = &d.spec.VolumeAttributesClassName
		}
		templates = append(templates, pvc)
	}

	return templates
}

// pvcVolumeNames returns the names of volumes that are PVC-backed (not hostPath).
func pvcVolumeNames(persistence *ledgerv1alpha1.PersistenceSpec) []string {
	var names []string
	if persistence.WAL.IsPVC() {
		names = append(names, "wal")
	}
	if persistence.Data.IsPVC() {
		names = append(names, "data")
	}
	if persistence.ColdCache.IsPVC() {
		names = append(names, "cold-cache")
	}

	return names
}

// raftPortFromBindAddr extracts the port number from a bind address like "0.0.0.0:7777".
func raftPortFromBindAddr(bindAddr string) int32 {
	parts := strings.SplitN(bindAddr, ":", 2)
	if len(parts) == 2 {
		var port int
		if _, err := fmt.Sscanf(parts[1], "%d", &port); err == nil {
			return int32(port)
		}
	}

	return 7777
}

// defaultLivenessProbe returns a sensible liveness probe for k8s that targets
// the /livez endpoint (always 200 when the process is alive).
func defaultLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/livez",
				Port: intstr.FromString("http"),
			},
		},
		InitialDelaySeconds: 15,
		PeriodSeconds:       10,
		FailureThreshold:    3,
	}
}

// defaultReadinessProbe returns a sensible readiness probe for k8s that targets
// the /readyz endpoint (200 only when the node is fully ready).
func defaultReadinessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/readyz",
				Port: intstr.FromString("http"),
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		FailureThreshold:    3,
	}
}

// defaultStartupProbe returns a startup probe that gives the process up to
// 5 minutes (failureThreshold 30 * periodSeconds 10) to finish initialising.
// This prevents the liveness probe from killing pods that are warming up a
// large Pebble database on cold start.
func defaultStartupProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/livez",
				Port: intstr.FromString("http"),
			},
		},
		PeriodSeconds:    10,
		FailureThreshold: 30,
	}
}

// mergeProbe overlays user-provided probe fields on top of a default probe.
// If override is nil, the default is returned unchanged.
// The ProbeHandler is replaced entirely if any handler is set in the override;
// scalar fields are overridden only when non-zero.
func mergeProbe(base, override *corev1.Probe) *corev1.Probe {
	if override == nil {
		return base
	}

	merged := base.DeepCopy()

	// Handler: replace entirely if the override specifies one.
	if override.HTTPGet != nil || override.TCPSocket != nil || override.Exec != nil || override.GRPC != nil {
		merged.ProbeHandler = override.ProbeHandler
	}

	if override.InitialDelaySeconds != 0 {
		merged.InitialDelaySeconds = override.InitialDelaySeconds
	}
	if override.TimeoutSeconds != 0 {
		merged.TimeoutSeconds = override.TimeoutSeconds
	}
	if override.PeriodSeconds != 0 {
		merged.PeriodSeconds = override.PeriodSeconds
	}
	if override.SuccessThreshold != 0 {
		merged.SuccessThreshold = override.SuccessThreshold
	}
	if override.FailureThreshold != 0 {
		merged.FailureThreshold = override.FailureThreshold
	}
	if override.TerminationGracePeriodSeconds != nil {
		merged.TerminationGracePeriodSeconds = override.TerminationGracePeriodSeconds
	}

	return merged
}

// volumeClaimTemplatesChanged returns true if the set of VolumeClaimTemplate
// names differs between the existing and desired StatefulSet specs.
// This is used to detect when a volume switches between PVC and hostPath modes,
// requiring the StatefulSet to be recreated (VCTs are immutable after creation).
func volumeClaimTemplatesChanged(existing, desired []corev1.PersistentVolumeClaim) bool {
	existingNames := make([]string, len(existing))
	for i, pvc := range existing {
		existingNames[i] = pvc.Name
	}
	desiredNames := make([]string, len(desired))
	for i, pvc := range desired {
		desiredNames[i] = pvc.Name
	}

	slices.Sort(existingNames)
	slices.Sort(desiredNames)

	return !slices.Equal(existingNames, desiredNames)
}
