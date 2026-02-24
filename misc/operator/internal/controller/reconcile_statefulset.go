package controller

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerReconciler) reconcileStatefulSet(ctx context.Context, ledger *ledgerv1alpha1.Ledger, specHash string) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ledger.Name,
			Namespace: ledger.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Labels = commonLabels(ledger)
		sts.Spec = buildStatefulSetSpec(ledger, specHash)
		return controllerutil.SetControllerReference(ledger, sts, r.Scheme)
	})
	return err
}

func buildStatefulSetSpec(ledger *ledgerv1alpha1.Ledger, specHash string) appsv1.StatefulSetSpec {
	replicas := int32(3)
	if ledger.Spec.Replicas != nil {
		replicas = *ledger.Spec.Replicas
	}

	parallel := appsv1.ParallelPodManagement

	spec := appsv1.StatefulSetSpec{
		ServiceName:         headlessServiceName(ledger),
		Replicas:            &replicas,
		PodManagementPolicy: parallel,
		Selector: &metav1.LabelSelector{
			MatchLabels: selectorLabels(ledger),
		},
		PersistentVolumeClaimRetentionPolicy: buildRetentionPolicy(ledger),
		Template:                             buildPodTemplate(ledger, specHash),
		VolumeClaimTemplates:                 buildVolumeClaimTemplates(ledger),
	}

	return spec
}

func buildRetentionPolicy(ledger *ledgerv1alpha1.Ledger) *appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy {
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

func buildPodTemplate(ledger *ledgerv1alpha1.Ledger, specHash string) corev1.PodTemplateSpec {
	cfg := &ledger.Spec.Config

	// Pod annotations with spec hash for rolling updates
	podAnnotations := make(map[string]string)
	for k, v := range ledger.Spec.PodAnnotations {
		podAnnotations[k] = v
	}
	podAnnotations[annotationSpecHash] = specHash

	// Container ports
	ports := []corev1.ContainerPort{
		{Name: "http", ContainerPort: cfg.HttpPort, Protocol: corev1.ProtocolTCP},
		{Name: "grpc", ContainerPort: cfg.GrpcPort, Protocol: corev1.ProtocolTCP},
		{Name: "raft", ContainerPort: raftPortFromBindAddr(cfg.BindAddr), Protocol: corev1.ProtocolTCP},
	}

	// Image pull policy
	pullPolicy := ledger.Spec.Image.PullPolicy
	if ledger.Spec.Image.Tag == "latest" {
		pullPolicy = corev1.PullAlways
	}

	// Volume mounts
	volumeMounts := []corev1.VolumeMount{
		{Name: "wal", MountPath: cfg.WalDir},
		{Name: "data", MountPath: cfg.DataDir},
	}

	// Volumes
	var volumes []corev1.Volume

	if cfg.ResponseSigning != nil && cfg.ResponseSigning.Enabled {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "response-signing",
			MountPath: "/response-signing",
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "response-signing",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: cfg.ResponseSigning.SecretName,
				},
			},
		})
	}

	if cfg.TLS != nil && cfg.TLS.Enabled {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "tls-certs",
			MountPath: "/tls",
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "tls-certs",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: cfg.TLS.SecretName,
				},
			},
		})
	}

	container := corev1.Container{
		Name:            "ledger",
		Image:           fmt.Sprintf("%s:%s", ledger.Spec.Image.Repository, ledger.Spec.Image.Tag),
		ImagePullPolicy: pullPolicy,
		Ports:           ports,
		Env:             buildEnvVars(ledger),
		Command:         buildCommand(ledger),
		VolumeMounts:    volumeMounts,
		Resources:       ledger.Spec.Resources,
	}

	if ledger.Spec.SecurityContext != nil {
		container.SecurityContext = ledger.Spec.SecurityContext
	}
	if ledger.Spec.LivenessProbe != nil {
		container.LivenessProbe = ledger.Spec.LivenessProbe
	}
	if ledger.Spec.ReadinessProbe != nil {
		container.ReadinessProbe = ledger.Spec.ReadinessProbe
	}

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

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      selectorLabels(ledger),
			Annotations: podAnnotations,
		},
		Spec: podSpec,
	}
}

func buildAffinity(ledger *ledgerv1alpha1.Ledger) *corev1.Affinity {
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

func buildCommand(ledger *ledgerv1alpha1.Ledger) []string {
	cfg := &ledger.Spec.Config
	hlsSvcName := headlessServiceName(ledger)

	var clusterLogic string
	if cfg.Restore {
		clusterLogic = `CLUSTER_FLAG="--restore"`
	} else {
		bootstrap0 := fmt.Sprintf("%s-0.%s.${POD_NAMESPACE}.svc.cluster.local", ledger.Name, hlsSvcName)
		clusterLogic = fmt.Sprintf(`if [ "$POD_INDEX" = "0" ]; then
  CLUSTER_FLAG="--bootstrap"
else
  BOOTSTRAP_HOST="%s"
  CLUSTER_FLAG="--join ${BOOTSTRAP_HOST}:${GRPC_PORT}"
fi`, bootstrap0)
	}

	var extraFlags string
	if cfg.Raft != nil && cfg.Raft.LearnerPromotionThreshold != nil {
		extraFlags += fmt.Sprintf(" \\\n  --learner-promotion-threshold %d", *cfg.Raft.LearnerPromotionThreshold)
	}
	if cfg.ResponseSigning != nil && cfg.ResponseSigning.Enabled {
		extraFlags += ` \
  --response-signing-key "$RESPONSE_SIGNING_KEY"`
	}

	script := fmt.Sprintf(`NODE_ID=$((POD_INDEX + 1))
RAFT_PORT=$(echo $BIND_ADDR | cut -d: -f2)
ADVERTISE_ADDR="${POD_NAME}.%s.${POD_NAMESPACE}.svc.cluster.local:${RAFT_PORT}"
%s
OTEL_RESOURCE_ATTRIBUTES="service.node_id=$NODE_ID"
exec ./ledger-v3-poc run \
  --node-id $NODE_ID \
  --advertise-addr "$ADVERTISE_ADDR"%s \
  $CLUSTER_FLAG`, hlsSvcName, clusterLogic, extraFlags)

	return []string{"/bin/sh", "-c", script}
}

func buildVolumeClaimTemplates(ledger *ledgerv1alpha1.Ledger) []corev1.PersistentVolumeClaim {
	walAccessMode := corev1.ReadWriteOnce
	if ledger.Spec.Persistence.WAL.AccessMode != "" {
		walAccessMode = corev1.PersistentVolumeAccessMode(ledger.Spec.Persistence.WAL.AccessMode)
	}

	dataAccessMode := corev1.ReadWriteOnce
	if ledger.Spec.Persistence.Data.AccessMode != "" {
		dataAccessMode = corev1.PersistentVolumeAccessMode(ledger.Spec.Persistence.Data.AccessMode)
	}

	walSize := ledger.Spec.Persistence.WAL.Size
	if walSize.IsZero() {
		walSize = resource.MustParse("5Gi")
	}

	dataSize := ledger.Spec.Persistence.Data.Size
	if dataSize.IsZero() {
		dataSize = resource.MustParse("10Gi")
	}

	templates := []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "wal"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{walAccessMode},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: walSize,
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "data"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{dataAccessMode},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: dataSize,
					},
				},
			},
		},
	}

	if ledger.Spec.Persistence.WAL.StorageClass != "" {
		templates[0].Spec.StorageClassName = &ledger.Spec.Persistence.WAL.StorageClass
	}
	if ledger.Spec.Persistence.Data.StorageClass != "" {
		templates[1].Spec.StorageClassName = &ledger.Spec.Persistence.Data.StorageClass
	}

	return templates
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
