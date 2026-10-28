package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// reconcileDebugBox manages the opt-in interactive ledgerctl pod and its
// dedicated superuser Credentials resource. The pod is intentionally idle so
// operators can exec into it without starting a second Ledger server.
func (r *ClusterReconciler) reconcileDebugBox(ctx context.Context, ledger *ledgerv1alpha1.Cluster) (bool, error) {
	enabled := ledger.Spec.DebugBox != nil && ledger.Spec.DebugBox.Enabled
	authEnabled := ledger.Spec.Auth != nil && ledger.Spec.Auth.Enabled != nil && *ledger.Spec.Auth.Enabled
	credentialsName := debugBoxCredentialsName(ledger)
	podName := debugBoxPodName(ledger)

	if !enabled || !authEnabled {
		if err := r.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: ledger.Namespace}}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if err := r.Delete(ctx, &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: credentialsName}}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}

		return true, nil
	}

	selectorValue := debugBoxCredentialsSelectorValue(ledger)
	if ledger.Labels == nil {
		ledger.Labels = make(map[string]string)
	}
	if ledger.Labels[labelDebugBoxCredential] != selectorValue {
		ledger.Labels[labelDebugBoxCredential] = selectorValue
		if err := r.Update(ctx, ledger); err != nil {
			return false, fmt.Errorf("persisting debug box selector label: %w", err)
		}
	}

	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: credentialsName}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, credentials, func() error {
		if credentials.Labels == nil {
			credentials.Labels = make(map[string]string)
		}
		credentials.Labels[labelManagedBy] = "ledger-operator"
		credentials.Labels[labelDebugBoxCredential] = selectorValue
		if credentials.Annotations == nil {
			credentials.Annotations = make(map[string]string)
		}
		credentials.Annotations[annotationDebugBoxCluster] = ledger.Namespace + "/" + ledger.Name
		credentials.Spec.Scopes = nil
		credentials.Spec.Superuser = true
		credentials.Spec.Selector = metav1.LabelSelector{MatchLabels: map[string]string{
			labelDebugBoxCredential: selectorValue,
		}}

		return nil
	}); err != nil {
		return false, fmt.Errorf("ensuring debug box Credentials: %w", err)
	}

	ready := credentials.Status.Phase == "Ready"
	distributed := false
	secretName := operatorCredentialsSecretName(credentialsName)
	for _, ref := range credentials.Status.DistributedSecretRefs {
		if ref.Namespace == ledger.Namespace {
			distributed = ref.Name == secretName

			break
		}
	}
	if !ready || !distributed {
		return false, nil
	}

	if err := r.reconcileDebugBoxPod(ctx, ledger, secretName); err != nil {
		return false, err
	}

	return true, nil
}

func (r *ClusterReconciler) reconcileDebugBoxPod(ctx context.Context, ledger *ledgerv1alpha1.Cluster, secretName string) error {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: debugBoxPodName(ledger), Namespace: ledger.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pod, func() error {
		pod.Labels = map[string]string{
			labelManagedBy: "ledger-operator",
			labelName:      "ledger-debug-box",
			labelInstance:  ledger.Name,
		}
		serverAddr := fmt.Sprintf("%s.%s.svc.cluster.local:%d", resourceName(ledger.Name), ledger.Namespace, ledger.Spec.GrpcPort)
		env := []corev1.EnvVar{
			strEnv("OPERATOR_SIGNING_KEY", "/operator-credentials/seed.hex"),
			secretKeyEnv("OPERATOR_SIGNING_KEY_ID", secretName, "key-id"),
			strEnv("LEDGERCTL_SERVER", serverAddr),
			strEnv("LEDGERCTL_TLS_SERVER_NAME", fmt.Sprintf("%s.%s.svc.cluster.local", resourceName(ledger.Name), ledger.Namespace)),
		}
		volumes := []corev1.Volume{{Name: "operator-credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secretName}}}}
		mounts := []corev1.VolumeMount{{Name: "operator-credentials", MountPath: "/operator-credentials", ReadOnly: true}}
		if ledger.Spec.TLS != nil && ledger.Spec.TLS.Enabled && ledger.Spec.TLS.SecretName != "" {
			if ledger.Spec.TLS.CASecretKey != "" {
				env = append(env, strEnv("LEDGERCTL_TLS_CA_CERT", "/tls/"+ledger.Spec.TLS.CASecretKey))
				volumes = append(volumes, corev1.Volume{Name: "tls-ca", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: ledger.Spec.TLS.SecretName,
					Items:      []corev1.KeyToPath{{Key: ledger.Spec.TLS.CASecretKey, Path: ledger.Spec.TLS.CASecretKey}},
				}}})
				mounts = append(mounts, corev1.VolumeMount{Name: "tls-ca", MountPath: "/tls", ReadOnly: true})
			}
		} else {
			env = append(env, strEnv("LEDGERCTL_INSECURE", "true"))
		}
		pod.Spec = corev1.PodSpec{
			ServiceAccountName: serviceAccountName(ledger),
			ImagePullSecrets:   ledger.Spec.ImagePullSecrets,
			SecurityContext:    ledger.Spec.PodSecurityContext,
			RestartPolicy:      corev1.RestartPolicyAlways,
			Volumes:            volumes,
			Containers: []corev1.Container{{
				Name:            "ledgerctl",
				Image:           fmt.Sprintf("%s:%s", ledger.Spec.Image.Repository, ledger.Spec.Image.Tag),
				ImagePullPolicy: ledger.Spec.Image.PullPolicy,
				Command:         []string{"/bin/bash", "-c"},
				Args: []string{`ledgerctl() {
  if [[ "$1" == "auth" && "$2" == "generate-token" ]]; then
    /app/ledgerctl "$@"
    return
  fi
  local token
  token="$(/app/ledgerctl auth generate-token --signing-key "$OPERATOR_SIGNING_KEY" --key-id "$OPERATOR_SIGNING_KEY_ID" --subject ledger-debug-box --scopes ledger:ReadOps,ledger:WriteOps,ledger:ReadCluster,ledger:WriteCluster --expiration 5m)" || return
  /app/ledgerctl "$@" --auth-token "$token"
}
export -f ledgerctl
trap : TERM INT
sleep infinity & wait`},
				Env:          env,
				VolumeMounts: mounts,
				Resources:    ledger.Spec.Resources,
			}},
		}
		if ledger.Spec.SecurityContext != nil {
			pod.Spec.Containers[0].SecurityContext = ledger.Spec.SecurityContext
		}

		return controllerutil.SetControllerReference(ledger, pod, r.Scheme)
	})

	return err
}
