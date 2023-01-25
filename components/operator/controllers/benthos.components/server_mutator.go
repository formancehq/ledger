package benthos_components

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	benthosv1beta2 "github.com/formancehq/operator/apis/benthos.components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/controllerutils"
	. "github.com/formancehq/operator/pkg/typeutils"
	"github.com/numary/auth/authclient"
	pkgError "github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	benthosImage = "jeffail/benthos:4.10.0"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=benthos.components.formance.com,resources=servers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=benthos.components.formance.com,resources=streams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=benthos.components.formance.com,resources=servers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=benthos.components.formance.com,resources=servers/finalizers,verbs=update

type ServerMutator struct {
	client client.Client
	scheme *runtime.Scheme
}

func (m *ServerMutator) SetupWithBuilder(mgr ctrl.Manager, blder *ctrl.Builder) error {
	blder.
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{})
	return nil
}

func (m *ServerMutator) Mutate(ctx context.Context, server *benthosv1beta2.Server) (*ctrl.Result, error) {

	apisv1beta2.SetProgressing(server)

	deployment, _, err := m.reconcileDeployment(ctx, server)
	if err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling deployment")
	}

	_, _, err = m.reconcileService(ctx, server, deployment)
	if err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling service")
	}

	apisv1beta2.SetReady(server)

	return nil, nil
}

func (r *ServerMutator) reconcileDeployment(ctx context.Context, server *benthosv1beta2.Server) (*appsv1.Deployment, controllerutil.OperationResult, error) {
	matchLabels := CreateMap("app.kubernetes.io/name", "benthos")

	return controllerutils.CreateOrUpdate(ctx, r.client, client.ObjectKeyFromObject(server),
		controllerutils.WithController[*appsv1.Deployment](server, r.scheme),
		controllerutils.WithReloaderAnnotations[*appsv1.Deployment](),
		func(deployment *appsv1.Deployment) error {
			command := []string{"/benthos"}
			if server.Spec.ResourcesConfigMap != "" {
				command = append(command, "-r", "/config/resources/*.yaml")
			}
			if server.Spec.TemplatesConfigMap != "" {
				command = append(command, "-t", "/config/templates/*.yaml")
			}
			if server.Spec.GlobalConfigMap != "" {
				command = append(command, "-c", "/config/global/config.yaml")
			}
			if server.Spec.Dev {
				command = append(command, "--log.level", "trace")
			}
			command = append(command, "streams")
			if server.Spec.StreamsConfigMap != "" {
				command = append(command, "/config/streams/*.yaml")
			}

			expectedContainer := corev1.Container{
				Name:            "benthos",
				Image:           benthosImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         command,
				Ports: []corev1.ContainerPort{{
					Name:          "benthos",
					ContainerPort: 4195,
				}},
				VolumeMounts: []corev1.VolumeMount{},
				Env:          server.Spec.Env,
			}
			volumes := make([]corev1.Volume, 0)

			addVolumeMount := func(name, configMap string) {
				expectedContainer.VolumeMounts = append(expectedContainer.VolumeMounts, corev1.VolumeMount{
					Name:      name,
					ReadOnly:  true,
					MountPath: fmt.Sprintf("/config/%s", name),
				})
				volumes = append(volumes, corev1.Volume{
					Name: name,
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: configMap,
							},
						},
					},
				})
			}
			if server.Spec.TemplatesConfigMap != "" {
				addVolumeMount("templates", server.Spec.TemplatesConfigMap)
			}
			if server.Spec.ResourcesConfigMap != "" {
				addVolumeMount("resources", server.Spec.ResourcesConfigMap)
			}
			if server.Spec.StreamsConfigMap != "" {
				addVolumeMount("streams", server.Spec.StreamsConfigMap)
			}
			if server.Spec.GlobalConfigMap != "" {
				addVolumeMount("global", server.Spec.GlobalConfigMap)
			}

			deployment.Spec = appsv1.DeploymentSpec{
				Replicas: authclient.PtrInt32(1),
				Selector: &metav1.LabelSelector{
					MatchLabels: matchLabels,
				},
				Strategy: appsv1.DeploymentStrategy{
					Type: appsv1.RecreateDeploymentStrategyType,
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: matchLabels,
					},
					Spec: corev1.PodSpec{
						Volumes:        volumes,
						InitContainers: server.Spec.InitContainers,
						Containers:     []corev1.Container{expectedContainer},
					},
				},
			}
			return nil
		})
}

func (r *ServerMutator) reconcileService(ctx context.Context, srv *benthosv1beta2.Server, pod *appsv1.Deployment) (*corev1.Service, controllerutil.OperationResult, error) {
	return controllerutils.CreateOrUpdate(ctx, r.client, client.ObjectKeyFromObject(srv),
		controllerutils.WithController[*corev1.Service](srv, r.scheme),
		func(service *corev1.Service) error {
			service.Spec = corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{
					Name:        "http",
					Port:        pod.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort,
					Protocol:    "TCP",
					AppProtocol: pointer.String("http"),
					TargetPort:  intstr.FromString(pod.Spec.Template.Spec.Containers[0].Ports[0].Name),
				}},
				Selector: pod.Labels,
			}
			return nil
		})
}

var _ controllerutils.Mutator[*benthosv1beta2.Server] = &ServerMutator{}

func NewServerMutator(client client.Client, scheme *runtime.Scheme) controllerutils.Mutator[*benthosv1beta2.Server] {
	return &ServerMutator{
		client: client,
		scheme: scheme,
	}
}
