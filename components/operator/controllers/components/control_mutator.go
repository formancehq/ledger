package components

import (
	"context"
	"strings"

	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/controllerutils"
	. "github.com/formancehq/operator/pkg/typeutils"
	pkgError "github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	autoscallingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

//+kubebuilder:rbac:groups=components.formance.com,resources=controls,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=components.formance.com,resources=controls/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=components.formance.com,resources=controls/finalizers,verbs=update

type ControlMutator struct {
	Client client.Client
	Scheme *runtime.Scheme
}

func (m *ControlMutator) SetupWithBuilder(mgr ctrl.Manager, builder *ctrl.Builder) error {
	builder.
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.Ingress{})
	return nil
}

func (m *ControlMutator) Mutate(ctx context.Context, control *componentsv1beta2.Control) (*ctrl.Result, error) {
	apisv1beta2.SetProgressing(control)

	_, err := m.reconcileDeployment(ctx, control)
	if err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling deployment")
	}

	if _, _, err := m.reconcileHPA(ctx, control); err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling HPA")
	}

	apisv1beta2.SetReady(control)

	return nil, nil
}

func (m *ControlMutator) reconcileDeployment(ctx context.Context, control *componentsv1beta2.Control) (*appsv1.Deployment, error) {
	matchLabels := CreateMap("app.kubernetes.io/name", "control")

	env := []corev1.EnvVar{
		apisv1beta2.Env("API_URL_BACK", control.Spec.ApiURLBack),
		apisv1beta2.Env("API_URL_FRONT", control.Spec.ApiURLFront),
		apisv1beta2.Env("API_URL", control.Spec.ApiURLFront),
	}

	if control.Spec.Dev {
		env = append(env, apisv1beta2.Env("UNSECURE_COOKIES", "true"))
	}

	if control.Spec.Monitoring != nil {
		env = append(env, control.Spec.Monitoring.Env("")...)
	}

	env = append(env,
		apisv1beta2.Env("ENCRYPTION_KEY", "9h44y2ZqrDuUy5R9NGLA9hca7uRUr932"),
		apisv1beta2.Env("ENCRYPTION_IV", "b6747T6eP9DnMvEw"),
		apisv1beta2.Env("CLIENT_ID", control.Spec.AuthClientConfiguration.ClientID),
		apisv1beta2.Env("CLIENT_SECRET", control.Spec.AuthClientConfiguration.ClientSecret),
		// TODO: Clean that mess
		apisv1beta2.Env("REDIRECT_URI", strings.TrimSuffix(control.Spec.ApiURLFront, "/api")),
	)

	ret, _, err := controllerutils.CreateOrUpdate(ctx, m.Client, client.ObjectKeyFromObject(control),
		controllerutils.WithController[*appsv1.Deployment](control, m.Scheme),
		func(deployment *appsv1.Deployment) error {
			deployment.Spec = appsv1.DeploymentSpec{
				Replicas: control.Spec.GetReplicas(),
				Selector: &metav1.LabelSelector{
					MatchLabels: matchLabels,
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: matchLabels,
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:            "control",
							Image:           controllerutils.GetImage("control", control.Spec.Version),
							ImagePullPolicy: controllerutils.ImagePullPolicy(control.Spec),
							Env:             env,
							Ports: []corev1.ContainerPort{{
								Name:          "http",
								ContainerPort: 3000,
							}},
						}},
					},
				},
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	selector, err := metav1.LabelSelectorAsSelector(ret.Spec.Selector)
	if err != nil {
		return nil, err
	}

	control.Status.Selector = selector.String()
	control.Status.Replicas = *control.Spec.GetReplicas()

	return ret, err
}

func (m *ControlMutator) reconcileHPA(ctx context.Context, control *componentsv1beta2.Control) (*autoscallingv2.HorizontalPodAutoscaler, controllerutil.OperationResult, error) {
	return controllerutils.CreateOrUpdate(ctx, m.Client, client.ObjectKeyFromObject(control),
		controllerutils.WithController[*autoscallingv2.HorizontalPodAutoscaler](control, m.Scheme),
		func(hpa *autoscallingv2.HorizontalPodAutoscaler) error {
			hpa.Spec = control.Spec.GetHPASpec(control)
			return nil
		})
}

var _ controllerutils.Mutator[*componentsv1beta2.Control] = &ControlMutator{}

func NewControlMutator(client client.Client, scheme *runtime.Scheme) controllerutils.Mutator[*componentsv1beta2.Control] {
	return &ControlMutator{
		Client: client,
		Scheme: scheme,
	}
}
