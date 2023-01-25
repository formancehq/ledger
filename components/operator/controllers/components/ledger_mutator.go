/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package components

import (
	"context"

	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/controllerutils"
	. "github.com/formancehq/operator/pkg/typeutils"
	pkgError "github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	autoscallingv1 "k8s.io/api/autoscaling/v1"
	autoscallingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Mutator reconciles a Auth object
type LedgerMutator struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=ledgers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=ledgers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=components.formance.com,resources=ledgers/finalizers,verbs=update

func (r *LedgerMutator) Mutate(ctx context.Context, ledger *componentsv1beta2.Ledger) (*ctrl.Result, error) {

	apisv1beta2.SetProgressing(ledger)

	_, err := r.reconcileDeployment(ctx, ledger)
	if err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling deployment")
	}

	if _, _, err := r.reconcileHPA(ctx, ledger); err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling HPA")
	}

	apisv1beta2.SetReady(ledger)

	return nil, nil
}

func (r *LedgerMutator) reconcileDeployment(ctx context.Context, ledger *componentsv1beta2.Ledger) (*appsv1.Deployment, error) {
	matchLabels := CreateMap("app.kubernetes.io/name", "ledger")

	env := []corev1.EnvVar{
		apisv1beta2.Env("NUMARY_SERVER_HTTP_BIND_ADDRESS", "0.0.0.0:8080"),
		apisv1beta2.Env("NUMARY_STORAGE_DRIVER", "postgres"),
	}
	env = append(env, ledger.Spec.Postgres.Env("NUMARY_")...)
	env = append(env, ledger.Spec.LockingStrategy.Env("NUMARY_")...)
	env = append(env, apisv1beta2.Env("NUMARY_STORAGE_POSTGRES_CONN_STRING", "$(NUMARY_POSTGRES_URI)"))
	env = append(env, ledger.Spec.DevProperties.EnvWithPrefix("NUMARY_")...)
	if ledger.Spec.Monitoring != nil {
		env = append(env, ledger.Spec.Monitoring.Env("NUMARY_")...)
	}
	if ledger.Spec.Collector != nil {
		env = append(env, ledger.Spec.Collector.Env("NUMARY_")...)
	}

	ret, _, err := controllerutils.CreateOrUpdate(ctx, r.Client, client.ObjectKeyFromObject(ledger),
		controllerutils.WithController[*appsv1.Deployment](ledger, r.Scheme),
		func(deployment *appsv1.Deployment) error {
			deployment.Spec = appsv1.DeploymentSpec{
				Replicas: ledger.Spec.GetReplicas(),
				Selector: &metav1.LabelSelector{
					MatchLabels: matchLabels,
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: matchLabels,
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:            "ledger",
							Image:           controllerutils.GetImage("ledger", ledger.Spec.Version),
							ImagePullPolicy: controllerutils.ImagePullPolicy(ledger.Spec),
							Env:             env,
							Ports: []corev1.ContainerPort{{
								Name:          "ledger",
								ContainerPort: 8080,
							}},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/_health",
										Port: intstr.IntOrString{
											IntVal: 8080,
										},
										Scheme: "HTTP",
									},
								},
								InitialDelaySeconds:           1,
								TimeoutSeconds:                30,
								PeriodSeconds:                 2,
								SuccessThreshold:              1,
								FailureThreshold:              10,
								TerminationGracePeriodSeconds: pointer.Int64(10),
							},
						}},
					},
				},
			}
			if ledger.Spec.Postgres.CreateDatabase {
				deployment.Spec.Template.Spec.InitContainers = []corev1.Container{
					ledger.Spec.Postgres.CreateDatabaseInitContainer(),
				}
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

	ledger.Status.Selector = selector.String()
	ledger.Status.Replicas = *ledger.Spec.GetReplicas()

	return ret, err
}

func (r *LedgerMutator) reconcileHPA(ctx context.Context, ledger *componentsv1beta2.Ledger) (*autoscallingv2.HorizontalPodAutoscaler, controllerutil.OperationResult, error) {
	return controllerutils.CreateOrUpdate(ctx, r.Client, client.ObjectKeyFromObject(ledger),
		controllerutils.WithController[*autoscallingv2.HorizontalPodAutoscaler](ledger, r.Scheme),
		func(hpa *autoscallingv2.HorizontalPodAutoscaler) error {
			hpa.Spec = ledger.Spec.GetHPASpec(ledger)
			return nil
		})
}

// SetupWithManager sets up the controller with the Manager.
func (r *LedgerMutator) SetupWithBuilder(mgr ctrl.Manager, builder *ctrl.Builder) error {
	builder.
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&autoscallingv1.HorizontalPodAutoscaler{})
	return nil
}

func NewLedgerMutator(client client.Client, scheme *runtime.Scheme) controllerutils.Mutator[*componentsv1beta2.Ledger] {
	return &LedgerMutator{
		Client: client,
		Scheme: scheme,
	}
}
