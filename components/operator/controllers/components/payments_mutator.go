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
type PaymentsMutator struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=payments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=payments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=components.formance.com,resources=payments/finalizers,verbs=update

func (r *PaymentsMutator) Mutate(ctx context.Context, payments *componentsv1beta2.Payments) (*ctrl.Result, error) {

	apisv1beta2.SetProgressing(payments)

	_, _, err := r.reconcileDeployment(ctx, payments)
	if err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling deployment")
	}

	apisv1beta2.SetReady(payments)

	return nil, nil
}

func (r *PaymentsMutator) reconcileDeployment(ctx context.Context, payments *componentsv1beta2.Payments) (*appsv1.Deployment, controllerutil.OperationResult, error) {
	matchLabels := CreateMap("app.kubernetes.io/name", "payments")

	env := payments.Spec.Postgres.Env("")
	env = append(env,
		apisv1beta2.Env("POSTGRES_DATABASE_NAME", "$(POSTGRES_DATABASE)"),
	)
	if payments.Spec.Debug {
		env = append(env, apisv1beta2.Env("DEBUG", "true"))
	}
	if payments.Spec.Monitoring != nil {
		env = append(env, payments.Spec.Monitoring.Env("")...)
	}
	if payments.Spec.Collector != nil {
		env = append(env, payments.Spec.Collector.Env("")...)
	}

	return controllerutils.CreateOrUpdate(ctx, r.Client, client.ObjectKeyFromObject(payments),
		controllerutils.WithController[*appsv1.Deployment](payments, r.Scheme),
		func(deployment *appsv1.Deployment) error {
			deployment.Spec = appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: matchLabels,
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: matchLabels,
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:            "payments",
							Image:           controllerutils.GetImage("payments", payments.Spec.Version),
							ImagePullPolicy: controllerutils.ImagePullPolicy(payments.Spec),
							Env:             env,
							Ports: []corev1.ContainerPort{{
								Name:          "payments",
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
			if payments.Spec.Postgres.CreateDatabase {
				deployment.Spec.Template.Spec.InitContainers = []corev1.Container{
					payments.Spec.Postgres.CreateDatabaseInitContainer(),
					{
						Name:            "migrate",
						Image:           controllerutils.GetImage("payments", payments.Spec.Version),
						ImagePullPolicy: controllerutils.ImagePullPolicy(payments.Spec),
						Env:             env,
						Command:         []string{"payments", "migrate", "up"},
					}}
			}
			return nil
		})
}

// SetupWithManager sets up the controller with the Manager.
func (r *PaymentsMutator) SetupWithBuilder(mgr ctrl.Manager, builder *ctrl.Builder) error {
	builder.
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.Ingress{})
	return nil
}

func NewPaymentsMutator(client client.Client, scheme *runtime.Scheme) controllerutils.Mutator[*componentsv1beta2.Payments] {
	return &PaymentsMutator{
		Client: client,
		Scheme: scheme,
	}
}
