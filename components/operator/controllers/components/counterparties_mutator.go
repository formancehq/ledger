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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// CounterpartiesMutator reconciles a Auth object
type CounterpartiesMutator struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=counterparties,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=counterparties/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=components.formance.com,resources=counterparties/finalizers,verbs=update

func (r *CounterpartiesMutator) Mutate(ctx context.Context, counterparties *componentsv1beta2.Counterparties) (*ctrl.Result, error) {

	apisv1beta2.SetProgressing(counterparties)

	if counterparties.Spec.Enabled {
		_, _, err := r.reconcileDeployment(ctx, counterparties)
		if err != nil {
			return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling deployment")
		}
	}

	apisv1beta2.SetReady(counterparties)

	return nil, nil
}

func counterpartiesEnvVars(counterparties *componentsv1beta2.Counterparties) []corev1.EnvVar {
	env := counterparties.Spec.Postgres.Env("")

	env = append(env, counterparties.Spec.DevProperties.Env()...)
	if counterparties.Spec.Monitoring != nil {
		env = append(env, counterparties.Spec.Monitoring.Env("")...)
	}
	return env
}

func (r *CounterpartiesMutator) reconcileDeployment(ctx context.Context, counterparties *componentsv1beta2.Counterparties) (*appsv1.Deployment, controllerutil.OperationResult, error) {
	matchLabels := CreateMap("app.kubernetes.io/name", "counterparties")

	return controllerutils.CreateOrUpdate(ctx, r.Client, client.ObjectKeyFromObject(counterparties),
		controllerutils.WithController[*appsv1.Deployment](counterparties, r.Scheme),
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
							Name:            "counterparties",
							Image:           controllerutils.GetImage("counterparties", counterparties.Spec.Version),
							ImagePullPolicy: controllerutils.ImagePullPolicy(counterparties.Spec),
							Env:             counterpartiesEnvVars(counterparties),
							Ports: []corev1.ContainerPort{{
								Name:          "counterparties",
								ContainerPort: 8080,
							}},
							LivenessProbe: controllerutils.DefaultLiveness(),
						}},
					},
				},
			}
			if counterparties.Spec.Postgres.CreateDatabase {
				deployment.Spec.Template.Spec.InitContainers = []corev1.Container{{
					Name:            "init-create-counterparties-db",
					Image:           "postgres:13",
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command: []string{
						"sh",
						"-c",
						`psql -Atx ${POSTGRES_NO_DATABASE_URI}/postgres -c "SELECT 1 FROM pg_database WHERE datname = '${POSTGRES_DATABASE}'" | grep -q 1 && echo "Base already exists" || psql -Atx ${POSTGRES_NO_DATABASE_URI}/postgres -c "CREATE DATABASE \"${POSTGRES_DATABASE}\""`,
					},
					Env: counterparties.Spec.Postgres.Env(""),
				}}
			}
			return nil
		})
}

// SetupWithBuilder SetupWithManager sets up the controller with the Manager.
func (r *CounterpartiesMutator) SetupWithBuilder(mgr ctrl.Manager, builder *ctrl.Builder) error {
	builder.
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.Ingress{})
	return nil
}

func NewCounterpartiesMutator(client client.Client, scheme *runtime.Scheme) controllerutils.Mutator[*componentsv1beta2.Counterparties] {
	return &CounterpartiesMutator{
		Client: client,
		Scheme: scheme,
	}
}
