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
	"fmt"
	"strings"

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

// WalletsMutator reconciles a Auth object
type WalletsMutator struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=wallets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=wallets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=components.formance.com,resources=wallets/finalizers,verbs=update

func (r *WalletsMutator) Mutate(ctx context.Context, wallets *componentsv1beta2.Wallets) (*ctrl.Result, error) {

	apisv1beta2.SetProgressing(wallets)

	_, _, err := r.reconcileDeployment(ctx, wallets)
	if err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling deployment")
	}

	apisv1beta2.SetReady(wallets)

	return nil, nil
}

func walletsEnvVars(wallets *componentsv1beta2.Wallets) []corev1.EnvVar {
	ledgerName := strings.Replace(wallets.GetName(), "-next", "-ledger", -1)
	env := make([]corev1.EnvVar, 0)
	env = append(env,
		apisv1beta2.Env("STORAGE_POSTGRES_CONN_STRING", "$(POSTGRES_URI)"),
		apisv1beta2.Env("LEDGER_URI", fmt.Sprintf("http://%s:8080", ledgerName)),
		apisv1beta2.Env("STACK_CLIENT_ID", wallets.Spec.Auth.ClientID),
		apisv1beta2.Env("STACK_CLIENT_SECRET", wallets.Spec.Auth.ClientSecret),
		apisv1beta2.Env("STACK_URL", wallets.Spec.StackURL),
	)

	env = append(env, wallets.Spec.DevProperties.Env()...)
	if wallets.Spec.Monitoring != nil {
		env = append(env, wallets.Spec.Monitoring.Env("")...)
	}
	return env
}

func (r *WalletsMutator) reconcileDeployment(ctx context.Context, wallets *componentsv1beta2.Wallets) (*appsv1.Deployment, controllerutil.OperationResult, error) {
	matchLabels := CreateMap("app.kubernetes.io/name", "wallets")

	return controllerutils.CreateOrUpdate(ctx, r.Client, client.ObjectKeyFromObject(wallets),
		controllerutils.WithController[*appsv1.Deployment](wallets, r.Scheme),
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
							Name:            "wallets",
							Image:           controllerutils.GetImage("wallets", wallets.Spec.Version),
							ImagePullPolicy: controllerutils.ImagePullPolicy(wallets.Spec),
							Env:             walletsEnvVars(wallets),
							Ports: []corev1.ContainerPort{{
								Name:          "wallets",
								ContainerPort: 8080,
							}},
							LivenessProbe: controllerutils.DefaultLiveness(),
						}},
					},
				},
			}
			return nil
		})
}

// SetupWithBuilder SetupWithManager sets up the controller with the Manager.
func (r *WalletsMutator) SetupWithBuilder(mgr ctrl.Manager, builder *ctrl.Builder) error {
	builder.
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.Ingress{})
	return nil
}

func NewWalletsMutator(client client.Client, scheme *runtime.Scheme) controllerutils.Mutator[*componentsv1beta2.Wallets] {
	return &WalletsMutator{
		Client: client,
		Scheme: scheme,
	}
}
