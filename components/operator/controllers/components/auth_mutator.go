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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/controllerutils"
	. "github.com/formancehq/operator/pkg/typeutils"
	pkgError "github.com/pkg/errors"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	autoscallingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Mutator reconciles a Auth object
type Mutator struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=auths,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=components.formance.com,resources=auths/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=components.formance.com,resources=auths/finalizers,verbs=update

func (r *Mutator) Mutate(ctx context.Context, auth *componentsv1beta2.Auth) (*ctrl.Result, error) {

	apisv1beta2.SetProgressing(auth)

	config, _, err := r.reconcileConfigFile(ctx, auth)
	if err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling config")
	}

	_, err = r.reconcileDeployment(ctx, auth, config)
	if err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling deployment")
	}

	if _, _, err := r.reconcileHPA(ctx, auth); err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling HPA")
	}

	apisv1beta2.SetReady(auth)

	return nil, nil
}

func (r *Mutator) reconcileDeployment(ctx context.Context, auth *componentsv1beta2.Auth, config *corev1.ConfigMap) (*appsv1.Deployment, error) {
	matchLabels := CreateMap("app.kubernetes.io/name", "auth")
	port := int32(8080)

	secret, _, err := r.reconcileSigningKeySecret(ctx, auth)
	if err != nil {
		return nil, err
	}

	env := make([]corev1.EnvVar, 0)
	env = append(env, apisv1beta2.Env("CONFIG", "/config/config.yaml"))
	env = append(env, auth.Spec.Postgres.Env("")...)
	env = append(env, auth.Spec.DelegatedOIDCServer.Env()...)
	env = append(env,
		apisv1beta2.Env("BASE_URL", auth.Spec.BaseURL),
		apisv1beta2.EnvFrom("SIGNING_KEY", &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: secret.Name,
				},
				Key: "signingKey",
			},
		}),
	)
	env = append(env, auth.Spec.DevProperties.Env()...)
	if auth.Spec.Dev {
		env = append(env,
			// TODO: Make auth server respect "DEV" env variable
			apisv1beta2.Env("CAOS_OIDC_DEV", "1"),
		)
	}
	if auth.Spec.Monitoring != nil {
		env = append(env, auth.Spec.Monitoring.Env("")...)
	}

	ret, _, err := controllerutils.CreateOrUpdate(ctx, r.Client, client.ObjectKeyFromObject(auth),
		controllerutils.WithController[*appsv1.Deployment](auth, r.Scheme),
		controllerutils.WithReloaderAnnotations[*appsv1.Deployment](),
		func(deployment *appsv1.Deployment) error {
			deployment.Spec = appsv1.DeploymentSpec{
				Replicas: auth.Spec.GetReplicas(),
				Selector: &metav1.LabelSelector{
					MatchLabels: matchLabels,
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: matchLabels,
					},
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: config.Name,
									},
								},
							},
						}},
						Containers: []corev1.Container{{
							Name:            "auth",
							Image:           controllerutils.GetImage("auth", auth.Spec.Version),
							Command:         []string{"/main", "serve"},
							Ports:           controllerutils.SinglePort("http", port),
							Env:             env,
							LivenessProbe:   controllerutils.DefaultLiveness(),
							ImagePullPolicy: controllerutils.ImagePullPolicy(auth.Spec),
							VolumeMounts: []corev1.VolumeMount{{
								Name:      "config",
								ReadOnly:  true,
								MountPath: "/config",
							}},
						}},
					},
				},
			}
			if auth.Spec.Postgres.CreateDatabase {
				deployment.Spec.Template.Spec.InitContainers = []corev1.Container{
					auth.Spec.Postgres.CreateDatabaseInitContainer(),
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

	auth.Status.Selector = selector.String()
	auth.Status.Replicas = *auth.Spec.GetReplicas()

	return ret, err
}

func (r *Mutator) reconcileConfigFile(ctx context.Context, auth *componentsv1beta2.Auth) (*corev1.ConfigMap, controllerutil.OperationResult, error) {
	return controllerutils.CreateOrUpdate(ctx, r.Client, client.ObjectKeyFromObject(auth),
		controllerutils.WithController[*corev1.ConfigMap](auth, r.Scheme),
		func(configMap *corev1.ConfigMap) error {
			yaml, err := yaml.Marshal(struct {
				Clients []authcomponentsv1beta2.StaticClient `yaml:"clients"`
			}{
				Clients: auth.Spec.StaticClients,
			})
			if err != nil {
				panic(err)
			}
			configMap.Data = map[string]string{
				"config.yaml": string(yaml),
			}
			return nil
		})
}

func (r *Mutator) reconcileSigningKeySecret(ctx context.Context, auth *componentsv1beta2.Auth) (*corev1.Secret, controllerutil.OperationResult, error) {
	return controllerutils.CreateOrUpdate(ctx, r.Client, types.NamespacedName{
		Namespace: auth.Namespace,
		Name:      fmt.Sprintf("%s-signing-key", auth.Name),
	}, controllerutils.WithController[*corev1.Secret](auth, r.Scheme),
		func(t *corev1.Secret) error {
			signingKey := auth.Spec.SigningKey
			if signingKey == "" {
				if _, ok := t.Data["signingKey"]; ok {
					return nil
				}
				privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
				if err != nil {
					return err
				}
				var privateKeyBytes = x509.MarshalPKCS1PrivateKey(privateKey)
				privateKeyBlock := &pem.Block{
					Type:  "RSA PRIVATE KEY",
					Bytes: privateKeyBytes,
				}
				buf := bytes.NewBufferString("")
				err = pem.Encode(buf, privateKeyBlock)
				if err != nil {
					return err
				}
				signingKey = buf.String()
			}
			t.StringData = map[string]string{
				"signingKey": signingKey,
			}
			return nil
		})
}

func (r *Mutator) reconcileHPA(ctx context.Context, auth *componentsv1beta2.Auth) (*autoscallingv2.HorizontalPodAutoscaler, controllerutil.OperationResult, error) {
	return controllerutils.CreateOrUpdate(ctx, r.Client, client.ObjectKeyFromObject(auth),
		controllerutils.WithController[*autoscallingv2.HorizontalPodAutoscaler](auth, r.Scheme),
		func(hpa *autoscallingv2.HorizontalPodAutoscaler) error {
			hpa.Spec = auth.Spec.GetHPASpec(auth)
			return nil
		})
}

// SetupWithBuilder sets up the controller with the Manager.
func (r *Mutator) SetupWithBuilder(mgr ctrl.Manager, builder *ctrl.Builder) error {
	builder.
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ConfigMap{})
	return nil
}

func NewMutator(client client.Client, scheme *runtime.Scheme) controllerutils.Mutator[*componentsv1beta2.Auth] {
	return &Mutator{
		Client: client,
		Scheme: scheme,
	}
}
