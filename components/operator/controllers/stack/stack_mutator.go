package stack

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/controllerutils"
	"github.com/formancehq/operator/pkg/typeutils"
	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	"github.com/imdario/mergo"
	traefik "github.com/traefik/traefik/v2/pkg/provider/kubernetes/crd/traefik/v1alpha1"
	networkingv1 "k8s.io/api/networking/v1"
	apiextensionv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"

	stackv1beta2 "github.com/formancehq/operator/apis/stack/v1beta2"
	pkgError "github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type serviceErrors struct {
	errors map[string]error
	mu     sync.Mutex
}

func (m *serviceErrors) Error() string {
	ret := ""
	for service, err := range m.errors {
		ret = fmt.Sprintf("%s: %s\r\n", service, err)
	}
	return ret
}

func (m *serviceErrors) setError(service string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.errors == nil {
		m.errors = map[string]error{}
	}
	m.errors[service] = err
}

// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=traefik.containo.us,resources=middlewares,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=stack.formance.com,resources=stacks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=stack.formance.com,resources=stacks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=stack.formance.com,resources=stacks/finalizers,verbs=update
// +kubebuilder:rbac:groups=stack.formance.com,resources=configurations,verbs=get;list;watch
// +kubebuilder:rbac:groups=stack.formance.com,resources=versions,verbs=get;list;watch

type Mutator struct {
	client   client.Client
	scheme   *runtime.Scheme
	dnsNames []string
}

func watch(mgr ctrl.Manager, field string) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		stacks := &stackv1beta2.StackList{}
		listOps := &client.ListOptions{
			FieldSelector: fields.OneTermEqualSelector(field, object.GetName()),
			Namespace:     object.GetNamespace(),
		}
		err := mgr.GetClient().List(context.TODO(), stacks, listOps)
		if err != nil {
			return []reconcile.Request{}
		}

		return typeutils.Map(stacks.Items, func(s stackv1beta2.Stack) reconcile.Request {
			return reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      s.GetName(),
					Namespace: s.GetNamespace(),
				},
			}
		})
	})
}

func (r *Mutator) SetupWithBuilder(mgr ctrl.Manager, bldr *ctrl.Builder) error {

	if err := mgr.GetFieldIndexer().
		IndexField(context.Background(), &stackv1beta2.Stack{}, ".spec.seed", func(rawObj client.Object) []string {
			return []string{rawObj.(*stackv1beta2.Stack).Spec.Seed}
		}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().
		IndexField(context.Background(), &stackv1beta2.Stack{}, ".spec.versions", func(rawObj client.Object) []string {
			return []string{rawObj.(*stackv1beta2.Stack).Spec.Versions}
		}); err != nil {
		return err
	}

	bldr.
		Owns(&componentsv1beta2.Auth{}).
		Owns(&componentsv1beta2.Ledger{}).
		Owns(&componentsv1beta2.Search{}).
		Owns(&componentsv1beta2.Payments{}).
		Owns(&componentsv1beta2.Webhooks{}).
		Owns(&componentsv1beta2.Wallets{}).
		Owns(&componentsv1beta2.Orchestration{}).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.Service{}).
		Owns(&traefik.Middleware{}).
		Owns(&networkingv1.Ingress{}).
		Watches(
			&source.Kind{Type: &stackv1beta2.Configuration{}},
			watch(mgr, ".spec.seed"),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Watches(
			&source.Kind{Type: &stackv1beta2.Versions{}},
			watch(mgr, ".spec.versions"),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		)

	return nil
}

func (r *Mutator) Mutate(ctx context.Context, stack *stackv1beta2.Stack) (*ctrl.Result, error) {
	apisv1beta2.SetProgressing(stack)

	configuration := &stackv1beta2.Configuration{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name: stack.Spec.Seed,
	}, configuration); err != nil {
		if errors.IsNotFound(err) {
			return nil, pkgError.New("Configuration object not found")
		}
		return controllerutils.Requeue(), fmt.Errorf("error retrieving Configuration object: %s", err)
	}

	configurationSpec := configuration.Spec
	// TODO: Reuse standard validation
	if err := configurationSpec.Validate(); len(err) > 0 {
		return nil, pkgError.Wrap(err.ToAggregate(), "Validating configuration")
	}

	version := &stackv1beta2.Versions{}
	if err := r.client.Get(ctx, types.NamespacedName{
		Name: stack.Spec.Versions,
	}, version); err != nil {
		if errors.IsNotFound(err) {
			return nil, pkgError.New("Versions object not found")
		}
		return controllerutils.Requeue(), fmt.Errorf("error retrieving Versions object: %s", err)
	}

	if _, _, err := r.reconcileNamespace(ctx, stack); err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling namespace")
	}

	if _, _, err := r.reconcileMiddleware(ctx, stack); err != nil {
		return controllerutils.Requeue(), pkgError.Wrap(err, "Reconciling middleware")
	}

	me := &serviceErrors{}
	for serviceName, serviceConfiguration := range configuration.Spec.GetServices() {
		if err := r.createComponent(ctx, stack, configuration, version, serviceName, serviceConfiguration); err != nil {
			me.setError(serviceName, err)
		}
	}
	if len(me.errors) > 0 {
		return controllerutils.Requeue(), me
	}

	apisv1beta2.SetReady(stack)
	return nil, nil
}

func (r *Mutator) createComponent(ctx context.Context, stack *stackv1beta2.Stack, configuration *stackv1beta2.Configuration, version *stackv1beta2.Versions, name string, serviceConfiguration stackv1beta2.ServiceConfiguration) error {
	if clientConfiguration := serviceConfiguration.AuthClientConfiguration(stack); clientConfiguration != nil {
		if stack.Status.StaticAuthClients == nil {
			stack.Status.StaticAuthClients = map[string]authcomponentsv1beta2.StaticClient{}
		}
		if _, ok := stack.Status.StaticAuthClients[name]; !ok {
			stack.Status.StaticAuthClients[name] = authcomponentsv1beta2.StaticClient{
				ID:                  name,
				Secrets:             []string{uuid.NewString()},
				ClientConfiguration: *clientConfiguration,
			}
		}
	}

	if _, _, err := r.createService(ctx, stack, name, serviceConfiguration); err != nil {
		return err
	}

	if _, _, err := r.createIngress(ctx, stack, configuration, name, serviceConfiguration); err != nil {
		return err
	}

	if _, err := r.createComponentObject(ctx, stack, configuration, version, name, serviceConfiguration); err != nil {
		return err
	}

	return nil
}

func (r *Mutator) createComponentObject(ctx context.Context, stack *stackv1beta2.Stack, configuration *stackv1beta2.Configuration,
	version *stackv1beta2.Versions, serviceName string, serviceConfiguration stackv1beta2.ServiceConfiguration) (controllerutil.OperationResult, error) {
	ret := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       strcase.ToCamel(serviceName),
			"apiVersion": componentsv1beta2.GroupVersion.String(),
			"metadata": map[string]any{
				"name":      stack.SubObjectName(serviceName),
				"namespace": stack.Name,
			},
		},
	}
	return controllerutil.CreateOrUpdate(ctx, r.client, ret, func() error {
		updatedPartialSpec := serviceConfiguration.Spec(stack, configuration.Spec)

		updatedPartialSpecAsMap := anyToMap(updatedPartialSpec)

		updatedPartialSpecAsMap["version"] = version.GetFromServiceName(serviceName)
		updatedPartialSpecAsMap["debug"] = stack.Spec.Debug
		updatedPartialSpecAsMap["dev"] = stack.Spec.Dev
		updatedPartialSpecAsMap["monitoring"] = anyToMap(configuration.Spec.Monitoring)

		if clientConfiguration := serviceConfiguration.AuthClientConfiguration(stack); clientConfiguration != nil {
			updatedPartialSpecAsMap["auth"] = map[string]any{
				"clientId":     stack.Status.StaticAuthClients[serviceName].ID,
				"clientSecret": stack.Status.StaticAuthClients[serviceName].Secrets[0],
			}
		}

		actualSpec, ok := ret.Object["spec"].(map[string]any)
		if !ok || actualSpec == nil {
			actualSpec = map[string]any{}
		}
		if err := mergo.Merge(&actualSpec, updatedPartialSpecAsMap, mergo.WithOverride); err != nil {
			return pkgError.Wrap(err, "merging spec")
		}

		actualMetadata, ok := ret.Object["metadata"].(map[string]any)
		if !ok || actualMetadata == nil {
			actualMetadata = map[string]any{}
			ret.Object["metadata"] = actualMetadata
		}
		actualLabels, ok := actualMetadata["labels"].(map[string]string)
		if !ok || actualMetadata == nil {
			actualLabels = map[string]string{}
			actualMetadata["labels"] = actualLabels
		}
		actualLabels["stack"] = "true"

		ret.Object["spec"] = actualSpec
		return controllerutil.SetControllerReference(stack, ret, r.scheme)
	})
}

func (r *Mutator) createService(ctx context.Context, stack *stackv1beta2.Stack, name string, serviceConfiguration stackv1beta2.ServiceConfiguration) (*corev1.Service, controllerutil.OperationResult, error) {
	port := serviceConfiguration.HTTPPort()
	return controllerutils.CreateOrUpdate(ctx, r.client, client.ObjectKey{
		Namespace: stack.Name,
		Name:      fmt.Sprintf("%s-%s", stack.Name, name),
	},
		controllerutils.WithController[*corev1.Service](stack, r.scheme),
		func(service *corev1.Service) error {
			service.Spec = corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{
					Name:        "http",
					Port:        int32(port),
					Protocol:    "TCP",
					AppProtocol: pointer.String("http"),
					TargetPort:  intstr.FromInt(port),
				}},
				Selector: typeutils.CreateMap("app.kubernetes.io/name", name),
			}
			return nil
		})
}

func (r *Mutator) createIngress(ctx context.Context, stack *stackv1beta2.Stack, configuration *stackv1beta2.Configuration, name string, serviceConfiguration stackv1beta2.ServiceConfiguration) (*networkingv1.Ingress, controllerutil.OperationResult, error) {
	annotations := configuration.Spec.Ingress.Annotations
	if annotations == nil {
		annotations = map[string]string{}
	} else {
		annotations = typeutils.CopyMap(annotations)
	}
	if serviceConfiguration.NeedAuthMiddleware() {
		middlewareAuth := fmt.Sprintf("%s-auth-middleware@kubernetescrd", stack.Name)
		annotations["traefik.ingress.kubernetes.io/router.middlewares"] = fmt.Sprintf("%s, %s", middlewareAuth, annotations["traefik.ingress.kubernetes.io/router.middlewares"])
	}

	return controllerutils.CreateOrUpdate(ctx, r.client, client.ObjectKey{
		Namespace: stack.Name,
		Name:      stack.SubObjectName(name),
	},
		controllerutils.WithController[*networkingv1.Ingress](stack, r.scheme),
		func(ingress *networkingv1.Ingress) error {
			pathType := networkingv1.PathTypePrefix
			ingress.ObjectMeta.Annotations = annotations
			ingress.Spec = networkingv1.IngressSpec{
				TLS: func() []networkingv1.IngressTLS {
					if configuration.Spec.Ingress.TLS == nil {
						return nil
					}
					return []networkingv1.IngressTLS{{
						SecretName: configuration.Spec.Ingress.TLS.SecretName,
					}}
				}(),
				Rules: []networkingv1.IngressRule{
					{
						Host: stack.Spec.Host,
						IngressRuleValue: networkingv1.IngressRuleValue{
							HTTP: &networkingv1.HTTPIngressRuleValue{
								Paths: []networkingv1.HTTPIngressPath{
									{
										Path: func() string {
											if v, ok := serviceConfiguration.(stackv1beta2.CustomPathServiceConfiguration); ok {
												return v.Path()
											}
											return fmt.Sprintf("/api/%s", name)
										}(),
										PathType: &pathType,
										Backend: networkingv1.IngressBackend{
											Service: &networkingv1.IngressServiceBackend{
												Name: fmt.Sprintf("%s-%s", stack.Name, name),
												Port: networkingv1.ServiceBackendPort{
													Name: "http",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}
			return nil
		})
}

func (r *Mutator) reconcileNamespace(ctx context.Context, stack *stackv1beta2.Stack) (*corev1.Namespace, controllerutil.OperationResult, error) {
	return controllerutils.CreateOrUpdate(ctx, r.client, types.NamespacedName{
		Name: stack.Name,
	}, controllerutils.WithController[*corev1.Namespace](stack, r.scheme), func(ns *corev1.Namespace) error {
		// No additional mutate needed
		return nil
	})
}

func (r *Mutator) reconcileMiddleware(ctx context.Context, stack *stackv1beta2.Stack) (*traefik.Middleware, controllerutil.OperationResult, error) {
	return controllerutils.CreateOrUpdate(ctx, r.client, types.NamespacedName{
		Namespace: stack.Name,
		Name:      "auth-middleware",
	}, controllerutils.WithController[*traefik.Middleware](stack, r.scheme), func(middleware *traefik.Middleware) error {
		middleware.Spec = traefik.MiddlewareSpec{
			Plugin: map[string]apiextensionv1.JSON{
				"auth": {
					Raw: []byte(fmt.Sprintf(`{"Issuer": "%s", "RefreshTime": "%s", "ExcludePaths": ["/_health", "/_healthcheck", "/.well-known/openid-configuration"]}`, stack.Spec.Scheme+"://"+stack.Spec.Host+"/api/auth", "10s")),
				},
			},
		}
		return nil
	})
}

var _ controllerutils.Mutator[*stackv1beta2.Stack] = &Mutator{}

func NewMutator(
	client client.Client,
	scheme *runtime.Scheme,
	dnsNames []string,
) controllerutils.Mutator[*stackv1beta2.Stack] {
	return &Mutator{
		client:   client,
		scheme:   scheme,
		dnsNames: dnsNames,
	}
}

func anyToMap(value any) map[string]any {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	ret := make(map[string]any)
	if err := json.Unmarshal(data, &ret); err != nil {
		panic(err)
	}
	return ret
}
