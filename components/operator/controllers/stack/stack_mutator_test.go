package stack

import (
	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	stackv1beta2 "github.com/formancehq/operator/apis/stack/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	. "github.com/formancehq/operator/pkg/testing"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("Stack controller (Auth)", func() {
	mutator := NewMutator(GetClient(), GetScheme(), []string{"*.example.com"})
	WithMutator(mutator, func() {
		var (
			configuration *stackv1beta2.Configuration
			versions      *stackv1beta2.Versions
		)
		BeforeEach(func() {
			configuration = NewDumbConfiguration()
			versions = NewDumbVersions()
		})
		When("Creating a stack with no Configuration nor Versions object", func() {
			var (
				stack stackv1beta2.Stack
			)
			BeforeEach(func() {
				name := NewStackName()

				stack = stackv1beta2.NewStack(name, stackv1beta2.StackSpec{
					Seed:     configuration.Name,
					Versions: versions.Name,
				})
				Expect(Create(&stack)).To(Succeed())
				Eventually(ConditionStatus(&stack, apisv1beta2.ConditionTypeError)).
					Should(Equal(metav1.ConditionTrue))
			})
			Context("Then save the configuration object", func() {
				BeforeEach(func() {
					Expect(Create(configuration)).To(Succeed())
					Expect(Create(versions)).To(Succeed())
				})
				It("Should resolve the error", func() {
					Eventually(ConditionStatus(&stack, apisv1beta2.ConditionTypeError)).
						Should(Equal(metav1.ConditionUnknown))
				})
			})
		})
		When("Creating a configuration", func() {
			BeforeEach(func() {
				Expect(Create(configuration)).To(Succeed())
				Expect(Create(versions)).To(Succeed())
			})
			Context("Then creating a stack", func() {
				var (
					stack stackv1beta2.Stack
				)
				BeforeEach(func() {
					name := NewStackName()
					stack = stackv1beta2.NewStack(name, stackv1beta2.StackSpec{
						Seed:     configuration.Name,
						Versions: versions.Name,
						Auth: stackv1beta2.StackAuthSpec{
							DelegatedOIDCServer: componentsv1beta2.DelegatedOIDCServerConfiguration{
								Issuer:       "http://example.net",
								ClientID:     "clientId",
								ClientSecret: "clientSecret",
							},
						},
					})

					Expect(Create(&stack)).To(Succeed())
					Eventually(ConditionStatus(&stack, apisv1beta2.ConditionTypeReady)).
						Should(Equal(metav1.ConditionTrue))
				})
				It("Should create a new namespace", func() {
					Expect(Get(types.NamespacedName{
						Name: stack.Name,
					}, &v1.Namespace{})).To(BeNil())
				})
				It("Should create all required services", func() {
					for _, s := range stackv1beta2.GetServiceList() {
						u := unstructured.Unstructured{
							Object: map[string]interface{}{
								"kind":       s,
								"apiVersion": componentsv1beta2.GroupVersion.String(),
							},
						}
						Expect(Get(types.NamespacedName{
							Namespace: stack.Name,
							Name:      stack.SubObjectName(s),
						}, &u)).To(BeNil())
						Expect(u.Object["spec"].(map[string]any)["version"]).To(Equal(
							versions.GetFromServiceName(s),
						))
						Expect(u.Object["metadata"].(map[string]any)["labels"]).To(Equal(map[string]any{
							"stack": "true",
						}))
					}
				})
				It("Should create an ingress for each services", func() {
					for _, s := range stackv1beta2.GetServiceList() {
						ingress := &networkingv1.Ingress{}
						Expect(Get(types.NamespacedName{
							Namespace: stack.Name,
							Name:      stack.SubObjectName(s),
						}, ingress)).To(BeNil())
					}
				})
				It("Should register a static auth client into stack status and use it on control", func() {
					Eventually(func() authcomponentsv1beta2.StaticClient {
						Expect(Get(types.NamespacedName{
							Namespace: stack.Namespace,
							Name:      stack.Name,
						}, &stack)).To(Succeed())
						return stack.Status.StaticAuthClients["control"]
					}).ShouldNot(BeZero())
					control := &componentsv1beta2.Control{
						ObjectMeta: metav1.ObjectMeta{
							Name:      stack.SubObjectName("control"),
							Namespace: stack.Name,
						},
					}
					Eventually(Exists(control)()).Should(BeTrue())
					Expect(control.Spec.AuthClientConfiguration).NotTo(BeNil())
					Expect(control.Spec.AuthClientConfiguration.ClientID).
						To(Equal(stack.Status.StaticAuthClients["control"].ID))
					Expect(control.Spec.AuthClientConfiguration.ClientSecret).
						To(Equal(stack.Status.StaticAuthClients["control"].Secrets[0]))
				})
			})
		})
	})
})
