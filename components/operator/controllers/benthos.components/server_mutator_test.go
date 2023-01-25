package benthos_components

import (
	benthoscomponentsv1beta2 "github.com/formancehq/operator/apis/benthos.components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/controllerutils"
	. "github.com/formancehq/operator/pkg/testing"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Server controller", func() {
	mutator := NewServerMutator(GetClient(), GetScheme())
	WithMutator(mutator, func() {
		WithNewNamespace(func() {
			Context("When creating a Benthos server", func() {
				var (
					server *benthoscomponentsv1beta2.Server
				)
				BeforeEach(func() {
					server = &benthoscomponentsv1beta2.Server{
						ObjectMeta: metav1.ObjectMeta{
							Name: "server",
						},
						Spec: benthoscomponentsv1beta2.ServerSpec{
							ResourcesConfigMap: "resources",
							TemplatesConfigMap: "templates",
						},
					}
					Expect(Create(server)).To(BeNil())
					Eventually(ConditionStatus(server, apisv1beta2.ConditionTypeReady)).Should(Equal(metav1.ConditionTrue))
				})
				It("Should create a deployment", func() {
					deployment := &appsv1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Name:      server.Name,
							Namespace: server.Namespace,
						},
					}
					Eventually(Exists(deployment)).Should(BeTrue())
					Expect(deployment.OwnerReferences).To(HaveLen(1))
					Expect(deployment.OwnerReferences).To(ContainElement(controllerutils.OwnerReference(server)))
				})
				It("Should create a service", func() {
					service := &corev1.Service{
						ObjectMeta: metav1.ObjectMeta{
							Name:      server.Name,
							Namespace: server.Namespace,
						},
					}

					Eventually(Exists(service)).Should(BeTrue())
					Expect(service.OwnerReferences).To(HaveLen(1))
					Expect(service.OwnerReferences).To(ContainElement(controllerutils.OwnerReference(server)))
				})
			})
		})
	})
})
