package components

import (
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/controllerutils"
	. "github.com/formancehq/operator/pkg/testing"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Webhooks controller", func() {
	mutator := NewWebhooksMutator(GetClient(), GetScheme())
	WithMutator(mutator, func() {
		WithNewNamespace(func() {
			Context("When creating a webhooks server", func() {
				var (
					webhooks *componentsv1beta2.Webhooks
				)
				BeforeEach(func() {
					webhooks = &componentsv1beta2.Webhooks{
						ObjectMeta: metav1.ObjectMeta{
							Name: "webhooks",
						},
						Spec: componentsv1beta2.WebhooksSpec{
							Collector: &componentsv1beta2.CollectorConfig{
								KafkaConfig: NewDumpKafkaConfig(),
								Topic:       "xxx",
							},
							Postgres: componentsv1beta2.PostgresConfigCreateDatabase{
								PostgresConfigWithDatabase: apisv1beta2.PostgresConfigWithDatabase{
									Database:       "webhooks",
									PostgresConfig: NewDumpPostgresConfig(),
								},
								CreateDatabase: false,
							},
						},
					}
					Expect(Create(webhooks)).To(BeNil())
					Eventually(ConditionStatus(webhooks, apisv1beta2.ConditionTypeReady)).Should(Equal(metav1.ConditionTrue))
				})
				It("Should create a deployment", func() {
					deployment := &appsv1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Name:      webhooks.Name,
							Namespace: webhooks.Namespace,
						},
					}
					Eventually(Exists(deployment)).Should(BeTrue())
					Expect(deployment.OwnerReferences).To(HaveLen(1))
					Expect(deployment.OwnerReferences).To(ContainElement(controllerutils.OwnerReference(webhooks)))
				})
			})
		})
	})
})
