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

var _ = Describe("Counterparties controller", func() {
	mutator := NewCounterpartiesMutator(GetClient(), GetScheme())
	WithMutator(mutator, func() {
		WithNewNamespace(func() {
			Context("When creating a counterparties server", func() {
				var (
					counterparties *componentsv1beta2.Counterparties
				)
				BeforeEach(func() {
					counterparties = &componentsv1beta2.Counterparties{
						ObjectMeta: metav1.ObjectMeta{
							Name: "counterparties",
						},
						Spec: componentsv1beta2.CounterpartiesSpec{
							Enabled: true,
							Postgres: componentsv1beta2.PostgresConfigCreateDatabase{
								PostgresConfigWithDatabase: apisv1beta2.PostgresConfigWithDatabase{
									Database:       "counterparties",
									PostgresConfig: NewDumpPostgresConfig(),
								},
								CreateDatabase: false,
							},
						},
					}
					Expect(Create(counterparties)).To(BeNil())
					Eventually(ConditionStatus(counterparties, apisv1beta2.ConditionTypeReady)).Should(Equal(metav1.ConditionTrue))
				})
				It("Should create a deployment", func() {
					deployment := &appsv1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Name:      counterparties.Name,
							Namespace: counterparties.Namespace,
						},
					}
					Eventually(Exists(deployment)).Should(BeTrue())
					Expect(deployment.OwnerReferences).To(HaveLen(1))
					Expect(deployment.OwnerReferences).To(ContainElement(controllerutils.OwnerReference(counterparties)))
				})
			})
		})
	})
})
