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

var _ = Describe("Ledger controller", func() {
	mutator := NewLedgerMutator(GetClient(), GetScheme())
	WithMutator(mutator, func() {
		WithNewNamespace(func() {
			Context("When creating a ledger server", func() {
				var (
					ledger *componentsv1beta2.Ledger
				)
				BeforeEach(func() {
					ledger = &componentsv1beta2.Ledger{
						ObjectMeta: metav1.ObjectMeta{
							Name: "ledger",
						},
						Spec: componentsv1beta2.LedgerSpec{
							Postgres: componentsv1beta2.PostgresConfigCreateDatabase{
								PostgresConfigWithDatabase: apisv1beta2.PostgresConfigWithDatabase{
									Database:       "ledger",
									PostgresConfig: NewDumpPostgresConfig(),
								},
								CreateDatabase: true,
							},
							Collector: &componentsv1beta2.CollectorConfig{
								KafkaConfig: apisv1beta2.KafkaConfig{
									Brokers: []string{"http://kafka"},
									TLS:     false,
									SASL:    nil,
								},
								Topic: "xxx",
							},
						},
					}
					Expect(Create(ledger)).To(BeNil())
					Eventually(ConditionStatus(ledger, apisv1beta2.ConditionTypeReady)).Should(Equal(metav1.ConditionTrue))
				})
				It("Should create a deployment", func() {
					deployment := &appsv1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Name:      ledger.Name,
							Namespace: ledger.Namespace,
						},
					}
					Eventually(Exists(deployment)).Should(BeTrue())
					Expect(deployment.OwnerReferences).To(HaveLen(1))
					Expect(deployment.OwnerReferences).To(ContainElement(controllerutils.OwnerReference(ledger)))
				})
			})
		})
	})
})
