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

var _ = Describe("Wallets controller", func() {
	mutator := NewWalletsMutator(GetClient(), GetScheme())
	WithMutator(mutator, func() {
		WithNewNamespace(func() {
			Context("When creating a wallets server", func() {
				var (
					wallets *componentsv1beta2.Wallets
				)
				BeforeEach(func() {
					wallets = &componentsv1beta2.Wallets{
						ObjectMeta: metav1.ObjectMeta{
							Name: "wallets",
						},
						Spec: componentsv1beta2.WalletsSpec{},
					}
					Expect(Create(wallets)).To(BeNil())
					Eventually(ConditionStatus(wallets, apisv1beta2.ConditionTypeReady)).Should(Equal(metav1.ConditionTrue))
				})
				It("Should create a deployment", func() {
					deployment := &appsv1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Name:      wallets.Name,
							Namespace: wallets.Namespace,
						},
					}
					Eventually(Exists(deployment)).Should(BeTrue())
					Expect(deployment.OwnerReferences).To(HaveLen(1))
					Expect(deployment.OwnerReferences).To(ContainElement(controllerutils.OwnerReference(wallets)))
				})
			})
		})
	})
})
