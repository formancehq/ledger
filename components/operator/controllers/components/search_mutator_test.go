package components

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"

	benthosv1beta2 "github.com/formancehq/operator/apis/benthos.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/controllerutils"
	. "github.com/formancehq/operator/pkg/testing"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Search controller", func() {

	var (
		addr         *url.URL
		fakeEsServer *httptest.Server
	)
	BeforeEach(func() {
		var err error
		fakeEsServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		addr, err = url.Parse(fakeEsServer.URL)
		Expect(err).To(BeNil())
	})
	AfterEach(func() {
		fakeEsServer.Close()
	})
	WithMutator(NewSearchMutator(GetClient(), GetScheme()), func() {
		WithNewNamespace(func() {
			Context("When creating a search server", func() {
				var (
					search *componentsv1beta2.Search
				)
				BeforeEach(func() {
					search = &componentsv1beta2.Search{
						ObjectMeta: metav1.ObjectMeta{
							Name: "search",
						},
						Spec: componentsv1beta2.SearchSpec{
							ElasticSearch: componentsv1beta2.ElasticSearchConfig{
								Host:   addr.Hostname(),
								Scheme: addr.Scheme,
								Port: func() uint16 {
									port, err := strconv.ParseUint(addr.Port(), 10, 16)
									if err != nil {
										panic(err)
									}
									return uint16(port)
								}(),
							},
							KafkaConfig: NewDumpKafkaConfig(),
							Index:       "documents",
							PostgresConfigs: componentsv1beta2.SearchPostgresConfigs{
								Ledger: apisv1beta2.PostgresConfigWithDatabase{
									PostgresConfig: NewDumpPostgresConfig(),
									Database:       "foo",
								},
							},
						},
					}
					Expect(Create(search)).To(BeNil())
					Eventually(ConditionStatus(search, apisv1beta2.ConditionTypeReady)).Should(Equal(metav1.ConditionTrue))
				})
				It("Should create a deployment", func() {
					deployment := &appsv1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Name:      search.Name,
							Namespace: search.Namespace,
						},
					}
					Eventually(Exists(deployment)).Should(BeTrue())
					Expect(deployment.OwnerReferences).To(HaveLen(1))
					Expect(deployment.OwnerReferences).To(ContainElement(controllerutils.OwnerReference(search)))
				})
				It("Should create a benthos server", func() {
					Eventually(ConditionStatus(search, componentsv1beta2.ConditionTypeBenthosReady)).Should(Equal(metav1.ConditionTrue))
					benthosServer := &benthosv1beta2.Server{
						ObjectMeta: metav1.ObjectMeta{
							Name:      search.Name + "-benthos",
							Namespace: search.Namespace,
						},
					}
					Expect(Exists(benthosServer)()).To(BeTrue())
					Expect(benthosServer.OwnerReferences).To(HaveLen(1))
					Expect(benthosServer.OwnerReferences).To(ContainElement(controllerutils.OwnerReference(search)))
					Expect(benthosServer.Spec.TemplatesConfigMap).To(Equal("benthos-templates-config"))
					Expect(benthosServer.Spec.ResourcesConfigMap).To(Equal("benthos-resources-config"))
					Expect(benthosServer.Spec.StreamsConfigMap).To(Equal("benthos-streams-config"))
				})
			})
		})
	})
})
