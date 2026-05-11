//go:build it

package test_suite

import (
	"encoding/json"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/formancehq/go-libs/v5/pkg/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v5/pkg/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Ledger experimental features on /_info", func() {
	Context("with experimental features enabled", func() {
		testServer := ginkgo.DeferTestServer(
			DeferMap(UseTemplatedDatabase(), (*pgtesting.Database).ConnectionOptions),
			testservice.WithInstruments(
				testservice.DebugInstrumentation(debug),
				testservice.OutputInstrumentation(GinkgoWriter),
				ExperimentalFeaturesInstrumentation(),
				ExperimentalExportersInstrumentation(),
			),
			testservice.WithLogger(GinkgoT()),
		)

		It("should expose experimental features in /_info", func(specContext SpecContext) {
			srv, err := testServer.Wait(specContext)
			Expect(err).To(BeNil())

			resp, err := http.DefaultClient.Get(testservice.GetServerURL(srv).String() + "/v2/_info")
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var body map[string]any
			Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())

			data, ok := body["data"].(map[string]any)
			Expect(ok).To(BeTrue())

			features, ok := data["experimentalFeatures"].([]any)
			Expect(ok).To(BeTrue())

			featureStrings := make([]string, len(features))
			for i, f := range features {
				featureStrings[i] = f.(string)
			}

			Expect(featureStrings).To(ContainElement("experimental-features"))
			Expect(featureStrings).To(ContainElement("experimental-exporters"))
		})
	})

	Context("without experimental features", func() {
		testServer := ginkgo.DeferTestServer(
			DeferMap(UseTemplatedDatabase(), (*pgtesting.Database).ConnectionOptions),
			testservice.WithInstruments(
				testservice.DebugInstrumentation(debug),
				testservice.OutputInstrumentation(GinkgoWriter),
			),
			testservice.WithLogger(GinkgoT()),
		)

		It("should return empty experimental features in /_info", func(specContext SpecContext) {
			srv, err := testServer.Wait(specContext)
			Expect(err).To(BeNil())

			resp, err := http.DefaultClient.Get(testservice.GetServerURL(srv).String() + "/v2/_info")
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var body map[string]any
			Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())

			data, ok := body["data"].(map[string]any)
			Expect(ok).To(BeTrue())

			_, hasField := data["experimentalFeatures"]
			Expect(hasField).To(BeTrue())
		})
	})
})
