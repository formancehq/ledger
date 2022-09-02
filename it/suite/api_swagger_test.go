package suite

import (
	"encoding/json"
	"net/http"

	. "github.com/numary/ledger/it/internal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gopkg.in/yaml.v3"
)

var _ = DescribeServerExecute("Swagger", func() {
	When("reading swagger as json", func() {
		var (
			httpResponse *http.Response
			err          error
		)
		BeforeEach(func() {
			httpResponse, err = GetSwaggerAsJSON()
			Expect(err).To(BeNil())
		})
		It("should respond with the swagger as json", func() {
			Expect(json.NewDecoder(httpResponse.Body).Decode(&map[string]any{})).To(BeNil())
		})
	})
	When("reading swagger as yaml", func() {
		var (
			httpResponse *http.Response
			err          error
		)
		BeforeEach(func() {
			httpResponse, err = GetSwaggerAsYAML()
			Expect(err).To(BeNil())
		})
		It("should respond with the swagger as yaml", func() {
			Expect(yaml.NewDecoder(httpResponse.Body).Decode(&map[string]any{})).To(BeNil())
		})
	})
})
