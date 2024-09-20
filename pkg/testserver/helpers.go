package testserver

import (
	. "github.com/formancehq/go-libs/testing/utils"
	. "github.com/onsi/ginkgo/v2"
)

func UseNewTestServer(configurationProvider func() Configuration) *Deferred[*Server] {
	d := NewDeferred[*Server]()
	BeforeEach(func() {
		d.Reset()
		d.SetValue(New(GinkgoT(), configurationProvider()))
	})
	return d
}
