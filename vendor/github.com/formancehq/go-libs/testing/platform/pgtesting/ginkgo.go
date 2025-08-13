package pgtesting

import (
	. "github.com/formancehq/go-libs/testing/docker/ginkgo"
	. "github.com/formancehq/go-libs/testing/utils"
	. "github.com/onsi/ginkgo/v2"
)

func WithNewPostgresServer(fn func(p *Deferred[*PostgresServer])) bool {
	return Context("With new postgres server", func() {
		ret := NewDeferred[*PostgresServer]()
		BeforeEach(func() {
			ret.Reset()
			ret.SetValue(CreatePostgresServer(
				GinkgoT(),
				ActualDockerPool(),
			))
		})
		fn(ret)
	})
}

func UsePostgresDatabase(server *Deferred[*PostgresServer]) *Deferred[*Database] {
	ret := &Deferred[*Database]{}
	BeforeEach(func() {
		ret.Reset()
		ret.SetValue(server.GetValue().NewDatabase(GinkgoT()))
	})
	return ret
}

func WithNewPostgresDatabase(server *Deferred[*PostgresServer], fn func(p *Deferred[*Database])) {
	Context("With new postgres database", func() {
		fn(UsePostgresDatabase(server))
	})
}
