package transport

import (
	"go.uber.org/fx"
)

// Module provides two named gRPC connection pools:
//   - name:"raft"    — connections to peers' Raft ports (internal transport)
//   - name:"service" — connections to peers' service ports (request forwarding)
func Module() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(NewConnectionPool, fx.ResultTags(`name:"raft"`))),
		fx.Provide(fx.Annotate(NewConnectionPool, fx.ResultTags(`name:"service"`))),
	)
}
