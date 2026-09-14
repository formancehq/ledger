module github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3

go 1.26.0

// The generated v3 gRPC stubs live in github.com/formancehq/ledger/v3/internal/...
// The Go internal rule admits this module only because its path is under
// github.com/formancehq/ledger/v3/ (preparation blocker B2). The relative
// replace mirrors tests/antithesis/workload/go.mod, the repository's existing
// nested-module convention, so the plugin always builds against the checked-out
// source rather than a published version.
replace github.com/formancehq/ledger/v3 => ../../../

require (
	github.com/alecthomas/participle/v2 v2.1.4
	github.com/formancehq/fctl-v2-poc/pkg/plugin v0.0.0
	github.com/formancehq/ledger/v3 v3.0.0-alpha.13.0.20260813135150-bb0297cce39c
	go.bytecodealliance.org/pkg v0.2.2
	go.yaml.in/yaml/v3 v3.0.4
	google.golang.org/protobuf v1.36.12
)

require (
	dario.cat/mergo v1.0.2 // indirect
	github.com/ThreeDotsLabs/watermill v1.5.1 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.2 // indirect
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic v1.15.3 // indirect
	github.com/bytedance/sonic/loader v0.5.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/formancehq/go-libs/v5 v5.7.2 // indirect
	github.com/go-chi/chi/v5 v5.3.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/holiman/uint256 v1.3.2 // indirect
	github.com/invopop/jsonschema v0.13.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/lithammer/shortuuid/v3 v3.0.7 // indirect
	github.com/mailru/easyjson v0.9.2 // indirect
	github.com/muhlemmer/gu v0.3.1 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/uptrace/opentelemetry-go-extra/otellogrus v0.3.2 // indirect
	github.com/uptrace/opentelemetry-go-extra/otelutil v0.3.2 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.9-0.20240816141633-0a40785b4f41 // indirect
	github.com/zitadel/oidc/v3 v3.45.3 // indirect
	github.com/zitadel/schema v1.3.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/log v0.17.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/mock v0.6.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
