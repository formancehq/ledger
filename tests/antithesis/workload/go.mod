module github.com/formancehq/ledger-v3-poc/tests/antithesis/workload

go 1.26.0

// replace github.com/formancehq/ledger-v3-poc/pkg/client => ../../pkg/client
replace github.com/formancehq/ledger-v3-poc => ../../../

// replace github.com/formancehq/ledger-v3-poc/tests/antithesis/workload => .

require (
	github.com/alitto/pond v1.9.2
	github.com/antithesishq/antithesis-sdk-go v0.6.0
)

require (
	github.com/formancehq/ledger-v3-poc v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.79.3
)

require (
	dario.cat/mergo v1.0.2 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.2 // indirect
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic v1.15.0 // indirect
	github.com/bytedance/sonic/loader v0.5.0 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/formancehq/go-libs/v5 v5.0.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	github.com/holiman/uint256 v1.3.2 // indirect
	github.com/invopop/jsonschema v0.13.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/mailru/easyjson v0.9.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.9-0.20240816141633-0a40785b4f41 // indirect
	go.etcd.io/etcd/api/v3 v3.6.6 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.6.6 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260209200024-4cfbd4190f57 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260209200024-4cfbd4190f57 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	go.etcd.io/etcd/client/v3 v3.6.6
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
)
