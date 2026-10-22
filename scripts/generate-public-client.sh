#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

client_module=github.com/formancehq/ledger/pkg/client/v3
public=(common signature audit bucket cluster restore)

mkdir -p build pkg/client/v3/grpc
python3 scripts/generate-public-protos.py

for name in "${public[@]}"; do
	rm -f "pkg/client/v3/grpc/${name}.pb.go" "pkg/client/v3/grpc/${name}_grpc.pb.go" \
		"pkg/client/v3/grpc/${name}_vtproto.pb.go" "pkg/client/v3/grpc/${name}_dethash.pb.go" \
		"pkg/client/v3/grpc/${name}_skippable.pb.go" "pkg/client/v3/grpc/${name}_reader.pb.go" \
		"pkg/client/v3/grpc/${name}"*_gen.go
done

protoc \
	--go_out=pkg/client/v3 --go_opt=module="${client_module}" \
	--go-grpc_out=pkg/client/v3 --go-grpc_opt=module="${client_module}" \
	-I pkg/client/v3/proto \
	"${public[@]/%/.proto}"

# Keep the descriptor artifact in lockstep with the generated Go contract. It
# is the language-neutral input for consumers such as TypeScript generators.
protoc \
	--descriptor_set_out=pkg/client/v3/proto/ledger-public.protoset \
	--include_imports \
	-I pkg/client/v3/proto \
	"${public[@]/%/.proto}"

go -C pkg/client/v3 mod tidy
python3 scripts/generate-client-protocol.py
