#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

client_module=github.com/formancehq/ledger/pkg/client/v3
proto_module=github.com/formancehq/ledger/pkg/proto/v3
public=(common signature audit bucket cluster restore)

mkdir -p build pkg/client/v3/grpc pkg/client/v3/proto pkg/proto/v3/ledgerpb pkg/proto/v3/proto

# Keep the public projection as a checked-in, language-neutral contract. The
# source schemas remain canonical under misc/proto; only their Go package path
# differs in the standalone client module.
for name in "${public[@]}"; do
	{
		printf '// Code generated from misc/proto/%s. DO NOT EDIT.\n\n' "$name.proto"
		sed -E 's|^[[:space:]]*option go_package[[:space:]]*=.*;[[:space:]]*$|option go_package = "github.com/formancehq/ledger/pkg/client/v3/grpc";|' \
			"misc/proto/${name}.proto"
	} | awk 'previous_option && $0 == "" { previous_option = 0; next } { print; previous_option = ($0 ~ /^option go_package =/) }' > "pkg/client/v3/proto/${name}.proto"
	{
		printf '// Code generated from misc/proto/%s. DO NOT EDIT.\n\n' "$name.proto"
		sed -E 's|^[[:space:]]*option go_package[[:space:]]*=.*;[[:space:]]*$|option go_package = "github.com/formancehq/ledger/pkg/proto/v3/ledgerpb";|' \
			"misc/proto/${name}.proto"
	} | awk 'previous_option && $0 == "" { previous_option = 0; next } { print; previous_option = ($0 ~ /^option go_package =/) }' > "pkg/proto/v3/proto/${name}.proto"
done

proto_only=false
if [[ "${1:-}" == "--proto-only" ]]; then
	proto_only=true
	shift
fi
if [[ $# -ne 0 ]]; then
	echo "usage: $0 [--proto-only]" >&2
	exit 2
fi
if [[ "$proto_only" == true ]]; then
	exit 0
fi

proto_files=()
shared_proto_files=()
for name in "${public[@]}"; do
	proto_files+=("pkg/client/v3/proto/${name}.proto")
	shared_proto_files+=("pkg/proto/v3/proto/${name}.proto")
done

for name in "${public[@]}"; do
	rm -f "pkg/client/v3/grpc/${name}.pb.go" "pkg/client/v3/grpc/${name}_grpc.pb.go" \
		"pkg/client/v3/grpc/${name}_vtproto.pb.go" "pkg/client/v3/grpc/${name}_dethash.pb.go" \
		"pkg/client/v3/grpc/${name}_skippable.pb.go" "pkg/client/v3/grpc/${name}_reader.pb.go" \
		"pkg/client/v3/grpc/${name}"*_gen.go
	rm -f "pkg/proto/v3/ledgerpb/${name}.pb.go" "pkg/proto/v3/ledgerpb/${name}_grpc.pb.go"
done

protoc \
	--go_out=pkg/client/v3 --go_opt=module="${client_module}" \
	--go-grpc_out=pkg/client/v3 --go-grpc_opt=module="${client_module}" \
	-I pkg/client/v3/proto \
	"${proto_files[@]}"

protoc \
	--go_out=pkg/proto/v3 --go_opt=module="${proto_module}" \
	--go-grpc_out=pkg/proto/v3 --go-grpc_opt=module="${proto_module}" \
	-I pkg/proto/v3/proto \
	"${shared_proto_files[@]}"

go -C pkg/client/v3 mod tidy
go -C pkg/proto/v3 mod tidy
python3 scripts/generate-client-protocol.py
