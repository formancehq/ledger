#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

client_module=github.com/formancehq/ledger/pkg/client/v3
public=(common signature audit bucket cluster restore)

mkdir -p build pkg/client/v3/grpc
for tool in dethash skippable; do
	go -C "tools/protoc-gen-${tool}" build -o "../../build/protoc-gen-${tool}" .
done

for name in "${public[@]}"; do
	rm -f "pkg/client/v3/grpc/${name}.pb.go" "pkg/client/v3/grpc/${name}_grpc.pb.go" \
		"pkg/client/v3/grpc/${name}_vtproto.pb.go" "pkg/client/v3/grpc/${name}_dethash.pb.go" \
		"pkg/client/v3/grpc/${name}_skippable.pb.go" "pkg/client/v3/grpc/${name}"*_gen.go
done

protoc \
	--go_out=pkg/client/v3 --go_opt=module="${client_module}" \
	--go-grpc_out=pkg/client/v3 --go-grpc_opt=module="${client_module}" \
	--go-vtproto_out=pkg/client/v3 --go-vtproto_opt=module="${client_module}" \
	--go-vtproto_opt=features=marshal+unmarshal+size+clone+equal+pool \
	--go-vtproto_opt=pool="${client_module}/grpc.AuditEntry" \
	--plugin=protoc-gen-dethash=build/protoc-gen-dethash \
	--dethash_out=pkg/client/v3 --dethash_opt=module="${client_module}" \
	--plugin=protoc-gen-skippable=build/protoc-gen-skippable \
	--skippable_out=pkg/client/v3 --skippable_opt=module="${client_module}" \
	-I pkg/client/v3/proto \
	"${public[@]/%/.proto}"

go -C pkg/client/v3 mod tidy
