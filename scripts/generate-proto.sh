#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

root_module=github.com/formancehq/ledger/v3
client_module=github.com/formancehq/ledger/pkg/client/v3
public=(common signature audit bucket cluster restore)
internal=(raft_transport cluster_bootstrap raft_cmd snapshot events proposal internal_common)

mkdir -p build pkg/client/v3/grpc pkg/client/v3/proto
for tool in dethash reader skippable queryfilter-validity ledger-log-category rpcauth; do
    go -C "tools/protoc-gen-${tool}" build -o "../../build/protoc-gen-${tool}" .
done

for name in "${public[@]}"; do
    rm -f "pkg/client/v3/grpc/${name}"*.pb.go "pkg/client/v3/grpc/${name}"*_gen.go
    cp "misc/proto/${name}.proto" "pkg/client/v3/proto/${name}.proto"
done
for name in "${internal[@]}"; do
    find internal/proto -name "${name}*.pb.go" -delete
    find internal/proto -name "${name}*_gen.go" -delete
done
for name in "${public[@]}"; do
    find internal/proto -name "${name}*.pb.go" -delete
    find internal/proto -name "${name}*_gen.go" -delete
done

plugins=(
    --plugin=protoc-gen-dethash=build/protoc-gen-dethash
    --plugin=protoc-gen-reader=build/protoc-gen-reader
)

generate() {
    local module=$1
    shift
    local output=.
    if [[ $module == "$client_module" ]]; then output=pkg/client/v3; fi
    local options=(
        "--go_out=${output}" "--go_opt=module=${module}"
        "--go-grpc_out=${output}" "--go-grpc_opt=module=${module}"
        "--go-vtproto_out=${output}" "--go-vtproto_opt=module=${module}"
        --go-vtproto_opt=features=marshal+unmarshal+size+clone+equal+pool
        "--dethash_out=${output}" "--dethash_opt=module=${module}"
        "--reader_out=${output}" "--reader_opt=module=${module}"
    )
    if [[ $module == "$root_module" ]]; then
        for type in Proposal ExecutionPlan AttributeValue AttributePlan Order TechnicalUpdate EventsSinkUpdate MirrorSyncUpdate; do
            options+=("--go-vtproto_opt=pool=${root_module}/internal/proto/raftcmdpb.${type}")
        done
        options+=("--go-vtproto_opt=pool=${root_module}/internal/proto/proposalpb.AppliedProposal")
    else
        options+=("--go-vtproto_opt=pool=${client_module}/grpc.AuditEntry")
        options+=(
            "--skippable_out=${output}" "--skippable_opt=module=${module}"
        )
    fi
    local files=()
    for name in "$@"; do files+=("misc/proto/${name}.proto"); done
    local enabled_plugins=("${plugins[@]}")
    if [[ $module == "$client_module" ]]; then
        enabled_plugins+=(
            --plugin=protoc-gen-skippable=build/protoc-gen-skippable
        )
    fi
    protoc "${options[@]}" "${enabled_plugins[@]}" -I misc/proto "${files[@]}"
}

generate "$client_module" "${public[@]}"
generate "$root_module" "${internal[@]}"

# These tables validate public proto options but are consumed only by the server.
# Generate them against the public schemas into the root module, without a
# second copy of the message types or their descriptor registry.
protoc -I misc/proto \
    --queryfilter-validity_out=. --plugin=protoc-gen-queryfilter-validity=build/protoc-gen-queryfilter-validity \
    --ledger-log-category_out=. --plugin=protoc-gen-ledger-log-category=build/protoc-gen-ledger-log-category \
    --rpcauth_out=. --plugin=protoc-gen-rpcauth=build/protoc-gen-rpcauth \
    "${public[@]/%/.proto}"

protoc -I misc/proto --include_imports \
    --descriptor_set_out=pkg/client/v3/proto/ledger-public.protoset \
    "${public[@]/%/.proto}"

go -C pkg/client/v3 mod tidy
python3 scripts/update-client-contract.py
gofmt -w pkg/client/v3/grpc/protocol.go
