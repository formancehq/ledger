#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly build_constraint='//go:build !fctl_component_guest'
readonly files=(
  internal/proto/commonpb/common_vtproto.pb.go
  internal/proto/commonpb/common_reader.pb.go
  internal/proto/commonpb/common_dethash.pb.go
  internal/proto/servicepb/bucket_vtproto.pb.go
  internal/proto/servicepb/bucket_reader.pb.go
  internal/proto/servicepb/bucket_dethash.pb.go
  internal/proto/servicepb/bucket_grpc.pb.go
  internal/proto/auditpb/audit_vtproto.pb.go
  internal/proto/auditpb/audit_reader.pb.go
  internal/proto/auditpb/audit_dethash.pb.go
  internal/proto/signaturepb/signature_vtproto.pb.go
  internal/proto/signaturepb/signature_reader.pb.go
  internal/proto/signaturepb/signature_dethash.pb.go
)

mode="${1:-apply}"
if [[ "$mode" != apply && "$mode" != --check ]]; then
  printf 'usage: %s [apply|--check]\n' "$0" >&2
  exit 2
fi

for relative_path in "${files[@]}"; do
  file="$repository_root/$relative_path"
  [[ -f "$file" ]] || {
    printf 'generated protobuf helper is missing: %s\n' "$relative_path" >&2
    exit 1
  }
  if [[ "$(head -n 1 "$file")" == "$build_constraint" ]]; then
    continue
  fi
  if [[ "$mode" == --check ]]; then
    printf 'generated protobuf helper lacks guest exclusion: %s\n' "$relative_path" >&2
    exit 1
  fi
  temporary="$(mktemp "${file}.guest-tag.XXXXXX")"
  {
    printf '%s\n\n' "$build_constraint"
    command cat "$file"
  } > "$temporary"
  chmod "$(stat -c '%a' "$file" 2>/dev/null || stat -f '%Lp' "$file")" "$temporary"
  mv "$temporary" "$file"
done
