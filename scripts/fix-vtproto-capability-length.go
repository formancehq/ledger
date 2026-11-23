//go:build ignore

// Patch the pinned vtprotobuf generator's packed fixed32 boundary check for
// proposal capability declarations. Keep this in generation so regenerated
// sources cannot silently restore the unsafe decoder.
package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	for _, path := range []string{
		"internal/proto/auditpb/audit_vtproto.pb.go",
		"internal/proto/raftcmdpb/raft_cmd_vtproto.pb.go",
	} {
		if err := fix(path); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func fix(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	source := string(raw)
	// Both generated decoders contain one packed RequiredCapabilities arm.
	anchor := "elementCount = packedLen / 4\n\t\t\t\tif elementCount != 0 && len(m.RequiredCapabilities)"
	if strings.Count(source, anchor) != 1 {
		return fmt.Errorf("%s: expected one generated packed capability arm", path)
	}
	position := strings.Index(source, anchor)
	start := strings.LastIndex(source[:position], "\t\t\t} else if wireType == 2 {")
	end := strings.Index(source[position:], "\t\t\t} else {")
	if start < 0 || end < 0 {
		return fmt.Errorf("%s: cannot locate generated packed capability arm", path)
	}
	end += position
	arm := source[start:end]
	lengthCheck := "if packedLen < 0 {"
	readCheck := "if (iNdEx + 4) > l {"
	if strings.Count(arm, lengthCheck) != 1 || strings.Count(arm, readCheck) != 1 {
		return fmt.Errorf("%s: unexpected generated packed capability decoder", path)
	}
	arm = strings.Replace(arm, lengthCheck, "if packedLen < 0 || packedLen%4 != 0 {", 1)
	arm = strings.Replace(arm, readCheck, "if (iNdEx + 4) > postIndex {", 1)
	updated := source[:start] + arm + source[end:]
	return os.WriteFile(path, []byte(updated), 0o644)
}
