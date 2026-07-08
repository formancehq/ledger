package explain

import (
	"testing"
)

func TestSpecFieldsNotEmpty(t *testing.T) {
	t.Parallel()

	fields := SpecFields()
	if len(fields) == 0 {
		t.Fatal("SpecFields() returned no fields")
	}

	// Verify some known top-level fields exist.
	known := map[string]bool{
		"replicas": false, "image": false, "clusterID": false,
		"raft": false, "pebble": false, "persistence": false,
		"monitoring": false, "auth": false, "debug": false,
		"logLevel": false,
	}

	for _, f := range fields {
		if _, ok := known[f.Name]; ok {
			known[f.Name] = true
		}
	}

	for name, found := range known {
		if !found {
			t.Errorf("expected field %q not found in SpecFields()", name)
		}
	}
}

func TestSpecFieldsRaftHasChildren(t *testing.T) {
	t.Parallel()

	raft, ok := Lookup(SpecFields(), "raft")
	if !ok {
		t.Fatal("raft field not found")
	}
	if len(raft.Children) == 0 {
		t.Fatal("raft field has no children")
	}

	_, ok = Lookup(raft.Children, "electionTick")
	if !ok {
		t.Error("raft.electionTick not found")
	}
}

func TestLookupNestedPath(t *testing.T) {
	t.Parallel()

	f, ok := Lookup(SpecFields(), "pebble.valueSeparation.enabled")
	if !ok {
		t.Fatal("pebble.valueSeparation.enabled not found")
	}
	if f.Type != "bool" {
		t.Errorf("expected type bool, got %s", f.Type)
	}
}

func TestAgentSpecFields(t *testing.T) {
	t.Parallel()

	fields := CredentialsSpecFields()
	if len(fields) == 0 {
		t.Fatal("CredentialsSpecFields() returned no fields")
	}

	found := false
	for _, f := range fields {
		if f.Name == "scopes" {
			found = true

			break
		}
	}
	if !found {
		t.Error("scopes field not found in CredentialsSpecFields()")
	}
}
