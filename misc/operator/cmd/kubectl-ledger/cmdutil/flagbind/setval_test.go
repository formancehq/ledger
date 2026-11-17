package flagbind

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSetValues_Simple(t *testing.T) {
	result, err := ParseSetValues([]string{"debug=true", "replicas=3"})
	if err != nil {
		t.Fatal(err)
	}

	// All values are strings — coercion happens later in ApplyToStruct.
	if got, want := result["debug"], "true"; got != want {
		t.Errorf("debug = %v, want %v", got, want)
	}
	if got, want := result["replicas"], "3"; got != want {
		t.Errorf("replicas = %v, want %v", got, want)
	}
}

func TestParseSetValues_Nested(t *testing.T) {
	result, err := ParseSetValues([]string{"raft.electionTick=15"})
	if err != nil {
		t.Fatal(err)
	}

	raft := result["raft"].(map[string]any)
	if got, want := raft["electionTick"], "15"; got != want {
		t.Errorf("raft.electionTick = %v, want %v", got, want)
	}
}

func TestParseSetValues_ArrayIndex(t *testing.T) {
	result, err := ParseSetValues([]string{
		"ingress.tls[0].secretName=my-secret",
		"ingress.tls[0].hosts[0]=example.com",
		"ingress.tls[0].hosts[1]=www.example.com",
	})
	if err != nil {
		t.Fatal(err)
	}

	ingress := result["ingress"].(map[string]any)
	tls := ingress["tls"].([]any)
	if len(tls) != 1 {
		t.Fatalf("expected 1 tls entry, got %d", len(tls))
	}

	entry := tls[0].(map[string]any)
	if got, want := entry["secretName"], "my-secret"; got != want {
		t.Errorf("secretName = %v, want %v", got, want)
	}

	hosts := entry["hosts"].([]any)
	if len(hosts) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hosts))
	}
	if got := hosts[0]; got != "example.com" {
		t.Errorf("hosts[0] = %v, want example.com", got)
	}
	if got := hosts[1]; got != "www.example.com" {
		t.Errorf("hosts[1] = %v, want www.example.com", got)
	}
}

func TestParseSetValues_EscapedDot(t *testing.T) {
	result, err := ParseSetValues([]string{`annotations.service\.beta\.kubernetes\.io/aws-load-balancer-internal=internal`})
	if err != nil {
		t.Fatal(err)
	}

	annotations := result["annotations"].(map[string]any)
	key := "service.beta.kubernetes.io/aws-load-balancer-internal"
	if got, want := annotations[key], "internal"; got != want {
		t.Errorf("annotations[%q] = %v, want %v", key, got, want)
	}
}

func TestParseSetValues_QuotedDotKey(t *testing.T) {
	result, err := ParseSetValues([]string{`ingress.annotations."traefik.ingress.kubernetes.io/router.tls"=true`})
	if err != nil {
		t.Fatal(err)
	}

	ingress := result["ingress"].(map[string]any)
	annotations := ingress["annotations"].(map[string]any)
	key := "traefik.ingress.kubernetes.io/router.tls"
	if got, want := annotations[key], "true"; got != want {
		t.Errorf("annotations[%q] = %v, want %v", key, got, want)
	}
}

func TestParseSetValues_StringValue(t *testing.T) {
	result, err := ParseSetValues([]string{"clusterID=my-cluster"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := result["clusterID"], "my-cluster"; got != want {
		t.Errorf("clusterID = %v, want %v", got, want)
	}
}

func TestParseSetValues_MissingEquals(t *testing.T) {
	_, err := ParseSetValues([]string{"badvalue"})
	if err == nil {
		t.Error("expected error for missing '='")
	}
}

func TestParseSetValues_InvalidPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		err  string
	}{
		{name: "negative index", path: "ingress.tls[-1].secretName", err: "must be a decimal integer"},
		{name: "negative nested index", path: "ingress.tls[0].hosts[-1]", err: "must be a decimal integer"},
		{name: "negative adjacent index", path: "matrix[0][-1]", err: "must be a decimal integer"},
		{name: "unsupported adjacent indices", path: "matrix[0][0][0]", err: "expected array for index"},
		{name: "too large", path: "hosts[65537]", err: "exceeds maximum 65536"},
		{name: "too large nested", path: "matrix[0][65537]", err: "exceeds maximum 65536"},
		{name: "integer overflow", path: "hosts[999999999999999999999999999999999]", err: "exceeds maximum 65536"},
		{name: "non numeric", path: "hosts[foo]", err: "must be a decimal integer"},
		{name: "fractional", path: "hosts[1.5]", err: "unclosed array index"},
		{name: "empty index", path: "hosts[]", err: "must be a decimal integer"},
		{name: "signed index", path: "hosts[+1]", err: "must be a decimal integer"},
		{name: "space in index", path: "hosts[ 1]", err: "must be a decimal integer"},
		{name: "missing closing bracket", path: "hosts[0", err: "unclosed array index"},
		{name: "nested opening bracket", path: "hosts[[0]]", err: "must be a decimal integer"},
		{name: "stray closing bracket", path: "hosts]", err: "unexpected ']'"},
		{name: "closing bracket before index", path: "hosts][0]", err: "unexpected ']'"},
		{name: "extra closing bracket", path: "hosts[0]]", err: "unexpected text after array index"},
		{name: "trailing characters", path: "hosts[0]suffix", err: "unexpected text after array index"},
		{name: "root index", path: "[0].hosts", err: "array index must follow a key"},
		{name: "index without key after dot", path: "hosts.[0]", err: "array index must follow a key"},
		{name: "empty path", path: "", err: "empty key"},
		{name: "empty leading key", path: ".hosts", err: "empty key"},
		{name: "empty middle key", path: "ingress..hosts", err: "empty key"},
		{name: "empty trailing key", path: "ingress.", err: "empty key"},
		{name: "empty quoted key", path: `annotations.""`, err: "empty key"},
		{name: "unclosed quote", path: `annotations."example.com`, err: "unclosed quoted key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assignment := test.path + "=value"
			result, err := ParseSetValues([]string{assignment})
			require.ErrorContains(t, err, test.err)
			require.Contains(t, err.Error(), fmt.Sprintf("--set %q", assignment))
			require.Nil(t, result)
		})
	}
}

func TestParseSetValues_ValidPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sets []string
		want map[string]any
	}{
		{
			name: "empty value and embedded equals",
			sets: []string{"empty=", "token=a=b"},
			want: map[string]any{"empty": "", "token": "a=b"},
		},
		{
			name: "sparse array and leading zero",
			sets: []string{"hosts[02]=example.com", "hosts[0]=localhost"},
			want: map[string]any{"hosts": []any{"localhost", nil, "example.com"}},
		},
		{
			name: "adjacent array indices",
			sets: []string{"matrix[0][1]=first", "matrix[0][0]=second"},
			want: map[string]any{"matrix": []any{[]any{"second", "first"}}},
		},
		{
			name: "nested arrays with keys",
			sets: []string{"matrix[0][1].hosts[0]=example.com"},
			want: map[string]any{"matrix": []any{[]any{nil, map[string]any{"hosts": []any{"example.com"}}}}},
		},
		{
			name: "escaped and quoted keys with arrays",
			sets: []string{`annotations."example.com/hosts"[0]=first`, `annotations.example\.com/hosts[1]=second`},
			want: map[string]any{"annotations": map[string]any{"example.com/hosts": []any{"first", "second"}}},
		},
		{
			name: "last assignment wins",
			sets: []string{"replicas=3", "replicas=5"},
			want: map[string]any{"replicas": "5"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result, err := ParseSetValues(test.sets)
			require.NoError(t, err)
			require.Equal(t, test.want, result)
		})
	}
}

func TestParseSetValues_MaximumArrayIndex(t *testing.T) {
	t.Parallel()

	result, err := ParseSetValues([]string{"hosts[65536]=example.com"})
	require.NoError(t, err)
	hosts := result["hosts"].([]any)
	require.Len(t, hosts, 65537)
	require.Equal(t, "example.com", hosts[65536])
	require.Nil(t, hosts[65535])
}

func TestSetNestedPath_InvalidPathDoesNotMutate(t *testing.T) {
	t.Parallel()

	root := map[string]any{"existing": "value"}
	err := setNestedPath(root, "ingress.tls[0].hosts[-1]", "example.com")
	require.ErrorContains(t, err, "must be a decimal integer")
	require.Equal(t, map[string]any{"existing": "value"}, root)
}
