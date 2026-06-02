package flagbind

import (
	"testing"
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
