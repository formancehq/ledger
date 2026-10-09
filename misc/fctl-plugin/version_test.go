package ledger

import "testing"

func TestServiceVersionBelongsToPluginInstance(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"3.0.0", "3.0.1", "3.0.0-beta.5"} {
		t.Run(version, func(t *testing.T) {
			t.Parallel()
			plugin := NewVersion(nil, version)
			manifest, err := plugin.GetManifest(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Version != version {
				t.Fatalf("manifest version: %q, want %q", manifest.Version, version)
			}
			manifest.Version = "corrupted"
			manifest.Root.Flags[0].Name = "corrupted"
			fresh, err := plugin.GetManifest(t.Context())
			if err != nil || fresh.Version != version || fresh.Root.Flags[0].Name != "ledger" {
				t.Fatalf("manifest mutation escaped caller: %#v, %v", fresh, err)
			}
		})
	}
}
