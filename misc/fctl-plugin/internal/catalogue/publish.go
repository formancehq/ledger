package catalogue

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type PublishOptions struct {
	ManifestPath  string
	ArtifactsPath string
	SourceRoot    string
	Registry      string
	Repository    string
	Revision      int
	Layout        string
}

// buildArtifact is the GoReleaser artifacts.json contract used by its Go builds.
type buildArtifact struct {
	Path  string `json:"path"`
	OS    string `json:"goos"`
	Arch  string `json:"goarch"`
	Type  string `json:"type"`
	Extra struct {
		ID string `json:"ID"`
	} `json:"extra"`
}

// Publish uploads raw GoReleaser executables and emits the catalogue only after
// every platform succeeds. Layout writes real OCI artifacts locally, without
// contacting a registry. Diagnostics never share the catalogue output stream.
func Publish(ctx context.Context, options PublishOptions, output, diagnostics io.Writer) error {
	if options.Layout != "" {
		var err error
		options.Layout, err = filepath.Abs(options.Layout)
		if err != nil {
			return err
		}
	}
	manifest, err := ReadManifest(options.ManifestPath)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(options.ArtifactsPath)
	if err != nil {
		return err
	}
	var artifacts []buildArtifact
	if err := json.Unmarshal(data, &artifacts); err != nil {
		return fmt.Errorf("read GoReleaser artifacts: %w", err)
	}
	sourceRoot, err := os.OpenRoot(options.SourceRoot)
	if err != nil {
		return err
	}
	defer func() { _ = sourceRoot.Close() }() // Read-only containment capability.
	sourcePath, err := filepath.Abs(options.SourceRoot)
	if err != nil {
		return err
	}
	stagePath, err := os.MkdirTemp(filepath.Dir(sourcePath), ".fctl-plugin-publish-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(stagePath) }() // Private staging directory, never an API-supplied path.
	stageRoot, err := os.OpenRoot(stagePath)
	if err != nil {
		return err
	}
	defer func() { _ = stageRoot.Close() }()
	var releases []Release
	seen := make(map[Platform]bool)
	for _, artifact := range artifacts {
		if artifact.Type != "Binary" || artifact.Extra.ID != "fctl-plugin-ledger" {
			continue
		}
		platform := Platform{OS: artifact.OS, Arch: artifact.Arch}
		if seen[platform] {
			return fmt.Errorf("duplicate binary for %s/%s", platform.OS, platform.Arch)
		}
		seen[platform] = true
		name, err := stageBinary(sourceRoot, sourcePath, stageRoot, artifact, manifest.Version, options.Revision)
		if err != nil {
			return err
		}
		// Validate the exact catalogue identity and raw checksum before publishing.
		release, err := NewRelease(manifest, filepath.Join(stagePath, name),
			Artifact{Registry: options.Registry, Repository: options.Repository, Digest: "sha256:" + strings.Repeat("0", 64)},
			platform, options.Revision)
		if err != nil {
			return err
		}
		releases = append(releases, release)
	}
	if len(releases) != 6 {
		return fmt.Errorf("expected six GoReleaser plugin platforms, got %d", len(releases))
	}
	origin, err := url.Parse(options.Registry)
	if err != nil {
		return err
	}
	for index, release := range releases {
		tag := fmt.Sprintf("%s-r%d-%s-%s-%s", strings.ReplaceAll(manifest.Version, "+", "_"), options.Revision,
			release.Platform.OS, release.Platform.Arch, release.SHA256[:16])
		if len(tag) > 128 || strings.ContainsAny(tag, ":@") {
			return errors.New("service version produces an invalid OCI tag")
		}
		destination := origin.Host + "/" + options.Repository + ":" + tag
		if options.Layout != "" {
			destination = options.Layout + ":" + tag
		}
		args := []string{"push", destination, "fctl-plugin-ledger:application/vnd.formance.fctl.plugin.executable.v1",
			"--artifact-type", "application/vnd.formance.fctl.plugin.v1", "--image-spec", "v1.1",
			"--config", "empty.json:application/vnd.oci.empty.v1+json", "--format", "json",
			"--annotation", "org.opencontainers.image.source=https://github.com/formancehq/ledger"}
		if options.Layout != "" {
			args = append(args, "--oci-layout")
		} else if origin.Scheme == "http" {
			args = append(args, "--plain-http")
		}
		command := exec.CommandContext(ctx, "oras", args...)
		command.Dir = filepath.Join(stagePath, release.Platform.OS+"_"+release.Platform.Arch)
		command.Stderr = diagnostics
		result, err := command.Output()
		if err != nil {
			return fmt.Errorf("publish %s/%s: %w", release.Platform.OS, release.Platform.Arch, err)
		}
		var pushed struct {
			Digest string `json:"digest"`
		}
		if err := json.Unmarshal(result, &pushed); err != nil {
			return fmt.Errorf("read ORAS push digest: %w", err)
		}
		releases[index].Artifact.Digest = pushed.Digest
		if err := validateArtifact(releases[index].Artifact); err != nil {
			return err
		}
	}

	return json.NewEncoder(output).Encode(Catalogue{SchemaVersion: 1, Releases: releases})
}

func stageBinary(sourceRoot *os.Root, sourcePath string, stageRoot *os.Root, artifact buildArtifact, version string, revision int) (string, error) {
	if artifact.OS != "linux" && artifact.OS != "darwin" && artifact.OS != "windows" || artifact.Arch != "amd64" && artifact.Arch != "arm64" {
		return "", errors.New("unsupported GoReleaser platform")
	}
	relative := artifact.Path
	if filepath.IsAbs(relative) {
		var err error
		relative, err = filepath.Rel(sourcePath, relative)
		if err != nil {
			return "", err
		}
	}
	if !filepath.IsLocal(relative) {
		return "", errors.New("GoReleaser executable path must remain inside its build root")
	}
	source, err := sourceRoot.Open(relative)
	if err != nil {
		return "", fmt.Errorf("open contained GoReleaser executable: %w", err)
	}
	defer func() { _ = source.Close() }()
	info, err := source.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > maxBinaryBytes {
		return "", errors.New("GoReleaser executable must be a regular file containing 1 byte to 128 MiB")
	}
	build, err := buildinfo.Read(source)
	if err != nil {
		return "", fmt.Errorf("read Go executable build identity: %w", err)
	}
	settings := make(map[string]string)
	for _, setting := range build.Settings {
		settings[setting.Key] = setting.Value
	}
	if settings["GOOS"] != artifact.OS || settings["GOARCH"] != artifact.Arch ||
		build.Path != "github.com/formancehq/ledger/misc/fctl-plugin/cmd/fctl-plugin-ledger" {
		return "", errors.New("GoReleaser binary platform or product entry point does not match catalogue inputs")
	}
	if !hasLinkValue(settings["-ldflags"], "main.serviceVersion", version) ||
		!hasLinkValue(settings["-ldflags"], "main.revision", strconv.Itoa(revision)) {
		return "", errors.New("GoReleaser binary service version or plugin revision does not match catalogue inputs; build without -trimpath to retain linker metadata")
	}
	directory := artifact.OS + "_" + artifact.Arch
	if err := stageRoot.Mkdir(directory, 0o700); err != nil {
		return "", err
	}
	if err := stageRoot.WriteFile(filepath.Join(directory, "empty.json"), []byte("{}"), 0o600); err != nil {
		return "", err
	}
	name := filepath.Join(directory, "fctl-plugin-ledger")
	target, err := stageRoot.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	_, copyErr := io.Copy(target, source)
	if err := errors.Join(copyErr, target.Close()); err != nil {
		return "", err
	}

	return name, nil
}

func hasLinkValue(flags, name, value string) bool {
	words := strings.Fields(flags)
	var actual string
	for index, word := range words {
		if word == "-X" && index+1 < len(words) {
			if field, ok := strings.CutPrefix(words[index+1], name+"="); ok {
				actual = field
			}
		}
	}

	return actual == value
}
