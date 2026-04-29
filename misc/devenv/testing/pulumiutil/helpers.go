// Package pulumiutil provides common helpers for Pulumi projects.
package pulumiutil

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pulumi/pulumi-docker-build/sdk/go/dockerbuild"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"gopkg.in/yaml.v3"
)

// GetBuildVersion generates a version string based on git commit and timestamp.
// Format: <short-commit>-<timestamp> (e.g., "abc1234-20260125-143022")
// If git is not available, falls back to timestamp only.
func GetBuildVersion(gitDir string) string {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	cmd.Dir = gitDir
	output, err := cmd.Output()

	timestamp := time.Now().Format("20060102-150405")

	if err != nil {
		return timestamp
	}

	commit := strings.TrimSpace(string(output))

	cmd = exec.Command("git", "status", "--porcelain")
	cmd.Dir = gitDir
	statusOutput, _ := cmd.Output()

	if len(statusOutput) > 0 {
		return fmt.Sprintf("%s-dirty-%s", commit, timestamp)
	}

	return fmt.Sprintf("%s-%s", commit, timestamp)
}

// GetConfigObject reads a config object from Pulumi config. If the object
// contains a "file" key, reads the YAML file instead (path relative to basePath).
func GetConfigObject(cfg *config.Config, key string, basePath string) (map[string]any, error) {
	var configObj map[string]any
	if err := cfg.GetObject(key, &configObj); err != nil {
		return nil, fmt.Errorf("failed to get config object %s: %w", key, err)
	}

	if filePath, ok := configObj["file"].(string); ok {
		fullPath := filepath.Join(basePath, filePath)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read values file %s: %w", fullPath, err)
		}

		var result map[string]any
		if err := yaml.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("failed to parse YAML file %s: %w", fullPath, err)
		}

		return result, nil
	}

	return configObj, nil
}

// GetConfigBool reads a boolean config value. Falls back to the provided default.
func GetConfigBool(cfg *config.Config, key string, fallback bool) bool {
	value := cfg.GetBool(key)
	if value {
		return true
	}
	if cfg.Get(key) == "false" {
		return false
	}
	return fallback
}

// K8sSetup holds the common Kubernetes provider and namespace setup.
type K8sSetup struct {
	Provider  pulumi.ProviderResource
	Namespace *v1.Namespace
}

// NewK8sSetup creates a Kubernetes provider and namespace from config.
func NewK8sSetup(ctx *pulumi.Context, cfg *config.Config) (*K8sSetup, error) {
	kubeContext := cfg.Require("k8s-context")

	namespaceName := cfg.Get("namespace")
	if namespaceName == "" {
		namespaceName = ctx.Stack()
	}

	k8sProvider, err := newK8sProviderInternal(ctx, kubeContext)
	if err != nil {
		return nil, err
	}

	namespace, err := v1.NewNamespace(ctx, "namespace", &v1.NamespaceArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Name: pulumi.String(namespaceName),
		},
	}, pulumi.Provider(k8sProvider))
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace: %w", err)
	}

	return &K8sSetup{
		Provider:  k8sProvider,
		Namespace: namespace,
	}, nil
}

// DockerConfig holds common Docker image build configuration.
type DockerConfig struct {
	Registry     string
	PullRegistry string
	BuilderName  string
	ImageTag     string
	Platforms    []string
	RegistryAuth dockerbuild.RegistryArray
}

// NewDockerConfig reads Docker config from Pulumi config.
func NewDockerConfig(ctx *pulumi.Context, cfg *config.Config) *DockerConfig {
	registry := cfg.Get("registry")
	if registry == "" {
		registry = "ghcr.io"
	}
	pullRegistry := cfg.Get("pull-registry")
	if pullRegistry == "" {
		pullRegistry = registry
	}
	builderName := cfg.Get("docker-builder-name")

	buildVersion := GetBuildVersion("../../..")
	imageTag := cfg.Get("imageTag")
	if imageTag == "" {
		imageTag = buildVersion
	}

	arch := cfg.Get("arch")
	if arch == "" {
		arch = "amd64"
	}
	platforms := make([]string, 0, len(allPlatforms))
	for _, p := range allPlatforms {
		if strings.HasSuffix(p, arch) {
			platforms = append(platforms, p)
		}
	}
	if len(platforms) == 0 {
		platforms = []string{"linux-" + arch}
	}

	return &DockerConfig{
		Registry:     registry,
		PullRegistry: pullRegistry,
		BuilderName:  builderName,
		ImageTag:     imageTag,
		Platforms:    platforms,
		RegistryAuth: dockerbuild.RegistryArray{
			dockerbuild.RegistryArgs{
				Address:  pulumi.String(registry),
				Username: config.GetSecret(ctx, "formance-dev-registry-username"),
				Password: config.GetSecret(ctx, "formance-dev-registry-password"),
			},
		},
	}
}

var allPlatforms = []string{"linux-amd64", "linux-arm64"}

// MultiArchImage wraps a multi-platform docker Index with its per-platform
// image builds. Use Ref for the pushed manifest tag and Resource() for deps.
type MultiArchImage struct {
	Index  *dockerbuild.Index
	Images []*dockerbuild.Image
	// Ref is the pushed index tag (e.g. "registry/name:latest@sha256:...").
	Ref pulumi.StringOutput
	// Digest is the sha256 digest extracted from the Ref (e.g. "sha256:abc...").
	Digest pulumi.StringOutput
}

// Resource returns the Index as a pulumi.Resource for DependsOn.
func (m *MultiArchImage) Resource() pulumi.Resource {
	return m.Index
}

// BuildImage builds one cached image per platform, then joins them into a
// multi-arch Index pushed with :latest and :<imageTag> tags.
func (dc *DockerConfig) BuildImage(
	ctx *pulumi.Context,
	name string,
	contextPath string,
	dockerfilePath string,
) (*MultiArchImage, error) {
	var sources pulumi.StringArray
	var images []*dockerbuild.Image

	for _, platform := range dc.Platforms {
		img, err := dockerbuild.NewImage(ctx, fmt.Sprintf("%s-%s", name, platform), &dockerbuild.ImageArgs{
			Context: dockerbuild.BuildContextArgs{
				Location: pulumi.String(contextPath),
			},
			Builder: dockerbuild.BuilderConfigArgs{
				Name: pulumi.String(dc.BuilderName),
			},
			CacheFrom: dockerbuild.CacheFromArray{
				dockerbuild.CacheFromArgs{
					Registry: dockerbuild.CacheFromRegistryArgs{
						Ref: pulumi.Sprintf("%s/%s:buildcache-%s", dc.Registry, name, platform),
					},
				},
			},
			CacheTo: dockerbuild.CacheToArray{
				dockerbuild.CacheToArgs{
					Registry: dockerbuild.CacheToRegistryArgs{
						Ref: pulumi.Sprintf("%s/%s:buildcache-%s,mode=max", dc.Registry, name, platform),
					},
				},
			},
			Dockerfile: dockerbuild.DockerfileArgs{
				Location: pulumi.String(dockerfilePath),
			},
			Platforms: dockerbuild.PlatformArray{
				dockerbuild.Platform(strings.ReplaceAll(platform, "-", "/")),
			},
			Push:       pulumi.Bool(true),
			Registries: dc.RegistryAuth,
			Tags: pulumi.StringArray{
				pulumi.Sprintf("%s/%s:%s-%s", dc.Registry, name, dc.ImageTag, platform),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to build %s for %s: %w", name, platform, err)
		}
		sources = append(sources, img.Ref)
		images = append(images, img)
	}

	idx, err := dockerbuild.NewIndex(ctx, name, &dockerbuild.IndexArgs{
		Sources: sources,
		Tag:     pulumi.Sprintf("%s/%s:%s", dc.Registry, name, dc.ImageTag),
		Push:    pulumi.Bool(true),
		Registry: dockerbuild.RegistryArgs{
			Address:  pulumi.String(dc.Registry),
			Username: dc.RegistryAuth[0].(dockerbuild.RegistryArgs).Username,
			Password: dc.RegistryAuth[0].(dockerbuild.RegistryArgs).Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create index for %s: %w", name, err)
	}

	digest := idx.Ref.ApplyT(func(ref string) string {
		if i := strings.Index(ref, "@"); i >= 0 {
			return ref[i+1:]
		}
		return ref
	}).(pulumi.StringOutput)

	return &MultiArchImage{
		Index:  idx,
		Images: images,
		Ref:    idx.Ref,
		Digest: digest,
	}, nil
}
