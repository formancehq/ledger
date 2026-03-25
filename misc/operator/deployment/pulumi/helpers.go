package main

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/pulumi/pulumi-docker-build/sdk/go/dockerbuild"
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

// getBuildVersion generates a version string based on git commit and timestamp.
func getBuildVersion(gitDir string) string {
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

// getConfigBool reads a boolean config value. Falls back to the provided default.
func getConfigBool(cfg *config.Config, key string, fallback bool) bool {
	value := cfg.GetBool(key)
	if value {
		return true
	}
	if cfg.Get(key) == "false" {
		return false
	}
	return fallback
}

// k8sSetup holds the common Kubernetes provider and namespace setup.
type k8sSetup struct {
	Provider  pulumi.ProviderResource
	Namespace *v1.Namespace
}

// newK8sSetup creates a Kubernetes provider and namespace from config.
func newK8sSetup(ctx *pulumi.Context, cfg *config.Config) (*k8sSetup, error) {
	kubeContext := cfg.Require("k8s-context")

	namespaceName := cfg.Get("namespace")
	if namespaceName == "" {
		namespaceName = ctx.Stack()
	}

	k8sProvider, err := kubernetes.NewProvider(ctx, "k8s", &kubernetes.ProviderArgs{
		Context: pulumi.StringPtr(kubeContext),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s provider: %w", err)
	}

	namespace, err := v1.NewNamespace(ctx, "namespace", &v1.NamespaceArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Name: pulumi.String(namespaceName),
		},
	}, pulumi.Provider(k8sProvider))
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace: %w", err)
	}

	return &k8sSetup{
		Provider:  k8sProvider,
		Namespace: namespace,
	}, nil
}

// dockerConfig holds common Docker image build configuration.
type dockerConfig struct {
	Registry     string
	PullRegistry string
	BuilderName  string
	ImageTag     string
	Platforms    []string
	RegistryAuth dockerbuild.RegistryArray
}

var allPlatforms = []string{"linux-amd64", "linux-arm64"}

// newDockerConfig reads Docker config from Pulumi config.
func newDockerConfig(ctx *pulumi.Context, cfg *config.Config) *dockerConfig {
	registry := cfg.Get("registry")
	if registry == "" {
		registry = "ghcr.io"
	}
	pullRegistry := cfg.Get("pull-registry")
	if pullRegistry == "" {
		pullRegistry = registry
	}
	builderName := cfg.Get("docker-builder-name")

	buildVersion := getBuildVersion("../../../..")
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

	return &dockerConfig{
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

// multiArchImage wraps a multi-platform docker Index with its per-platform image builds.
type multiArchImage struct {
	Index  *dockerbuild.Index
	Images []*dockerbuild.Image
	Ref    pulumi.StringOutput
	Digest pulumi.StringOutput
}

// Resource returns the Index as a pulumi.Resource for DependsOn.
func (m *multiArchImage) Resource() pulumi.Resource {
	return m.Index
}

// buildImage builds one cached image per platform, then joins them into a
// multi-arch Index pushed with :<imageTag> tag.
func (dc *dockerConfig) buildImage(
	ctx *pulumi.Context,
	name string,
	contextPath string,
	dockerfilePath string,
) (*multiArchImage, error) {
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

	return &multiArchImage{
		Index:  idx,
		Images: images,
		Ref:    idx.Ref,
		Digest: digest,
	}, nil
}