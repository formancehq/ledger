package cmd

import (
	"fmt"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"golang.org/x/mod/semver"
	"strings"
)

type VersionReference struct {
	hash     string
	branch   string
	imageTag string
}

func (r VersionReference) getGitRepo() auto.GitRepo {
	return auto.GitRepo{
		URL:         "https://github.com/formancehq/ledger",
		ProjectPath: "deployments/pulumi",
		Branch:      r.branch,
		CommitHash:  r.hash,
		Shallow:     true,
	}
}

func resolveReferenceFromVersion(version string) (*VersionReference, error) {
	rem := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{"https://github.com/formancehq/ledger"},
	})

	refs, err := rem.List(&git.ListOptions{
		PeelingOption: git.AppendPeeled,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list references: %w", err)
	}

	switch {
	case semver.IsValid(version):
		switch {
		case semver.MajorMinor(version) == version:
			var (
				found *plumbing.Reference
			)
			for _, ref := range refs {
				if !ref.Name().IsTag() {
					continue
				}
				tag := strings.TrimPrefix(ref.Name().String(), "refs/tags/")
				if !semver.IsValid(tag) {
					continue
				}
				if !strings.HasPrefix(tag, version) {
					continue
				}

				if found == nil || semver.Compare(strings.TrimPrefix(found.Name().String(), "refs/tags/"), tag) < 0 {
					found = ref
				}
			}

			if found == nil {
				return nil, fmt.Errorf("no tag found for %s", version)
			}

			return &VersionReference{
				imageTag: strings.TrimPrefix(found.Name().String(), "refs/tags/"),
				branch:   "release/" + version,
			}, nil
		default:
			return &VersionReference{
				imageTag: version,
				branch:   "release/" + semver.MajorMinor(version),
			}, nil
		}
	default:
		return &VersionReference{
			imageTag: version,
			hash:     version,
		}, nil
	}
}