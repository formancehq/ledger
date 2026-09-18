package main

import (
	"fmt"
	"os"

	"golang.org/x/mod/modfile"
)

const (
	rootGoModPath     = "go.mod"
	workloadGoModPath = "tests/antithesis/workload/go.mod"
	etcdServerModule  = "go.etcd.io/etcd/server/v3"
)

// etcdPin is how one module resolves the etcd server package: the version it
// requires, and the replacement that version carries when one is in force.
type etcdPin struct {
	version     string
	replaceFrom string
	replaceTo   string
}

func (p etcdPin) String() string {
	if p.replaceTo == "" {
		return fmt.Sprintf("require %s, no replace", p.version)
	}

	return fmt.Sprintf("require %s, replace %s => %s", p.version, p.replaceFrom, p.replaceTo)
}

// checkEtcdPinParity keeps the antithesis workload module resolving the same
// etcd server build as the root module.
//
// The root module pins a patched etcd through a version-scoped replace
// (see the replay-validity section of the storage documentation). The workload
// module builds the same code through a local module replacement, so it carries
// a mirror of that replace. Nothing in the module system ties the two together:
// bumping the root require past the replaced version escapes the replace and
// the root guard tests fail loudly, but the workload module would keep resolving
// the fork and silently exercise a different etcd than the one that ships.
func checkEtcdPinParity(rootSource, workloadSource []byte) ([]finding, error) {
	root, err := parseEtcdPin(rootGoModPath, rootSource)
	if err != nil {
		return nil, err
	}

	workload, err := parseEtcdPin(workloadGoModPath, workloadSource)
	if err != nil {
		return nil, err
	}

	if root.version == "" || workload.version == "" {
		// Neither module requires etcd any more; there is nothing to keep in sync.
		return nil, nil
	}

	if root == workload {
		return nil, nil
	}

	return []finding{{
		path:   workloadGoModPath,
		line:   1,
		column: 1,
		message: fmt.Sprintf(
			"ETCD_PIN_DIVERGED: %s resolves %s as %q while %s resolves it as %q; "+
				"both modules must move together or the workload tests a different etcd than ships",
			workloadGoModPath, etcdServerModule, workload.String(),
			rootGoModPath, root.String(),
		),
	}}, nil
}

func parseEtcdPin(path string, source []byte) (etcdPin, error) {
	parsed, err := modfile.Parse(path, source, nil)
	if err != nil {
		return etcdPin{}, fmt.Errorf("parsing %s: %w", path, err)
	}

	var pin etcdPin

	for _, require := range parsed.Require {
		if require.Mod.Path == etcdServerModule {
			pin.version = require.Mod.Version

			break
		}
	}

	for _, replace := range parsed.Replace {
		if replace.Old.Path != etcdServerModule {
			continue
		}

		// An unversioned replace applies to every version, which is the shape the
		// pin deliberately avoids; record it as-is so a divergence still shows up.
		pin.replaceFrom = replace.Old.Version
		pin.replaceTo = replace.New.Path + "@" + replace.New.Version

		break
	}

	return pin, nil
}

// checkEtcdPinParityFiles reads both module files and compares them. A missing
// workload module is not a failure: it means the mirror no longer exists.
func checkEtcdPinParityFiles() ([]finding, error) {
	rootSource, err := os.ReadFile(rootGoModPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", rootGoModPath, err)
	}

	workloadSource, err := os.ReadFile(workloadGoModPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading %s: %w", workloadGoModPath, err)
	}

	return checkEtcdPinParity(rootSource, workloadSource)
}
