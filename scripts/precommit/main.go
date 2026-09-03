package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const maxFixpointPasses = 4

type selection struct {
	BaseSHA              string              `json:"baseSha"`
	TrustedToolSHA       string              `json:"trustedToolSha"`
	CandidateFingerprint string              `json:"candidateFingerprint"`
	FullFallback         bool                `json:"fullFallback"`
	FallbackReasons      []string            `json:"fallbackReasons,omitempty"`
	ChangedPaths         []string            `json:"changedPaths"`
	Selected             map[string][]string `json:"selected"`
	Skipped              []string            `json:"skipped"`
}

type componentState struct {
	Inputs  string
	Outputs string
}

type runner struct {
	repo           string
	toolRoot       string
	trustedToolSHA string
	components     []component
	execute        func(component) error
}

func main() {
	var repo, toolRoot, baseSHA, trustedToolSHA string
	var planOnly bool
	flag.StringVar(&repo, "repo", "", "candidate repository worktree")
	flag.StringVar(&toolRoot, "tool-root", "", "trusted repository containing the component map and recipes")
	flag.StringVar(&baseSHA, "base", os.Getenv("PRECOMMIT_BASE_SHA"), "exact target commit")
	flag.StringVar(&trustedToolSHA, "trusted-tool-sha", os.Getenv("PRECOMMIT_TRUSTED_TOOL_SHA"), "expected trusted tool HEAD")
	flag.BoolVar(&planOnly, "plan", false, "print the selection without running components")
	flag.Parse()

	resolvedRepo, err := resolveDirectory(repo)
	if err != nil {
		fatal(fmt.Errorf("resolving candidate repository: %w", err))
	}
	resolvedToolRoot, err := resolveDirectory(toolRoot)
	if err != nil {
		fatal(fmt.Errorf("resolving trusted tool repository: %w", err))
	}

	r := runner{
		repo:           resolvedRepo,
		toolRoot:       resolvedToolRoot,
		trustedToolSHA: strings.TrimSpace(trustedToolSHA),
		components:     componentMap(),
	}
	selected, err := r.selectComponents(strings.TrimSpace(baseSHA), strings.TrimSpace(trustedToolSHA))
	if err != nil {
		fatal(err)
	}
	// Identity uncertainty selects the full recipe. Only retain the mutation
	// guard when this invocation actually started from the expected clean tool
	// snapshot; otherwise there is no trusted snapshot to compare later.
	if err := r.verifyTrustedTool(); err != nil {
		r.trustedToolSHA = ""
	}
	r.execute = r.executeComponent
	encoded, err := json.Marshal(selected)
	if err != nil {
		fatal(fmt.Errorf("encoding pre-commit selection: %w", err))
	}
	fmt.Printf("PRECOMMIT_SELECTION=%s\n", encoded)
	if planOnly {
		return
	}
	if err := r.runFixpoint(selected); err != nil {
		fatal(err)
	}
}

func (r runner) selectComponents(baseSHA, trustedToolSHA string) (selection, error) {
	plan := selection{
		BaseSHA:        baseSHA,
		TrustedToolSHA: trustedToolSHA,
		Selected:       make(map[string][]string),
	}
	workspaceFingerprint, err := r.workspaceFingerprint()
	if err != nil {
		return selection{}, fmt.Errorf("fingerprinting candidate workspace: %w", err)
	}
	plan.CandidateFingerprint = workspaceFingerprint

	if baseSHA == "" {
		plan.FallbackReasons = append(plan.FallbackReasons, "missing_exact_base_sha")
	}
	if trustedToolSHA == "" {
		plan.FallbackReasons = append(plan.FallbackReasons, "missing_trusted_tool_sha")
	}
	for _, item := range r.components {
		if !item.Complete {
			plan.FallbackReasons = append(plan.FallbackReasons, "incomplete_component_mapping:"+item.Name)
		}
	}
	actualToolSHA, toolErr := gitText(r.toolRoot, "rev-parse", "--verify", "HEAD^{commit}")
	if toolErr != nil {
		plan.FallbackReasons = append(plan.FallbackReasons, "trusted_tool_identity_unavailable")
	} else if trustedToolSHA != "" && actualToolSHA != trustedToolSHA {
		plan.FallbackReasons = append(plan.FallbackReasons, "trusted_tool_sha_mismatch")
	}
	toolStatus, statusErr := gitOutput(r.toolRoot, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if statusErr != nil || len(toolStatus) != 0 {
		plan.FallbackReasons = append(plan.FallbackReasons, "trusted_tool_worktree_not_clean")
	}
	if baseSHA != "" {
		if _, err := gitOutput(r.repo, "cat-file", "-e", baseSHA+"^{commit}"); err != nil {
			plan.FallbackReasons = append(plan.FallbackReasons, "base_commit_unavailable")
		} else {
			head, headErr := gitText(r.repo, "rev-parse", "--verify", "HEAD^{commit}")
			mergeBase, mergeErr := gitText(r.repo, "merge-base", baseSHA, "HEAD")
			if headErr != nil || mergeErr != nil || (head != baseSHA && mergeBase != baseSHA) {
				plan.FallbackReasons = append(plan.FallbackReasons, "base_is_not_candidate_ancestor")
			}
		}
	}

	if len(plan.FallbackReasons) == 0 {
		generatorDirectories, generatorErr := goGenerateDirectories(r.toolRoot)
		if generatorErr != nil {
			plan.FallbackReasons = append(plan.FallbackReasons, "generator_input_discovery_failed")
		}
		changedPaths, err := changedPaths(r.repo, baseSHA)
		if err != nil {
			plan.FallbackReasons = append(plan.FallbackReasons, "candidate_diff_unavailable")
		} else {
			plan.ChangedPaths = changedPaths
			for _, path := range changedPaths {
				if affectsEveryComponent(path) {
					plan.FallbackReasons = append(plan.FallbackReasons, "tool_or_map_identity_changed:"+path)

					continue
				}
				matched := false
				for _, item := range r.components {
					inputChanged := item.Inputs(path)
					if item.Name == "mock-code-generation" {
						inputChanged, err = r.mockGenerationTrigger(path, baseSHA, generatorDirectories)
						if err != nil {
							plan.FallbackReasons = append(plan.FallbackReasons, "generator_input_identity_unavailable:"+path)

							continue
						}
					}
					switch {
					case item.Outputs(path):
						addReason(plan.Selected, item.Name, "declared_output_changed:"+path)
						matched = true
					case inputChanged:
						addReason(plan.Selected, item.Name, "input_changed:"+path)
						matched = true
					case item.Config(path):
						addReason(plan.Selected, item.Name, "config_changed:"+path)
						matched = true
					}
				}
				if !matched && !knownIrrelevant(path) {
					plan.FallbackReasons = append(plan.FallbackReasons, "unknown_path:"+path)
				}
			}
		}
	}
	if vendorPresent(r.repo) {
		addReason(plan.Selected, "dashboards", "untrusted_or_unversioned_jsonnet_vendor")
	}

	if len(plan.FallbackReasons) != 0 {
		plan.FullFallback = true
		for _, item := range r.components {
			addReason(plan.Selected, item.Name, "full_fallback")
		}
	}
	slices.Sort(plan.FallbackReasons)
	slices.Sort(plan.ChangedPaths)
	for _, item := range r.components {
		reasons, ok := plan.Selected[item.Name]
		if !ok {
			plan.Skipped = append(plan.Skipped, item.Name)

			continue
		}
		slices.Sort(reasons)
		plan.Selected[item.Name] = slices.Compact(reasons)
	}

	return plan, nil
}

func (r runner) mockGenerationTrigger(path, baseSHA string, generatorDirectories map[string]struct{}) (bool, error) {
	if mockGenerationOutput(path) || protoGenerationOutput(path) || isModuleManifest(path, "") {
		return true, nil
	}
	if !rootModuleGo(path) {
		return false, nil
	}
	if _, ok := generatorDirectories[filepath.ToSlash(filepath.Dir(path))]; ok {
		return true, nil
	}
	workspaceContent, workspaceErr := readLocalFile(r.repo, path)
	if workspaceErr != nil && !errors.Is(workspaceErr, os.ErrNotExist) {
		return false, workspaceErr
	}
	baseContent, baseErr := gitOutput(r.repo, "show", baseSHA+":"+path)
	if baseErr != nil {
		baseContent = nil // A new or untracked path has no base content.
	}

	return containsGoGenerate(workspaceContent) || containsGoGenerate(baseContent), nil
}

func goGenerateDirectories(repository string) (map[string]struct{}, error) {
	output, err := gitOutput(repository, "ls-files", "-z", "--", "*.go")
	if err != nil {
		return nil, err
	}
	directories := make(map[string]struct{})
	for _, path := range splitNUL(output) {
		path = normalizePath(path)
		if !rootModuleGo(path) {
			continue
		}
		content, err := readLocalFile(repository, path)
		if err != nil {
			return nil, err
		}
		if containsGoGenerate(content) {
			directories[filepath.ToSlash(filepath.Dir(path))] = struct{}{}
		}
	}

	return directories, nil
}

func containsGoGenerate(content []byte) bool {
	return bytes.HasPrefix(content, []byte("//go:generate ")) || bytes.Contains(content, []byte("\n//go:generate "))
}

func readLocalFile(repository, path string) ([]byte, error) {
	localPath := filepath.FromSlash(normalizePath(path))
	if !filepath.IsLocal(localPath) {
		return nil, fmt.Errorf("path is not local: %q", path)
	}
	root, err := os.OpenRoot(repository)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(localPath)
	if err != nil {
		return nil, err
	}
	content, readErr := io.ReadAll(file)
	closeErr := file.Close()

	return content, errors.Join(readErr, closeErr)
}

func (r runner) runFixpoint(plan selection) error {
	currentWorkspace, err := r.workspaceFingerprint()
	if err != nil {
		return fmt.Errorf("recapturing selected workspace: %w", err)
	}
	if currentWorkspace != plan.CandidateFingerprint {
		return errors.New("candidate workspace changed after component selection")
	}
	pending := make(map[string]bool, len(plan.Selected))
	for name := range plan.Selected {
		pending[name] = true
	}
	if len(pending) == 0 {
		fmt.Println("PRECOMMIT_FIXPOINT: PASS passes=0 reason=no_relevant_components")

		return nil
	}

	for pass := 1; pass <= maxFixpointPasses; pass++ {
		if err := r.verifyTrustedTool(); err != nil {
			return err
		}
		beforeWorkspace, err := r.workspaceFingerprint()
		if err != nil {
			return fmt.Errorf("capturing pass %d workspace: %w", pass, err)
		}
		beforeStates, err := r.captureStates()
		if err != nil {
			return err
		}
		postRun := make(map[string]componentState)
		ran := make(map[string]bool)
		for _, item := range r.components {
			if !pending[item.Name] {
				fmt.Printf("PRECOMMIT_COMPONENT: SKIP name=%s pass=%d reason=not_invalidated\n", item.Name, pass)

				continue
			}
			started := time.Now()
			fmt.Printf("PRECOMMIT_COMPONENT: RUN name=%s pass=%d\n", item.Name, pass)
			if err := r.execute(item); err != nil {
				return fmt.Errorf("component %s failed: %w", item.Name, err)
			}
			state, err := r.captureState(item)
			if err != nil {
				return fmt.Errorf("capturing %s post-run identity: %w", item.Name, err)
			}
			postRun[item.Name] = state
			ran[item.Name] = true
			fmt.Printf("PRECOMMIT_COMPONENT: PASS name=%s pass=%d duration=%s inputs=%s outputs=%s\n",
				item.Name, pass, time.Since(started).Round(time.Millisecond), state.Inputs, state.Outputs)
		}

		afterWorkspace, err := r.workspaceFingerprint()
		if err != nil {
			return fmt.Errorf("capturing pass %d final workspace: %w", pass, err)
		}
		afterStates, err := r.captureStates()
		if err != nil {
			return err
		}
		next := make(map[string]bool)
		mappedEffect := false
		for _, item := range r.components {
			if beforeStates[item.Name] != afterStates[item.Name] {
				mappedEffect = true
			}
			if ran[item.Name] {
				if afterStates[item.Name] != postRun[item.Name] {
					next[item.Name] = true
					fmt.Printf("PRECOMMIT_INVALIDATED: name=%s reason=changed_after_component\n", item.Name)
				}

				continue
			}
			if beforeStates[item.Name] != afterStates[item.Name] {
				next[item.Name] = true
				fmt.Printf("PRECOMMIT_INVALIDATED: name=%s reason=input_or_output_changed_by_normalization\n", item.Name)
			}
		}
		if beforeWorkspace != afterWorkspace && !mappedEffect {
			fmt.Println("PRECOMMIT_INVALIDATED: all reason=unmapped_component_effect_full_fallback")
			for _, item := range r.components {
				next[item.Name] = true
			}
		}
		if len(next) == 0 {
			reason := "first_pass_clean"
			if beforeWorkspace != afterWorkspace {
				reason = "all_component_identities_stable"
			}
			if err := r.verifyTrustedTool(); err != nil {
				return err
			}
			fmt.Printf("PRECOMMIT_FIXPOINT: PASS passes=%d reason=%s candidateFingerprint=%s\n", pass, reason, afterWorkspace)

			return nil
		}
		pending = next
	}

	return fmt.Errorf("PRECOMMIT_NON_DETERMINISTIC: fixpoint not reached after %d passes", maxFixpointPasses)
}

func (r runner) executeComponent(item component) error {
	if err := r.verifyTrustedTool(); err != nil {
		return err
	}
	command := exec.Command("just", "--no-dotenv", "--justfile", filepath.Join(r.toolRoot, "justfile"),
		"--working-directory", r.repo, item.Recipe)
	command.Dir = r.repo
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.Stdin = os.Stdin
	if err := command.Run(); err != nil {
		return fmt.Errorf("running trusted recipe %s: %w", item.Recipe, err)
	}

	return r.verifyTrustedTool()
}

func (r runner) verifyTrustedTool() error {
	if r.trustedToolSHA == "" {
		return nil
	}
	head, err := gitText(r.toolRoot, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return fmt.Errorf("TRUSTED_TOOL_MUTATION_DETECTED: resolving HEAD: %w", err)
	}
	status, err := gitOutput(r.toolRoot, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return fmt.Errorf("TRUSTED_TOOL_MUTATION_DETECTED: reading status: %w", err)
	}
	if head != r.trustedToolSHA || len(status) != 0 {
		return fmt.Errorf("TRUSTED_TOOL_MUTATION_DETECTED: expected clean %s, got HEAD %s with status %q",
			r.trustedToolSHA, head, status)
	}

	return nil
}

func (r runner) captureStates() (map[string]componentState, error) {
	states := make(map[string]componentState, len(r.components))
	for _, item := range r.components {
		state, err := r.captureState(item)
		if err != nil {
			return nil, fmt.Errorf("capturing %s identity: %w", item.Name, err)
		}
		states[item.Name] = state
	}

	return states, nil
}

func (r runner) captureState(item component) (componentState, error) {
	files, err := repositoryFiles(r.repo, item.Name == "dashboards")
	if err != nil {
		return componentState{}, err
	}
	inputs, err := fingerprintMatching(r.repo, files, func(path string) bool { return item.Inputs(path) || item.Config(path) })
	if err != nil {
		return componentState{}, err
	}
	outputs, err := fingerprintMatching(r.repo, files, item.Outputs)
	if err != nil {
		return componentState{}, err
	}

	return componentState{Inputs: inputs, Outputs: outputs}, nil
}

func (r runner) workspaceFingerprint() (string, error) {
	files, err := repositoryFiles(r.repo, true)
	if err != nil {
		return "", err
	}

	return fingerprintMatching(r.repo, files, func(string) bool { return true })
}

func repositoryFiles(repository string, includeDashboardVendor bool) ([]string, error) {
	output, err := gitOutput(repository, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, fmt.Errorf("listing repository files: %w", err)
	}
	paths := splitNUL(output)
	if includeDashboardVendor && vendorPresent(repository) {
		vendorRoot := filepath.Join(repository, "misc", "devenv", "monitoring-dashboards", "jsonnet", "vendor")
		err := filepath.WalkDir(vendorRoot, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			relative, err := filepath.Rel(repository, path)
			if err != nil || !filepath.IsLocal(relative) {
				return fmt.Errorf("dashboard vendor path is not local: %q", path)
			}
			paths = append(paths, normalizePath(relative))

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walking dashboard vendor: %w", err)
		}
	}
	slices.Sort(paths)

	return slices.Compact(paths), nil
}

func fingerprintMatching(repository string, paths []string, matches func(string) bool) (string, error) {
	root, err := os.OpenRoot(repository)
	if err != nil {
		return "", err
	}
	defer func() { _ = root.Close() }()
	hasher := sha256.New()
	writeField(hasher, []byte(componentMapVersion))
	for _, rawPath := range paths {
		path := normalizePath(rawPath)
		if !matches(path) {
			continue
		}
		if !filepath.IsLocal(filepath.FromSlash(path)) {
			return "", fmt.Errorf("repository path is not local: %q", path)
		}
		info, err := root.Lstat(filepath.FromSlash(path))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("stating %s: %w", path, err)
		}
		writeField(hasher, []byte(path))
		writeField(hasher, []byte(info.Mode().String()))
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := root.Readlink(filepath.FromSlash(path))
			if err != nil {
				return "", fmt.Errorf("reading symlink %s: %w", path, err)
			}
			writeField(hasher, []byte(target))
		case info.Mode().IsRegular():
			file, err := root.Open(filepath.FromSlash(path))
			if err != nil {
				return "", fmt.Errorf("opening %s: %w", path, err)
			}
			contentHasher := sha256.New()
			logicalBytes, copyErr := io.Copy(contentHasher, file)
			openedInfo, statErr := file.Stat()
			closeErr := file.Close()
			if copyErr != nil || statErr != nil || closeErr != nil {
				return "", errors.Join(copyErr, statErr, closeErr)
			}
			if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) ||
				openedInfo.Mode() != info.Mode() || openedInfo.Size() != logicalBytes ||
				!openedInfo.ModTime().Equal(info.ModTime()) {
				return "", fmt.Errorf("repository file %s changed while being fingerprinted", path)
			}
			writeField(hasher, contentHasher.Sum(nil))
		default:
			return "", fmt.Errorf("unsupported repository file type at %s", path)
		}
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func changedPaths(repository, baseSHA string) ([]string, error) {
	tracked, err := gitOutput(repository, "diff", "--name-only", "--no-renames", "-z", baseSHA, "--")
	if err != nil {
		return nil, err
	}
	untracked, err := gitOutput(repository, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, err
	}
	paths := append(splitNUL(tracked), splitNUL(untracked)...)
	for index := range paths {
		paths[index] = normalizePath(paths[index])
		if !filepath.IsLocal(filepath.FromSlash(paths[index])) {
			return nil, fmt.Errorf("changed path is not local: %q", paths[index])
		}
	}
	slices.Sort(paths)

	return slices.Compact(paths), nil
}

func vendorPresent(repository string) bool {
	info, err := os.Stat(filepath.Join(repository, "misc", "devenv", "monitoring-dashboards", "jsonnet", "vendor"))

	return err == nil && info.IsDir()
}

func splitNUL(content []byte) []string {
	var values []string
	for part := range bytes.SplitSeq(content, []byte{0}) {
		if len(part) != 0 {
			values = append(values, string(part))
		}
	}

	return values
}

func addReason(selected map[string][]string, name, reason string) {
	selected[name] = append(selected[name], reason)
}

func resolveDirectory(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("path is not a directory")
	}

	return resolved, nil
}

func gitText(directory string, arguments ...string) (string, error) {
	output, err := gitOutput(directory, arguments...)

	return strings.TrimSpace(string(output)), err
}

func gitOutput(directory string, arguments ...string) ([]byte, error) {
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.Output()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return nil, fmt.Errorf("git %s: %s: %w", strings.Join(arguments, " "), strings.TrimSpace(string(exitError.Stderr)), err)
		}

		return nil, err
	}

	return output, nil
}

func writeField(hasher hash.Hash, value []byte) {
	_, _ = fmt.Fprintf(hasher, "%d:", len(value))
	_, _ = hasher.Write(value)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "precommit: %v\n", err)
	os.Exit(1)
}
