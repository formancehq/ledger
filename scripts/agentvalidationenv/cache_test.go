package agentvalidationenv

import (
	"bytes"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testModulePath    = "github.com/stretchr/testify"
	testModuleVersion = "v1.11.1"
	testModuleSum     = "h1:7s2iGBzp5EwR7/aIZr8ao5+dra3wiQyKjjFuvgVKu7U="
	testGoModSum      = "h1:wZwfW3scLgRK+23gO65QZefKpKQRnfz6sD981Nm4B6U="
)

type cacheFixture struct {
	trustedRoot string
	upstream    string
	helper      string
	wrapper     string
}

func TestDownloadSeedKeepsExtractedModulesRunLocal(t *testing.T) {
	t.Parallel()

	fixture := newCacheFixture(t)
	runA := fixture.newRun(t)
	output, err := fixture.prepare(runA, nil)
	require.NoError(t, err, output)
	require.Contains(t, output, "MODULE_DOWNLOAD_CACHE=POPULATED")

	generation := fixture.generation(t)
	requireSeedContainsDownloadsOnly(t, generation)
	runB := fixture.newRun(t)
	output, err = fixture.prepare(runB, nil)
	require.NoError(t, err, output)
	require.Contains(t, output, "MODULE_DOWNLOAD_CACHE=REUSED")

	extractedA := extractedAssertionPath(runA)
	extractedB := extractedAssertionPath(runB)
	require.FileExists(t, extractedA)
	require.FileExists(t, extractedB)
	infoA, err := os.Stat(extractedA)
	require.NoError(t, err)
	originalA, err := os.ReadFile(extractedA)
	require.NoError(t, err)
	originalB, err := os.ReadFile(extractedB)
	require.NoError(t, err)
	require.NotEmpty(t, originalA)
	poisonedA := append([]byte(nil), originalA...)
	poisonedA[len(poisonedA)/2] ^= 0xff
	require.NoError(t, os.Chmod(extractedA, 0o644))
	require.NoError(t, os.WriteFile(extractedA, poisonedA, 0o644))
	require.NoError(t, os.Chtimes(extractedA, infoA.ModTime(), infoA.ModTime()))
	afterB, err := os.ReadFile(extractedB)
	require.NoError(t, err)
	require.Equal(t, originalB, afterB, "one run's extracted tree must not affect another run")

	sharedZip := filepath.Join(generation, "download", "github.com", "stretchr", "testify", "@v", testModuleVersion+".zip")
	require.FileExists(t, sharedZip)
	sharedBefore, err := os.ReadFile(sharedZip)
	require.NoError(t, err)
	localZipA := filepath.Join(runA, "go-mod-cache", "cache", "download", "github.com", "stretchr", "testify", "@v", testModuleVersion+".zip")
	localZipB := filepath.Join(runB, "go-mod-cache", "cache", "download", "github.com", "stretchr", "testify", "@v", testModuleVersion+".zip")
	localB, err := os.ReadFile(localZipB)
	require.NoError(t, err)
	localA, err := os.ReadFile(localZipA)
	require.NoError(t, err)
	localA[len(localA)/3] ^= 0xff
	require.NoError(t, os.Chmod(localZipA, 0o644))
	require.NoError(t, os.WriteFile(localZipA, localA, 0o644))
	localBAfter, err := os.ReadFile(localZipB)
	require.NoError(t, err)
	require.Equal(t, localB, localBAfter, "one run's downloaded archive must not affect another run")

	makeWritable(t, filepath.Join(runA, "go-mod-cache"))
	require.NoError(t, os.RemoveAll(filepath.Join(runA, "go-mod-cache")))
	require.FileExists(t, extractedB)
	runC := fixture.newRun(t)
	output, err = fixture.prepare(runC, nil)
	require.NoError(t, err, output)
	require.FileExists(t, extractedAssertionPath(runC), "same-size/same-mtime poisoning in run A must not reach run C")
	require.NoError(t, runGo(runC, "clean", "-modcache"))
	require.FileExists(t, extractedB)
	sharedAfter, err := os.ReadFile(sharedZip)
	require.NoError(t, err)
	require.Equal(t, sharedBefore, sharedAfter, "go clean -modcache must not touch the shared download seed")
}

func TestPoisonedSharedArchiveFailsClosed(t *testing.T) {
	t.Parallel()

	fixture := newCacheFixture(t)
	runA := fixture.newRun(t)
	output, err := fixture.prepare(runA, nil)
	require.NoError(t, err, output)
	sharedZip := filepath.Join(fixture.generation(t), "download", "github.com", "stretchr", "testify", "@v", testModuleVersion+".zip")
	info, err := os.Stat(sharedZip)
	require.NoError(t, err)
	archive, err := os.ReadFile(sharedZip)
	require.NoError(t, err)
	require.NotEmpty(t, archive)
	archive[len(archive)/2] ^= 0xff
	require.NoError(t, os.Chmod(sharedZip, 0o644))
	require.NoError(t, os.WriteFile(sharedZip, archive, 0o644))
	require.NoError(t, os.Chtimes(sharedZip, info.ModTime(), info.ModTime()))

	runB := fixture.newRun(t)
	output, err = fixture.prepare(runB, nil)
	require.Error(t, err, output)
	require.True(t,
		strings.Contains(output, "checksum mismatch") || strings.Contains(output, "not a valid zip file"),
		"poisoned same-size archive must fail validation: %s", output)
	require.NoFileExists(t, extractedAssertionPath(runB))
}

func TestPoisonedSharedGoModFailsClosed(t *testing.T) {
	t.Parallel()

	fixture := newCacheFixture(t)
	runA := fixture.newRun(t)
	output, err := fixture.prepare(runA, nil)
	require.NoError(t, err, output)
	sharedGoMod := filepath.Join(fixture.generation(t), "download", "github.com", "stretchr", "testify", "@v", testModuleVersion+".mod")
	moduleFile, err := os.ReadFile(sharedGoMod)
	require.NoError(t, err)
	require.NotEmpty(t, moduleFile)
	moduleFile[len(moduleFile)/2] ^= 0xff
	require.NoError(t, os.Chmod(sharedGoMod, 0o644))
	require.NoError(t, os.WriteFile(sharedGoMod, moduleFile, 0o644))

	runB := fixture.newRun(t)
	output, err = fixture.prepare(runB, nil)
	require.Error(t, err, output)
	require.Contains(t, output, "checksum mismatch")
	require.NoFileExists(t, extractedAssertionPath(runB))
}

func TestSharedZiphashAndUnlistedArtifactsAreNeverReused(t *testing.T) {
	t.Parallel()

	fixture := newCacheFixture(t)
	runA := fixture.newRun(t)
	output, err := fixture.prepare(runA, nil)
	require.NoError(t, err, output)
	generation := fixture.generation(t)
	versionDir := filepath.Join(generation, "download", "github.com", "stretchr", "testify", "@v")
	require.NoError(t, os.WriteFile(filepath.Join(versionDir, testModuleVersion+".ziphash"), []byte("h1:poison\n"), 0o644))
	extraDir := filepath.Join(generation, "download", "example.com", "unlisted", "@v")
	require.NoError(t, os.MkdirAll(extraDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(extraDir, "v1.0.0.zip"), []byte("not a module zip"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(extraDir, "v1.0.0.mod"), []byte("module example.com/unlisted\n"), 0o644))

	runB := fixture.newRun(t)
	output, err = fixture.prepare(runB, nil)
	require.NoError(t, err, output)
	localVersionDir := filepath.Join(runB, "go-mod-cache", "cache", "download", "github.com", "stretchr", "testify", "@v")
	localHash, err := os.ReadFile(filepath.Join(localVersionDir, testModuleVersion+".ziphash"))
	require.NoError(t, err)
	require.Equal(t, testModuleSum, strings.TrimSpace(string(localHash)))
	require.NoFileExists(t, filepath.Join(runB, "go-mod-cache", "cache", "download", "example.com", "unlisted", "@v", "v1.0.0.zip"))
}

func TestDifferentGoSumRejectsVerifiedRunCache(t *testing.T) {
	t.Parallel()

	fixture := newCacheFixture(t)
	runDir := fixture.newRun(t)
	output, err := fixture.prepare(runDir, nil)
	require.NoError(t, err, output)

	candidate := t.TempDir()
	writeModuleFiles(t, candidate, "h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	command := exec.Command("go", "mod", "download")
	command.Dir = candidate
	command.Env = fixture.environment(runDir, "GOPROXY=off")
	outputBytes, err := command.CombinedOutput()
	require.Error(t, err, string(outputBytes))
	require.Contains(t, string(outputBytes), "checksum mismatch")
}

func TestPrivateOrNoSumDBPolicyDisablesSharing(t *testing.T) {
	t.Parallel()

	for _, policy := range []string{"GOPRIVATE=github.com/*", "GONOSUMDB=github.com/*", "GONOPROXY=github.com/*", "GOSUMDB=off"} {
		t.Run(policy, func(t *testing.T) {
			fixture := newCacheFixture(t)
			runDir := fixture.newRun(t)
			output, err := fixture.prepare(runDir, []string{policy})
			require.NoError(t, err, output)
			require.Contains(t, output, "MODULE_DOWNLOAD_CACHE=DISABLED_PRIVATE_OR_NOSUMDB_POLICY")
			require.NoDirExists(t, filepath.Join(fixture.trustedRoot, ".git", "ledger-agent-module-download-cache"))
			require.NoFileExists(t, extractedAssertionPath(runDir))
		})
	}
}

func TestNewCandidateDependencyIsNotInventedByTheSharedSeed(t *testing.T) {
	t.Parallel()

	fixture := newCacheFixture(t)
	runDir := fixture.newRun(t)
	output, err := fixture.prepare(runDir, nil)
	require.NoError(t, err, output)

	candidate := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(candidate, "go.mod"), []byte(
		"module example.com/candidate\n\ngo 1.26.0\n\nrequire rsc.io/quote v1.5.2\n"), 0o644))
	command := exec.Command("go", "mod", "download")
	command.Dir = candidate
	command.Env = fixture.environment(runDir, "GOPROXY=off")
	outputBytes, err := command.CombinedOutput()
	require.Error(t, err, string(outputBytes))
	require.Contains(t, string(outputBytes), "module lookup disabled by GOPROXY=off")
	require.NotContains(t, string(outputBytes), "checksum mismatch")
}

func TestConcurrentPreparationProducesIndependentRuns(t *testing.T) {
	fixture := newCacheFixture(t)
	runs := []string{fixture.newRun(t), fixture.newRun(t)}
	outputs := make([]string, len(runs))
	errors := make([]error, len(runs))
	var waitGroup sync.WaitGroup
	for index := range runs {
		waitGroup.Go(func() {
			outputs[index], errors[index] = fixture.prepare(runs[index], nil)
		})
	}
	waitGroup.Wait()
	for index := range runs {
		require.NoError(t, errors[index], outputs[index])
		require.FileExists(t, extractedAssertionPath(runs[index]))
	}
	require.NotEqual(t, extractedAssertionPath(runs[0]), extractedAssertionPath(runs[1]))
	requireSeedContainsDownloadsOnly(t, fixture.generation(t))
}

func TestValidationWrapperDoesNotExposeSharedCacheAuthority(t *testing.T) {
	t.Parallel()

	fixture := newCacheFixture(t)
	runDir := fixture.newRun(t)
	command := exec.Command("bash", fixture.wrapper, runDir, "env")
	command.Dir = runDir
	command.Env = fixture.environment(runDir, "SHARE_DOWNLOAD_CACHE_ONLY=1")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.NotContains(t, string(output), "SHARE_DOWNLOAD_CACHE_ONLY=")
	require.NotContains(t, string(output), "ledger-agent-module-download-cache")
	require.Contains(t, string(output), "GOMODCACHE="+filepath.Join(runDir, "go-mod-cache"))
}

func newCacheFixture(t *testing.T) cacheFixture {
	t.Helper()

	root := t.TempDir()
	trustedRoot := filepath.Join(root, "trusted")
	require.NoError(t, os.MkdirAll(filepath.Join(trustedRoot, "scripts"), 0o755))
	helperSource, err := filepath.Abs(filepath.Join("..", "agent-module-download-cache"))
	require.NoError(t, err)
	wrapperSource, err := filepath.Abs(filepath.Join("..", "agent-validation-env"))
	require.NoError(t, err)
	helper := filepath.Join(trustedRoot, "scripts", "agent-module-download-cache")
	wrapper := filepath.Join(trustedRoot, "scripts", "agent-validation-env")
	copyFile(t, helperSource, helper, 0o755)
	copyFile(t, wrapperSource, wrapper, 0o755)
	writeModuleFiles(t, trustedRoot, testModuleSum)
	runGit(t, trustedRoot, "init")
	runGit(t, trustedRoot, "config", "user.name", "Module Cache Test")
	runGit(t, trustedRoot, "config", "user.email", "module-cache@example.com")
	runGit(t, trustedRoot, "add", ".")
	runGit(t, trustedRoot, "commit", "-m", "trusted base")

	upstream := filepath.Join(root, "upstream")
	versionDir := filepath.Join(upstream, "github.com", "stretchr", "testify", "@v")
	require.NoError(t, os.MkdirAll(versionDir, 0o755))
	goModCache := strings.TrimSpace(runCommand(t, "", nil, "go", "env", "GOMODCACHE"))
	sourceVersionDir := filepath.Join(goModCache, "cache", "download", "github.com", "stretchr", "testify", "@v")
	for _, suffix := range []string{".info", ".mod", ".zip"} {
		copyFile(t, filepath.Join(sourceVersionDir, testModuleVersion+suffix), filepath.Join(versionDir, testModuleVersion+suffix), 0o644)
	}
	require.NoError(t, os.WriteFile(filepath.Join(versionDir, "list"), []byte(testModuleVersion+"\n"), 0o644))

	return cacheFixture{trustedRoot: trustedRoot, upstream: upstream, helper: helper, wrapper: wrapper}
}

func (fixture cacheFixture) newRun(t *testing.T) string {
	t.Helper()

	runDir := t.TempDir()
	physicalRunDir, err := filepath.EvalSymlinks(runDir)
	require.NoError(t, err)
	for _, directory := range []string{"home", "go-cache", "go-mod-cache", "go-path", "tmp", "cache"} {
		require.NoError(t, os.MkdirAll(filepath.Join(physicalRunDir, directory), 0o755))
	}
	t.Cleanup(func() {
		require.NoError(t, filepath.WalkDir(physicalRunDir, func(walkPath string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}

			return os.Chmod(walkPath, 0o755)
		}))
	})

	return physicalRunDir
}

func (fixture cacheFixture) prepare(runDir string, extraEnvironment []string) (string, error) {
	command := exec.Command("bash", fixture.helper, runDir)
	command.Dir = runDir
	command.Env = fixture.environment(runDir, extraEnvironment...)
	output, err := command.CombinedOutput()

	return string(output), err
}

func (fixture cacheFixture) environment(runDir string, extraEnvironment ...string) []string {
	proxyURL := (&url.URL{Scheme: "file", Path: fixture.upstream}).String()
	values := []string{
		"HOME=" + filepath.Join(runDir, "home"),
		"GOCACHE=" + filepath.Join(runDir, "go-cache"),
		"GOMODCACHE=" + filepath.Join(runDir, "go-mod-cache"),
		"GOPATH=" + filepath.Join(runDir, "go-path"),
		"TMPDIR=" + filepath.Join(runDir, "tmp"),
		"XDG_CACHE_HOME=" + filepath.Join(runDir, "cache"),
		"GOPROXY=" + proxyURL,
		"GOSUMDB=sum.golang.org",
		"GOPRIVATE=",
		"GONOSUMDB=",
		"GONOPROXY=",
		"GOENV=off",
		"GOTOOLCHAIN=local",
	}
	values = append(values, extraEnvironment...)

	return replaceEnvironment(os.Environ(), values)
}

func (fixture cacheFixture) generation(t *testing.T) string {
	t.Helper()

	cacheRoot := filepath.Join(fixture.trustedRoot, ".git", "ledger-agent-module-download-cache")
	var generations []string
	require.NoError(t, filepath.WalkDir(cacheRoot, func(walkPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Name() == "READY" {
			generations = append(generations, filepath.Dir(walkPath))
		}

		return nil
	}))
	require.Len(t, generations, 1)

	return generations[0]
}

func requireSeedContainsDownloadsOnly(t *testing.T, generation string) {
	t.Helper()

	require.NoDirExists(t, filepath.Join(generation, "github.com"), "extracted module trees must not be shared")
	require.NoError(t, filepath.WalkDir(generation, func(walkPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		require.False(t, entry.Type()&os.ModeSymlink != 0, walkPath)
		if entry.IsDir() || entry.Name() == "READY" {
			return nil
		}
		require.True(t, strings.HasSuffix(entry.Name(), ".zip") || strings.HasSuffix(entry.Name(), ".mod"), walkPath)

		return nil
	}))
	require.NoFileExists(t, filepath.Join(generation, "download", "github.com", "stretchr", "testify", "@v", testModuleVersion+".ziphash"))
}

func extractedAssertionPath(runDir string) string {
	return filepath.Join(runDir, "go-mod-cache", "github.com", "stretchr", "testify@"+testModuleVersion, "assert", "assertions.go")
}

func runGo(runDir string, arguments ...string) error {
	command := exec.Command("go", arguments...)
	command.Env = replaceEnvironment(os.Environ(), []string{
		"HOME=" + filepath.Join(runDir, "home"),
		"GOCACHE=" + filepath.Join(runDir, "go-cache"),
		"GOMODCACHE=" + filepath.Join(runDir, "go-mod-cache"),
		"GOPATH=" + filepath.Join(runDir, "go-path"),
		"TMPDIR=" + filepath.Join(runDir, "tmp"),
		"GOENV=off",
	})

	return command.Run()
}

func makeWritable(t *testing.T, root string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(root, func(walkPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		return os.Chmod(walkPath, 0o755)
	}))
}

func writeModuleFiles(t *testing.T, directory, moduleSum string) {
	t.Helper()

	goMod := fmt.Sprintf("module example.com/trusted\n\ngo 1.26.0\n\nrequire %s %s\n", testModulePath, testModuleVersion)
	goSum := fmt.Sprintf("%s %s %s\n%s %s/go.mod %s\n", testModulePath, testModuleVersion, moduleSum, testModulePath, testModuleVersion, testGoModSum)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "go.mod"), []byte(goMod), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "go.sum"), []byte(goSum), 0o644))
}

func copyFile(t *testing.T, source, destination string, mode fs.FileMode) {
	t.Helper()

	content, err := os.ReadFile(source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(destination, content, mode))
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	runCommand(t, directory, nil, "git", arguments...)
}

func runCommand(t *testing.T, directory string, environment []string, name string, arguments ...string) string {
	t.Helper()

	command := exec.Command(name, arguments...)
	command.Dir = directory
	if environment != nil {
		command.Env = environment
	}
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	return string(bytes.TrimSpace(output))
}

func replaceEnvironment(base, replacements []string) []string {
	names := make(map[string]struct{}, len(replacements))
	for _, replacement := range replacements {
		name, _, _ := strings.Cut(replacement, "=")
		names[name] = struct{}{}
	}
	result := make([]string, 0, len(base)+len(replacements))
	for _, value := range base {
		name, _, _ := strings.Cut(value, "=")
		if _, replaced := names[name]; !replaced {
			result = append(result, value)
		}
	}

	return append(result, replacements...)
}
