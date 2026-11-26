// Command checkprotocolversion enforces the frozen service protocol revision
// from docs/technical/architecture/subsystems/api/protocol-compatibility.md
// against a base commit:
//
//   - pkg/grpcprotocol.Version must not change;
//   - no change may be wire-incompatible for a type reachable from a gated
//     service, as classified by `buf breaking` with misc/proto/buf.yaml,
//     unless misc/proto/wire-exceptions.json lists it;
//   - a listed exception names an operator mandatoryStopVersions entry that
//     the next build of the target branch crosses, so nodes on either side
//     of the change never forward to each other;
//   - a listed exception is never removed or changed, so its stop keeps
//     protecting clusters that upgrade from before the change.
//
// Semantic changes that keep the `.proto` text unchanged remain a reviewer
// classification.
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/formancehq/ledger/v3/scripts/internal/bufplugin"
)

const (
	protocolFile = "pkg/grpcprotocol/protocol.go"
	protoDir     = "misc/proto"
	bufConfig    = "misc/proto/buf.yaml"
	// exceptionsFile is data so that its content at the base is readable.
	exceptionsFile = "misc/proto/wire-exceptions.json"
	stopFile       = "misc/operator/internal/controller/required_upgrade.go"
	stopVar        = "mandatoryStopVersions"

	// bufFindingsExitCode is the exit status `buf breaking` uses when it
	// reports findings; any other non-zero status is a tool failure.
	bufFindingsExitCode = 100
)

// gatedServices are the services whose RPCs require the protocol revision.
// Keep in sync with the ServiceServer gate described in
// protocol-compatibility.md ("Wire contract and failure behavior").
func gatedServices() []string {
	return []string{"ledger.BucketService", "cluster.ClusterService", "restore.RestoreService"}
}

// wireException is a wire-incompatible change to a gated type that ships at
// an unchanged revision. Clients built before it misdecode the type.
type wireException struct {
	// Declaration is the fully qualified message, enum or service.
	Declaration string `json:"declaration"`
	// Rule is the buf rule.
	Rule string `json:"rule"`
	// Message is the buf finding text up to its documentation link. It
	// names the field and both types.
	Message string `json:"message"`
	// Stop is the mandatoryStopVersions entry that keeps nodes on either
	// side of the change from forwarding to each other.
	Stop string `json:"stop"`
}

// parseExceptions reads the exceptions file. Adding an entry requires the
// review in protocol-compatibility.md ("Wire exceptions").
func parseExceptions(data []byte) ([]wireException, error) {
	var file struct {
		Exceptions []wireException `json:"exceptions"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, err
	}
	for i, exception := range file.Exceptions {
		if exception.Declaration == "" || exception.Rule == "" || exception.Message == "" {
			return nil, fmt.Errorf("exception %d needs a declaration, a rule and a message", i)
		}
		if _, ok := parseTag(exception.Stop); !ok {
			return nil, fmt.Errorf("exception %d: stop %q is not a canonical version", i, exception.Stop)
		}
	}

	return file.Exceptions, nil
}

type checker struct {
	repoRoot string
	base     string
	// baseRef is the branch the change targets, when known.
	baseRef  string
	buf      string
	services []string
	// exceptions is the worktree's list, loaded by run.
	exceptions []wireException
	// pluginRoot is the repository providing tools/buf-plugin-ledger, which
	// misc/proto/buf.yaml requires on PATH for every buf command.
	pluginRoot string
	env        []string
}

type bufFinding struct {
	Path        string `json:"path"`
	StartLine   int    `json:"start_line"`
	StartColumn int    `json:"start_column"`
	Type        string `json:"type"`
	Message     string `json:"message"`
}

type result struct {
	baseVersion    int
	headVersion    int
	clientFindings []bufFinding
	// excepted counts the findings of this change that the exceptions file
	// lists.
	excepted        int
	otherFindings   int
	exceptionErrors []error
}

func main() {
	var base, baseRef string

	flags := flag.NewFlagSet("checkprotocolversion", flag.ContinueOnError)
	flags.StringVar(&base, "base", "", "commit the service contract is compared against")
	flags.StringVar(&baseRef, "base-ref", "", "branch the change targets; a release/vX.Y branch ships only its own version line")
	if err := flags.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if base == "" || flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: checkprotocolversion --base <commit> [--base-ref <branch>]")
		os.Exit(2)
	}

	root, err := gitOutput("", "rev-parse", "--show-toplevel")
	if err != nil {
		fail(fmt.Errorf("locating repository root: %w", err))
	}

	c := checker{
		repoRoot: strings.TrimSpace(string(root)),
		base:     base,
		baseRef:  baseRef,
		buf:      "buf",
		services: gatedServices(),
	}
	c.pluginRoot = c.repoRoot

	res, err := c.run()
	if err != nil {
		fail(err)
	}

	if err := res.verdict(); err != nil {
		for _, item := range res.clientFindings {
			fmt.Printf("%s:%d:%d: BREAKING: %s (%s)\n", item.Path, item.StartLine, item.StartColumn, item.Message, item.Type)
		}
		for line := range strings.SplitSeq(err.Error(), "\n") {
			fmt.Printf("%s:1:1: ERROR: %s\n", protocolFile, line)
		}
		fmt.Fprintln(os.Stderr, "check-protocol-version: FAIL")
		os.Exit(1)
	}

	fmt.Printf("check-protocol-version: PASS (revision %d unchanged, no unlisted gated wire break, %d listed exception(s), %d internal-only)\n",
		res.headVersion, res.excepted, res.otherFindings)
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "check-protocol-version: %v\n", err)
	fmt.Fprintln(os.Stderr, "check-protocol-version: FAIL")
	os.Exit(1)
}

// verdict applies the revision rule. It is pure so the policy is unit-tested
// independently of git and buf.
func (r result) verdict() error {
	var errs []error
	if r.headVersion != r.baseVersion {
		errs = append(errs, fmt.Errorf(
			"protocol revision changed from %d to %d; it is frozen during the v3 release-candidate phase: restore %q and keep the contract compatible, or escalate",
			r.baseVersion, r.headVersion, strconv.Itoa(r.baseVersion)))
	}
	if len(r.clientFindings) > 0 {
		errs = append(errs, fmt.Errorf(
			"%d wire-incompatible change(s) reach a gated service; the revision is frozen: add fields instead of deleting, renumbering or retyping them, or escalate",
			len(r.clientFindings)))
	}

	return errors.Join(append(errs, r.exceptionErrors...)...)
}

func (c checker) run() (result, error) {
	var res result

	baseSource, err := gitOutput(c.repoRoot, "show", c.base+":"+protocolFile)
	if err != nil {
		return res, fmt.Errorf("reading %s at %s: %w", protocolFile, c.base, err)
	}
	if res.baseVersion, err = parseVersion(baseSource); err != nil {
		return res, fmt.Errorf("%s at %s: %w", protocolFile, c.base, err)
	}

	var removed []wireException
	if c.exceptions, removed, err = c.loadExceptions(); err != nil {
		return res, err
	}
	for _, exception := range removed {
		res.exceptionErrors = append(res.exceptionErrors, fmt.Errorf(
			"the wire exception for %s (stop %s) was removed or changed in %s: exceptions are permanent, because the stop keeps protecting clusters that upgrade from before the change",
			exception.Declaration, exception.Stop, exceptionsFile))
	}

	headSource, err := os.ReadFile(filepath.Join(c.repoRoot, protocolFile))
	if err != nil {
		return res, fmt.Errorf("reading %s: %w", protocolFile, err)
	}
	if res.headVersion, err = parseVersion(headSource); err != nil {
		return res, fmt.Errorf("%s: %w", protocolFile, err)
	}

	tmp, err := os.MkdirTemp("", "checkprotocolversion-")
	if err != nil {
		return res, fmt.Errorf("creating temporary directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmp) }() // best-effort cleanup of scratch data

	if c.env, err = bufplugin.Environment(c.pluginRoot, filepath.Join(tmp, "bin")); err != nil {
		return res, err
	}

	baseTree := filepath.Join(tmp, "base")
	if err := c.extractBaseProtos(baseTree); err != nil {
		return res, err
	}
	// Both sides use the current configuration so a base commit without (or
	// with an older) buf.yaml is classified under today's rules.
	if err := copyConfig(filepath.Join(c.repoRoot, bufConfig), filepath.Join(baseTree, bufConfig)); err != nil {
		return res, err
	}

	headImage, err := c.buildImage(filepath.Join(c.repoRoot, protoDir), filepath.Join(tmp, "head.binpb"))
	if err != nil {
		return res, err
	}

	gated, err := reachableTypes(headImage, c.services)
	if err != nil {
		return res, err
	}

	findings, err := c.breaking(filepath.Join(baseTree, protoDir))
	if err != nil {
		return res, err
	}

	index := newSpanIndex(headImage)
	var introduced []wireException
	for _, item := range findings {
		if !isGated(item, index, gated) {
			res.otherFindings++
		} else if exception, ok := c.exceptionFor(item, index); ok {
			res.excepted++
			introduced = append(introduced, exception)
		} else {
			res.clientFindings = append(res.clientFindings, item)
		}
	}

	stopErrors, err := c.exceptionStopErrors(introduced)
	if err != nil {
		return res, err
	}
	res.exceptionErrors = append(res.exceptionErrors, stopErrors...)

	return res, nil
}

// loadExceptions returns the worktree's exceptions and the base's exceptions
// that the worktree no longer lists unchanged. A missing file lists nothing.
func (c checker) loadExceptions() (listed, removed []wireException, err error) {
	data, err := os.ReadFile(filepath.Join(c.repoRoot, exceptionsFile))
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return nil, nil, fmt.Errorf("reading %s: %w", exceptionsFile, err)
	default:
		if listed, err = parseExceptions(data); err != nil {
			return nil, nil, fmt.Errorf("%s: %w", exceptionsFile, err)
		}
	}

	tracked, err := gitOutput(c.repoRoot, "ls-tree", "--name-only", c.base, "--", exceptionsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("looking up %s at %s: %w", exceptionsFile, c.base, err)
	}
	if len(bytes.TrimSpace(tracked)) == 0 {
		return listed, nil, nil
	}
	data, err = gitOutput(c.repoRoot, "show", c.base+":"+exceptionsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("reading %s at %s: %w", exceptionsFile, c.base, err)
	}
	before, err := parseExceptions(data)
	if err != nil {
		return nil, nil, fmt.Errorf("%s at %s: %w", exceptionsFile, c.base, err)
	}
	for _, exception := range before {
		if !slices.Contains(listed, exception) {
			removed = append(removed, exception)
		}
	}

	return listed, removed, nil
}

// exceptionFor returns the exception that lists a gated finding.
func (c checker) exceptionFor(item bufFinding, index spanIndex) (wireException, bool) {
	file := strings.TrimPrefix(filepath.ToSlash(item.Path), protoDir+"/")
	name, ok := index.innermost(file, item.StartLine, item.StartColumn)
	if !ok {
		return wireException{}, false
	}
	message, _, _ := strings.Cut(item.Message, " See https://")
	i := slices.IndexFunc(c.exceptions, func(e wireException) bool {
		return e.Declaration == name && e.Rule == item.Type && e.Message == message
	})
	if i < 0 {
		return wireException{}, false
	}

	return c.exceptions[i], true
}

// exceptionStopErrors checks the stops of the listed exceptions. Every stop
// must stay in the operator's list. The stop of an exception introduced by
// the checked change must also be crossed by the next build of the branch.
func (c checker) exceptionStopErrors(introduced []wireException) ([]error, error) {
	if len(c.exceptions) == 0 {
		return nil, nil
	}

	source, err := os.ReadFile(filepath.Join(c.repoRoot, stopFile))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", stopFile, err)
	}
	listed, err := parseStopVersions(source)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", stopFile, err)
	}

	var errs []error
	for _, stop := range exceptionStops(c.exceptions) {
		if !slices.Contains(listed, stop) {
			errs = append(errs, fmt.Errorf(
				"a wire exception relies on the stopped upgrade %s, which %s (%s) does not list: nodes on either side of the change would forward to each other in a rolling upgrade",
				stop, stopVar, stopFile))
		}
	}
	if len(introduced) == 0 {
		return errs, nil
	}

	lower, floor, err := c.nextBuildBounds()
	if err != nil {
		return nil, err
	}
	for _, name := range exceptionStops(introduced) {
		stop, ok := parseTag(name)
		if !ok {
			// parseExceptions rejects such an entry.
			return nil, fmt.Errorf("wire exception stop %q is not a canonical version", name)
		}
		if (lower != nil && !lower.LessThan(stop)) || floor.LessThan(stop) {
			errs = append(errs, fmt.Errorf(
				"the wire exception stop %s does not separate the latest release from the next build of the target branch: use v%s, the lowest version that build can be deployed as",
				name, floor))
		}
	}

	return errs, nil
}

// exceptionStops returns the distinct stops of some exceptions, in order.
func exceptionStops(exceptions []wireException) []string {
	var stops []string
	for _, exception := range exceptions {
		if !slices.Contains(stops, exception.Stop) {
			stops = append(stops, exception.Stop)
		}
	}

	return stops
}

// nextBuildBounds returns the latest release below the next build of the
// target branch, if any, and the lowest version that build can be deployed
// as. A stop S separates the two when lower < S <= floor, the operator's
// rule. The target's own history places it: a tag that only the checked
// change carries is not a release of the target branch.
func (c checker) nextBuildBounds() (lower, floor *semver.Version, err error) {
	reachable, err := c.versionTags("--merged", c.base)
	if err != nil {
		return nil, nil, err
	}
	reachable = slices.DeleteFunc(reachable, func(tag *semver.Version) bool { return !onReleaseLine(c.baseRef, tag) })
	if len(reachable) == 0 {
		return nil, nil, errors.New("no version tag of the target branch's line is reachable from the base commit; fetch tags to place the branch among the releases")
	}
	if floor, err = deployFloor(slices.MaxFunc(reachable, (*semver.Version).Compare)); err != nil {
		return nil, nil, err
	}

	tags, err := c.versionTags()
	if err != nil {
		return nil, nil, err
	}
	for _, tag := range tags {
		// A release at or above the floor is not an upgrade source for this
		// branch.
		if tag.LessThan(floor) && (lower == nil || lower.LessThan(tag)) {
			lower = tag
		}
	}

	return lower, floor, nil
}

// versionTags lists the canonical version tags selected by the extra
// `git tag` arguments.
func (c checker) versionTags(args ...string) ([]*semver.Version, error) {
	output, err := gitOutput(c.repoRoot, append([]string{"tag", "--list", "v*"}, args...)...)
	if err != nil {
		return nil, fmt.Errorf("listing version tags: %w", err)
	}

	var tags []*semver.Version
	for name := range strings.FieldsSeq(string(output)) {
		if tag, ok := parseTag(name); ok {
			tags = append(tags, tag)
		}
	}

	return tags, nil
}

// markerPrerelease is the prerelease of an RFC-0013 version-line marker.
const markerPrerelease = "0"

var (
	rcPrerelease  = regexp.MustCompile(`^rc\.([1-9][0-9]*)$`)
	releaseBranch = regexp.MustCompile(`^(?:refs/heads/)?release/v(0|[1-9][0-9]*)(?:\.(0|[1-9][0-9]*))?$`)
)

// onReleaseLine reports whether a build of the target branch can follow a
// tag. A release/vX.Y or release/vX branch ships only its own version line,
// which matters once the next line's marker sits on the branch point.
func onReleaseLine(baseRef string, tag *semver.Version) bool {
	line := releaseBranch.FindStringSubmatch(baseRef)
	if line == nil {
		return true
	}
	if strconv.FormatUint(tag.Major(), 10) != line[1] {
		return false
	}

	return line[2] == "" || strconv.FormatUint(tag.Minor(), 10) == line[2]
}

// parseTag accepts the canonical vX.Y.Z[-prerelease] form the operator
// requires of image tags.
func parseTag(name string) (*semver.Version, bool) {
	text, ok := strings.CutPrefix(name, "v")
	if !ok {
		return nil, false
	}
	version, err := semver.StrictNewVersion(text)
	if err != nil || version.Metadata() != "" || version.String() != text {
		return nil, false
	}

	return version, true
}

// deployFloor returns the lowest version the operator can deploy from a
// build that follows the latest tag. See
// docs/technical/contributing/release-versioning.md for the build versions.
func deployFloor(latest *semver.Version) (*semver.Version, error) {
	core := fmt.Sprintf("%d.%d.%d", latest.Major(), latest.Minor(), latest.Patch())
	prerelease := latest.Prerelease()

	var floor string
	switch {
	case prerelease == "":
		floor = fmt.Sprintf("%d.%d.%d-%s", latest.Major(), latest.Minor(), latest.Patch()+1, markerPrerelease)
	case prerelease == markerPrerelease:
		return latest, nil
	case rcPrerelease.MatchString(prerelease):
		// From an RC, the operator rejects every -rc target except the next RC.
		ordinal, err := strconv.ParseUint(rcPrerelease.FindStringSubmatch(prerelease)[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("tag v%s: %w", latest, err)
		}
		floor = fmt.Sprintf("%s-rc.%d", core, ordinal+1)
	default:
		floor = core + "-" + prerelease + ".0"
	}

	version, err := semver.StrictNewVersion(floor)
	if err != nil {
		return nil, fmt.Errorf("deriving the version after v%s: %w", latest, err)
	}

	return version, nil
}

// parseStopVersions reads the mandatory stop versions without compiling the
// operator module.
func parseStopVersions(source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), stopFile, source, 0)
	if err != nil {
		return nil, fmt.Errorf("parsing: %w", err)
	}

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if name.Name != stopVar || i >= len(value.Values) {
					continue
				}

				return stringSliceLiteral(value.Values[i])
			}
		}
	}

	return nil, fmt.Errorf("the %s variable was not found", stopVar)
}

func stringSliceLiteral(expr ast.Expr) ([]string, error) {
	composite, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, fmt.Errorf("%s must be a []string literal", stopVar)
	}
	if array, ok := composite.Type.(*ast.ArrayType); !ok || array.Len != nil {
		return nil, fmt.Errorf("%s must be a []string literal", stopVar)
	} else if elem, ok := array.Elt.(*ast.Ident); !ok || elem.Name != "string" {
		return nil, fmt.Errorf("%s must be a []string literal", stopVar)
	}

	out := make([]string, 0, len(composite.Elts))
	for _, elt := range composite.Elts {
		lit, ok := elt.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return nil, fmt.Errorf("%s entries must be string literals", stopVar)
		}
		text, err := strconv.Unquote(lit.Value)
		if err != nil {
			return nil, fmt.Errorf("unquoting a %s entry: %w", stopVar, err)
		}
		out = append(out, text)
	}

	return out, nil
}

// parseVersion reads the Version constant without compiling the package, so
// any historical revision of the file can be inspected.
func parseVersion(source []byte) (int, error) {
	file, err := parser.ParseFile(token.NewFileSet(), protocolFile, source, 0)
	if err != nil {
		return 0, fmt.Errorf("parsing: %w", err)
	}

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if name.Name != "Version" || i >= len(value.Values) {
					continue
				}
				lit, ok := value.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return 0, errors.New("the Version constant must be a string literal")
				}
				text, err := strconv.Unquote(lit.Value)
				if err != nil {
					return 0, fmt.Errorf("unquoting Version: %w", err)
				}
				version, err := strconv.Atoi(text)
				if err != nil || version < 1 || strconv.Itoa(version) != text {
					return 0, fmt.Errorf("the Version constant %q is not a canonical positive decimal counter", text)
				}

				return version, nil
			}
		}
	}

	return 0, errors.New("the Version constant was not found")
}

// extractBaseProtos materializes misc/proto at the base commit. Archive entry
// names are validated and every write goes through a root-scoped handle.
func (c checker) extractBaseProtos(dest string) error {
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", dest, err)
	}
	root, err := os.OpenRoot(dest)
	if err != nil {
		return fmt.Errorf("opening %s: %w", dest, err)
	}
	defer func() { _ = root.Close() }() // read-only use after extraction

	archive, err := gitOutput(c.repoRoot, "archive", "--format=tar", c.base, protoDir)
	if err != nil {
		return fmt.Errorf("archiving %s at %s: %w", protoDir, c.base, err)
	}

	reader := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading archive: %w", err)
		}
		name := filepath.FromSlash(strings.TrimSuffix(header.Name, "/"))
		if !filepath.IsLocal(name) {
			return fmt.Errorf("archive entry %q escapes the extraction root", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := root.MkdirAll(name, 0o755); err != nil {
				return fmt.Errorf("creating %s: %w", name, err)
			}
		case tar.TypeReg:
			if err := root.MkdirAll(filepath.Dir(name), 0o755); err != nil {
				return fmt.Errorf("creating %s: %w", filepath.Dir(name), err)
			}
			if err := writeRootFile(root, name, reader); err != nil {
				return err
			}
		case tar.TypeXGlobalHeader:
			// git archive stores the commit id here; it carries no file.
		default:
			return fmt.Errorf("archive entry %q has unsupported type %q", header.Name, header.Typeflag)
		}
	}
}

func writeRootFile(root *os.Root, name string, content io.Reader) error {
	file, err := root.Create(name)
	if err != nil {
		return fmt.Errorf("creating %s: %w", name, err)
	}
	if _, err := io.Copy(file, content); err != nil {
		_ = file.Close() // the copy error is the one worth reporting

		return fmt.Errorf("writing %s: %w", name, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", name, err)
	}

	return nil
}

func copyConfig(from, to string) error {
	content, err := os.ReadFile(from)
	if err != nil {
		return fmt.Errorf("reading %s: %w", from, err)
	}
	if err := os.WriteFile(to, content, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", to, err)
	}

	return nil
}

func (c checker) buildImage(dir, out string) (*descriptorpb.FileDescriptorSet, error) {
	cmd := exec.Command(c.buf, "build", dir, "-o", out)
	cmd.Dir = c.repoRoot
	cmd.Env = c.env
	if output, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("buf build %s: %w\n%s", dir, err, output)
	}

	raw, err := os.ReadFile(out)
	if err != nil {
		return nil, fmt.Errorf("reading buf image: %w", err)
	}
	var image descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &image); err != nil {
		return nil, fmt.Errorf("decoding buf image: %w", err)
	}

	return &image, nil
}

func (c checker) breaking(againstDir string) ([]bufFinding, error) {
	cmd := exec.Command(c.buf, "breaking", protoDir, "--against", againstDir, "--error-format", "json")
	cmd.Dir = c.repoRoot
	cmd.Env = c.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != bufFindingsExitCode {
			return nil, fmt.Errorf("buf breaking: %w\n%s%s", err, stdout.Bytes(), stderr.Bytes())
		}
	}

	var findings []bufFinding
	scanner := bufio.NewScanner(&stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var item bufFinding
		if err := json.Unmarshal(line, &item); err != nil {
			return nil, fmt.Errorf("decoding buf finding %q: %w", line, err)
		}
		findings = append(findings, item)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading buf output: %w", err)
	}

	return findings, nil
}

// gatedSet holds the fully qualified names (without leading dot) of the gated
// services and every message or enum they transitively reference, plus the
// files declaring them.
type gatedSet struct {
	names map[string]bool
	files map[string]bool
}

func reachableTypes(image *descriptorpb.FileDescriptorSet, services []string) (gatedSet, error) {
	set := gatedSet{names: map[string]bool{}, files: map[string]bool{}}
	messages := map[string]*descriptorpb.DescriptorProto{}
	declaredIn := map[string]string{}
	found := map[string]bool{}
	var queue []string

	wanted := map[string]bool{}
	for _, name := range services {
		wanted[name] = true
	}

	for _, file := range image.GetFile() {
		pkg := file.GetPackage()
		walkTypes(file, func(name string, message *descriptorpb.DescriptorProto) {
			declaredIn[name] = file.GetName()
			if message != nil {
				messages[name] = message
			}
		})
		for _, service := range file.GetService() {
			name := qualify(pkg, service.GetName())
			if !wanted[name] {
				continue
			}
			found[name] = true
			set.names[name] = true
			set.files[file.GetName()] = true
			for _, method := range service.GetMethod() {
				queue = append(queue, strings.TrimPrefix(method.GetInputType(), "."), strings.TrimPrefix(method.GetOutputType(), "."))
			}
		}
	}

	for _, name := range services {
		if !found[name] {
			return set, fmt.Errorf("gated service %s is not declared in %s; update gatedServices()", name, protoDir)
		}
	}

	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if set.names[name] {
			continue
		}
		file, ok := declaredIn[name]
		if !ok {
			// Well-known and imported types outside misc/proto are not part
			// of the image's own declarations; they cannot change here.
			continue
		}
		set.names[name] = true
		set.files[file] = true
		for _, field := range messages[name].GetField() {
			if ref := field.GetTypeName(); ref != "" {
				queue = append(queue, strings.TrimPrefix(ref, "."))
			}
		}
	}

	return set, nil
}

// walkTypes visits every message and enum declared in a file, nested ones
// included. message is nil for enums.
func walkTypes(file *descriptorpb.FileDescriptorProto, visit func(name string, message *descriptorpb.DescriptorProto)) {
	var walkMessage func(prefix string, message *descriptorpb.DescriptorProto)
	walkMessage = func(prefix string, message *descriptorpb.DescriptorProto) {
		name := qualify(prefix, message.GetName())
		visit(name, message)
		for _, enum := range message.GetEnumType() {
			visit(qualify(name, enum.GetName()), nil)
		}
		for _, nested := range message.GetNestedType() {
			walkMessage(name, nested)
		}
	}

	for _, enum := range file.GetEnumType() {
		visit(qualify(file.GetPackage(), enum.GetName()), nil)
	}
	for _, message := range file.GetMessageType() {
		walkMessage(file.GetPackage(), message)
	}
}

func qualify(prefix, name string) string {
	if prefix == "" {
		return name
	}

	return prefix + "." + name
}

// declSpan is the source range of one message, enum or service declaration.
type declSpan struct {
	name        string
	startLine   int // 1-based, inclusive
	startColumn int // 1-based, inclusive
	endLine     int // 1-based, inclusive
	endColumn   int // 1-based, exclusive on endLine
}

// spanIndex maps a file to its type declarations so a buf finding location
// can be attributed to the innermost enclosing type.
type spanIndex map[string][]declSpan

// Descriptor field numbers used in SourceCodeInfo paths.
const (
	fileMessageTypeField    = 4
	fileEnumTypeField       = 5
	fileServiceField        = 6
	messageNestedTypeField  = 3
	messageEnumTypeField    = 4
	sourceSpanWithEndLine   = 4
	sourceSpanSameLineWidth = 3
)

func newSpanIndex(image *descriptorpb.FileDescriptorSet) spanIndex {
	index := spanIndex{}
	for _, file := range image.GetFile() {
		names := map[string]string{}
		pkg := file.GetPackage()

		var walkMessage func(path []int32, prefix string, message *descriptorpb.DescriptorProto)
		walkMessage = func(path []int32, prefix string, message *descriptorpb.DescriptorProto) {
			name := qualify(prefix, message.GetName())
			names[pathKey(path)] = name
			for i, enum := range message.GetEnumType() {
				names[pathKey(appendPath(path, messageEnumTypeField, i))] = qualify(name, enum.GetName())
			}
			for i, nested := range message.GetNestedType() {
				walkMessage(appendPath(path, messageNestedTypeField, i), name, nested)
			}
		}
		for i, message := range file.GetMessageType() {
			walkMessage([]int32{fileMessageTypeField, int32(i)}, pkg, message)
		}
		for i, enum := range file.GetEnumType() {
			names[pathKey([]int32{fileEnumTypeField, int32(i)})] = qualify(pkg, enum.GetName())
		}
		for i, service := range file.GetService() {
			names[pathKey([]int32{fileServiceField, int32(i)})] = qualify(pkg, service.GetName())
		}

		var spans []declSpan
		for _, location := range file.GetSourceCodeInfo().GetLocation() {
			name, ok := names[pathKey(location.GetPath())]
			if !ok {
				continue
			}
			span := location.GetSpan()
			switch len(span) {
			case sourceSpanWithEndLine:
				spans = append(spans, declSpan{
					name: name, startLine: int(span[0]) + 1, startColumn: int(span[1]) + 1,
					endLine: int(span[2]) + 1, endColumn: int(span[3]) + 1,
				})
			case sourceSpanSameLineWidth:
				spans = append(spans, declSpan{
					name: name, startLine: int(span[0]) + 1, startColumn: int(span[1]) + 1,
					endLine: int(span[0]) + 1, endColumn: int(span[2]) + 1,
				})
			}
		}
		index[file.GetName()] = spans
	}

	return index
}

func appendPath(path []int32, field int32, i int) []int32 {
	out := make([]int32, 0, len(path)+2)
	out = append(out, path...)

	return append(out, field, int32(i))
}

func pathKey(path []int32) string {
	parts := make([]string, len(path))
	for i, part := range path {
		parts[i] = strconv.Itoa(int(part))
	}

	return strings.Join(parts, ".")
}

// innermost returns the smallest declaration enclosing a source position.
func (s spanIndex) innermost(file string, line, column int) (string, bool) {
	var best *declSpan
	for _, span := range s[file] {
		if line < span.startLine || line > span.endLine ||
			(line == span.startLine && column < span.startColumn) ||
			(line == span.endLine && column >= span.endColumn) {
			continue
		}
		if best == nil ||
			span.startLine > best.startLine ||
			(span.startLine == best.startLine && span.startColumn > best.startColumn) ||
			(span.startLine == best.startLine && span.startColumn == best.startColumn && span.endLine < best.endLine) ||
			(span.startLine == best.startLine && span.startColumn == best.startColumn && span.endLine == best.endLine && span.endColumn < best.endColumn) {
			current := span
			best = &current
		}
	}
	if best == nil {
		return "", false
	}

	return best.name, true
}

// isGated decides whether a finding can change what a gated-service peer
// decodes. A finding that cannot be attributed to a declaration (for example
// a deleted top-level type) counts when its file declares any gated type:
// over-reporting costs a revision bump, under-reporting a silent mis-decode.
func isGated(item bufFinding, index spanIndex, gated gatedSet) bool {
	file := strings.TrimPrefix(filepath.ToSlash(item.Path), protoDir+"/")
	if name, ok := index.innermost(file, item.StartLine, item.StartColumn); ok {
		return gated.names[name]
	}

	return gated.files[file]
}

func gitOutput(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}

	return out, nil
}
