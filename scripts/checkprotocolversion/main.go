// Command checkprotocolversion enforces the service protocol revision rules
// from docs/technical/architecture/subsystems/api/protocol-compatibility.md
// against a base commit:
//
//   - pkg/grpcprotocol.Version never decreases;
//   - a change that is wire-incompatible for a type reachable from a gated
//     service, as classified by `buf breaking` with misc/proto/buf.yaml,
//     requires a higher Version;
//   - a higher Version requires the operator's mandatoryStopVersions to list
//     the earliest release that can follow the latest tag reachable from
//     HEAD. The operator stops every upgrade that crosses a listed version,
//     so whichever release ships next is a stopped upgrade. Peers forward
//     requests through the gated services, so nodes on different revisions
//     cannot coexist in a rolling upgrade.
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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/formancehq/ledger/v3/scripts/internal/bufplugin"
)

const (
	protocolFile = "pkg/grpcprotocol/protocol.go"
	protoDir     = "misc/proto"
	bufConfig    = "misc/proto/buf.yaml"
	stopFile     = "misc/operator/internal/controller/required_upgrade.go"
	stopVar      = "mandatoryStopVersions"

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

type checker struct {
	repoRoot string
	base     string
	buf      string
	services []string
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
	otherFindings  int
	// nextRelease is the earliest release that can follow the latest
	// reachable tag, and nextStopped whether the operator stops at it. Both
	// are only resolved when the revision increases.
	nextRelease string
	nextStopped bool
}

func main() {
	var base string

	flags := flag.NewFlagSet("checkprotocolversion", flag.ContinueOnError)
	flags.StringVar(&base, "base", "", "commit the service contract is compared against")
	if err := flags.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if base == "" || flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: checkprotocolversion --base <commit>")
		os.Exit(2)
	}

	root, err := gitOutput("", "rev-parse", "--show-toplevel")
	if err != nil {
		fail(fmt.Errorf("locating repository root: %w", err))
	}

	c := checker{
		repoRoot: strings.TrimSpace(string(root)),
		base:     base,
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

	if res.headVersion == res.baseVersion {
		fmt.Printf("check-protocol-version: PASS (revision %d unchanged, no gated wire break, %d internal-only)\n",
			res.headVersion, res.otherFindings)

		return
	}
	fmt.Printf("check-protocol-version: PASS (revision %d to %d with stopped upgrade to %s, %d gated break(s), %d internal-only)\n",
		res.baseVersion, res.headVersion, res.nextRelease, len(res.clientFindings), res.otherFindings)
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "check-protocol-version: %v\n", err)
	fmt.Fprintln(os.Stderr, "check-protocol-version: FAIL")
	os.Exit(1)
}

// verdict applies the revision rule. It is pure so the policy is unit-tested
// independently of git and buf.
func (r result) verdict() error {
	switch {
	case r.headVersion < r.baseVersion:
		return fmt.Errorf("protocol revision decreased from %d to %d; restore %q",
			r.baseVersion, r.headVersion, strconv.Itoa(r.baseVersion))
	case r.headVersion == r.baseVersion && len(r.clientFindings) > 0:
		return fmt.Errorf(
			"%d wire-incompatible change(s) reach a gated service at revision %d: add fields instead of deleting, renumbering or retyping them, or increase the revision in a release listed in %s (%s)",
			len(r.clientFindings), r.baseVersion, stopVar, stopFile)
	case r.headVersion > r.baseVersion && !r.nextStopped:
		return fmt.Errorf(
			"protocol revision changed from %d to %d without a stopped upgrade at the next release: peers on different revisions reject each other's forwarded requests, so add %s to %s (%s) or restore %q",
			r.baseVersion, r.headVersion, r.nextRelease, stopVar, stopFile, strconv.Itoa(r.baseVersion))
	}

	return nil
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

	headSource, err := os.ReadFile(filepath.Join(c.repoRoot, protocolFile))
	if err != nil {
		return res, fmt.Errorf("reading %s: %w", protocolFile, err)
	}
	if res.headVersion, err = parseVersion(headSource); err != nil {
		return res, fmt.Errorf("%s: %w", protocolFile, err)
	}

	if res.headVersion > res.baseVersion {
		if res.nextRelease, res.nextStopped, err = c.nextStop(); err != nil {
			return res, err
		}
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
	for _, item := range findings {
		if isGated(item, index, gated) {
			res.clientFindings = append(res.clientFindings, item)
		} else {
			res.otherFindings++
		}
	}

	return res, nil
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

// nextStop returns the earliest release that can follow the latest tag
// reachable from HEAD, and whether the operator lists it as a stop.
func (c checker) nextStop() (string, bool, error) {
	source, err := os.ReadFile(filepath.Join(c.repoRoot, stopFile))
	if err != nil {
		return "", false, fmt.Errorf("reading %s: %w", stopFile, err)
	}
	stops, err := parseStopVersions(source)
	if err != nil {
		return "", false, fmt.Errorf("%s: %w", stopFile, err)
	}

	tags, err := gitOutput(c.repoRoot, "tag", "--merged", "HEAD", "--list", "v*")
	if err != nil {
		return "", false, fmt.Errorf("listing release tags: %w", err)
	}

	return nextReleaseStop(stops, strings.Fields(string(tags)))
}

type stage int

const (
	stageMarker stage = iota // vX.Y.Z-0, the RFC-0013 version-line marker
	stageRC
	stageFinal
)

// release is a version of the form vX.Y.Z, vX.Y.Z-rc.N or vX.Y.Z-0.
type release struct {
	major, minor, patch int
	stage               stage
	rc                  int
}

var releasePattern = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.([1-9][0-9]*)|(-0))?$`)

func parseRelease(text string) (release, bool) {
	m := releasePattern.FindStringSubmatch(text)
	if m == nil {
		return release{}, false
	}
	r := release{stage: stageFinal}
	r.major, _ = strconv.Atoi(m[1]) // the pattern admits only decimal digits
	r.minor, _ = strconv.Atoi(m[2])
	r.patch, _ = strconv.Atoi(m[3])
	switch {
	case m[4] != "":
		r.stage = stageRC
		r.rc, _ = strconv.Atoi(m[4])
	case m[5] != "":
		r.stage = stageMarker
	}

	return r, true
}

func (r release) String() string {
	switch r.stage {
	case stageMarker:
		return fmt.Sprintf("v%d.%d.%d-0", r.major, r.minor, r.patch)
	case stageRC:
		return fmt.Sprintf("v%d.%d.%d-rc.%d", r.major, r.minor, r.patch, r.rc)
	default:
		return fmt.Sprintf("v%d.%d.%d", r.major, r.minor, r.patch)
	}
}

// less orders releases by semantic version precedence.
func (r release) less(o release) bool {
	return slices.Compare(
		[]int{r.major, r.minor, r.patch, int(r.stage), r.rc},
		[]int{o.major, o.minor, o.patch, int(o.stage), o.rc},
	) < 0
}

// earliestSuccessor is the lowest version a release after r can carry. The
// operator stops every upgrade from r that crosses it, whatever ships next.
func (r release) earliestSuccessor() release {
	switch r.stage {
	case stageMarker:
		return release{r.major, r.minor, r.patch, stageRC, 1}
	case stageRC:
		return release{r.major, r.minor, r.patch, stageRC, r.rc + 1}
	default:
		return release{r.major, r.minor, r.patch + 1, stageRC, 1}
	}
}

// nextReleaseStop returns the earliest release that can follow the latest
// tag, and whether stops lists it. Tags that are not releases or markers,
// such as nightly, are ignored.
func nextReleaseStop(stops, tags []string) (string, bool, error) {
	var latest *release
	for _, tag := range tags {
		r, ok := parseRelease(tag)
		if !ok {
			continue
		}
		if latest == nil || latest.less(r) {
			latest = &r
		}
	}
	if latest == nil {
		return "", false, errors.New("no release tags reachable from HEAD; fetch tags to tell the next release from released ones")
	}
	next := latest.earliestSuccessor()

	stopped := false
	for _, stop := range stops {
		r, ok := parseRelease(stop)
		if !ok || r.stage == stageMarker {
			return "", false, fmt.Errorf("%s entry %q is not vX.Y.Z or vX.Y.Z-rc.N", stopVar, stop)
		}
		stopped = stopped || r == next
	}

	return next.String(), stopped, nil
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
