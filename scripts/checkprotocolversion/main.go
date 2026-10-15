// Command checkprotocolversion enforces the service protocol revision rule from
// docs/technical/architecture/subsystems/api/protocol-compatibility.md against
// a base commit:
//
//   - pkg/grpcprotocol.Version is a decimal counter that only moves by +1;
//   - a wire-incompatible change to a type reachable from a gated service
//     (as classified by `buf breaking` with misc/proto/buf.yaml) requires
//     that +1 in the same change.
//
// A bump without a detected wire break is accepted: semantic changes keep the
// `.proto` text unchanged and remain a reviewer classification.
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
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	protocolFile = "pkg/grpcprotocol/protocol.go"
	protoDir     = "misc/proto"
	bufConfig    = "misc/proto/buf.yaml"

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

	res, err := c.run()
	if err != nil {
		fail(err)
	}

	if err := res.verdict(); err != nil {
		for _, item := range res.clientFindings {
			fmt.Printf("%s:%d:%d: BREAKING: %s (%s)\n", item.Path, item.StartLine, item.StartColumn, item.Message, item.Type)
		}
		fmt.Printf("%s:1:1: ERROR: %v\n", protocolFile, err)
		fmt.Fprintln(os.Stderr, "check-protocol-version: FAIL")
		os.Exit(1)
	}

	fmt.Printf("check-protocol-version: PASS (revision %d -> %d, %d gated wire break(s), %d internal-only)\n",
		res.baseVersion, res.headVersion, len(res.clientFindings), res.otherFindings)
	if res.headVersion == res.baseVersion+1 && len(res.clientFindings) == 0 {
		fmt.Println("check-protocol-version: NOTE: revision bumped without a detected wire break; state the semantic change in the PR")
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "check-protocol-version: %v\n", err)
	fmt.Fprintln(os.Stderr, "check-protocol-version: FAIL")
	os.Exit(1)
}

// verdict applies the revision rule. It is pure so the policy is unit-tested
// independently of git and buf.
func (r result) verdict() error {
	switch delta := r.headVersion - r.baseVersion; {
	case delta < 0:
		return fmt.Errorf("protocol revision decreased from %d to %d; it is a monotonic counter", r.baseVersion, r.headVersion)
	case delta > 1:
		return fmt.Errorf("protocol revision jumped from %d to %d; increment by exactly one from the target branch", r.baseVersion, r.headVersion)
	case delta == 0 && len(r.clientFindings) > 0:
		return fmt.Errorf("%d wire-incompatible change(s) reach a gated service; set Version to %q",
			len(r.clientFindings), strconv.Itoa(r.baseVersion+1))
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

	tmp, err := os.MkdirTemp("", "checkprotocolversion-")
	if err != nil {
		return res, fmt.Errorf("creating temporary directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmp) }() // best-effort cleanup of scratch data

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
	name      string
	startLine int // 1-based, inclusive
	endLine   int // 1-based, inclusive
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
				spans = append(spans, declSpan{name: name, startLine: int(span[0]) + 1, endLine: int(span[2]) + 1})
			case sourceSpanSameLineWidth:
				spans = append(spans, declSpan{name: name, startLine: int(span[0]) + 1, endLine: int(span[0]) + 1})
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

// innermost returns the smallest declaration enclosing line.
func (s spanIndex) innermost(file string, line int) (string, bool) {
	candidates := make([]declSpan, 0, 4)
	for _, span := range s[file] {
		if span.startLine <= line && line <= span.endLine {
			candidates = append(candidates, span)
		}
	}
	if len(candidates) == 0 {
		return "", false
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].endLine-candidates[i].startLine < candidates[j].endLine-candidates[j].startLine
	})

	return candidates[0].name, true
}

// isGated decides whether a finding can change what a gated-service peer
// decodes. A finding that cannot be attributed to a declaration (for example
// a deleted top-level type) counts when its file declares any gated type:
// over-reporting costs a revision bump, under-reporting a silent mis-decode.
func isGated(item bufFinding, index spanIndex, gated gatedSet) bool {
	file := strings.TrimPrefix(filepath.ToSlash(item.Path), protoDir+"/")
	if name, ok := index.innermost(file, item.StartLine); ok {
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
