package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

const (
	requiredCIContractPath = ".github/required-ci.json"
	requiredCIWorkflowPath = ".github/workflows/main.yml"
)

type finding struct {
	path    string
	line    int
	column  int
	message string
}

func main() {
	fuzzInventoryOnly := len(os.Args) == 2 && os.Args[1] == "--fuzz-inventory"
	if len(os.Args) > 1 && !fuzzInventoryOnly {
		fmt.Fprintln(os.Stderr, "usage: check-repo-invariants [--fuzz-inventory]")
		os.Exit(2)
	}

	files, err := repositoryFiles()
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-repo-invariants: listing repository files: %v\n", err)
		os.Exit(1)
	}

	failed := false
	if !fuzzInventoryOnly {
		topologyFindings, err := checkRequiredCIRepository(files)
		if err != nil {
			fmt.Fprintf(os.Stderr, "check-repo-invariants: checking Required CI topology: %v\n", err)
			failed = true
		} else {
			for _, item := range topologyFindings {
				printFinding(item)
				failed = true
			}
		}

		for _, path := range files {
			findings, err := checkFile(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "check-repo-invariants: checking %s: %v\n", path, err)
				failed = true

				continue
			}

			for _, item := range findings {
				printFinding(item)
				failed = true
			}
		}
	}

	fuzzFindings, err := checkFuzzInventory(files)
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-repo-invariants: checking fuzz inventory: %v\n", err)
		failed = true
	} else {
		for _, item := range fuzzFindings {
			printFinding(item)
			failed = true
		}
	}

	if failed {
		fmt.Fprintln(os.Stderr, "check-repo-invariants: FAIL")
		os.Exit(1)
	}

	fmt.Println("check-repo-invariants: PASS")
}

func printFinding(item finding) {
	fmt.Printf("%s:%d:%d: ERROR: %s\n", item.path, item.line, item.column, item.message)
}

type requiredCIContract struct {
	AggregateJob string            `json:"aggregateJob"`
	OptionalJobs map[string]string `json:"optionalJobs"`
}

type parsedWorkflow struct {
	path             string
	root             *yaml.Node
	pullRequest      *yaml.Node
	pullRequestKey   *yaml.Node
	jobs             map[string]*yaml.Node
	jobKeys          map[string]*yaml.Node
	jobsNode         *yaml.Node
	pullRequestFound bool
}

func checkRequiredCIRepository(files []string) ([]finding, error) {
	contractSource, err := os.ReadFile(requiredCIContractPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", requiredCIContractPath, err)
	}

	workflows := make(map[string][]byte)
	for _, path := range files {
		if !isWorkflowPath(path) {
			continue
		}

		source, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return nil, fmt.Errorf("reading workflow %s: %w", path, err)
		}

		workflows[path] = source
	}

	return checkRequiredCITopology(workflows, contractSource)
}

func checkRequiredCITopology(
	workflowSources map[string][]byte,
	contractSource []byte,
) ([]finding, error) {
	var contract requiredCIContract
	if err := json.Unmarshal(contractSource, &contract); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", requiredCIContractPath, err)
	}

	if strings.TrimSpace(contract.AggregateJob) == "" {
		return []finding{contractFinding("REQUIRED_CI_CONTRACT: aggregateJob must not be empty")}, nil
	}

	if contract.OptionalJobs == nil {
		return []finding{contractFinding("REQUIRED_CI_CONTRACT: optionalJobs must be an object")}, nil
	}

	parsed := make(map[string]*parsedWorkflow, len(workflowSources))
	for path, source := range workflowSources {
		workflow, err := parseWorkflow(path, source)
		if err != nil {
			return nil, err
		}

		parsed[path] = workflow
	}

	primary, ok := parsed[requiredCIWorkflowPath]
	if !ok {
		return []finding{contractFinding(
			"REQUIRED_CI_WORKFLOW_MISSING: " + requiredCIWorkflowPath,
		)}, nil
	}

	var findings []finding
	findings = append(findings, checkRequiredCITrigger(primary)...)

	aggregate, ok := primary.jobs[contract.AggregateJob]
	if !ok {
		findings = append(findings, yamlFinding(
			primary.path,
			primary.jobsNode,
			"REQUIRED_CI_AGGREGATE_MISSING: "+contract.AggregateJob,
		))

		return append(findings, classifyWorkflowJobs(parsed, contract, nil)...), nil
	}

	findings = append(findings, checkRequiredCIJob(primary, aggregate, contract)...)
	needs, needsNode := workflowJobNeeds(aggregate)
	needSet := make(map[string]struct{}, len(needs))
	for _, need := range needs {
		if _, duplicate := needSet[need]; duplicate {
			findings = append(findings, yamlFinding(
				primary.path,
				needsNode,
				"DUPLICATE_REQUIRED_CI_NEED: "+need,
			))
		}

		needSet[need] = struct{}{}
		if _, exists := primary.jobs[need]; !exists {
			findings = append(findings, yamlFinding(
				primary.path,
				needsNode,
				"UNKNOWN_REQUIRED_CI_NEED: "+need,
			))
		}

		qualified := qualifiedJob(primary.path, need)
		if _, optional := contract.OptionalJobs[qualified]; optional {
			findings = append(findings, yamlFinding(
				primary.path,
				needsNode,
				"OPTIONAL_PR_JOB_AGGREGATED: "+qualified,
			))
		}
	}

	findings = append(findings, classifyWorkflowJobs(parsed, contract, needSet)...)

	return findings, nil
}

func parseWorkflow(path string, source []byte) (*parsedWorkflow, error) {
	var document yaml.Node
	if err := yaml.Unmarshal(source, &document); err != nil {
		return nil, fmt.Errorf("parsing workflow %s: %w", path, err)
	}

	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("parsing workflow %s: expected a top-level mapping", path)
	}

	root := document.Content[0]
	_, onNode := yamlMappingValue(root, "on")
	pullRequestKey, pullRequest := yamlMappingValue(onNode, "pull_request")
	_, jobsNode := yamlMappingValue(root, "jobs")
	if jobsNode == nil || jobsNode.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("parsing workflow %s: jobs must be a mapping", path)
	}

	jobs := make(map[string]*yaml.Node, len(jobsNode.Content)/2)
	jobKeys := make(map[string]*yaml.Node, len(jobsNode.Content)/2)
	for index := 0; index < len(jobsNode.Content); index += 2 {
		key := jobsNode.Content[index]
		value := jobsNode.Content[index+1]
		jobs[key.Value] = value
		jobKeys[key.Value] = key
	}

	return &parsedWorkflow{
		path:             path,
		root:             root,
		pullRequest:      pullRequest,
		pullRequestKey:   pullRequestKey,
		jobs:             jobs,
		jobKeys:          jobKeys,
		jobsNode:         jobsNode,
		pullRequestFound: yamlTriggerContains(onNode, "pull_request"),
	}, nil
}

func checkRequiredCITrigger(workflow *parsedWorkflow) []finding {
	if !workflow.pullRequestFound {
		return []finding{yamlFinding(
			workflow.path,
			workflow.root,
			"REQUIRED_CI_PULL_REQUEST_TRIGGER_MISSING",
		)}
	}

	if workflow.pullRequest == nil || workflow.pullRequest.Kind == yaml.ScalarNode && workflow.pullRequest.Tag == "!!null" {
		return nil
	}

	if workflow.pullRequest.Kind != yaml.MappingNode {
		return []finding{yamlFinding(
			workflow.path,
			workflow.pullRequestKey,
			"REQUIRED_CI_PULL_REQUEST_TRIGGER_INVALID: expected a mapping or null",
		)}
	}

	var findings []finding
	for _, filter := range []string{"branches", "branches-ignore", "paths", "paths-ignore"} {
		key, _ := yamlMappingValue(workflow.pullRequest, filter)
		if key != nil {
			findings = append(findings, yamlFinding(
				workflow.path,
				key,
				"REQUIRED_CI_PULL_REQUEST_FILTER_FORBIDDEN: "+filter,
			))
		}
	}

	_, typesNode := yamlMappingValue(workflow.pullRequest, "types")
	if typesNode == nil {
		return findings
	}

	types := yamlStringList(typesNode)
	typeSet := make(map[string]struct{}, len(types))
	for _, eventType := range types {
		typeSet[eventType] = struct{}{}
	}

	for _, requiredType := range []string{"opened", "synchronize", "reopened"} {
		if _, ok := typeSet[requiredType]; !ok {
			findings = append(findings, yamlFinding(
				workflow.path,
				typesNode,
				"REQUIRED_CI_PULL_REQUEST_EVENT_MISSING: "+requiredType,
			))
		}
	}

	return findings
}

func checkRequiredCIJob(
	workflow *parsedWorkflow,
	aggregate *yaml.Node,
	contract requiredCIContract,
) []finding {
	var findings []finding
	_, nameNode := yamlMappingValue(aggregate, "name")
	if nameNode == nil || nameNode.Value != "Required CI" {
		findings = append(findings, yamlFinding(
			workflow.path,
			nameNode,
			"REQUIRED_CI_CHECK_NAME: aggregate job name must be exactly Required CI",
		))
	}

	_, ifNode := yamlMappingValue(aggregate, "if")
	if ifNode == nil || strings.TrimSpace(ifNode.Value) != "always()" {
		findings = append(findings, yamlFinding(
			workflow.path,
			ifNode,
			"REQUIRED_CI_IF: aggregate job must use if: always()",
		))
	}

	_, needsNode := yamlMappingValue(aggregate, "needs")
	if needsNode == nil || len(yamlStringList(needsNode)) == 0 {
		findings = append(findings, yamlFinding(
			workflow.path,
			needsNode,
			"REQUIRED_CI_NEEDS_EMPTY",
		))
	}

	_, envNode := yamlMappingValue(aggregate, "env")
	_, resultsNode := yamlMappingValue(envNode, "REQUIRED_CI_NEEDS")
	if resultsNode == nil || strings.TrimSpace(resultsNode.Value) != "${{ toJSON(needs) }}" {
		findings = append(findings, yamlFinding(
			workflow.path,
			resultsNode,
			"REQUIRED_CI_RESULTS_INPUT: REQUIRED_CI_NEEDS must be exactly ${{ toJSON(needs) }}",
		))
	}

	_, stepsNode := yamlMappingValue(aggregate, "steps")
	if !yamlTreeContainsScalar(stepsNode, "nix develop --command bash scripts/required-ci") {
		findings = append(findings, yamlFinding(
			workflow.path,
			stepsNode,
			"REQUIRED_CI_GATE_COMMAND_MISSING: nix develop --command bash scripts/required-ci",
		))
	}

	aggregateKey := qualifiedJob(workflow.path, contract.AggregateJob)
	if _, optional := contract.OptionalJobs[aggregateKey]; optional {
		findings = append(findings, contractFinding(
			"REQUIRED_CI_AGGREGATE_OPTIONAL: "+aggregateKey,
		))
	}

	return findings
}

func classifyWorkflowJobs(
	workflows map[string]*parsedWorkflow,
	contract requiredCIContract,
	needs map[string]struct{},
) []finding {
	var findings []finding
	seenOptional := make(map[string]struct{}, len(contract.OptionalJobs))

	for path, workflow := range workflows {
		if path != requiredCIWorkflowPath && !workflow.pullRequestFound {
			continue
		}

		for jobID := range workflow.jobs {
			qualified := qualifiedJob(path, jobID)
			if path == requiredCIWorkflowPath && jobID == contract.AggregateJob {
				continue
			}

			if _, optional := contract.OptionalJobs[qualified]; optional {
				seenOptional[qualified] = struct{}{}

				continue
			}

			if path == requiredCIWorkflowPath {
				if _, required := needs[jobID]; required {
					continue
				}
			}

			findings = append(findings, yamlFinding(
				path,
				workflow.jobKeys[jobID],
				"UNAGGREGATED_PR_JOB: "+qualified,
			))
		}
	}

	for optional, reason := range contract.OptionalJobs {
		if strings.TrimSpace(reason) == "" {
			findings = append(findings, contractFinding(
				"OPTIONAL_PR_JOB_REASON_MISSING: "+optional,
			))
		}

		if _, seen := seenOptional[optional]; !seen {
			findings = append(findings, contractFinding(
				"STALE_OPTIONAL_PR_JOB: "+optional,
			))
		}
	}

	return findings
}

func workflowJobNeeds(job *yaml.Node) ([]string, *yaml.Node) {
	_, needsNode := yamlMappingValue(job, "needs")

	return yamlStringList(needsNode), needsNode
}

func yamlMappingValue(mapping *yaml.Node, key string) (*yaml.Node, *yaml.Node) {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil, nil
	}

	for index := 0; index < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index], mapping.Content[index+1]
		}
	}

	return nil, nil
}

func yamlTriggerContains(node *yaml.Node, trigger string) bool {
	if node == nil {
		return false
	}

	switch node.Kind {
	case yaml.MappingNode:
		key, _ := yamlMappingValue(node, trigger)

		return key != nil
	case yaml.SequenceNode:
		for _, item := range node.Content {
			if item.Value == trigger {
				return true
			}
		}
	case yaml.ScalarNode:
		return node.Value == trigger
	}

	return false
}

func yamlStringList(node *yaml.Node) []string {
	if node == nil {
		return nil
	}

	switch node.Kind {
	case yaml.ScalarNode:
		if node.Tag == "!!null" {
			return nil
		}

		return []string{node.Value}
	case yaml.SequenceNode:
		values := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			values = append(values, item.Value)
		}

		return values
	default:
		return nil
	}
}

func yamlTreeContainsScalar(node *yaml.Node, value string) bool {
	if node == nil {
		return false
	}

	if node.Kind == yaml.ScalarNode && strings.TrimSpace(node.Value) == value {
		return true
	}

	for _, child := range node.Content {
		if yamlTreeContainsScalar(child, value) {
			return true
		}
	}

	return false
}

func yamlFinding(path string, node *yaml.Node, message string) finding {
	line := 1
	column := 1
	if node != nil {
		line = node.Line
		column = node.Column
	}

	return finding{path: path, line: line, column: column, message: message}
}

func contractFinding(message string) finding {
	return finding{path: requiredCIContractPath, line: 1, column: 1, message: message}
}

func qualifiedJob(path, jobID string) string {
	return path + "#" + jobID
}

func isWorkflowPath(path string) bool {
	return strings.HasPrefix(path, ".github/workflows/") &&
		(strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml"))
}

func repositoryFiles() ([]string, error) {
	cmd := exec.Command("git", "ls-files", "--cached", "--others", "--exclude-standard", "-z")

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running git ls-files: %w", err)
	}

	parts := bytes.Split(output, []byte{0})
	files := make([]string, 0, len(parts))

	for _, part := range parts {
		if len(part) == 0 {
			continue
		}

		files = append(files, string(part))
	}

	return files, nil
}

func checkFile(path string) ([]finding, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("stating file: %w", err)
	}

	if !info.Mode().IsRegular() {
		return nil, nil
	}

	source, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	switch {
	case path == defaultCIWorkflowPath:
		return checkModelWorkloadTestReachability(path, source)
	case strings.HasSuffix(path, ".go"):
		return checkGoSource(path, source)
	case isProtoPath(path):
		return checkProtoSource(path, source), nil
	default:
		return nil, nil
	}
}

func checkGoSource(path string, source []byte) ([]finding, error) {
	checkSleep := strings.HasSuffix(path, "_test.go")
	checkEnvironment := isDeterministicFSMPath(path) && !checkSleep
	if !checkSleep && !checkEnvironment {
		return nil, nil
	}

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, source, 0)
	if err != nil {
		return nil, fmt.Errorf("parsing Go source: %w", err)
	}

	var (
		timeNames = map[string]struct{}{}
		osNames   = map[string]struct{}{}
		timeDot   bool
		osDot     bool
	)

	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, fmt.Errorf("decoding import %q: %w", spec.Path.Value, err)
		}

		name := filepath.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}

		switch importPath {
		case "time":
			timeDot = name == "."
			if name != "." && name != "_" {
				timeNames[name] = struct{}{}
			}
		case "os":
			osDot = name == "."
			if name != "." && name != "_" {
				osNames[name] = struct{}{}
			}
		}
	}

	var findings []finding

	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		if checkSleep && callsImportedFunction(call.Fun, timeNames, timeDot, "Sleep") {
			findings = append(findings, goFinding(
				fileSet,
				path,
				call.Fun.Pos(),
				"tests must not use time.Sleep; use deterministic synchronization or require.Eventually",
			))
		}

		if checkEnvironment && callsAnyImportedFunction(
			call.Fun,
			osNames,
			osDot,
			"Getenv",
			"LookupEnv",
			"Environ",
		) {
			findings = append(findings, goFinding(
				fileSet,
				path,
				call.Fun.Pos(),
				"deterministic FSM paths must not read node-local environment state directly; gate local policy in admission instead",
			))
		}

		return true
	})

	return findings, nil
}

func callsAnyImportedFunction(
	expression ast.Expr,
	packageNames map[string]struct{},
	dotImport bool,
	functionNames ...string,
) bool {
	for _, name := range functionNames {
		if callsImportedFunction(expression, packageNames, dotImport, name) {
			return true
		}
	}

	return false
}

func callsImportedFunction(
	expression ast.Expr,
	packageNames map[string]struct{},
	dotImport bool,
	functionName string,
) bool {
	switch expression := expression.(type) {
	case *ast.SelectorExpr:
		identifier, ok := expression.X.(*ast.Ident)
		if !ok || identifier.Obj != nil || expression.Sel.Name != functionName {
			return false
		}

		_, ok = packageNames[identifier.Name]

		return ok
	case *ast.Ident:
		return dotImport && expression.Obj == nil && expression.Name == functionName
	default:
		return false
	}
}

func goFinding(fileSet *token.FileSet, path string, position token.Pos, message string) finding {
	location := fileSet.Position(position)

	return finding{
		path:    path,
		line:    location.Line,
		column:  location.Column,
		message: message,
	}
}

func checkProtoSource(path string, source []byte) []finding {
	masked := maskProtoCommentsAndStrings(source)
	declaration := regexp.MustCompile(`(?m)(?:^|[;{}])[ \t\r\n]*reserved(?:[ \t\r\n]|$)`)

	var findings []finding

	for _, indexes := range declaration.FindAllIndex(masked, -1) {
		segment := masked[indexes[0]:indexes[1]]
		relative := bytes.Index(segment, []byte("reserved"))
		if relative < 0 {
			continue
		}

		offset := indexes[0] + relative
		line, column := lineAndColumn(source, offset)
		findings = append(findings, finding{
			path:    path,
			line:    line,
			column:  column,
			message: "Ledger v3 is unreleased; protobuf reserved declarations are intentionally forbidden by AGENTS.md",
		})
	}

	return findings
}

func maskProtoCommentsAndStrings(source []byte) []byte {
	masked := append([]byte(nil), source...)

	const (
		stateCode = iota
		stateLineComment
		stateBlockComment
		stateString
	)

	state := stateCode
	var quote byte

	for index := 0; index < len(masked); index++ {
		current := masked[index]

		switch state {
		case stateCode:
			switch {
			case current == '/' && index+1 < len(masked) && masked[index+1] == '/':
				masked[index] = ' '
				masked[index+1] = ' '
				index++
				state = stateLineComment
			case current == '/' && index+1 < len(masked) && masked[index+1] == '*':
				masked[index] = ' '
				masked[index+1] = ' '
				index++
				state = stateBlockComment
			case current == '\'' || current == '"':
				quote = current
				masked[index] = ' '
				state = stateString
			}
		case stateLineComment:
			if current == '\n' {
				state = stateCode
			} else {
				masked[index] = ' '
			}
		case stateBlockComment:
			if current == '*' && index+1 < len(masked) && masked[index+1] == '/' {
				masked[index] = ' '
				masked[index+1] = ' '
				index++
				state = stateCode
			} else if current != '\n' {
				masked[index] = ' '
			}
		case stateString:
			switch {
			case current == '\\' && index+1 < len(masked):
				masked[index] = ' '
				index++
				if masked[index] != '\n' {
					masked[index] = ' '
				}
			case current == quote:
				masked[index] = ' '
				state = stateCode
			case current != '\n':
				masked[index] = ' '
			}
		}
	}

	return masked
}

func lineAndColumn(source []byte, offset int) (int, int) {
	line := bytes.Count(source[:offset], []byte{'\n'}) + 1
	lastNewline := bytes.LastIndex(source[:offset], []byte{'\n'})

	return line, offset - lastNewline
}

func isDeterministicFSMPath(path string) bool {
	for _, prefix := range []string{
		"internal/infra/state/",
		"internal/infra/plan/",
		"internal/infra/preload/",
		"internal/domain/processing/",
	} {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

func isProtoPath(path string) bool {
	return strings.HasPrefix(path, "misc/proto/") && strings.HasSuffix(path, ".proto")
}
