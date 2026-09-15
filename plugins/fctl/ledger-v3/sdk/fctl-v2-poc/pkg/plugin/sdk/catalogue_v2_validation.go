package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxV2SchemaBytes                      = 256 << 10
	maxV2Commands                         = 2048
	maxV2DocumentationResources           = 128
	maxV2DocumentationReferences          = 16
	maxV2DocumentationMetadataBytes       = 16 << 10
	maxV2BundledDocumentBytes       int64 = 1 << 20
	maxV2BundledDocumentsBytes      int64 = 8 << 20
	maxV2SensitiveOutputs                 = 8
	maxV2InputArtifactBytes         int64 = 64 << 20
	maxV2InputArtifactMediaTypes          = 16
	maxV2DescriptorBytes                  = 1 << 20
	maxV2SchemaDepth                      = 32
	maxV2SchemaNodes                      = 4096
	maxV2SchemaRegexBytes                 = 1024
	maxV2PathSegments                     = 16
	maxV2GrammarFields                    = 64
	maxV2AliasesPerField                  = 16
	maxV2CompletionCandidates             = 128
	maxV2CompletionValueBytes             = 1024
	maxV2CompletionDescriptionBytes       = 2048
)

// ValidateCatalogue validates a complete v2 descriptor as one atomic install
// record. ValidateCommands remains source-compatible with protocol-v1 callers.
func ValidateCatalogue(commands []Command, resources []DocumentationResource) error {
	if len(commands) == 0 || len(commands) > maxV2Commands {
		return fmt.Errorf("catalogue command count is invalid")
	}
	encoded, err := json.Marshal(struct {
		Commands  []Command               `json:"commands"`
		Resources []DocumentationResource `json:"documentation_resources"`
	}{Commands: commands, Resources: resources})
	if err != nil || len(encoded) > maxV2DescriptorBytes {
		return fmt.Errorf("catalogue descriptor size is invalid")
	}
	for _, command := range commands {
		if command.ExecutionKind == "" {
			return fmt.Errorf("command %q has no execution kind", command.ID)
		}
	}
	if err := ValidateCommands(commands); err != nil {
		return err
	}
	if err := validateV2CommandTree(commands); err != nil {
		return err
	}
	resourceByID, err := validateDocumentationResources(resources)
	if err != nil {
		return err
	}
	for _, command := range commands {
		if len(command.DocumentationIDs) > maxV2DocumentationReferences {
			return fmt.Errorf("command %q has too many documentation references", command.ID)
		}
		seen := make(map[string]struct{}, len(command.DocumentationIDs))
		for _, id := range command.DocumentationIDs {
			resource, ok := resourceByID[id]
			if !ok {
				return fmt.Errorf("command %q references unknown documentation %q", command.ID, id)
			}
			if _, exists := seen[id]; exists {
				return fmt.Errorf("command %q repeats documentation %q", command.ID, id)
			}
			seen[id] = struct{}{}
			if command.ExecutionKind == ExecutionKindLocal && len(resource.SupportedMajors) != 0 {
				return fmt.Errorf("local command %q references service documentation %q", command.ID, id)
			}
			if command.ExecutionKind == ExecutionKindService && len(resource.SupportedMajors) == 0 {
				return fmt.Errorf("service command %q references local documentation %q", command.ID, id)
			}
		}
	}
	return nil
}

func validateV2Command(command Command) error {
	if len(command.Path) > maxV2PathSegments || len(command.Arguments) > maxV2GrammarFields || len(command.Flags) > maxV2GrammarFields {
		return fmt.Errorf("command grammar exceeds limits")
	}
	if command.Pagination.Supported {
		if command.ExecutionKind != ExecutionKindService {
			return fmt.Errorf("pagination requires service execution")
		}
		if len(command.Operations) != 1 {
			return fmt.Errorf("pagination requires exactly one declared operation")
		}
	}
	for _, token := range command.Path {
		if reservedHostCommandToken(token) {
			return fmt.Errorf("command path uses reserved host token %q", token)
		}
	}
	for _, aliases := range command.PathAliases {
		if len(aliases) > maxV2AliasesPerField {
			return fmt.Errorf("path aliases exceed limit")
		}
		for _, alias := range aliases {
			if reservedHostCommandToken(alias) {
				return fmt.Errorf("command path alias uses reserved host token %q", alias)
			}
		}
	}
	for _, flag := range command.Flags {
		if len(flag.Aliases) > maxV2AliasesPerField {
			return fmt.Errorf("flag aliases exceed limit")
		}
		for _, token := range append([]string{flag.Name}, flag.Aliases...) {
			if reservedHostCommandToken(token) {
				return fmt.Errorf("command flag uses reserved host token %q", token)
			}
		}
	}
	if command.ExecutionKind != ExecutionKindService && command.ExecutionKind != ExecutionKindLocal {
		return fmt.Errorf("unsupported execution kind %q", command.ExecutionKind)
	}
	if command.Summary == "" || command.Long == "" || command.Example == "" {
		return fmt.Errorf("help metadata is incomplete")
	}
	if containsTerminalControl(command.Summary, false) || containsTerminalControl(command.Long, true) || containsTerminalControl(command.Example, true) {
		return fmt.Errorf("help metadata contains terminal control byte")
	}
	if command.Risk != RiskRead && command.Risk != RiskMutation {
		return fmt.Errorf("unsupported risk %q", command.Risk)
	}
	if err := validateArguments(command.Arguments); err != nil {
		return err
	}
	if err := validateV2Flags(command.Flags); err != nil {
		return err
	}
	if err := validateCompatibility(command.Compatibility); err != nil {
		return err
	}
	if err := validateCommandAuth(command); err != nil {
		return err
	}
	if err := validateCommandExecutionPolicy(command.ExecutionPolicy); err != nil {
		return fmt.Errorf("command execution policy is invalid: %w", err)
	}
	if command.ExecutionKind == ExecutionKindLocal {
		if len(command.Compatibility) != 0 || len(command.Operations) != 0 || command.RequestSigning != nil {
			return fmt.Errorf("local execution declares service policy")
		}
		if kind, err := normalizedTargetKind(command.Target); err != nil || kind != TargetNone {
			return fmt.Errorf("local execution requires target none")
		}
	} else {
		if len(command.Compatibility) == 0 {
			return fmt.Errorf("service execution requires compatibility")
		}
		if err := validateSingleServiceTransport(command.Operations); err != nil {
			return err
		}
		kind, err := normalizedTargetKind(command.Target)
		if err != nil || kind == TargetNone {
			return fmt.Errorf("service execution requires a target")
		}
	}
	if err := validateV2Schemas(command); err != nil {
		return err
	}
	if err := validateSensitiveOutputs(command.SensitiveOutputs); err != nil {
		return err
	}
	if err := validateSensitiveCommandBoundary(command); err != nil {
		return err
	}
	if err := validateInputArtifacts(command); err != nil {
		return err
	}
	return validateRequestSigning(command)
}

func validateSensitiveCommandBoundary(command Command) error {
	if len(command.SensitiveOutputs) == 0 {
		return nil
	}
	if command.ExecutionKind != ExecutionKindService {
		return errors.New("sensitive outputs require service execution")
	}
	if len(command.Operations) != 1 {
		return errors.New("sensitive outputs require exactly one operation")
	}
	if command.OutputMediaType != "application/json" {
		return errors.New("sensitive outputs require JSON output")
	}
	operation := command.Operations[0]
	if operation.GRPC != nil && operation.GRPC.ServerStreaming {
		return errors.New("sensitive outputs forbid streaming")
	}
	if command.Pagination.Supported {
		return errors.New("sensitive outputs forbid pagination")
	}
	if command.ExecutionPolicy == nil || command.ExecutionPolicy.MaxHostRequests != 1 {
		return errors.New("sensitive outputs require exactly one host request")
	}
	if command.ExecutionPolicy.Wait != nil {
		return errors.New("sensitive outputs forbid host wait")
	}
	return nil
}

func validateCommandExecutionPolicy(policy *CommandExecutionPolicy) error {
	if policy == nil {
		return nil
	}
	if policy.MaxHostRequests == 0 || policy.MaxHostRequests > PortableMaxHostRequests {
		return fmt.Errorf("max host requests must be between 1 and %d", PortableMaxHostRequests)
	}
	if policy.Wait == nil {
		return nil
	}
	wait := policy.Wait
	if wait.MaxDelayMilliseconds == 0 || wait.MaxDelayMilliseconds > PortableMaxWaitDelayMilliseconds {
		return fmt.Errorf("max wait delay must be between 1 and %d milliseconds", PortableMaxWaitDelayMilliseconds)
	}
	if wait.MaxWaits == 0 || wait.MaxWaits > PortableMaxWaits {
		return fmt.Errorf("max waits must be between 1 and %d", PortableMaxWaits)
	}
	if wait.MaxAggregateDelayMilliseconds == 0 || wait.MaxAggregateDelayMilliseconds > PortableMaxAggregateDelayMilliseconds {
		return fmt.Errorf("max aggregate wait delay must be between 1 and %d milliseconds", PortableMaxAggregateDelayMilliseconds)
	}
	if wait.MaxDelayMilliseconds > wait.MaxAggregateDelayMilliseconds {
		return errors.New("max wait delay exceeds max aggregate wait delay")
	}
	return nil
}

func validateSingleServiceTransport(operations []OperationPolicy) error {
	// Commands without a product operation remain valid for host-owned
	// service-facing flows. When operations are declared, however, the v4
	// command facet admits only one service and one transport family.
	if len(operations) == 0 {
		return nil
	}
	service := operations[0].Service
	transport := operationTransport(operations[0])
	for _, operation := range operations[1:] {
		if operation.Service != service {
			return errors.New("service command operations must use one service")
		}
		if operationTransport(operation) != transport {
			return errors.New("service command operations must use one transport")
		}
	}
	return nil
}

func operationTransport(operation OperationPolicy) string {
	if operation.HTTP != nil {
		return "http"
	}
	return "grpc"
}

func reservedHostCommandToken(value string) bool {
	return value == "all"
}

func validateCommandAuth(command Command) error {
	switch command.ExecutionKind {
	case ExecutionKindLocal:
		if command.AuthMode != AuthModeNone {
			return fmt.Errorf("local execution requires auth mode %q", AuthModeNone)
		}
		if len(command.Auth) != 0 {
			return fmt.Errorf("auth mode %q requires no auth requirements", AuthModeNone)
		}
	case ExecutionKindService:
		switch command.AuthMode {
		case AuthModeNone:
			if len(command.Auth) != 0 {
				return fmt.Errorf("auth mode %q requires no auth requirements", AuthModeNone)
			}
		case AuthModeCapability:
			if len(command.Auth) != 1 || command.Auth[0].Optional {
				return fmt.Errorf("auth mode %q requires exactly one non-optional auth requirement", AuthModeCapability)
			}
		default:
			return fmt.Errorf("unsupported auth mode %q", command.AuthMode)
		}
	}
	return nil
}

func validateArguments(arguments []Argument) error {
	seen := make(map[string]struct{}, len(arguments))
	optionalSeen := false
	for index, argument := range arguments {
		if !safeCommandToken(argument.Name) || argument.Usage == "" {
			return fmt.Errorf("argument %d is incomplete", index)
		}
		if containsTerminalControl(argument.Usage, false) {
			return fmt.Errorf("argument %q usage contains terminal control byte", argument.Name)
		}
		if _, exists := seen[argument.Name]; exists {
			return fmt.Errorf("argument %q is repeated", argument.Name)
		}
		seen[argument.Name] = struct{}{}
		if argument.Type != ArgumentString && argument.Type != ArgumentStringArray && argument.Type != ArgumentInt32 && argument.Type != ArgumentBool {
			return fmt.Errorf("argument %q has unsupported type %q", argument.Name, argument.Type)
		}
		if argument.Required && optionalSeen {
			return fmt.Errorf("required argument %q follows optional argument", argument.Name)
		}
		repeated := argument.Repeated || argument.Type == ArgumentStringArray
		if !argument.Required || repeated {
			optionalSeen = true
		}
		if repeated && index != len(arguments)-1 {
			return fmt.Errorf("repeated argument %q is not last", argument.Name)
		}
		if err := validateCompletionSpec(argument.Completion); err != nil {
			return fmt.Errorf("argument %q: %w", argument.Name, err)
		}
	}
	return nil
}

func validateV2Flags(flags []Flag) error {
	seen := make(map[string]struct{}, len(flags))
	for _, flag := range flags {
		if flag.Usage == "" {
			return fmt.Errorf("flag %q has no usage", flag.Name)
		}
		if containsTerminalControl(flag.Usage, false) {
			return fmt.Errorf("flag %q usage contains terminal control byte", flag.Name)
		}
		for _, name := range append([]string{flag.Name}, flag.Aliases...) {
			if _, exists := seen[name]; exists {
				return fmt.Errorf("flag name or alias %q is repeated", name)
			}
			seen[name] = struct{}{}
		}
		if flag.Type == FlagStringArray && (flag.HasDefault || flag.DefaultValue != "") {
			return fmt.Errorf("repeated flag %q cannot declare a default", flag.Name)
		}
		if !flag.HasDefault && flag.DefaultValue != "" {
			return fmt.Errorf("flag %q has a value without a default", flag.Name)
		}
		if err := validateCompletionSpec(flag.Completion); err != nil {
			return fmt.Errorf("flag %q: %w", flag.Name, err)
		}
	}
	return nil
}

func validateCompletionSpec(spec CompletionSpec) error {
	if spec.Kind != CompletionNone && spec.Kind != CompletionStatic && spec.Kind != CompletionDynamic {
		return fmt.Errorf("unsupported completion kind %q", spec.Kind)
	}
	if spec.Kind != CompletionStatic && len(spec.Candidates) != 0 {
		return fmt.Errorf("non-static completion declares candidates")
	}
	if len(spec.Candidates) > maxV2CompletionCandidates {
		return fmt.Errorf("completion candidate count exceeds limit")
	}
	seenValues := make(map[string]struct{}, len(spec.Candidates))
	for _, candidate := range spec.Candidates {
		if candidate.Value == "" {
			return fmt.Errorf("completion candidate has no value")
		}
		if len(candidate.Value) > maxV2CompletionValueBytes || len(candidate.Description) > maxV2CompletionDescriptionBytes {
			return fmt.Errorf("completion candidate exceeds byte limit")
		}
		if containsShellCompletionControl(candidate.Value) || containsShellCompletionControl(candidate.Description) {
			return fmt.Errorf("completion candidate contains shell control byte")
		}
		if _, exists := seenValues[candidate.Value]; exists {
			return fmt.Errorf("completion candidate is duplicated")
		}
		seenValues[candidate.Value] = struct{}{}
	}
	return nil
}

func containsShellCompletionControl(value string) bool {
	return containsTerminalControl(value, false)
}

func containsTerminalControl(value string, allowLineFeed bool) bool {
	return strings.IndexFunc(value, func(character rune) bool {
		if allowLineFeed && character == '\n' {
			return false
		}
		return character < 0x20 || character == 0x7f
	}) >= 0
}

func validateCompatibility(compatibility []ServiceCompatibility) error {
	seen := make(map[Service]struct{}, len(compatibility))
	for _, item := range compatibility {
		if !supportedNetworkService(item.Service) || len(item.Majors) == 0 {
			return fmt.Errorf("service compatibility is incomplete")
		}
		if _, exists := seen[item.Service]; exists {
			return fmt.Errorf("service compatibility %q is repeated", item.Service)
		}
		seen[item.Service] = struct{}{}
		previous := uint32(0)
		for index, major := range item.Majors {
			if index > 0 && major <= previous {
				return fmt.Errorf("service compatibility majors are not sorted and unique")
			}
			previous = major
		}
	}
	return nil
}

func supportedNetworkService(service Service) bool {
	return service != ServiceNumscript && supportedService(service)
}

func supportedService(service Service) bool {
	switch service {
	case ServiceMembership, ServiceConnectivity, ServiceLedger, ServiceAuth,
		ServicePayments, ServiceWallets, ServiceReconciliation, ServiceFlows,
		ServiceTransactionPlane, ServiceNumscript, ServiceStudioApps, ServiceBankingBridge:
		return true
	default:
		return false
	}
}

func validateV2Schemas(command Command) error {
	input, err := parseV2Schema("input", command.InputSchema)
	if err != nil {
		return err
	}
	if input["type"] != "object" {
		return fmt.Errorf("input schema root is not an object")
	}
	if closed, ok := input["additionalProperties"].(bool); !ok || closed {
		return fmt.Errorf("input schema must reject additional properties")
	}
	properties, ok := input["properties"].(map[string]any)
	if !ok {
		return fmt.Errorf("input schema has no properties")
	}
	type grammarField struct {
		required     bool
		repeated     bool
		primitive    string
		hasDefault   bool
		defaultValue string
	}
	grammar := make(map[string]grammarField, len(command.Arguments)+len(command.Flags))
	for _, argument := range command.Arguments {
		grammar[argument.Name] = grammarField{required: argument.Required, repeated: argument.Repeated || argument.Type == ArgumentStringArray, primitive: argumentSchemaType(argument.Type)}
	}
	for _, flag := range command.Flags {
		if _, exists := grammar[flag.Name]; exists {
			return fmt.Errorf("argument and flag %q collide", flag.Name)
		}
		grammar[flag.Name] = grammarField{required: flag.Required, repeated: flag.Type == FlagStringArray, primitive: flagSchemaType(flag.Type), hasDefault: flag.HasDefault, defaultValue: flag.DefaultValue}
	}
	if len(properties) != len(grammar) {
		return fmt.Errorf("input schema properties do not match grammar")
	}
	for name, field := range grammar {
		property, exists := properties[name].(map[string]any)
		if !exists {
			return fmt.Errorf("input schema omits grammar field %q", name)
		}
		expectedType := field.primitive
		if field.repeated {
			expectedType = "array"
		}
		if property["type"] != expectedType {
			return fmt.Errorf("input schema type for %q conflicts with grammar", name)
		}
		if field.repeated {
			items, ok := property["items"].(map[string]any)
			if !ok || items["type"] != field.primitive {
				return fmt.Errorf("input schema item type for %q conflicts with grammar", name)
			}
			if field.required && numericKeyword(property["minItems"]) < 1 {
				return fmt.Errorf("input schema cardinality for %q conflicts with grammar", name)
			}
		} else if property["minItems"] != nil || property["maxItems"] != nil {
			return fmt.Errorf("input schema cardinality for %q conflicts with grammar", name)
		}
		defaultValue, hasSchemaDefault := property["default"]
		if hasSchemaDefault != field.hasDefault || (hasSchemaDefault && !schemaDefaultMatches(field.primitive, field.defaultValue, defaultValue)) {
			return fmt.Errorf("input schema default for %q conflicts with grammar", name)
		}
	}
	required := make(map[string]struct{})
	if values, ok := input["required"].([]any); ok {
		for _, value := range values {
			name, ok := value.(string)
			if !ok {
				return fmt.Errorf("input schema required entry is invalid")
			}
			required[name] = struct{}{}
		}
	}
	for name, field := range grammar {
		_, schemaRequired := required[name]
		if schemaRequired != field.required {
			return fmt.Errorf("input schema required state for %q conflicts with grammar", name)
		}
	}
	raw, err := parseV2Schema("raw output", command.RawOutputSchema)
	if err != nil {
		return err
	}
	public, err := parseV2Schema("public output", command.PublicOutputSchema)
	if err != nil {
		return err
	}
	if command.Pagination.Supported && (raw["type"] != "array" || public["type"] != "array") {
		return fmt.Errorf("pagination requires collection output schemas")
	}
	mediaType := command.OutputMediaType
	if mediaType == "" || mediaType == "application/json" {
		if len(command.SensitiveOutputs) == 0 && !bytes.Equal(command.RawOutputSchema, command.PublicOutputSchema) {
			return fmt.Errorf("ordinary JSON output schemas must be identical")
		}
		if len(command.SensitiveOutputs) != 0 && (raw["type"] != "object" || public["type"] != "object") {
			return fmt.Errorf("sensitive JSON output schemas require object roots")
		}
		if raw["type"] == nil || raw["type"] != public["type"] {
			return fmt.Errorf("output schemas conflict with JSON result shape")
		}
	} else if !binarySchemaMatches(raw, mediaType) || !binarySchemaMatches(public, mediaType) {
		return fmt.Errorf("output schemas conflict with binary media type")
	}
	return nil
}

func parseV2Schema(label string, encoded []byte) (map[string]any, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("%s schema size is invalid", label)
	}
	if len(encoded) > maxV2SchemaBytes {
		return nil, fmt.Errorf("%s schema size exceeds limit", label)
	}
	if !utf8.Valid(encoded) {
		return nil, fmt.Errorf("%s schema is not valid UTF-8", label)
	}
	decoded, err := decodeStrictJSON(encoded)
	if err != nil {
		return nil, fmt.Errorf("%s schema is invalid: %w", label, err)
	}
	schema, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s schema root is invalid", label)
	}
	if schema["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		return nil, fmt.Errorf("%s schema dialect is unsupported", label)
	}
	if err := validateSchemaGraph(schema); err != nil {
		return nil, fmt.Errorf("%s schema is invalid: %w", label, err)
	}
	return schema, nil
}

func decodeStrictJSON(encoded []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	value, err := decodeStrictJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

func decodeStrictJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, composite := token.(json.Delim)
	if !composite {
		return token, nil
	}
	switch delim {
	case '{':
		object := map[string]any{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, fmt.Errorf("object key is not a string")
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate object key %q", key)
			}
			value, err := decodeStrictJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
			return nil, fmt.Errorf("unterminated object")
		}
		return object, nil
	case '[':
		var array []any
		for decoder.More() {
			value, err := decodeStrictJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
			return nil, fmt.Errorf("unterminated array")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected delimiter %q", delim)
	}
}

func validateSchemaGraph(schema map[string]any) error {
	nodes := 0
	var walk func(any, int, map[string]struct{}) error
	walk = func(value any, depth int, references map[string]struct{}) error {
		if depth > maxV2SchemaDepth {
			return fmt.Errorf("nesting depth exceeds limit")
		}
		nodes++
		if nodes > maxV2SchemaNodes {
			return fmt.Errorf("node count exceeds limit")
		}
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				if key == "$ref" {
					ref, ok := child.(string)
					if !ok || !strings.HasPrefix(ref, "#/") {
						return fmt.Errorf("remote or malformed reference")
					}
					resolved, ok := resolveSchemaPointer(schema, ref)
					if !ok {
						return fmt.Errorf("unresolved local reference %q", ref)
					}
					if _, exists := references[ref]; exists {
						return fmt.Errorf("reference cycle")
					}
					next := make(map[string]struct{}, len(references)+1)
					for existing := range references {
						next[existing] = struct{}{}
					}
					next[ref] = struct{}{}
					if err := walk(resolved, depth+1, next); err != nil {
						return err
					}
				}
				if (key == "pattern" || key == "patternProperties") && schemaRegexBytes(child) > maxV2SchemaRegexBytes {
					return fmt.Errorf("regex exceeds limit")
				}
				if err := walk(child, depth+1, references); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range typed {
				if err := walk(child, depth+1, references); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(schema, 1, map[string]struct{}{})
}

func resolveSchemaPointer(schema map[string]any, ref string) (any, bool) {
	current := any(schema)
	for _, encoded := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		token := strings.ReplaceAll(strings.ReplaceAll(encoded, "~1", "/"), "~0", "~")
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[token]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func schemaRegexBytes(value any) int {
	switch typed := value.(type) {
	case string:
		return len(typed)
	case map[string]any:
		total := 0
		for key := range typed {
			total += len(key)
		}
		return total
	default:
		return 0
	}
}

func argumentSchemaType(value ArgumentType) string {
	switch value {
	case ArgumentInt32:
		return "integer"
	case ArgumentBool:
		return "boolean"
	default:
		return "string"
	}
}

func flagSchemaType(value FlagType) string {
	switch value {
	case FlagInt32:
		return "integer"
	case FlagBool:
		return "boolean"
	default:
		return "string"
	}
}

func numericKeyword(value any) int64 {
	number, ok := value.(json.Number)
	if !ok {
		return 0
	}
	parsed, _ := number.Int64()
	return parsed
}

func schemaDefaultMatches(kind, encoded string, value any) bool {
	switch kind {
	case "boolean":
		parsed, err := strconv.ParseBool(encoded)
		return err == nil && value == parsed
	case "integer":
		parsed, err := strconv.ParseInt(encoded, 10, 32)
		number, ok := value.(json.Number)
		actual, numberErr := number.Int64()
		return err == nil && ok && numberErr == nil && actual == parsed
	default:
		actual, ok := value.(string)
		return ok && actual == encoded
	}
}

func binarySchemaMatches(schema map[string]any, mediaType string) bool {
	return schema["type"] == "string" && schema["contentEncoding"] == "base64" && schema["contentMediaType"] == mediaType
}

func validateSensitiveOutputs(outputs []SensitiveOutput) error {
	if len(outputs) > maxV2SensitiveOutputs {
		return fmt.Errorf("too many sensitive outputs")
	}
	seen := make(map[string]struct{}, len(outputs))
	for _, output := range outputs {
		if !validJSONPointer(output.JSONPointer) || len(output.AllowedDeliveries) == 0 {
			return fmt.Errorf("sensitive output %q is invalid", output.JSONPointer)
		}
		for existing := range seen {
			if strings.HasPrefix(existing+"/", output.JSONPointer+"/") || strings.HasPrefix(output.JSONPointer+"/", existing+"/") {
				return fmt.Errorf("sensitive outputs overlap")
			}
		}
		seen[output.JSONPointer] = struct{}{}
		deliveries := make(map[SensitiveDelivery]struct{}, len(output.AllowedDeliveries))
		for _, delivery := range output.AllowedDeliveries {
			if delivery != SensitiveDisplayOnce && delivery != SensitiveProfileAuth {
				return fmt.Errorf("sensitive delivery %q is unsupported", delivery)
			}
			if _, exists := deliveries[delivery]; exists {
				return fmt.Errorf("sensitive delivery %q is repeated", delivery)
			}
			deliveries[delivery] = struct{}{}
		}
	}
	return nil
}

func validJSONPointer(value string) bool {
	if value == "" || !strings.HasPrefix(value, "/") {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] != '~' {
			continue
		}
		if index+1 >= len(value) || (value[index+1] != '0' && value[index+1] != '1') {
			return false
		}
		index++
	}
	return true
}

func validateInputArtifacts(command Command) error {
	arguments := make(map[string]struct{}, len(command.Arguments))
	flags := make(map[string]Flag, len(command.Flags))
	for _, argument := range command.Arguments {
		arguments[argument.Name] = struct{}{}
	}
	for _, flag := range command.Flags {
		flags[flag.Name] = flag
	}
	for _, artifact := range command.InputArtifacts {
		if (artifact.ArgumentName == "") == (artifact.FlagName == "") || len(artifact.MediaTypes) == 0 || len(artifact.MediaTypes) > maxV2InputArtifactMediaTypes || artifact.MaxBytes <= 0 || artifact.MaxBytes > maxV2InputArtifactBytes || (!artifact.AllowFile && !artifact.AllowStdin) {
			return fmt.Errorf("input artifact is invalid")
		}
		if artifact.ArgumentName != "" {
			if _, ok := arguments[artifact.ArgumentName]; !ok {
				return fmt.Errorf("input artifact references unknown argument %q", artifact.ArgumentName)
			}
			if artifact.Repeated {
				return fmt.Errorf("repeated input artifact cannot reference an argument")
			}
		}
		if artifact.FlagName != "" {
			flag, ok := flags[artifact.FlagName]
			if !ok {
				return fmt.Errorf("input artifact references unknown flag %q", artifact.FlagName)
			}
			if flag.HasDefault || flag.DefaultValue != "" {
				return fmt.Errorf("input artifact flag %q cannot have a default", artifact.FlagName)
			}
			if artifact.Repeated && flag.Type != FlagStringArray {
				return fmt.Errorf("repeated input artifact requires string-array flag %q", artifact.FlagName)
			}
		}
	}
	return nil
}

func validateRequestSigning(command Command) error {
	if command.RequestSigning == nil {
		return nil
	}
	signing := command.RequestSigning
	if command.ExecutionKind != ExecutionKindService || signing.Capability == "" || signing.ProductMajor == 0 || signing.PayloadType == "" || signing.OperationID == "" || signing.Algorithm != "Ed25519" || !ValidOpaqueProtobufSigningRecipe(signing.Protobuf) {
		return fmt.Errorf("request signing contract is invalid")
	}
	var selected *OperationPolicy
	for index := range command.Operations {
		if command.Operations[index].ID == signing.OperationID {
			if selected != nil {
				return fmt.Errorf("request signing operation is duplicated")
			}
			selected = &command.Operations[index]
		}
	}
	if selected == nil || selected.GRPC == nil || selected.GRPC.FullMethod == "" {
		return fmt.Errorf("request signing operation is invalid")
	}
	if selected.GRPC.GeneratedClient == nil || uint64(selected.GRPC.GeneratedClient.MaxRequestMessageBytes) != signing.Protobuf.MaxSignedMessageBytes {
		return fmt.Errorf("request signing operation ceiling is invalid")
	}
	if len(command.Compatibility) != 1 || command.Compatibility[0].Service != selected.Service || len(command.Compatibility[0].Majors) != 1 || command.Compatibility[0].Majors[0] != signing.ProductMajor {
		return fmt.Errorf("request signing service major is invalid")
	}
	methodOperations := 0
	for _, operation := range command.Operations {
		if operation.GRPC != nil && operation.GRPC.FullMethod == selected.GRPC.FullMethod {
			methodOperations++
		}
	}
	if methodOperations != 1 {
		return fmt.Errorf("request signing method must be unique")
	}
	return nil
}

// ValidOpaqueProtobufSigningRecipe applies the transport-independent structural
// bounds shared by descriptor validation and native/browser signer sessions.
func ValidOpaqueProtobufSigningRecipe(recipe *OpaqueProtobufSigningRecipe) bool {
	if recipe == nil || recipe.MaxPayloadBytes == 0 || recipe.MaxSignedMessageBytes == 0 || recipe.MaxPayloadBytes > recipe.MaxSignedMessageBytes || recipe.MaxSignedMessageBytes > uint64(GeneratedClientMaxMessageBytes) || recipe.MaxKeyIDBytes == 0 || recipe.MaxKeyIDBytes > 1024 || recipe.SignatureLength != 64 {
		return false
	}
	numbers := []uint32{recipe.UnsignedField, recipe.SignedField, recipe.EnvelopeKeyIDField, recipe.EnvelopeSignatureField, recipe.EnvelopePayloadField}
	for _, number := range numbers {
		if !validProtobufFieldNumber(number) {
			return false
		}
	}
	if recipe.UnsignedField == recipe.SignedField || recipe.EnvelopeKeyIDField == recipe.EnvelopeSignatureField || recipe.EnvelopeKeyIDField == recipe.EnvelopePayloadField || recipe.EnvelopeSignatureField == recipe.EnvelopePayloadField {
		return false
	}
	outer := map[uint32]struct{}{recipe.UnsignedField: {}, recipe.SignedField: {}}
	for _, field := range recipe.PassthroughFields {
		if !validProtobufFieldNumber(field.Number) || !validProtobufWireType(field.WireType) {
			return false
		}
		if _, exists := outer[field.Number]; exists {
			return false
		}
		outer[field.Number] = struct{}{}
	}
	return true
}

func validProtobufFieldNumber(number uint32) bool {
	return number != 0 && number < 1<<29 && (number < 19000 || number > 19999)
}

func validProtobufWireType(wireType uint32) bool {
	switch wireType {
	case 0, 1, 2, 5:
		return true
	default:
		return false
	}
}

func containsMajor(majors []uint32, major uint32) bool {
	for _, candidate := range majors {
		if candidate == major {
			return true
		}
	}
	return false
}

func validateV2CommandTree(commands []Command) error {
	tokens := make(map[string]string)
	for _, command := range commands {
		parent := ""
		for index, canonical := range command.Path {
			var aliases []string
			if len(command.PathAliases) != 0 {
				aliases = command.PathAliases[index]
			}
			values := append([]string{canonical}, aliases...)
			for _, value := range values {
				key := parent + "\x00" + value
				if existing, ok := tokens[key]; ok && existing != canonical {
					return fmt.Errorf("command path token %q collides", value)
				}
				tokens[key] = canonical
			}
			parent += "\x00" + canonical
		}
	}
	return nil
}

func validateDocumentationResources(resources []DocumentationResource) (map[string]DocumentationResource, error) {
	if len(resources) > maxV2DocumentationResources {
		return nil, fmt.Errorf("too many documentation resources")
	}
	byID := make(map[string]DocumentationResource, len(resources))
	var bundledBytes int64
	for _, resource := range resources {
		if resource.ID == "" || resource.Title == "" || resource.Description == "" || resource.MediaType == "" || resource.Locale == "" || !supportedDocumentationKind(resource.Kind) {
			return nil, fmt.Errorf("documentation resource is incomplete")
		}
		if containsTerminalControl(resource.Title, false) || containsTerminalControl(resource.Description, false) || containsTerminalControl(resource.BundledPath, false) {
			return nil, fmt.Errorf("documentation resource %q contains terminal control byte", resource.ID)
		}
		if documentationMetadataSemanticBytes(resource) > maxV2DocumentationMetadataBytes {
			return nil, fmt.Errorf("documentation resource %q metadata exceeds limit", resource.ID)
		}
		if !supportedDocumentationMediaType(resource.MediaType) || resource.Locale != "en" {
			return nil, fmt.Errorf("documentation resource %q has unsupported media type or locale", resource.ID)
		}
		if resource.FixtureBytes < 0 {
			return nil, fmt.Errorf("documentation resource %q has invalid fixture bytes", resource.ID)
		}
		if _, exists := byID[resource.ID]; exists {
			return nil, fmt.Errorf("documentation resource %q is repeated", resource.ID)
		}
		if (resource.URL == "") == (resource.BundledPath == "") {
			return nil, fmt.Errorf("documentation resource %q requires exactly one location", resource.ID)
		}
		if resource.URL != "" {
			parsed, err := url.Parse(resource.URL)
			if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
				return nil, fmt.Errorf("documentation resource %q has invalid URL", resource.ID)
			}
		} else {
			if !validBundledPath(resource.BundledPath) {
				return nil, fmt.Errorf("documentation resource %q has invalid bundled path", resource.ID)
			}
			if resource.FixtureBytes > maxV2BundledDocumentBytes {
				return nil, fmt.Errorf("documentation resource %q bundled content exceeds limit", resource.ID)
			}
			bundledBytes += resource.FixtureBytes
		}
		for index, major := range resource.SupportedMajors {
			if index > 0 && major <= resource.SupportedMajors[index-1] {
				return nil, fmt.Errorf("documentation resource %q has invalid majors", resource.ID)
			}
		}
		byID[resource.ID] = resource
	}
	if bundledBytes > maxV2BundledDocumentsBytes {
		return nil, fmt.Errorf("bundled documentation exceeds total limit")
	}
	return byID, nil
}

// documentationMetadataSemanticBytes is independent of JSON encoder escaping.
// Each supported major contributes its fixed uint32 width; document content
// bytes are accounted by FixtureBytes and are not metadata.
func documentationMetadataSemanticBytes(resource DocumentationResource) int {
	return len(resource.ID) + len(resource.Kind) + len(resource.Title) + len(resource.Description) + len(resource.URL) + len(resource.BundledPath) + len(resource.MediaType) + len(resource.Locale) + 4*len(resource.SupportedMajors)
}

func supportedDocumentationKind(kind DocumentationKind) bool {
	switch kind {
	case DocumentationGuide, DocumentationTutorial, DocumentationAPIReference, DocumentationConcept, DocumentationChangelog:
		return true
	default:
		return false
	}
}

func supportedDocumentationMediaType(mediaType string) bool {
	return mediaType == "text/html" || mediaType == "text/markdown"
}

func validBundledPath(value string) bool {
	if value == "" || strings.Contains(value, "\\") || strings.ContainsRune(value, '\x00') || strings.HasPrefix(value, "/") {
		return false
	}
	clean := path.Clean(value)
	return clean == value && clean != "." && !strings.HasPrefix(clean, "../") && !strings.Contains(clean, "/../") && !strings.Contains(clean, "/./")
}

// ValidateExecuteRequest rejects adapter context that contradicts the frozen
// descriptor. Live-version provenance is established by the host before this.
func ValidateExecuteRequest(command Command, request ExecuteRequest) error {
	if err := ValidateContinuationControl(command.Pagination, request.Continuation); err != nil {
		return err
	}
	if command.ExecutionKind == ExecutionKindLocal {
		if request.Target.OrganizationID != "" || request.Target.StackID != "" || len(request.ServiceVersions) != 0 {
			return fmt.Errorf("local execution context contains service state")
		}
		return nil
	}
	if command.ExecutionKind != ExecutionKindService {
		return fmt.Errorf("execution kind is invalid")
	}
	versions := make(map[Service]ServiceVersion, len(request.ServiceVersions))
	for _, version := range request.ServiceVersions {
		if version.Service == "" || version.Version == "" {
			return fmt.Errorf("service version is invalid")
		}
		if _, exists := versions[version.Service]; exists {
			return fmt.Errorf("service version is repeated")
		}
		versions[version.Service] = version
	}
	for _, compatibility := range command.Compatibility {
		version, ok := versions[compatibility.Service]
		if !ok || !containsMajor(compatibility.Majors, version.Major) {
			return fmt.Errorf("service version is incompatible")
		}
	}
	if len(versions) != len(command.Compatibility) {
		return fmt.Errorf("service version context is not exact")
	}
	return nil
}

// ValidateContinuationControl rejects a host choice that contradicts the
// locked descriptor before admission or product traffic.
func ValidateContinuationControl(pagination PaginationSpec, control ContinuationControl) error {
	switch control.Mode {
	case ContinuationUnspecified, ContinuationSinglePage:
		if control.MaxPages != 0 || control.MaxItems != 0 || control.MaxBytes != 0 {
			return fmt.Errorf("single-page continuation contains traversal ceilings")
		}
		return nil
	case ContinuationAllPages:
		if !pagination.Supported {
			return fmt.Errorf("all-pages continuation requires a paginated command")
		}
		if control != AllPagesContinuationControl() {
			return fmt.Errorf("all-pages continuation does not contain the canonical host ceilings")
		}
		return nil
	default:
		return fmt.Errorf("continuation mode is invalid")
	}
}
