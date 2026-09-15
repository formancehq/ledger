package sdk

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	GeneratedClientMaxMessageBytes             int64  = 4 << 20
	GeneratedClientHTTPMaxRequestBytes         int64  = 4 << 20
	GeneratedClientHTTPMaxResponseBytes        int64  = 4 << 20
	GeneratedClientHTTPMaxPathBytes                   = 4096
	GeneratedClientHTTPMaxContentTypeBytes            = 4096
	GeneratedClientHTTPMaxQueryPairs           uint32 = 128
	GeneratedClientHTTPMaxQueryKeyBytes               = 1024
	GeneratedClientHTTPMaxQueryValueBytes             = 8192
	GeneratedClientHTTPMaxQueryAggregateBytes         = 32768
	GeneratedClientHTTPMaxHeaderValues         uint32 = 32
	GeneratedClientHTTPMaxHeaderNameBytes             = 128
	GeneratedClientHTTPMaxHeaderValueBytes            = 8192
	GeneratedClientHTTPMaxHeaderAggregateBytes        = 16384
	GeneratedClientMaxResponseMessages         uint32 = 1024
	GeneratedClientMaxAggregateResponseBytes   int64  = 16 << 20
)

var forbiddenHTTPHeaders = map[string]struct{}{
	"authorization": {}, "proxy-authorization": {}, "cookie": {}, "set-cookie": {},
	"host": {}, "connection": {}, "transfer-encoding": {}, "te": {}, "trailer": {},
	"upgrade": {}, "forwarded": {}, "content-type": {}, "content-length": {},
}

var discardedHTTPHeaders = map[string]struct{}{
	"user-agent": {}, "accept-encoding": {}, "expect": {}, "via": {},
}

func validateGeneratedHTTPPolicy(policy HTTPOperationPolicy) error {
	if policy.Method == "" || policy.Method != strings.ToUpper(policy.Method) || !isHTTPToken(policy.Method) {
		return fmt.Errorf("invalid method")
	}
	generated := policy.GeneratedClient
	if generated == nil {
		return fmt.Errorf("generated-client envelope is required")
	}
	if (policy.Path == "") == (generated.PathTemplate == "") {
		return fmt.Errorf("exactly one path or path template is required")
	}
	if policy.Path != "" {
		if _, err := normalizePolicyPath(policy.Path, false); err != nil {
			return err
		}
	} else if _, err := normalizePolicyPath(generated.PathTemplate, true); err != nil {
		return err
	}
	if generated.MaxRequestBytes <= 0 || generated.MaxRequestBytes > GeneratedClientHTTPMaxRequestBytes {
		return fmt.Errorf("request limit is outside the common profile")
	}
	if err := validateResponseLimits(generated.ResponseLimits, GeneratedClientHTTPMaxResponseBytes, 1, true); err != nil {
		return err
	}
	seenContentTypes := make(map[string]struct{}, len(generated.RequestContentTypes))
	for _, value := range generated.RequestContentTypes {
		canonical, err := canonicalContentType(value)
		if err != nil {
			return fmt.Errorf("invalid request content type: %w", err)
		}
		if _, exists := seenContentTypes[canonical]; exists {
			return fmt.Errorf("duplicate request content type")
		}
		seenContentTypes[canonical] = struct{}{}
	}
	seenHeaders := make(map[string]struct{}, len(generated.RequestHeaders))
	for _, name := range generated.RequestHeaders {
		if len(name) == 0 || len(name) > GeneratedClientHTTPMaxHeaderNameBytes || !isHTTPToken(name) {
			return fmt.Errorf("invalid request header")
		}
		canonical := strings.ToLower(name)
		if _, exists := seenHeaders[canonical]; exists {
			return fmt.Errorf("duplicate request header")
		}
		seenHeaders[canonical] = struct{}{}
		if _, forbidden := forbiddenHTTPHeaders[canonical]; forbidden || strings.HasPrefix(canonical, "x-forwarded-") {
			return fmt.Errorf("forbidden request header")
		}
		if _, discarded := discardedHTTPHeaders[canonical]; discarded {
			return fmt.Errorf("transport-only request header")
		}
	}
	return nil
}

// ValidateGeneratedClientOperationPolicy validates one command-v4 operation
// that uses RFC 0011 generated-client fields.
func ValidateGeneratedClientOperationPolicy(operation OperationPolicy) error {
	if !operationUsesGeneratedClientSurface(operation) {
		return fmt.Errorf("operation does not use the generated-client surface")
	}
	// This operation-only validator has no command auth context. Capability mode
	// accepts both the explicit empty set and canonical non-empty sets; the
	// command-level validator separately enforces auth:none's empty-set rule.
	return validateCommandOperationPolicy(AuthModeCapability, operation)
}

// NormalizeGeneratedHTTPPath validates RawPath agreement, canonicalizes the
// request path, and matches it against one exact or templated operation path.
func NormalizeGeneratedHTTPPath(path, rawPath string, policy HTTPOperationPolicy) (string, error) {
	if err := validateGeneratedHTTPPolicy(policy); err != nil {
		return "", err
	}
	escaped := path
	fromEscapedBytes := false
	if rawPath != "" {
		decoded, err := url.PathUnescape(rawPath)
		if err != nil || decoded != path {
			return "", fmt.Errorf("raw path does not agree with path")
		}
		escaped = rawPath
		fromEscapedBytes = true
	}
	var canonical string
	var err error
	if fromEscapedBytes {
		canonical, err = normalizeEscapedRequestPath(escaped)
	} else {
		canonical, err = normalizeDecodedPath(escaped)
	}
	if err != nil {
		return "", err
	}
	if policy.Path != "" {
		want, err := normalizePolicyPath(policy.Path, false)
		if err != nil || canonical != want {
			return "", fmt.Errorf("path does not match operation policy")
		}
		return canonical, nil
	}
	template, err := normalizePolicyPath(policy.GeneratedClient.PathTemplate, true)
	if err != nil || !matchesPathTemplate(canonical, template) {
		return "", fmt.Errorf("path does not match operation policy")
	}
	return canonical, nil
}

// NormalizeGeneratedHTTPQuery parses one strict form-encoded query into a
// canonical logical map. Returned keys and value slices never alias input.
func NormalizeGeneratedHTTPQuery(rawQuery string) (map[string][]string, error) {
	result := make(map[string][]string)
	if rawQuery == "" {
		return result, nil
	}
	pairs := strings.Split(rawQuery, "&")
	if len(pairs) > int(GeneratedClientHTTPMaxQueryPairs) {
		return nil, fmt.Errorf("query has too many pairs")
	}
	aggregate := 0
	for _, pair := range pairs {
		rawKey, rawValue, ok := strings.Cut(pair, "=")
		if !ok || rawKey == "" {
			return nil, fmt.Errorf("query pair is malformed")
		}
		key, err := url.QueryUnescape(rawKey)
		if err != nil || !utf8.ValidString(key) || key == "" || len(key) > GeneratedClientHTTPMaxQueryKeyBytes {
			return nil, fmt.Errorf("query key is invalid")
		}
		value, err := url.QueryUnescape(rawValue)
		if err != nil || !utf8.ValidString(value) || len(value) > GeneratedClientHTTPMaxQueryValueBytes {
			return nil, fmt.Errorf("query value is invalid")
		}
		if _, exists := result[key]; !exists {
			aggregate += len(key)
		}
		aggregate += len(value)
		if aggregate > GeneratedClientHTTPMaxQueryAggregateBytes {
			return nil, fmt.Errorf("query exceeds aggregate limit")
		}
		result[key] = append(result[key], value)
	}
	return result, nil
}

// NormalizeGeneratedHTTPContentType returns the sole canonical serialization
// used by command-v3 generated HTTP adapters and hosts.
func NormalizeGeneratedHTTPContentType(value string) (string, error) {
	return canonicalContentType(value)
}

// NormalizeGeneratedHTTPHeaders validates and filters request headers against
// an immutable operation policy, returning lowercase keys and copied values.
func NormalizeGeneratedHTTPHeaders(headers map[string][]string, policy HTTPOperationPolicy) (map[string][]string, error) {
	if err := validateGeneratedHTTPPolicy(policy); err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(policy.GeneratedClient.RequestHeaders))
	for _, name := range policy.GeneratedClient.RequestHeaders {
		allowed[strings.ToLower(name)] = struct{}{}
	}
	canonicalKeys := make(map[string]struct{}, len(headers))
	result := make(map[string][]string)
	valueCount := uint32(0)
	aggregate := 0
	for name, values := range headers {
		if len(name) == 0 || len(name) > GeneratedClientHTTPMaxHeaderNameBytes || !isHTTPToken(name) {
			return nil, fmt.Errorf("request header name is invalid")
		}
		canonical := strings.ToLower(name)
		if _, exists := canonicalKeys[canonical]; exists {
			return nil, fmt.Errorf("request contains duplicate header name")
		}
		canonicalKeys[canonical] = struct{}{}
		if _, forbidden := forbiddenHTTPHeaders[canonical]; forbidden || strings.HasPrefix(canonical, "x-forwarded-") {
			return nil, fmt.Errorf("request contains forbidden header")
		}
		if _, discarded := discardedHTTPHeaders[canonical]; discarded {
			continue
		}
		if _, declared := allowed[canonical]; !declared {
			return nil, fmt.Errorf("request contains undeclared header")
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("request header has no values")
		}
		if len(values) > int(GeneratedClientHTTPMaxHeaderValues-valueCount) {
			return nil, fmt.Errorf("request has too many header values")
		}
		valueCount += uint32(len(values))
		aggregate += len(canonical)
		if aggregate > GeneratedClientHTTPMaxHeaderAggregateBytes {
			return nil, fmt.Errorf("request headers exceed aggregate limit")
		}
		copied := make([]string, len(values))
		for index, value := range values {
			if len(value) > GeneratedClientHTTPMaxHeaderValueBytes || !validHTTPHeaderValue(value) {
				return nil, fmt.Errorf("request header value is invalid")
			}
			aggregate += len(value)
			if aggregate > GeneratedClientHTTPMaxHeaderAggregateBytes {
				return nil, fmt.Errorf("request headers exceed aggregate limit")
			}
			copied[index] = value
		}
		result[canonical] = copied
	}
	return result, nil
}

func validateGeneratedGRPCPolicy(policy GRPCOperationPolicy) error {
	if policy.FullMethod == "" || !strings.HasPrefix(policy.FullMethod, "/") {
		return fmt.Errorf("invalid full method")
	}
	generated := policy.GeneratedClient
	if generated == nil {
		return fmt.Errorf("generated-client envelope is required")
	}
	if generated.MaxRequestMessageBytes <= 0 || generated.MaxRequestMessageBytes > GeneratedClientMaxMessageBytes {
		return fmt.Errorf("request limit is outside the common profile")
	}
	maxMessages := GeneratedClientMaxResponseMessages
	requireExactCount := false
	if !policy.ServerStreaming {
		maxMessages = 1
		requireExactCount = true
	}
	return validateResponseLimits(generated.ResponseLimits, GeneratedClientMaxMessageBytes, maxMessages, requireExactCount)
}

func validateResponseLimits(limits ResponseLimits, maxMessageBytes int64, maxMessages uint32, requireExactCount bool) error {
	if limits.MaxMessageBytes <= 0 || limits.MaxMessageBytes > maxMessageBytes {
		return fmt.Errorf("response message limit is outside the common profile")
	}
	if limits.MaxMessages == 0 || limits.MaxMessages > maxMessages || requireExactCount && limits.MaxMessages != maxMessages {
		return fmt.Errorf("response count limit is outside the common profile")
	}
	if limits.MaxAggregateBytes <= 0 || limits.MaxAggregateBytes > GeneratedClientMaxAggregateResponseBytes {
		return fmt.Errorf("response aggregate limit is outside the common profile")
	}
	return nil
}

func normalizePolicyPath(value string, template bool) (string, error) {
	return normalizePath(value, template, true, true)
}

func normalizeDecodedPath(value string) (string, error) {
	return normalizePath(value, false, false, false)
}

func normalizeEscapedRequestPath(value string) (string, error) {
	return normalizePath(value, false, true, false)
}

func normalizePath(value string, template, decodeSegments, policySyntax bool) (string, error) {
	if value == "/" {
		return value, nil
	}
	if !strings.HasPrefix(value, "/") || policySyntax && strings.ContainsAny(value, "?#") {
		return "", fmt.Errorf("unsafe path")
	}
	parts := strings.Split(value[1:], "/")
	canonical := make([]string, len(parts))
	for index, part := range parts {
		if part == "" {
			return "", fmt.Errorf("empty path segment")
		}
		decoded := part
		var err error
		if decodeSegments {
			decoded, err = url.PathUnescape(part)
		}
		if err != nil || !utf8.ValidString(decoded) {
			return "", fmt.Errorf("invalid path segment")
		}
		if decoded == "." || decoded == ".." || strings.ContainsAny(decoded, "/\\\x00") || containsDoubleEncoding(decoded) {
			return "", fmt.Errorf("unsafe path segment")
		}
		if template && isPathPlaceholder(decoded) {
			canonical[index] = decoded
			continue
		}
		if policySyntax && strings.ContainsAny(decoded, "{}") {
			return "", fmt.Errorf("invalid path placeholder")
		}
		canonical[index] = escapePathSegment(decoded)
	}
	normalized := "/" + strings.Join(canonical, "/")
	if len(normalized) > GeneratedClientHTTPMaxPathBytes {
		return "", fmt.Errorf("path exceeds common profile")
	}
	return normalized, nil
}

func matchesPathTemplate(path, template string) bool {
	if path == "/" || template == "/" {
		return path == template
	}
	pathSegments := strings.Split(path[1:], "/")
	templateSegments := strings.Split(template[1:], "/")
	if len(pathSegments) != len(templateSegments) {
		return false
	}
	for index, segment := range templateSegments {
		if isPathPlaceholder(segment) {
			continue
		}
		if pathSegments[index] != segment {
			return false
		}
	}
	return true
}

func isPathPlaceholder(value string) bool {
	if len(value) < 3 || value[0] != '{' || value[len(value)-1] != '}' {
		return false
	}
	name := value[1 : len(value)-1]
	for index := 0; index < len(name); index++ {
		character := name[index]
		if index == 0 {
			if !isASCIIAlpha(character) {
				return false
			}
			continue
		}
		if !isASCIIAlpha(character) && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func isASCIIAlpha(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func containsDoubleEncoding(value string) bool {
	for index := 0; index+2 < len(value); index++ {
		if value[index] == '%' && isHex(value[index+1]) && isHex(value[index+2]) {
			return true
		}
	}
	return false
}

func isHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}

func escapePathSegment(value string) string {
	const hex = "0123456789ABCDEF"
	var builder strings.Builder
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || strings.ContainsRune("-._~", rune(character)) {
			builder.WriteByte(character)
			continue
		}
		builder.WriteByte('%')
		builder.WriteByte(hex[character>>4])
		builder.WriteByte(hex[character&15])
	}
	return builder.String()
}

func canonicalContentType(value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("malformed media type")
	}
	mediaType, parameters, err := parseCanonicalContentType(value)
	if err != nil {
		return "", err
	}
	parts := strings.Split(mediaType, "/")
	if parts[0] == "*" || parts[1] == "*" {
		return "", fmt.Errorf("malformed media type")
	}
	names := make([]string, 0, len(parameters))
	for name, parameterValue := range parameters {
		name = strings.ToLower(name)
		if !isHTTPToken(name) || !utf8.ValidString(parameterValue) {
			return "", fmt.Errorf("malformed media type parameter")
		}
		if name == "charset" {
			parameters[name] = asciiLower(parameterValue)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	var builder strings.Builder
	builder.WriteString(mediaType)
	for _, name := range names {
		builder.WriteString("; ")
		builder.WriteString(name)
		builder.WriteByte('=')
		parameterValue := parameters[name]
		if isHTTPToken(parameterValue) {
			builder.WriteString(parameterValue)
		} else {
			builder.WriteByte('"')
			for _, character := range parameterValue {
				if character == '\\' || character == '"' {
					builder.WriteByte('\\')
				}
				builder.WriteRune(character)
			}
			builder.WriteByte('"')
		}
	}
	if builder.Len() > GeneratedClientHTTPMaxContentTypeBytes {
		return "", fmt.Errorf("media type exceeds common profile")
	}
	return builder.String(), nil
}

func parseCanonicalContentType(value string) (string, map[string]string, error) {
	index := 0
	typeName, index := parseHTTPTokenAt(value, index)
	if typeName == "" || index >= len(value) || value[index] != '/' {
		return "", nil, fmt.Errorf("malformed media type")
	}
	index++
	subtype, index := parseHTTPTokenAt(value, index)
	if subtype == "" {
		return "", nil, fmt.Errorf("malformed media type")
	}

	parameters := make(map[string]string)
	for index < len(value) {
		if value[index] != ';' {
			return "", nil, fmt.Errorf("malformed media type")
		}
		index++
		index = skipHTTPOptionalWhitespace(value, index)
		name, next := parseHTTPTokenAt(value, index)
		if name == "" || strings.Contains(name, "*") {
			return "", nil, fmt.Errorf("malformed media type parameter")
		}
		index = skipHTTPOptionalWhitespace(value, next)
		if index >= len(value) || value[index] != '=' {
			return "", nil, fmt.Errorf("malformed media type parameter")
		}
		index++
		index = skipHTTPOptionalWhitespace(value, index)
		parameterValue, next, err := parseContentTypeParameterValue(value, index)
		if err != nil {
			return "", nil, err
		}
		index = next
		canonicalName := asciiLower(name)
		if _, exists := parameters[canonicalName]; exists {
			return "", nil, fmt.Errorf("duplicate media type parameter")
		}
		parameters[canonicalName] = parameterValue
	}

	return asciiLower(typeName) + "/" + asciiLower(subtype), parameters, nil
}

func parseHTTPTokenAt(value string, index int) (string, int) {
	start := index
	for index < len(value) && isHTTPTokenByte(value[index]) {
		index++
	}
	return value[start:index], index
}

func skipHTTPOptionalWhitespace(value string, index int) int {
	for index < len(value) && (value[index] == ' ' || value[index] == '\t') {
		index++
	}
	return index
}

func parseContentTypeParameterValue(value string, index int) (string, int, error) {
	if index >= len(value) {
		return "", index, fmt.Errorf("malformed media type parameter")
	}
	if value[index] != '"' {
		token, next := parseHTTPTokenAt(value, index)
		if token == "" || next < len(value) && value[next] != ';' {
			return "", index, fmt.Errorf("malformed media type parameter")
		}
		return token, next, nil
	}

	index++
	var builder strings.Builder
	for index < len(value) {
		character := value[index]
		switch character {
		case '"':
			index++
			if index < len(value) && value[index] != ';' {
				return "", index, fmt.Errorf("malformed media type parameter")
			}
			return builder.String(), index, nil
		case '\\':
			index++
			if index >= len(value) || value[index] != '\\' && value[index] != '"' {
				return "", index, fmt.Errorf("malformed media type parameter")
			}
			builder.WriteByte(value[index])
			index++
		default:
			if character == 0x7f || character < 0x20 && character != '\t' {
				return "", index, fmt.Errorf("malformed media type parameter")
			}
			builder.WriteByte(character)
			index++
		}
	}
	return "", index, fmt.Errorf("malformed media type parameter")
}

func asciiLower(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'A' && character <= 'Z' {
			character += 'a' - 'A'
		}
		builder.WriteByte(character)
	}
	return builder.String()
}

func validHTTPHeaderValue(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character == 0x7f || character < 0x20 && character != '\t' {
			return false
		}
	}
	return true
}

func isHTTPToken(value string) bool {
	if value == "" {
		return false
	}
	for index := 0; index < len(value); index++ {
		if isHTTPTokenByte(value[index]) {
			continue
		}
		return false
	}
	return true
}

func isHTTPTokenByte(character byte) bool {
	return character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
		strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character))
}
