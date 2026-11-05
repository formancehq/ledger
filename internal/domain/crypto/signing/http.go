package signing

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// The Ledger RFC 9421 profile deliberately supports one signature and one
// digest algorithm. Requiring the complete component set prevents callers from
// authenticating a body while leaving its destination or idempotency unsigned.
const httpComponents = `"@method" "@authority" "@path" "@query" "content-type" "content-digest" "idempotency-key"`

type httpSignatureInput struct {
	components []string
	parameters string
	keyID      string
}

// HTTPKeyID inspects untrusted signature parameters to select the verification
// key. It does not authenticate the request or authorize its execution.
func HTTPKeyID(request *signaturepb.SignedHTTPRequest) (string, error) {
	if request == nil {
		return "", ErrMissingSignature
	}
	input, err := parseHTTPSignatureInput(request.GetSignatureInput())
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidSignature, err)
	}

	return input.keyID, nil
}

// SignHTTP signs ordinary HTTP request bytes. It fills the three RFC headers
// without translating JSON to protobuf. Retries must preserve the signed bytes
// and idempotency key. Signatures alone do not prevent request replay.
func SignHTTP(request *signaturepb.SignedHTTPRequest, keyID string, privateKey ed25519.PrivateKey) error {
	if request == nil || len(privateKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("%w: missing request or invalid private key", ErrInvalidSignature)
	}
	quotedKey, err := quoteHTTPString(keyID)
	if err != nil || keyID == "" {
		return fmt.Errorf("%w: invalid key ID", ErrInvalidSignature)
	}
	if request.GetScheme() == "" {
		request.Scheme = "http"
	}
	digest := sha256.Sum256(request.GetBody())
	request.ContentDigest = "sha-256=:" + base64.StdEncoding.EncodeToString(digest[:]) + ":"
	request.SignatureInput = "sig1=(" + httpComponents + ");keyid=" + quotedKey + `;alg="ed25519"`
	input, err := parseHTTPSignatureInput(request.GetSignatureInput())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSignature, err)
	}
	base, err := httpSignatureBase(request, input)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSignature, err)
	}
	request.Signature = "sig1=:" + base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(base))) + ":"

	return nil
}

// VerifyHTTP authenticates the original RFC 9421 signature base and verifies
// RFC 9530 Content-Digest against the retained body. Admission must subsequently
// derive commands from these authenticated bytes, never a client-supplied batch.
func VerifyHTTP(request *signaturepb.SignedHTTPRequest, publicKey ed25519.PublicKey) error {
	if request == nil {
		return ErrMissingSignature
	}
	input, err := parseHTTPSignatureInput(request.GetSignatureInput())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSignature, err)
	}
	base, err := httpSignatureBase(request, input)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSignature, err)
	}
	signature, err := httpByteSequence(request.GetSignature(), "sig1")
	if err != nil || len(signature) != ed25519.SignatureSize || len(publicKey) != ed25519.PublicKeySize || !ed25519.Verify(publicKey, []byte(base), signature) {
		return ErrInvalidSignature
	}

	return nil
}

func httpSignatureBase(request *signaturepb.SignedHTTPRequest, input httpSignatureInput) (string, error) {
	// The HTTP router applies the body limit before capture. Repeat the bound
	// here because trusted peer forwarding and public gRPC share this verifier.
	if len(request.GetBody()) > 4<<20 || request.SizeVT() > (4<<20)+(16<<10) {
		return "", errors.New("HTTP signature evidence exceeds request limit")
	}
	for _, value := range []string{request.GetMethod(), request.GetRequestTarget(), request.GetAuthority(), request.GetContentType(), request.GetContentDigest(), request.GetIdempotencyKey(), request.GetScheme()} {
		if strings.ContainsAny(value, "\r\n\x00") || value != strings.TrimSpace(value) {
			return "", errors.New("invalid HTTP signature component")
		}
	}
	if request.GetMethod() == "" || request.GetAuthority() == "" || request.GetContentType() != "application/json" {
		return "", errors.New("missing or unsupported HTTP signature component")
	}
	authority, err := httpAuthority(request.GetScheme(), request.GetAuthority())
	if err != nil {
		return "", err
	}
	target := request.GetRequestTarget()
	if !strings.HasPrefix(target, "/") || strings.HasPrefix(target, "//") {
		return "", errors.New("HTTP target must use origin form")
	}
	parsed, err := url.ParseRequestURI(target)
	if err != nil || parsed.Fragment != "" {
		return "", errors.New("invalid HTTP request target")
	}
	gotDigest, err := httpByteSequence(request.GetContentDigest(), "sha-256")
	wantDigest := sha256.Sum256(request.GetBody())
	if err != nil || !bytes.Equal(gotDigest, wantDigest[:]) {
		return "", errors.New("HTTP content digest does not match body")
	}
	values := map[string]string{
		"@method": request.GetMethod(), "@authority": authority,
		"@path": parsed.EscapedPath(), "@query": "?" + parsed.RawQuery,
		"content-type": request.GetContentType(), "content-digest": request.GetContentDigest(),
		"idempotency-key": request.GetIdempotencyKey(),
	}
	var base strings.Builder
	for _, component := range input.components {
		base.WriteString(`"` + component + `": ` + values[component] + "\n")
	}
	base.WriteString(`"@signature-params": ` + input.parameters)

	return base.String(), nil
}

func httpAuthority(scheme, authority string) (string, error) {
	if scheme != "http" && scheme != "https" {
		return "", errors.New("HTTP signature requires an http or https scheme")
	}
	if strings.ContainsAny(authority, "/?#@\\") {
		return "", errors.New("invalid HTTP authority")
	}
	parsed, err := url.Parse(scheme + "://" + authority)
	if err != nil || parsed.Hostname() == "" {
		return "", errors.New("invalid HTTP authority")
	}
	normalized := strings.ToLower(authority)
	if (scheme == "http" && parsed.Port() == "80") || (scheme == "https" && parsed.Port() == "443") {
		normalized = strings.TrimSuffix(normalized, ":"+parsed.Port())
	}

	return normalized, nil
}

// parseHTTPSignatureInput parses the restricted Structured Fields profile from
// RFC 9421. Component and parameter ordering is retained, while inner-list and
// parameter whitespace is serialized canonically for @signature-params. Unknown
// components, parameters, duplicate members and multiple signatures fail closed.
func parseHTTPSignatureInput(header string) (httpSignatureInput, error) {
	var result httpSignatureInput
	if len(header) > 4096 || !strings.HasPrefix(header, "sig1=(") {
		return result, errors.New("expected one sig1 signature input")
	}
	rest := header[len("sig1=("):]
	seen := make(map[string]bool, 7)
	var parameters strings.Builder
	parameters.WriteByte('(')
	for {
		rest = strings.TrimLeft(rest, " ")
		if strings.HasPrefix(rest, ")") {
			rest = rest[1:]

			break
		}
		component, next, err := consumeHTTPString(rest)
		if err != nil || seen[component] {
			return result, errors.New("invalid or duplicate covered component")
		}
		switch component {
		case "@method", "@authority", "@path", "@query", "content-type", "content-digest", "idempotency-key":
		default:
			return result, errors.New("unknown covered component")
		}
		seen[component] = true
		if len(result.components) != 0 {
			parameters.WriteByte(' ')
		}
		parameters.WriteString(`"` + component + `"`)
		result.components = append(result.components, component)
		if next == "" || (next[0] != ' ' && next[0] != ')') {
			return result, errors.New("invalid covered component separator")
		}
		rest = next
	}
	if len(seen) != 7 {
		return result, errors.New("all Ledger signature components are required")
	}
	parameters.WriteByte(')')
	seenParams := make(map[string]bool, 2)
	for rest != "" {
		if rest[0] != ';' {
			return result, errors.New("invalid signature parameters")
		}
		rest = strings.TrimLeft(rest[1:], " ")
		name, tail, ok := strings.Cut(rest, "=")
		if !ok || (name != "keyid" && name != "alg") || seenParams[name] {
			return result, errors.New("unknown or duplicate signature parameter")
		}
		value, next, err := consumeHTTPString(tail)
		if err != nil || value == "" || (name == "alg" && value != "ed25519") {
			return result, errors.New("invalid signature parameter value")
		}
		seenParams[name] = true
		if name == "keyid" {
			result.keyID = value
		}
		quoted, err := quoteHTTPString(value)
		if err != nil {
			return result, err
		}
		parameters.WriteString(";" + name + "=" + quoted)
		rest = next
	}
	if len(seenParams) != 2 {
		return result, errors.New("keyid and alg parameters are required")
	}
	result.parameters = parameters.String()

	return result, nil
}

func consumeHTTPString(value string) (string, string, error) {
	if !strings.HasPrefix(value, `"`) {
		return "", "", errors.New("expected Structured Fields string")
	}
	var decoded strings.Builder
	for i := 1; i < len(value); i++ {
		c := value[i]
		if c == '"' {
			return decoded.String(), value[i+1:], nil
		}
		if c == '\\' {
			i++
			if i == len(value) || (value[i] != '\\' && value[i] != '"') {
				return "", "", errors.New("invalid Structured Fields escape")
			}
			c = value[i]
		}
		if c < 0x20 || c > 0x7e {
			return "", "", errors.New("invalid Structured Fields string byte")
		}
		decoded.WriteByte(c)
	}

	return "", "", errors.New("unterminated Structured Fields string")
}

func quoteHTTPString(value string) (string, error) {
	var quoted strings.Builder
	quoted.WriteByte('"')
	for i := range len(value) {
		c := value[i]
		if c < 0x20 || c > 0x7e {
			return "", errors.New("invalid Structured Fields string byte")
		}
		if c == '"' || c == '\\' {
			quoted.WriteByte('\\')
		}
		quoted.WriteByte(c)
	}
	quoted.WriteByte('"')

	return quoted.String(), nil
}

func httpByteSequence(header, name string) ([]byte, error) {
	prefix := name + "=:"
	if len(header) <= len(prefix) || !strings.HasPrefix(header, prefix) || !strings.HasSuffix(header, ":") || strings.ContainsAny(header, "\r\n") {
		return nil, fmt.Errorf("expected one %s byte sequence", name)
	}

	return base64.StdEncoding.Strict().DecodeString(header[len(prefix) : len(header)-1])
}
