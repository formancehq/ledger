// Package numscriptanalysis inspects a script without resolving dependencies or
// touching ledger state.
package numscriptanalysis

import (
	"cmp"
	"errors"
	"slices"

	numscript "github.com/formancehq/numscript"
)

type Input struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Diagnostic struct {
	Kind        string  `json:"kind"`
	Code        string  `json:"code"`
	Message     string  `json:"message"`
	StartLine   *uint32 `json:"startLine,omitempty"`
	StartColumn *uint32 `json:"startColumn,omitempty"`
	EndLine     *uint32 `json:"endLine,omitempty"`
	EndColumn   *uint32 `json:"endColumn,omitempty"`
}

type Result struct {
	Inputs      []Input      `json:"inputs"`
	Diagnostics []Diagnostic `json:"diagnostics"`
}

// Analyze accepts the current Numscript language version, v1. It compiles the
// source to catch static errors, but never binds values or runs the program.
func Analyze(source, version string) (result Result, internalErr error) {
	result = Result{Inputs: []Input{}, Diagnostics: []Diagnostic{}}
	defer func() {
		if recover() != nil {
			result = Result{}
			internalErr = errors.New("numscript analysis failed internally")
		}
	}()
	if version != "v1" {
		result.Diagnostics = append(result.Diagnostics, Diagnostic{Kind: "version", Code: "UNSUPPORTED_VERSION", Message: "supported Numscript version: v1"})

		return result, nil
	}
	if source == "" {
		result.Diagnostics = append(result.Diagnostics, Diagnostic{Kind: "syntax", Code: "EMPTY_SOURCE", Message: "Numscript source is required"})

		return result, nil
	}
	if code := sourceLimitation(source); code != "" {
		result.Diagnostics = append(result.Diagnostics, Diagnostic{Kind: "syntax", Code: code, Message: "Numscript source exceeds the analysis complexity limit"})

		return result, nil
	}
	parsed := numscript.Parse(source)
	for _, parseErr := range parsed.GetParsingErrors() {
		result.Diagnostics = append(result.Diagnostics, Diagnostic{Kind: "syntax", Code: "SYNTAX_ERROR", Message: parseErr.Msg,
			StartLine: coordinate(parseErr.Start.Line), StartColumn: coordinate(parseErr.Start.Character),
			EndLine: coordinate(parseErr.End.Line), EndColumn: coordinate(parseErr.End.Character)})
	}
	if len(result.Diagnostics) != 0 {
		return result, nil
	}
	_, _, err := parsed.Compile()
	if err != nil {
		var ranged numscript.CompilerError
		if !errors.As(err, &ranged) {
			return Result{}, errors.New("numscript compilation failed internally")
		}
		kind := "compile"
		var invalidFeature numscript.CompilerInvalidFeature
		var experimentalFeature numscript.CompilerExperimentalFeature
		if errors.As(err, &invalidFeature) || errors.As(err, &experimentalFeature) {
			kind = "feature"
		}
		d := Diagnostic{Kind: kind, Code: compilerCode(err), Message: err.Error()}
		r := ranged.GetRange()
		d.StartLine, d.StartColumn = coordinate(r.Start.Line), coordinate(r.Start.Character)
		d.EndLine, d.EndColumn = coordinate(r.End.Line), coordinate(r.End.Character)
		result.Diagnostics = append(result.Diagnostics, d)

		return result, nil
	}
	for name, typ := range parsed.GetNeededVariables() {
		result.Inputs = append(result.Inputs, Input{Name: name, Type: typ})
	}
	slices.SortFunc(result.Inputs, func(a, b Input) int { return cmp.Compare(a.Name, b.Name) })

	return result, nil
}

const (
	maxAnalyzedSourceBytes = 64 << 10
	maxBlockCommentDepth   = 32
)

// sourceLimitation bounds work before the upstream Numscript lexer sees the
// source. Its MULTILINE_COMMENT rule is recursive, so a small input with deep
// block comments can exhaust process memory before a panic can be recovered.
// The source-size budget also bounds other parser work on this read-only API.
func sourceLimitation(source string) string {
	if len(source) > maxAnalyzedSourceBytes {
		return "SOURCE_TOO_LONG"
	}

	depth := 0
	for i := 0; i+1 < len(source); i++ {
		switch source[i : i+2] {
		case "/*":
			// Count markers even in malformed strings or line comments. This
			// conservative scan cannot be bypassed by a lexer recovery path.
			depth++
			if depth > maxBlockCommentDepth {
				return "SOURCE_TOO_COMPLEX"
			}
			i++
		case "*/":
			if depth > 0 {
				depth--
			}
			i++
		}
	}

	return ""
}

func compilerCode(err error) string {
	{
		var e numscript.CompilerTypeError
		var e1 numscript.CompilerInvalidUncappedSource
		var e2 numscript.CompilerDuplicateRemaining
		var e3 numscript.CompilerInvalidMetaPosition
		var e4 numscript.CompilerCannotCastToString
		var e5 numscript.CompilerCannotStoreScopedAccountInMeta
		var e6 numscript.CompilerExperimentalFeature
		var e7 numscript.CompilerInvalidFeature
		switch {
		case errors.As(err, &e):
			switch e.Kind.(type) {
			case numscript.TypecheckTypeMismatch:
				return "TYPE_MISMATCH"
			case numscript.TypecheckUnboundVariable:
				return "UNBOUND_VARIABLE"
			case numscript.TypecheckInvalidType:
				return "INVALID_TYPE"
			case numscript.TypecheckBadArity:
				return "BAD_ARITY"
			case numscript.TypecheckUnknownFunction:
				return "UNKNOWN_FUNCTION"
			case numscript.TypecheckDuplicateVariable:
				return "DUPLICATE_VARIABLE"
			default:
				return "TYPE_ERROR"
			}
		case errors.As(err, &e1):
			return "INVALID_UNCAPPED_SOURCE"
		case errors.As(err, &e2):
			return "DUPLICATE_REMAINING"
		case errors.As(err, &e3):
			return "INVALID_META_POSITION"
		case errors.As(err, &e4):
			return "CANNOT_CAST_TO_STRING"
		case errors.As(err, &e5):
			return "CANNOT_STORE_SCOPED_ACCOUNT_IN_META"
		case errors.As(err, &e6):
			return "EXPERIMENTAL_FEATURE"
		case errors.As(err, &e7):
			return "INVALID_FEATURE"
		default:
			return "COMPILE_ERROR"
		}
	}
}

func coordinate(n int) *uint32 {
	if n < 0 {
		return nil
	}
	v := uint32(n)

	return &v
}
