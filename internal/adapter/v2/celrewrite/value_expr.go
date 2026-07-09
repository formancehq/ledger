package celrewrite

import (
	"errors"
	"fmt"

	"github.com/google/cel-go/cel"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// valueProducer is a per-action closure that yields the string value to write
// at commit time. For a literal value it just returns the pre-validated string;
// for a CEL value_expr it evaluates the pre-compiled program against the
// current variant and coerces the result to a string.
type valueProducer func(entry *raftcmdpb.MirrorLogEntry) (string, error)

// valueSource describes what a setter's `source` oneof carries: either a
// literal string, a CEL expression source, or neither (empty literal).
type valueSource struct {
	kind valueSourceKind
	// One of the below is set, according to `kind`.
	literal string
	expr    string
}

type valueSourceKind int

const (
	valueSourceUnset valueSourceKind = iota
	valueSourceLiteral
	valueSourceExpr
)

// extractSetMetadataSource / extractSetAccountMetadataSource discriminate the
// setter's `source` oneof. Kept as function values so buildValueProducer stays
// agnostic of the specific proto type.
func extractSetMetadataSource(src any) valueSource {
	switch s := src.(type) {
	case *commonpb.SetMetadataAction_Value:
		return valueSource{kind: valueSourceLiteral, literal: s.Value}
	case *commonpb.SetMetadataAction_ValueExpr:
		return valueSource{kind: valueSourceExpr, expr: s.ValueExpr}
	default:
		return valueSource{kind: valueSourceUnset}
	}
}

func extractSetAccountMetadataSource(src any) valueSource {
	switch s := src.(type) {
	case *commonpb.SetAccountMetadataAction_Value:
		return valueSource{kind: valueSourceLiteral, literal: s.Value}
	case *commonpb.SetAccountMetadataAction_ValueExpr:
		return valueSource{kind: valueSourceExpr, expr: s.ValueExpr}
	default:
		return valueSource{kind: valueSourceUnset}
	}
}

// buildValueProducer compiles a setter's value source into a runtime producer.
//   - Literal values are validated at admission and returned as-is.
//   - value_expr expressions are compiled against `env` (typed to the current
//     variant), and their output type is required to be `string` at admission.
//     At runtime they are evaluated with `log = <current variant>` and the
//     produced string is validated against the metadata charset before being
//     written — a bad value fails the batch loudly.
//   - Unset source is equivalent to an empty literal (deliberately permissive:
//     an operator who genuinely wants an empty metadata value can omit both
//     fields, matching proto3 defaults).
func buildValueProducer(env *cel.Env, actionName, key string, source any, extract func(any) valueSource) (valueProducer, error) {
	src := extract(source)

	switch src.kind {
	case valueSourceUnset, valueSourceLiteral:
		lit := src.literal
		if err := validateValue(lit); err != nil {
			return nil, fmt.Errorf("%s: invalid literal value for %q: %w", actionName, key, err)
		}

		return func(*raftcmdpb.MirrorLogEntry) (string, error) {
			return lit, nil
		}, nil

	case valueSourceExpr:
		prog, err := compileStringExpr(env, src.expr)
		if err != nil {
			return nil, fmt.Errorf("%s: value_expr for %q: %w", actionName, key, err)
		}

		return func(entry *raftcmdpb.MirrorLogEntry) (string, error) {
			out, err := evalStringExpr(prog, entry)
			if err != nil {
				return "", fmt.Errorf("%s value_expr for %q: %w", actionName, key, err)
			}

			if err := validateValue(out); err != nil {
				return "", fmt.Errorf("%s value_expr for %q produced invalid value %q: %w", actionName, key, out, err)
			}

			return out, nil
		}, nil
	}

	return nil, fmt.Errorf("%s: unknown value source", actionName)
}

// compileStringExpr compiles a CEL expression whose output type must be string,
// bounded by the same cost limit as `match`. This is called at admission time.
func compileStringExpr(env *cel.Env, src string) (cel.Program, error) {
	if src == "" {
		return nil, errors.New("value_expr must not be empty")
	}

	if len(src) > MaxExprLen {
		return nil, fmt.Errorf("value_expr too long (%d > %d)", len(src), MaxExprLen)
	}

	ast, iss := env.Compile(src)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("compile: %w", iss.Err())
	}

	if out := ast.OutputType(); out.String() != cel.StringType.String() {
		return nil, fmt.Errorf("value_expr must return string, got %s", out.String())
	}

	prog, err := env.Program(ast, cel.CostLimit(maxEvalCost))
	if err != nil {
		return nil, fmt.Errorf("program: %w", err)
	}

	return prog, nil
}

// evalStringExpr runs a compiled value_expr against the current variant. The
// activation binds `log` to the variant proto (matching the env's variable
// type), so the expression can read fields directly (e.g. `log.reference`,
// `log.metadata["k"].string_value`).
func evalStringExpr(prog cel.Program, entry *raftcmdpb.MirrorLogEntry) (string, error) {
	log, err := variantForEnv(entry)
	if err != nil {
		return "", err
	}

	out, _, err := prog.Eval(map[string]any{"log": log})
	if err != nil {
		return "", err
	}

	s, ok := out.Value().(string)
	if !ok {
		return "", fmt.Errorf("value_expr did not return a string (got %T)", out.Value())
	}

	return s, nil
}

// parseOptionalMetadataType parses an optional metadata type token (e.g.
// "int64", "bool", "datetime"). An empty token means the default STRING and
// returns `typed=false` so callers can skip the coercion step entirely. An
// unknown token is rejected loudly at admission.
func parseOptionalMetadataType(token string) (commonpb.MetadataType, bool, error) {
	if token == "" {
		return commonpb.MetadataType_METADATA_TYPE_STRING, false, nil
	}

	t, err := commonpb.ParseMetadataType(token)
	if err != nil {
		return 0, false, err
	}

	return t, true, nil
}

// coerceValue turns a computed string into a typed MetadataValue via the
// platform conversion matrix. `typed=false` means "leave as string" — the
// callers pass through untouched. A value that doesn't parse as the declared
// type becomes a null value preserving the original string (consistent with
// how the platform handles typed-metadata writes elsewhere).
func coerceValue(value string, typ commonpb.MetadataType, typed bool) *commonpb.MetadataValue {
	sv := commonpb.NewStringValue(value)
	if !typed {
		return sv
	}

	return commonpb.ConvertMetadataValue(sv, typ)
}

// variantForEnv returns the entry's current variant as the value CEL should
// bind to `log`. It mirrors the per-variant env choice made at compile time —
// action closures only fire when the source variant is set (the compileXRule
// gate guarantees it), so this is a defensive lookup.
func variantForEnv(entry *raftcmdpb.MirrorLogEntry) (any, error) {
	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		return data.CreatedTransaction, nil
	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		return data.RevertedTransaction, nil
	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		return data.SavedMetadata, nil
	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		return data.DeletedMetadata, nil
	default:
		return nil, errors.New("value_expr: entry has no rewritable variant")
	}
}
