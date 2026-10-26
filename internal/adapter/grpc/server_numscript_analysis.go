package grpc

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/application/numscriptanalysis"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func (impl *BucketServiceServerImpl) AnalyzeNumscript(_ context.Context, req *servicepb.AnalyzeNumscriptRequest) (*servicepb.AnalyzeNumscriptResponse, error) {
	result, err := numscriptanalysis.Analyze(req.GetSource(), req.GetVersion())
	if err != nil {
		return nil, err
	}
	response := &servicepb.AnalyzeNumscriptResponse{
		Inputs:      make([]*servicepb.NumscriptInput, 0, len(result.Inputs)),
		Diagnostics: make([]*servicepb.NumscriptDiagnostic, 0, len(result.Diagnostics)),
	}
	for _, input := range result.Inputs {
		response.Inputs = append(response.Inputs, &servicepb.NumscriptInput{Name: input.Name, Type: input.Type})
	}
	for _, diagnostic := range result.Diagnostics {
		kind := servicepb.NumscriptDiagnosticKind_NUMSCRIPT_DIAGNOSTIC_KIND_COMPILE
		switch diagnostic.Kind {
		case "version":
			kind = servicepb.NumscriptDiagnosticKind_NUMSCRIPT_DIAGNOSTIC_KIND_VERSION
		case "syntax":
			kind = servicepb.NumscriptDiagnosticKind_NUMSCRIPT_DIAGNOSTIC_KIND_SYNTAX
		case "feature":
			kind = servicepb.NumscriptDiagnosticKind_NUMSCRIPT_DIAGNOSTIC_KIND_FEATURE
		}
		response.Diagnostics = append(response.Diagnostics, &servicepb.NumscriptDiagnostic{
			Kind: kind, Code: diagnostic.Code, Message: diagnostic.Message,
			StartLine: diagnostic.StartLine, StartColumn: diagnostic.StartColumn,
			EndLine: diagnostic.EndLine, EndColumn: diagnostic.EndColumn,
		})
	}

	return response, nil
}
