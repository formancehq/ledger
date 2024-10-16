package ledger

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source numscript_parser.go -destination numscript_parser_generated_test.go -package ledger . NumscriptParser

type NumscriptParser interface {
	// Parse can return following errors:
	//  * ErrCompilationFailed
	Parse(script string) (NumscriptRuntime, error)
}

type DefaultNumscriptParser struct {
	compiler Compiler
}

func (d *DefaultNumscriptParser) Parse(script string) (NumscriptRuntime, error) {
	ret, err := d.compiler.Compile(script)
	if err != nil {
		return nil, err
	}
	return NewMachineNumscriptRuntimeAdapter(*ret), nil
}

func NewDefaultNumscriptParser(compiler Compiler) *DefaultNumscriptParser {
	return &DefaultNumscriptParser{
		compiler: compiler,
	}
}

var _ NumscriptParser = (*DefaultNumscriptParser)(nil)
