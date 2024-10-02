package ledger

import "github.com/formancehq/numscript"

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source machine_factory.go -destination machine_factory_generated_test.go -package ledger . MachineFactory
type MachineFactory interface {
	// Make can return following errors:
	//  * ErrCompilationFailed
	Make(script string) (Machine, error)
}

type DefaultMachineFactory struct {
	compiler Compiler
}

func (d *DefaultMachineFactory) Make(script string) (Machine, error) {
	ret, err := d.compiler.Compile(script)
	if err != nil {
		return nil, err
	}
	return NewDefaultMachine(*ret), nil
}

func NewDefaultMachineFactory(compiler Compiler) *DefaultMachineFactory {
	return &DefaultMachineFactory{
		compiler: compiler,
	}
}

var _ MachineFactory = (*DefaultMachineFactory)(nil)

// numscript rewrite implementation
type DefaultInterpreterMachineFactory struct{}

var _ MachineFactory = (*DefaultInterpreterMachineFactory)(nil)

func (*DefaultInterpreterMachineFactory) Make(script string) (Machine, error) {
	parseResult := numscript.Parse(script)
	if len(parseResult.GetParsingErrors()) != 0 {
		return nil, nil
	}

	return NewDefaultInterpreterMachineAdapter(parseResult), nil
}
