package ledger

//go:generate mockgen -source machine_factory.go -destination machine_factory_generated.go -package ledger . MachineFactory

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
