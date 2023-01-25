package compiler

import (
	"github.com/formancehq/machine/core"
	"github.com/formancehq/machine/vm/program"
)

func (p *parseVisitor) AppendInstruction(instruction byte) {
	p.instructions = append(p.instructions, instruction)
}

func (p *parseVisitor) PushAddress(addr core.Address) {
	p.instructions = append(p.instructions, program.OP_APUSH)
	bytes := addr.ToBytes()
	p.instructions = append(p.instructions, bytes...)
}

func (p *parseVisitor) PushInteger(val core.Number) error {
	addr, err := p.AllocateResource(program.Constant{Inner: val})
	if err != nil {
		return err
	}
	p.instructions = append(p.instructions, program.OP_APUSH)
	bytes := addr.ToBytes()
	p.instructions = append(p.instructions, bytes...)
	return nil
}

func (p *parseVisitor) Bump(n int64) error {
	err := p.PushInteger(core.NewNumber(n))
	if err != nil {
		return err
	}
	p.instructions = append(p.instructions, program.OP_BUMP)
	return nil
}
