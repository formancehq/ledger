package compiler

import (
	"github.com/formancehq/ledger/pkg/machine/internal"
	"github.com/formancehq/ledger/pkg/machine/vm/program"
)

func (p *parseVisitor) AppendInstruction(instruction byte) {
	p.instructions = append(p.instructions, instruction)
}

func (p *parseVisitor) PushAddress(addr internal.Address) {
	p.instructions = append(p.instructions, program.OP_APUSH)
	bytes := addr.ToBytes()
	p.instructions = append(p.instructions, bytes...)
}

func (p *parseVisitor) PushInteger(val internal.Number) error {
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
	err := p.PushInteger(internal.NewNumber(n))
	if err != nil {
		return err
	}
	p.instructions = append(p.instructions, program.OP_BUMP)
	return nil
}
