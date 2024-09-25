package compiler

import (
	"github.com/formancehq/ledger/v2/internal/machine"
	program2 "github.com/formancehq/ledger/v2/internal/machine/vm/program"
)

func (p *parseVisitor) AppendInstruction(instruction byte) {
	p.instructions = append(p.instructions, instruction)
}

func (p *parseVisitor) PushAddress(addr machine.Address) {
	p.instructions = append(p.instructions, program2.OP_APUSH)
	bytes := addr.ToBytes()
	p.instructions = append(p.instructions, bytes...)
}

func (p *parseVisitor) PushInteger(val machine.Number) error {
	addr, err := p.AllocateResource(program2.Constant{Inner: val})
	if err != nil {
		return err
	}
	p.instructions = append(p.instructions, program2.OP_APUSH)
	bytes := addr.ToBytes()
	p.instructions = append(p.instructions, bytes...)
	return nil
}

func (p *parseVisitor) Bump(n int64) error {
	err := p.PushInteger(machine.NewNumber(n))
	if err != nil {
		return err
	}
	p.instructions = append(p.instructions, program2.OP_BUMP)
	return nil
}
