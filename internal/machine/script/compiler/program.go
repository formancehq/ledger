package compiler

import (
	internal2 "github.com/formancehq/ledger/internal/machine/internal"
	program2 "github.com/formancehq/ledger/internal/machine/vm/program"
)

func (p *parseVisitor) AppendInstruction(instruction byte) {
	p.instructions = append(p.instructions, instruction)
}

func (p *parseVisitor) PushAddress(addr internal2.Address) {
	p.instructions = append(p.instructions, program2.OP_APUSH)
	bytes := addr.ToBytes()
	p.instructions = append(p.instructions, bytes...)
}

func (p *parseVisitor) PushInteger(val internal2.Number) error {
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
	err := p.PushInteger(internal2.NewNumber(n))
	if err != nil {
		return err
	}
	p.instructions = append(p.instructions, program2.OP_BUMP)
	return nil
}
