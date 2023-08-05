package program

type Overdraft struct {
	Unbounded bool
	UpTo      *Expr // invariant: if unbounbed then up_to == nil
}

type Source interface {
	isSource()
}

type SourceAccount struct {
	Account   Expr
	Overdraft *Overdraft
}

func (s SourceAccount) isSource() {}

type SourceMaxed struct {
	Source Source
	Max    Expr
}

func (s SourceMaxed) isSource() {}

type SourceInOrder []Source

func (s SourceInOrder) isSource() {}

// invariant: if remaining then expr == nil
type AllotmentPortion struct {
	Expr      Expr
	Remaining bool
}

type ValueAwareSource interface {
	isValueAwareSource()
}

type ValueAwareSourceSource struct {
	Source Source
}

func (v ValueAwareSourceSource) isValueAwareSource() {}

type ValueAwareSourcePart struct {
	Portion AllotmentPortion
	Source  Source
}
type ValueAwareSourceAllotment []ValueAwareSourcePart

func (v ValueAwareSourceAllotment) isValueAwareSource() {}
