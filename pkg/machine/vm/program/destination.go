package program

type KeptOrDestination struct {
	Kept        bool
	Destination Destination
}

type Destination interface {
	isDestination()
}

type DestinationAccount struct{ Expr Expr }

func (d DestinationAccount) isDestination() {}

type DestinationInOrderPart struct {
	Max Expr
	Kod KeptOrDestination
}

type DestinationInOrder struct {
	Parts     []DestinationInOrderPart
	Remaining KeptOrDestination
}

func (d DestinationInOrder) isDestination() {}

type DestinationAllotmentPart struct {
	Portion AllotmentPortion
	Kod     KeptOrDestination
}
type DestinationAllotment []DestinationAllotmentPart

func (d DestinationAllotment) isDestination() {}
