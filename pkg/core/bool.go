package core

import "fmt"

type Bool bool

func (Bool) GetType() Type { return TypeBool }
func (b Bool) String() string {
	return fmt.Sprintf("%v", bool(b))
}
