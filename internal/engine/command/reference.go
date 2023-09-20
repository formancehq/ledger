package command

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

type Reference int

const (
	referenceReverts = iota
	referenceIks
	referenceTxReference
)

type Referencer struct {
	references map[Reference]*sync.Map
}

func (r *Referencer) take(ref Reference, key any) error {
	_, loaded := r.references[ref].LoadOrStore(fmt.Sprintf("%d/%s", ref, key), struct{}{})
	if loaded {
		return errors.New("already taken")
	}
	return nil
}

func (r *Referencer) release(ref Reference, key any) {
	r.references[ref].Delete(fmt.Sprintf("%d/%s", ref, key))
}

func NewReferencer() *Referencer {
	return &Referencer{
		references: map[Reference]*sync.Map{
			referenceReverts:     {},
			referenceIks:         {},
			referenceTxReference: {},
		},
	}
}
