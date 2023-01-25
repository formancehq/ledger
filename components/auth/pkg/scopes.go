package auth

import (
	"github.com/google/uuid"
)

type Scope struct {
	ID              string   `json:"id" gorm:"primarykey"`
	Label           string   `json:"label" gorm:"unique"`
	TransientScopes []Scope  `json:"transient" gorm:"many2many:transient_scopes;"`
	Metadata        Metadata `gorm:"type:text"`
}

func (s *Scope) AddTransientScope(scope *Scope) *Scope {
	s.TransientScopes = append(s.TransientScopes, *scope)
	return s
}

func (s *Scope) Update(opts ScopeOptions) {
	s.Label = opts.Label
	s.Metadata = opts.Metadata
}

func NewScope(opts ScopeOptions) *Scope {
	return &Scope{
		ID:       uuid.NewString(),
		Label:    opts.Label,
		Metadata: opts.Metadata,
	}
}

type ScopeOptions struct {
	Label    string   `json:"label"`
	Metadata Metadata `json:"metadata"`
}
