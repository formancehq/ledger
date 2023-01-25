package auth

import (
	"time"
)

type AccessToken struct {
	ID             string `gorm:"primarykey"`
	ApplicationID  string
	UserID         string
	Audience       Array[string] `gorm:"type:text"`
	Expiration     time.Time
	Scopes         Array[string] `gorm:"type:text"`
	RefreshTokenID string        `json:"refreshTokenID"`
}
