package auth

import (
	"time"

	"github.com/zitadel/oidc/pkg/oidc"
	"golang.org/x/text/language"
)

type AuthRequest struct {
	ID            string `gorm:"primarykey"`
	CreatedAt     time.Time
	ApplicationID string
	CallbackURI   string
	TransferState string
	Prompt        Array[string]       `gorm:"type:text"`
	UiLocales     Array[language.Tag] `gorm:"type:text"`
	LoginHint     string
	MaxAuthAge    *time.Duration
	Scopes        Array[string] `gorm:"type:text"`
	ResponseType  oidc.ResponseType
	Nonce         string
	CodeChallenge *OIDCCodeChallenge `gorm:"embedded"`
	UserID        string
	AuthTime      time.Time
	Code          string
}

func (a *AuthRequest) GetID() string {
	return a.ID
}

func (a *AuthRequest) GetACR() string {
	return "" //we won't handle acr in this example
}

func (a *AuthRequest) GetAMR() []string {
	return nil
}

func (a *AuthRequest) GetAudience() []string {
	return []string{a.ApplicationID}
}

func (a *AuthRequest) GetAuthTime() time.Time {
	return a.AuthTime
}

func (a *AuthRequest) GetClientID() string {
	return a.ApplicationID
}

func (a *AuthRequest) GetCodeChallenge() *oidc.CodeChallenge {
	return CodeChallengeToOIDC(a.CodeChallenge)
}

func (a *AuthRequest) GetNonce() string {
	return a.Nonce
}

func (a *AuthRequest) GetRedirectURI() string {
	return a.CallbackURI
}

func (a *AuthRequest) GetResponseType() oidc.ResponseType {
	return a.ResponseType
}

func (a *AuthRequest) GetResponseMode() oidc.ResponseMode {
	return "" //we won't handle response mode in this example
}

func (a *AuthRequest) GetScopes() []string {
	return a.Scopes
}

func (a *AuthRequest) GetState() string {
	return a.TransferState
}

func (a *AuthRequest) GetSubject() string {
	return a.UserID
}

func (a *AuthRequest) Done() bool {
	return a.UserID != ""
}

func PromptToInternal(oidcPrompt oidc.SpaceDelimitedArray) []string {
	prompts := make([]string, len(oidcPrompt))
	for _, oidcPrompt := range oidcPrompt {
		switch oidcPrompt {
		case oidc.PromptNone,
			oidc.PromptLogin,
			oidc.PromptConsent,
			oidc.PromptSelectAccount:
			prompts = append(prompts, oidcPrompt)
		}
	}
	return prompts
}

func MaxAgeToInternal(maxAge *uint) *time.Duration {
	if maxAge == nil {
		return nil
	}
	dur := time.Duration(*maxAge) * time.Second
	return &dur
}
