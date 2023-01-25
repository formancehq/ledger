package oidc

import (
	"embed"
	"html/template"
	"net/http"

	auth "github.com/formancehq/auth/pkg"
	"github.com/formancehq/auth/pkg/delegatedauth"
	"github.com/google/uuid"
	"github.com/zitadel/oidc/pkg/client/rp"
	"github.com/zitadel/oidc/pkg/op"
)

//go:embed templates
var templateFs embed.FS

func authorizeErrorHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authError := r.URL.Query().Get("error")
		tpl := template.Must(template.New("error.tmpl").
			ParseFS(templateFs, "templates/error.tmpl"))
		if err := tpl.Execute(w, map[string]interface{}{
			"Error":            authError,
			"ErrorDescription": r.URL.Query().Get("error_description"),
		}); err != nil {
			panic(err)
		}
	}
}

func authorizeCallbackHandler(
	provider op.OpenIDProvider,
	storage Storage,
	relyingParty rp.RelyingParty,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		state, err := delegatedauth.DecodeDelegatedState(r.URL.Query().Get("state"))
		if err != nil {
			panic(err)
		}

		authRequest, err := storage.FindAuthRequest(r.Context(), state.AuthRequestID)
		if err != nil {
			panic(err)
		}

		tokens, err := rp.CodeExchange(r.Context(), r.URL.Query().Get("code"), relyingParty)
		if err != nil {
			panic(err)
		}

		userInfos, err := rp.Userinfo(tokens.AccessToken, "Bearer", tokens.IDTokenClaims.GetSubject(), relyingParty)
		if err != nil {
			panic(err)
		}

		user, err := storage.FindUserBySubject(r.Context(), tokens.IDTokenClaims.GetSubject())
		if err != nil {
			user = &auth.User{
				ID:      uuid.NewString(),
				Subject: userInfos.GetSubject(),
				Email:   userInfos.GetEmail(),
			}
			if err := storage.SaveUser(r.Context(), *user); err != nil {
				panic(err)
			}
		}

		authRequest.UserID = user.ID

		if err := storage.UpdateAuthRequest(r.Context(), *authRequest); err != nil {
			panic(err)
		}

		w.Header().Set("Location", op.AuthCallbackURL(provider)(state.AuthRequestID))
		w.WriteHeader(http.StatusFound)
	}
}
