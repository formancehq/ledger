package api

import (
	"net/http"

	auth "github.com/formancehq/auth/pkg"
	_ "github.com/formancehq/go-libs/api"
	"github.com/gorilla/mux"
	"gorm.io/gorm"
)

func addClientRoutes(db *gorm.DB, router *mux.Router) {
	router.Path("/clients").Methods(http.MethodPost).HandlerFunc(createClient(db))
	router.Path("/clients").Methods(http.MethodGet).HandlerFunc(listClients(db))
	router.Path("/clients/{clientId}").Methods(http.MethodPut).HandlerFunc(updateClient(db))
	router.Path("/clients/{clientId}").Methods(http.MethodGet).HandlerFunc(readClient(db))
	router.Path("/clients/{clientId}").Methods(http.MethodDelete).HandlerFunc(deleteClient(db))
	router.Path("/clients/{clientId}/secrets").Methods(http.MethodPost).HandlerFunc(createSecret(db))
	router.Path("/clients/{clientId}/secrets/{secretId}").Methods(http.MethodDelete).HandlerFunc(deleteSecret(db))
	router.Path("/clients/{clientId}/scopes/{scopeId}").Methods(http.MethodPut).HandlerFunc(addScopeToClient(db))
	router.Path("/clients/{clientId}/scopes/{scopeId}").Methods(http.MethodDelete).HandlerFunc(deleteScopeOfClient(db))
}

type clientSecretView struct {
	auth.ClientSecret
	Hash string `json:"-"`
}

type clientView struct {
	auth.ClientOptions
	ID      string                       `json:"id"`
	Scopes  []string                     `json:"scopes"`
	Secrets auth.Array[clientSecretView] `json:"secrets" gorm:"type:text"`
}

func mapBusinessClient(c auth.Client) clientView {
	return clientView{
		ClientOptions: auth.ClientOptions{
			Public:                 c.Public,
			RedirectURIs:           c.RedirectURIs,
			Description:            c.Description,
			Name:                   c.Name,
			PostLogoutRedirectUris: c.PostLogoutRedirectUris,
			Metadata:               c.Metadata,
		},
		ID: c.Id,
		Scopes: func() []string {
			ret := make([]string, 0)
			for _, scope := range c.Scopes {
				ret = append(ret, scope.ID)
			}
			return ret
		}(),
		Secrets: mapList(c.Secrets, func(i auth.ClientSecret) clientSecretView {
			return clientSecretView{
				ClientSecret: i,
			}
		}),
	}
}

type secretCreateResult struct {
	ID         string `json:"id"`
	LastDigits string `json:"lastDigits"`
	Name       string `json:"name"`
	Clear      string `json:"clear"`
}

func deleteSecret(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		client := findById[auth.Client](w, r, db, "clientId")
		if client == nil {
			return
		}

		if !client.DeleteSecret(mux.Vars(r)["secretId"]) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if err := saveObject(w, r, db, client); err != nil {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func createSecret(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		client := findById[auth.Client](w, r, db, "clientId")
		if client == nil {
			return
		}

		sc := readJSONObject[auth.SecretCreate](w, r)
		if sc == nil {
			return
		}

		secret, clear := client.GenerateNewSecret(*sc)

		if err := saveObject(w, r, db, client); err != nil {
			return
		}

		writeJSONObject(w, r, secretCreateResult{
			ID:         secret.ID,
			LastDigits: secret.LastDigits,
			Name:       secret.Name,
			Clear:      clear,
		})
	}
}

func readClient(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		client := findById[auth.Client](w, r, db, "clientId")
		if client == nil {
			return
		}
		if err := loadAssociation(w, r, db, client, "Scopes", &client.Scopes); err != nil {
			return
		}
		writeJSONObject(w, r, mapBusinessClient(*client))
	}
}

func deleteClient(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := db.
			WithContext(r.Context()).
			Delete(&auth.Client{}, "id = ?", mux.Vars(r)["clientId"]).
			Error
		if err != nil {
			internalServerError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func listClients(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clients := make([]auth.Client, 0)
		if err := db.
			WithContext(r.Context()).
			Preload("Scopes").
			Find(&clients).Error; err != nil {
			internalServerError(w, r, err)
			return
		}
		writeJSONObject(w, r, mapList(clients, mapBusinessClient))
	}
}

func updateClient(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		c := findById[auth.Client](w, r, db, "clientId")
		if c == nil {
			return
		}

		opts := readJSONObject[auth.ClientOptions](w, r)
		if opts == nil {
			return
		}

		c.Update(*opts)

		if err := saveObject(w, r, db, c); err != nil {
			return
		}

		if err := loadAssociation(w, r, db, c, "Scopes", &c.Scopes); err != nil {
			return
		}

		writeJSONObject(w, r, mapBusinessClient(*c))
	}
}

func createClient(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		opts := readJSONObject[auth.ClientOptions](w, r)
		if opts == nil {
			return
		}

		c := auth.NewClient(*opts)
		if err := createObject(w, r, db, c); err != nil {
			return
		}

		writeCreatedJSONObject(w, r, mapBusinessClient(*c), c.Id)
	}
}

func deleteScopeOfClient(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		client := findById[auth.Client](w, r, db, "clientId")
		if client == nil {
			return
		}
		scope := findById[auth.Scope](w, r, db, "scopeId")
		if scope == nil {
			return
		}
		if err := loadAssociation(w, r, db, client, "Scopes", &client.Scopes); err != nil {
			return
		}
		if !client.HasScope(scope.ID) {
			return
		}
		if err := removeFromAssociation(w, r, db, client, "Scopes", scope); err != nil {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func addScopeToClient(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		client := findById[auth.Client](w, r, db, "clientId")
		if client == nil {
			return
		}
		scope := findById[auth.Scope](w, r, db, "scopeId")
		if scope == nil {
			return
		}
		if err := loadAssociation(w, r, db, client, "Scopes", &client.Scopes); err != nil {
			return
		}
		if client.HasScope(scope.ID) {
			return
		}
		if err := appendToAssociation(w, r, db, client, "Scopes", scope); err != nil {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
