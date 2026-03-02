package cmdutil

import (
	"context"
	"errors"

	"github.com/spf13/cobra"
	"github.com/zalando/go-keyring"
)

const keyringServiceName = "ledgerctl"

// ErrTokenNotFound is returned when no token is stored for the given server.
var ErrTokenNotFound = errors.New("no token stored for this server")

// Keyring abstracts credential storage for auth tokens keyed by server address.
type Keyring interface {
	Get(server string) (string, error)
	Set(server, token string) error
	Delete(server string) error
}

type contextKeyKeyring struct{}

// GetKeyring returns the Keyring from the command context, falling back to the OS keyring.
func GetKeyring(cmd *cobra.Command) Keyring {
	if kr, ok := cmd.Context().Value(contextKeyKeyring{}).(Keyring); ok {
		return kr
	}
	return &osKeyring{}
}

// WithKeyring injects a Keyring into the command's context.
// Useful for testing with a mock keyring.
func WithKeyring(cmd *cobra.Command, kr Keyring) {
	cmd.SetContext(context.WithValue(cmd.Context(), contextKeyKeyring{}, kr))
}

// osKeyring wraps the OS-level keyring (macOS Keychain, Linux libsecret, Windows Credential Manager).
type osKeyring struct{}

func (k *osKeyring) Get(server string) (string, error) {
	token, err := keyring.Get(keyringServiceName, server)
	if errors.Is(err, keyring.ErrNotFound) {
		return "", ErrTokenNotFound
	}
	if err != nil {
		return "", err
	}
	return token, nil
}

func (k *osKeyring) Set(server, token string) error {
	return keyring.Set(keyringServiceName, server, token)
}

func (k *osKeyring) Delete(server string) error {
	err := keyring.Delete(keyringServiceName, server)
	if errors.Is(err, keyring.ErrNotFound) {
		return ErrTokenNotFound
	}
	return err
}
