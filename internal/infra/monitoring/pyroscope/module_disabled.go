//go:build !pyroscope

package pyroscope

import "go.uber.org/fx"

// Module returns a no-op fx.Option when Pyroscope is not compiled in.
func Module(_ Config) fx.Option {
	return fx.Options()
}
