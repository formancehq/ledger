module github.com/formancehq/fctl

go 1.18

require (
	github.com/athul/shelby v1.0.6
	github.com/c-bata/go-prompt v0.2.6
	github.com/formancehq/fctl/membershipclient v0.0.0-00010101000000-000000000000
	github.com/formancehq/formance-sdk-go v1.0.0-rc.1.0.20230105103853-efecdbdedb04
	github.com/iancoleman/strcase v0.2.0
	github.com/mattn/go-shellwords v1.0.12
	github.com/pkg/errors v0.9.1
	github.com/pterm/pterm v0.12.50
	github.com/spf13/cobra v1.6.1
	github.com/zitadel/oidc v1.8.0
	golang.org/x/oauth2 v0.3.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	atomicgo.dev/cursor v0.1.1 // indirect
	atomicgo.dev/keyboard v0.2.8 // indirect
	github.com/containerd/console v1.0.3 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gookit/color v1.5.2 // indirect
	github.com/gorilla/schema v1.2.0 // indirect
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/lithammer/fuzzysearch v1.1.5 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/mattn/go-tty v0.0.4 // indirect
	github.com/pkg/term v1.2.0-beta.2 // indirect
	github.com/rivo/uniseg v0.4.3 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/talal/go-bits v0.0.0-20200204154716-071e9f3e66e1 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	golang.org/x/crypto v0.4.0 // indirect
	golang.org/x/net v0.4.0 // indirect
	golang.org/x/sys v0.3.0 // indirect
	golang.org/x/term v0.3.0 // indirect
	golang.org/x/text v0.5.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/square/go-jose.v2 v2.6.0 // indirect
)

replace github.com/zitadel/oidc => github.com/formancehq/oidc v0.0.0-20220923202448-e2960a99b71c

replace github.com/formancehq/fctl/membershipclient => ./membershipclient

replace github.com/spf13/cobra v1.6.1 => github.com/formancehq/cobra v0.0.0-20221112160629-60a6d6d55ef9

replace github.com/formancehq/formance-sdk-go => ../../sdks/go
