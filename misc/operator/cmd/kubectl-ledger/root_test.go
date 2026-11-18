package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRootCommandSurface(t *testing.T) {
	t.Parallel()

	commands := newRootCommand().Commands()
	names := make([]string, 0, len(commands))
	for _, command := range commands {
		names = append(names, command.Name())
	}
	require.ElementsMatch(t, []string{
		"backup", "create", "credentials", "delete", "explain", "get", "list",
		"logs", "port-forward", "restart", "scale", "update", "version",
	}, names)

	tests := []struct {
		path []string
		name string
	}{
		{path: []string{"ls"}, name: "list"},
		{path: []string{"describe"}, name: "get"},
		{path: []string{"show"}, name: "get"},
		{path: []string{"inspect"}, name: "get"},
		{path: []string{"status"}, name: "get"},
		{path: []string{"st"}, name: "get"},
		{path: []string{"rm"}, name: "delete"},
		{path: []string{"del"}, name: "delete"},
		{path: []string{"log"}, name: "logs"},
		{path: []string{"pf"}, name: "port-forward"},
		{path: []string{"rollout"}, name: "restart"},
		{path: []string{"schema"}, name: "explain"},
		{path: []string{"fields"}, name: "explain"},
		{path: []string{"backups"}, name: "backup"},
		{path: []string{"bk"}, name: "backup"},
		{path: []string{"credential"}, name: "credentials"},
		{path: []string{"creds"}, name: "credentials"},
		{path: []string{"cred"}, name: "credentials"},
		{path: []string{"bk", "ls"}, name: "list"},
		{path: []string{"creds", "ls"}, name: "list"},
		{path: []string{"creds", "rm"}, name: "delete"},
		{path: []string{"creds", "del"}, name: "delete"},
	}
	for _, test := range tests {
		t.Run(test.path[0]+"/"+test.name, func(t *testing.T) {
			t.Parallel()
			command, remaining, err := newRootCommand().Find(test.path)
			require.NoError(t, err)
			require.Empty(t, remaining)
			require.Equal(t, test.name, command.Name())
		})
	}
}
