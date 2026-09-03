package ledgers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataTypeCommandHelpDescribesIndexOnlyContract(t *testing.T) {
	t.Parallel()

	setHelp := strings.Join(strings.Fields(NewSetMetadataTypeCommand().Long), " ")
	require.Contains(t, setHelp, "Metadata values are neither validated nor converted by this declaration")
	require.Contains(t, setHelp, "existing primary values remain unchanged")
	require.Contains(t, setHelp, "Any attached index is rewritten asynchronously")
	require.NotContains(t, setHelp, "must conform")
	require.NotContains(t, setHelp, "converted in the background")

	removeHelp := strings.Join(strings.Fields(NewRemoveMetadataTypeCommand().Long), " ")
	require.Contains(t, removeHelp, "Stored metadata values remain unchanged")
	require.Contains(t, removeHelp, "the index is dropped")
	require.NotContains(t, removeHelp, "accept values of any type again")
}
