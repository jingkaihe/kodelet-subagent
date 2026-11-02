package paths

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveAppliesParentAfterSymlinks(t *testing.T) {
	root := Resolve(t.TempDir())
	workspace := filepath.Join(root, "workspace")
	outside := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(filepath.Join(outside, "nested"), 0o755))
	require.NoError(t, os.Mkdir(workspace, 0o755))
	require.NoError(t, os.Symlink(filepath.Join(outside, "nested"), filepath.Join(workspace, "link")))

	require.Equal(t, outside, Resolve(workspace+"/link/.."))
	require.Equal(t, filepath.Join(outside, "nested", "missing", "child"), Resolve(workspace+"/link/missing/child"))
	require.Equal(t, workspace, Resolve(workspace+"/./"))
}
