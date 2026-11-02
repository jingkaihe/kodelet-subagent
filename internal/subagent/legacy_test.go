package subagent

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func TestFirstUseImportsThePythonExtensionsAgents(t *testing.T) {
	f := newFixture(t)
	legacy := db.NewStore(filepath.Join(f.root, LegacyDataDirName, db.Filename), "python-runtime", nil)
	reviewed, err := legacy.Create(f.ctx, "owner-1", "python-reviewer", "review", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = legacy.MarkRunning(f.ctx, reviewed.Lease, "child-reviewer")
	require.NoError(t, err)
	_, err = legacy.Terminal(f.ctx, reviewed.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("python review")})
	require.NoError(t, err)
	running, err := legacy.Create(f.ctx, "owner-1", "python-runner", "long task", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = legacy.MarkRunning(f.ctx, running.Lease, "child-runner")
	require.NoError(t, err)
	require.NoError(t, legacy.Close())

	call := f.call("owner-1")
	listed := f.list(call)
	require.Len(t, listed.Data["agents"], 2)
	require.Contains(t, listed.Content, "python-reviewer")
	require.Contains(t, listed.Content, "python-runner")
	require.Contains(t, f.wait(call, reviewed.Agent.ID, 0).Content, "python review")
	store := f.store(call)
	require.Equal(t, filepath.Join(f.dataDir, db.Filename), store.Path())
	require.Equal(t, types.AgentInterrupted, f.get(store, "owner-1", running.Agent.ID).Status)

	followup := f.followup(call, running.Agent.ID, "resume imported work")
	require.Empty(t, followup.Error, followup.Content)
	require.Equal(t, "result for resume imported work", f.wait(call, running.Agent.ID, 1000).Content)
	require.Equal(t, "child-runner", f.acp.client(t, 0).options(t).Resume)
}

func TestLegacyDatabaseIsASiblingDataDirectory(t *testing.T) {
	path := filepath.Join("/base", "extensions", "data", "subagent", db.Filename)
	require.Equal(t,
		filepath.Join("/base", "extensions", "data", "jingkaihe@kodelet-subagent_subagent", db.Filename),
		legacyDatabasePath(path),
	)
}
