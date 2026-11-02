package db

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// legacyAgents records one agent in each state the Python extension can leave
// behind and returns their IDs by state.
func legacyAgents(t *testing.T, path string) map[string]string {
	t.Helper()
	ctx := context.Background()
	old := NewStore(path, "python-runtime", nil)
	ids := map[string]string{}
	create := func(owner, name string) types.Claim {
		claim, err := old.Create(ctx, owner, name, name+" task", filepath.Dir(path), types.ContextFresh)
		require.NoError(t, err)
		ids[name] = claim.Agent.ID
		return claim
	}
	idle := create("owner-a", "idle-worker")
	_, err := old.MarkRunning(ctx, idle.Lease, "child-idle")
	require.NoError(t, err)
	_, err = old.Terminal(ctx, idle.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("idle result")})
	require.NoError(t, err)
	create("owner-a", "starting-worker")
	running := create("owner-b", "running-worker")
	_, err = old.MarkRunning(ctx, running.Lease, "child-running")
	require.NoError(t, err)
	canceling := create("owner-b", "canceling-worker")
	_, err = old.MarkRunning(ctx, canceling.Lease, "child-canceling")
	require.NoError(t, err)
	canceled, err := old.Cancel(ctx, "owner-b", canceling.Agent.ID)
	require.NoError(t, err)
	require.Equal(t, types.AgentCanceling, canceled.Status)
	require.NoError(t, old.Close())
	return ids
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	data, err := os.ReadFile(from)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(to), 0o755))
	require.NoError(t, os.WriteFile(to, data, 0o600))
}

func requireAbsent(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		_, err := os.Lstat(path)
		require.ErrorIs(t, err, os.ErrNotExist, path)
	}
}

func TestImportCopiesAgentsAndEndsOrphanedClaims(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "legacy", Filename)
	target := filepath.Join(dir, "subagent", Filename)
	ids := legacyAgents(t, source)
	before, err := os.ReadFile(source)
	require.NoError(t, err)

	imported, err := migrate(target, source)
	require.NoError(t, err)
	require.True(t, imported)

	store := newTestStore(t, target, "go-runtime", nil)
	get := func(name, owner string) types.Agent {
		agent, err := store.Get(ctx, owner, ids[name], "")
		require.NoError(t, err)
		return agent
	}
	idle := get("idle-worker", "owner-a")
	require.Equal(t, types.AgentIdle, idle.Status)
	require.Equal(t, "idle result", *idle.Run.Result)
	for _, name := range []string{"starting-worker", "running-worker"} {
		owner := map[string]string{"starting-worker": "owner-a", "running-worker": "owner-b"}[name]
		agent := get(name, owner)
		require.Equal(t, types.AgentInterrupted, agent.Status, name)
		require.Equal(t, types.RunInterrupted, agent.Run.Status, name)
		require.Equal(t, importedClaimMessage, *agent.Run.Error, name)
	}
	require.Equal(t, types.AgentCanceled, get("canceling-worker", "owner-b").Status)

	resumed, err := store.Claim(ctx, "owner-b", ids["running-worker"], "resume after import")
	require.NoError(t, err)
	require.Equal(t, 2, resumed.Agent.Generation)
	require.Equal(t, "child-running", *resumed.Agent.ConversationID)

	after, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, before, after, "the source database must not change")
	require.Equal(t, HeadRevision, readRevision(t, source))
	requireAbsent(t, target+".import", target+".import-journal")
}

func TestImportReadsTheWriteAheadLogOfAStoppedProcess(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	live := filepath.Join(dir, "live", Filename)
	old := newTestStore(t, live, "python-runtime", nil)
	claim, err := old.Create(ctx, "owner", "wal-worker", "work", dir, types.ContextFresh)
	require.NoError(t, err)
	info, err := os.Stat(live + "-wal")
	require.NoError(t, err)
	require.Positive(t, info.Size(), "the agent must still be in the write-ahead log")

	// Copy the files while the store is open, as a process that crashed would
	// leave them.
	source := filepath.Join(dir, "legacy", Filename)
	copyFile(t, live, source)
	copyFile(t, live+"-wal", source+"-wal")
	require.NoError(t, old.Close())

	target := filepath.Join(dir, "subagent", Filename)
	imported, err := migrate(target, source)
	require.NoError(t, err)
	require.True(t, imported)
	agent, err := newTestStore(t, target, "go-runtime", nil).Get(ctx, "owner", claim.Agent.ID, "")
	require.NoError(t, err)
	require.Equal(t, types.AgentInterrupted, agent.Status)
}

func TestImportUpgradesSourcesAtTheInitialRevision(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "legacy", Filename)
	migrateTo(t, source, initialRevision)
	db := rawDB(t, source)
	db.MustExec(`
		INSERT INTO agents (
		    id, name, owner_conversation_id, child_conversation_id,
		    context_mode, cwd, status, active_run_id, generation,
		    lease_runtime_id, lease_token, lease_expires_at, created_at,
		    updated_at
		) VALUES (
		    'agt_historical', 'historical-worker', 'owner', 'child-historical',
		    'fresh', '/tmp', 'idle', 'run_historical', 1,
		    NULL, NULL, NULL, 900.0, 901.0
		)`)
	db.MustExec(`
		INSERT INTO runs (
		    id, agent_id, generation, lease_token, task, status,
		    result, error, created_at, started_at, completed_at, updated_at
		) VALUES (
		    'run_historical', 'agt_historical', 1, 'token-historical',
		    'historical task', 'completed', 'historical result', NULL,
		    900.0, 900.5, 901.0, 901.0
		)`)
	require.NoError(t, db.Close())

	target := filepath.Join(dir, "subagent", Filename)
	imported, err := migrate(target, source)
	require.NoError(t, err)
	require.True(t, imported)
	require.Equal(t, HeadRevision, readRevision(t, target))
	require.Equal(t, initialRevision, readRevision(t, source))
	agent, err := newTestStore(t, target, "go-runtime", nil).Get(ctx, "owner", "agt_historical", "")
	require.NoError(t, err)
	require.Equal(t, "historical result", *agent.Run.Result)
}

func TestImportSkipsExistingTargetsAndMissingOrEmptySources(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "legacy", Filename)
	ids := legacyAgents(t, source)

	existing := filepath.Join(dir, "existing", Filename)
	store := newTestStore(t, existing, "go-runtime", nil)
	kept, err := store.Create(ctx, "owner-a", "kept-worker", "work", dir, types.ContextFresh)
	require.NoError(t, err)
	imported, err := migrate(existing, source)
	require.NoError(t, err)
	require.False(t, imported)
	_, err = store.Get(ctx, "owner-a", ids["idle-worker"], "")
	require.ErrorIs(t, err, ErrAgentNotFound)
	_, err = store.Get(ctx, "owner-a", kept.Agent.ID, "")
	require.NoError(t, err)

	imported, err = migrate(source, source)
	require.NoError(t, err)
	require.False(t, imported)

	missing := filepath.Join(dir, "missing", Filename)
	empty := filepath.Join(dir, "empty", Filename)
	require.NoError(t, os.MkdirAll(filepath.Dir(empty), 0o755))
	require.NoError(t, os.WriteFile(empty, nil, 0o600))
	for name, from := range map[string]string{"from-missing": missing, "from-empty": empty} {
		target := filepath.Join(dir, name, Filename)
		imported, err := migrate(target, from)
		require.NoError(t, err, name)
		require.False(t, imported, name)
		agents, err := newTestStore(t, target, "go-runtime", nil).List(ctx, "owner-a")
		require.NoError(t, err)
		require.Empty(t, agents, name)
	}
	requireAbsent(t, missing)
	info, err := os.Stat(empty)
	require.NoError(t, err)
	require.Zero(t, info.Size())
}

func TestFailedImportLeavesNoTarget(t *testing.T) {
	dir := t.TempDir()
	unversioned := filepath.Join(dir, "unversioned", Filename)
	require.NoError(t, os.MkdirAll(filepath.Dir(unversioned), 0o755))
	rawDB(t, unversioned).MustExec("CREATE TABLE legacy_agents (id TEXT PRIMARY KEY)")
	unknown := filepath.Join(dir, "unknown", Filename)
	require.NoError(t, os.MkdirAll(filepath.Dir(unknown), 0o755))
	db := rawDB(t, unknown)
	db.MustExec(versionTableSQL)
	db.MustExec("INSERT INTO alembic_version (version_num) VALUES ('unknown_revision')")
	require.NoError(t, db.Close())
	corrupt := filepath.Join(dir, "corrupt", Filename)
	require.NoError(t, os.MkdirAll(filepath.Dir(corrupt), 0o755))
	require.NoError(t, os.WriteFile(corrupt, []byte("not a sqlite database"), 0o600))

	for name, source := range map[string]string{
		"no Alembic metadata": unversioned,
		"locate revision":     unknown,
		"failed to inspect":   corrupt,
	} {
		target := filepath.Join(dir, "subagent", Filename)
		imported, err := migrate(target, source)
		require.ErrorIs(t, err, ErrDatabaseMigration, name)
		require.ErrorContains(t, err, name)
		require.ErrorContains(t, err, "move it aside to start with an empty database")
		require.False(t, imported)
		requireAbsent(t, target, target+".import", target+".import-journal")
	}

	require.NoError(t, os.Rename(unknown, unknown+".aside"))
	target := filepath.Join(dir, "subagent", Filename)
	imported, err := migrate(target, unknown)
	require.NoError(t, err)
	require.False(t, imported)
	require.Equal(t, HeadRevision, readRevision(t, target))
}

func TestConcurrentImportsCopyOnce(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "legacy", Filename)
	legacyAgents(t, source)
	target := filepath.Join(dir, "subagent", Filename)
	var imports atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			imported, err := migrate(target, source)
			if err != nil {
				t.Error(err)
			}
			if imported {
				imports.Add(1)
			}
		})
	}
	wg.Wait()
	require.Equal(t, int32(1), imports.Load())
}

func TestStoreImportsOnFirstUseAndNotifiesOnce(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "legacy", Filename)
	ids := legacyAgents(t, source)
	target := filepath.Join(dir, "subagent", Filename)
	var notified [][2]string
	notify := func(from, to string) { notified = append(notified, [2]string{from, to}) }

	store := NewStore(target, "go-runtime", nil, ImportFrom(source, notify))
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Initialize(ctx))
	agents, err := store.List(ctx, "owner-a")
	require.NoError(t, err)
	require.Len(t, agents, 2)
	require.Equal(t, [][2]string{{source, store.Path()}}, notified)

	again := NewStore(target, "go-runtime", nil, ImportFrom(source, notify))
	t.Cleanup(func() { _ = again.Close() })
	require.NoError(t, again.Initialize(ctx))
	_, err = again.Get(ctx, "owner-a", ids["idle-worker"], "")
	require.NoError(t, err)
	require.Len(t, notified, 1)
}
