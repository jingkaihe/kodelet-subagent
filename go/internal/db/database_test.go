package db

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

const initialRevision = "0001_initial"

func rawDB(t *testing.T, path string) *sqlx.DB {
	t.Helper()
	db, err := singleConnection(databaseURI(path, nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func readRevision(t *testing.T, path string) string {
	t.Helper()
	var revision string
	require.NoError(t, rawDB(t, path).Get(&revision, "SELECT version_num FROM alembic_version"))
	return revision
}

func migrateTo(t *testing.T, path, revision string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, configureDatabaseFile(path))
	require.NoError(t, runMigrationsTo(path, revision))
}

func names(t *testing.T, db *sqlx.DB, kind string) map[string]bool {
	t.Helper()
	var found []string
	require.NoError(t, db.Select(&found, "SELECT name FROM sqlite_master WHERE type = ?", kind))
	set := map[string]bool{}
	for _, name := range found {
		set[name] = true
	}
	return set
}

func TestFreshDatabaseUpgradesToHeadWithRequiredPragmas(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh", Filename)
	require.NoError(t, Migrate(path))

	db := rawDB(t, path)
	tables := names(t, db, "table")
	for _, table := range []string{"agents", "runs", "steering_messages", "alembic_version"} {
		require.True(t, tables[table], table)
	}
	indexes := names(t, db, "index")
	for _, index := range []string{
		"idx_agents_owner_updated", "idx_agents_owner_name", "idx_agents_status_lease",
		"idx_agents_runtime_status", "idx_runs_agent_created", "idx_steering_run",
	} {
		require.True(t, indexes[index], index)
	}
	var journalMode string
	require.NoError(t, db.Get(&journalMode, "PRAGMA journal_mode"))
	require.Equal(t, "wal", journalMode)
	require.Equal(t, HeadRevision, readRevision(t, path))

	opened, err := Open(path)
	require.NoError(t, err)
	defer opened.Close()
	opened.SetMaxOpenConns(1)
	var foreignKeys, busyTimeout, synchronous int
	require.NoError(t, opened.Get(&foreignKeys, "PRAGMA foreign_keys"))
	require.NoError(t, opened.Get(&busyTimeout, "PRAGMA busy_timeout"))
	require.NoError(t, opened.Get(&synchronous, "PRAGMA synchronous"))
	require.Equal(t, 1, foreignKeys)
	require.Equal(t, SQLiteBusyTimeoutMS, busyTimeout)
	require.Equal(t, 1, synchronous)
}

func TestFreshSchemaMatchesAlembicDDL(t *testing.T) {
	path := filepath.Join(t.TempDir(), Filename)
	require.NoError(t, Migrate(path))
	var agents, owner string
	db := rawDB(t, path)
	require.NoError(t, db.Get(&agents, "SELECT sql FROM sqlite_master WHERE name = 'agents'"))
	require.NoError(t, db.Get(&owner, "SELECT sql FROM sqlite_master WHERE name = 'idx_agents_owner_updated'"))
	require.Contains(t, agents, `CREATE TABLE "agents" (`)
	require.Contains(t, agents, "'starting', 'running', 'canceling', 'idle', 'failed', 'interrupted', 'canceled'")
	require.Equal(t, "CREATE INDEX idx_agents_owner_updated ON agents (owner_conversation_id, updated_at)", owner)
}

func TestMigrationIsIdempotentAndPreservesManagedData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "managed.sqlite")
	require.NoError(t, Migrate(path))
	db := rawDB(t, path)
	db.MustExec(`
		INSERT INTO agents (
		    id, name, owner_conversation_id, child_conversation_id,
		    context_mode, cwd, status, active_run_id, generation,
		    lease_runtime_id, lease_token, lease_expires_at, created_at,
		    updated_at
		) VALUES (
		    'agt_managed', 'managed-worker', 'owner', 'child',
		    'fresh', '/tmp', 'idle', 'run_managed', 1,
		    NULL, NULL, NULL, 1000.0, 1001.0
		)`)
	db.MustExec(`
		INSERT INTO runs (
		    id, agent_id, generation, lease_token, task, status,
		    result, error, created_at, started_at, completed_at, updated_at
		) VALUES (
		    'run_managed', 'agt_managed', 1, 'token', 'managed task',
		    'completed', 'managed result', NULL, 1000.0, 1000.5,
		    1001.0, 1001.0
		)`)
	require.NoError(t, db.Close())
	require.NoError(t, Migrate(path))
	require.NoError(t, Migrate(path))

	var row struct {
		Name   string `db:"name"`
		Task   string `db:"task"`
		Result string `db:"result"`
	}
	require.NoError(t, rawDB(t, path).Get(&row, `
		SELECT agents.name, runs.task, runs.result
		FROM agents JOIN runs ON runs.agent_id = agents.id
		WHERE agents.id = 'agt_managed'`))
	require.Equal(t, "managed-worker", row.Name)
	require.Equal(t, "managed task", row.Task)
	require.Equal(t, "managed result", row.Result)
	require.Equal(t, HeadRevision, readRevision(t, path))
}

func TestUpgradeFromInitialPreservesActiveAndHistoricalRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upgrade", Filename)
	migrateTo(t, path, initialRevision)
	db := rawDB(t, path)
	db.MustExec(`PRAGMA foreign_keys = ON`)
	db.MustExec(`
		INSERT INTO agents (
		    id, name, owner_conversation_id, child_conversation_id,
		    context_mode, cwd, status, active_run_id, generation,
		    lease_runtime_id, lease_token, lease_expires_at, created_at,
		    updated_at
		) VALUES
		    ('agt_active', 'active-worker', 'owner', 'child-active',
		     'fresh', '/tmp', 'running', 'run_active', 1,
		     'runtime-a', 'token-active', 2000.0, 1000.0, 1001.0),
		    ('agt_historical', 'historical-worker', 'owner', 'child-historical',
		     'fresh', '/tmp', 'idle', 'run_historical', 1,
		     NULL, NULL, NULL, 900.0, 901.0)`)
	db.MustExec(`
		INSERT INTO runs (
		    id, agent_id, generation, lease_token, task, status,
		    result, error, created_at, started_at, completed_at, updated_at
		) VALUES
		    ('run_active', 'agt_active', 1, 'token-active', 'active task',
		     'running', NULL, NULL, 1000.0, 1000.5, NULL, 1001.0),
		    ('run_historical', 'agt_historical', 1, 'token-historical',
		     'historical task', 'completed', 'historical result', NULL,
		     900.0, 900.5, 901.0, 901.0)`)
	db.MustExec(`
		INSERT INTO steering_messages (agent_id, run_id, generation, message, created_at)
		VALUES ('agt_active', 'run_active', 1, 'preserve this steering', 1001.0)`)
	require.Equal(t, initialRevision, readRevision(t, path))
	require.NoError(t, db.Close())

	require.NoError(t, Migrate(path))
	require.NoError(t, Migrate(path))

	db = rawDB(t, path)
	type joined struct {
		ID             string  `db:"id"`
		Status         string  `db:"status"`
		LeaseRuntimeID *string `db:"lease_runtime_id"`
		RunStatus      string  `db:"run_status"`
		Result         *string `db:"result"`
	}
	var rows []joined
	require.NoError(t, db.Select(&rows, `
		SELECT agents.id, agents.status, agents.lease_runtime_id,
		       runs.status AS run_status, runs.result
		FROM agents JOIN runs ON runs.agent_id = agents.id
		ORDER BY agents.id`))
	require.Len(t, rows, 2)
	require.Equal(t, "agt_active", rows[0].ID)
	require.Equal(t, "running", rows[0].Status)
	require.Equal(t, "runtime-a", *rows[0].LeaseRuntimeID)
	require.Equal(t, "running", rows[0].RunStatus)
	require.Nil(t, rows[0].Result)
	require.Equal(t, "agt_historical", rows[1].ID)
	require.Equal(t, "idle", rows[1].Status)
	require.Nil(t, rows[1].LeaseRuntimeID)
	require.Equal(t, "completed", rows[1].RunStatus)
	require.Equal(t, "historical result", *rows[1].Result)

	var steering []struct {
		AgentID    string `db:"agent_id"`
		RunID      string `db:"run_id"`
		Generation int    `db:"generation"`
		Message    string `db:"message"`
	}
	require.NoError(t, db.Select(&steering, "SELECT agent_id, run_id, generation, message FROM steering_messages"))
	require.Len(t, steering, 1)
	require.Equal(t, "agt_active", steering[0].AgentID)
	require.Equal(t, "run_active", steering[0].RunID)
	require.Equal(t, 1, steering[0].Generation)
	require.Equal(t, "preserve this steering", steering[0].Message)

	db.MustExec("UPDATE runs SET status = 'canceled' WHERE id = 'run_active'")
	db.MustExec("UPDATE agents SET status = 'canceling' WHERE id = 'agt_active'")
	_, err := db.Exec(`
		INSERT INTO agents (
		    id, name, owner_conversation_id, child_conversation_id,
		    context_mode, cwd, status, active_run_id, generation,
		    lease_runtime_id, lease_token, lease_expires_at, created_at,
		    updated_at
		) VALUES (
		    'agt_invalid', 'invalid-worker', 'owner', NULL,
		    'fresh', '/tmp', 'canceling', 'run_invalid', 1,
		    NULL, NULL, NULL, 1000.0, 1000.0
		)`)
	require.Error(t, err)
	require.True(t, isConstraintViolation(err))
	require.Equal(t, HeadRevision, readRevision(t, path))
}

func TestExistingEmptyDatabaseIsInitialized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.sqlite")
	require.NoError(t, os.WriteFile(path, nil, 0o644))
	require.NoError(t, Migrate(path))
	require.Equal(t, HeadRevision, readRevision(t, path))
}

func TestConcurrentFreshMigrationsShareTheDatabaseLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent", Filename)
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = Migrate(path)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, HeadRevision, readRevision(t, path))
	var quickCheck string
	require.NoError(t, rawDB(t, path).Get(&quickCheck, "PRAGMA quick_check"))
	require.Equal(t, "ok", quickCheck)
}

func TestNonemptyUnversionedDatabaseIsRejectedWithoutAdoption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unversioned.sqlite")
	db := rawDB(t, path)
	db.MustExec("CREATE TABLE agents (id TEXT PRIMARY KEY)")
	require.NoError(t, db.Close())

	err := Migrate(path)
	require.ErrorIs(t, err, ErrUnsupportedDatabase)
	require.ErrorIs(t, err, ErrDatabaseBootstrap)
	require.ErrorContains(t, err, "without Alembic metadata")

	db = rawDB(t, path)
	var columns []string
	require.NoError(t, db.Select(&columns, "SELECT name FROM pragma_table_info('agents')"))
	require.Equal(t, []string{"id"}, columns)
	var versionTables int
	require.NoError(t, db.Get(&versionTables, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'alembic_version'"))
	require.Zero(t, versionTables)
}

func TestDatabasePathMustBeARegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), Filename)
	require.NoError(t, os.Mkdir(path, 0o755))
	err := Migrate(path)
	require.ErrorIs(t, err, ErrUnsupportedDatabase)
	require.ErrorContains(t, err, "is not a file")
}

func TestCorruptDatabaseIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.sqlite")
	require.NoError(t, os.WriteFile(path, []byte("not a sqlite database"), 0o644))
	err := Migrate(path)
	require.ErrorIs(t, err, ErrUnsupportedDatabase)
	require.ErrorContains(t, err, "failed to inspect")
}

func TestUnknownRevisionIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unknown-revision.sqlite")
	db := rawDB(t, path)
	db.MustExec(versionTableSQL)
	db.MustExec("INSERT INTO alembic_version (version_num) VALUES ('unknown_revision')")
	require.NoError(t, db.Close())

	err := Migrate(path)
	require.ErrorIs(t, err, ErrDatabaseMigration)
	require.ErrorContains(t, err, "failed to migrate")
}

func TestManagedDatabaseWithForeignKeyViolationsIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-foreign-key.sqlite")
	require.NoError(t, Migrate(path))
	rawDB(t, path).MustExec(`
		INSERT INTO runs (
		    id, agent_id, generation, lease_token, task, status,
		    result, error, created_at, started_at, completed_at, updated_at
		) VALUES (
		    'run_orphan', 'agt_missing', 1, 'token', 'orphan task',
		    'completed', 'result', NULL, 1000.0, 1000.0, 1001.0, 1001.0
		)`)
	err := Migrate(path)
	require.ErrorIs(t, err, ErrDatabaseMigration)
	require.ErrorContains(t, err, "foreign-key violations")
}

func TestResolvePathAppliesParentAfterSymlinks(t *testing.T) {
	root := ResolvePath(t.TempDir())
	workspace := filepath.Join(root, "workspace")
	outside := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(filepath.Join(outside, "nested"), 0o755))
	require.NoError(t, os.Mkdir(workspace, 0o755))
	require.NoError(t, os.Symlink(filepath.Join(outside, "nested"), filepath.Join(workspace, "link")))

	require.Equal(t, outside, ResolvePath(workspace+"/link/.."))
	require.Equal(t, filepath.Join(outside, "nested", "missing", "child"), ResolvePath(workspace+"/link/missing/child"))
	require.Equal(t, workspace, ResolvePath(workspace+"/./"))
}
