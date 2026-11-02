package db

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

// Filename is the SQLite file created in the extension data directory.
const Filename = "subagents.sqlite"

// SQLiteBusyTimeoutMS bounds how long a connection waits for SQLite locks.
const SQLiteBusyTimeoutMS = 5_000

// HeadRevision is the newest packaged schema revision.
const HeadRevision = "0002_canceling_state"

// versionTableSQL matches the table Alembic created for pre-Go databases, so
// alembic_version remains the sole schema-version authority.
const versionTableSQL = `CREATE TABLE alembic_version (
	version_num VARCHAR(32) NOT NULL, 
	CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
)`

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	revision string
	file     string
}

// migrations are applied in order, one transaction per revision.
var migrations = []migration{
	{revision: "0001_initial", file: "migrations/0001_initial.sql"},
	{revision: "0002_canceling_state", file: "migrations/0002_canceling_state.sql"},
}

var (
	initializationLocksMu sync.Mutex
	initializationLocks   = map[string]*sync.Mutex{}
)

func initializationLock(path string) *sync.Mutex {
	initializationLocksMu.Lock()
	defer initializationLocksMu.Unlock()
	lock := initializationLocks[path]
	if lock == nil {
		lock = &sync.Mutex{}
		initializationLocks[path] = lock
	}
	return lock
}

// databaseURI returns a SQLite URI for path with driver parameters.
func databaseURI(path string, query url.Values) string {
	uri := url.URL{Scheme: "file", Path: path}
	if query != nil {
		uri.RawQuery = query.Encode()
	}
	return uri.String()
}

func connectionParameters() url.Values {
	return url.Values{
		"_pragma": {
			fmt.Sprintf("busy_timeout(%d)", SQLiteBusyTimeoutMS),
			"foreign_keys(1)",
			"synchronous(NORMAL)",
		},
	}
}

// Open opens a pooled connection to a migrated database. Connections
// enforce foreign keys, wait for locks, and begin IMMEDIATE transactions.
func Open(path string) (*sqlx.DB, error) {
	query := connectionParameters()
	query.Set("_txlock", "immediate")
	db, err := sqlx.Open("sqlite", databaseURI(path, query))
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Migrate creates or upgrades a subagent database to HeadRevision.
// Non-empty databases without version metadata are rejected, not adopted.
func Migrate(path string) error {
	resolved := ResolvePath(ExpandUser(path))
	if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
		return err
	}
	lock := initializationLock(resolved)
	lock.Lock()
	defer lock.Unlock()
	unlock, err := crossProcessLock(resolved)
	if err != nil {
		return err
	}
	defer unlock()
	return bootstrapLocked(resolved)
}

func bootstrapLocked(path string) error {
	info, err := os.Stat(path)
	switch {
	case err == nil && !info.Mode().IsRegular():
		return newError(kindUnsupportedDatabase, "subagent database is not a file: "+path)
	case err == nil:
		hasVersion, hasSchema, err := inspectDatabase(path)
		if err != nil {
			return err
		}
		if hasSchema && !hasVersion {
			return newError(kindUnsupportedDatabase, fmt.Sprintf(
				"unsupported subagent database without Alembic metadata: %s; remove it to initialize a new pre-release database",
				path,
			))
		}
	case !errors.Is(err, os.ErrNotExist):
		return err
	}
	if err := configureDatabaseFile(path); err != nil {
		return err
	}
	if err := runMigrations(path); err != nil {
		return wrapError(kindDatabaseMigration, fmt.Sprintf("failed to migrate subagent database %s: %v", path, err), err)
	}
	return verifyMigratedDatabase(path)
}

func singleConnection(dsn string) (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Temporary tables and pragmas are per connection.
	db.SetMaxOpenConns(1)
	return db, nil
}

func inspectDatabase(path string) (hasVersion, hasSchema bool, err error) {
	inspectErr := func(cause error) error {
		return wrapError(kindUnsupportedDatabase, fmt.Sprintf("failed to inspect subagent database %s: %v", path, cause), cause)
	}
	query := url.Values{"mode": {"ro"}, "_pragma": {fmt.Sprintf("busy_timeout(%d)", SQLiteBusyTimeoutMS)}}
	db, err := singleConnection(databaseURI(path, query))
	if err != nil {
		return false, false, inspectErr(err)
	}
	defer db.Close()
	var quickCheck string
	if err := db.Get(&quickCheck, "PRAGMA quick_check"); err != nil {
		return false, false, inspectErr(err)
	}
	if !strings.EqualFold(quickCheck, "ok") {
		return false, false, newError(kindUnsupportedDatabase, fmt.Sprintf("subagent database failed SQLite quick_check: %s: %s", path, quickCheck))
	}
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'alembic_version'"); err != nil {
		return false, false, inspectErr(err)
	}
	hasVersion = count > 0
	if err := db.Get(&count, "SELECT COUNT(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND name != 'alembic_version'"); err != nil {
		return false, false, inspectErr(err)
	}
	return hasVersion, count > 0, nil
}

func configureDatabaseFile(path string) error {
	db, err := singleConnection(databaseURI(path, connectionParameters()))
	if err != nil {
		return err
	}
	defer db.Close()
	var journalMode string
	if err := db.Get(&journalMode, "PRAGMA journal_mode"); err != nil {
		return err
	}
	if strings.EqualFold(journalMode, "wal") {
		return nil
	}
	if err := db.Get(&journalMode, "PRAGMA journal_mode = WAL"); err != nil {
		return err
	}
	if !strings.EqualFold(journalMode, "wal") {
		return newError(kindDatabaseMigration, fmt.Sprintf("failed to enable WAL journal mode; SQLite selected %q", journalMode))
	}
	return nil
}

func runMigrations(path string) error {
	return runMigrationsTo(path, HeadRevision)
}

// runMigrationsTo upgrades the database to target, which must be packaged.
func runMigrationsTo(path, target string) error {
	end := -1
	for i, candidate := range migrations {
		if candidate.revision == target {
			end = i
		}
	}
	if end < 0 {
		return fmt.Errorf("unknown target revision %s", target)
	}
	db, err := singleConnection(databaseURI(path, connectionParameters()))
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	var tables int
	if err := db.Get(&tables, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'alembic_version'"); err != nil {
		return err
	}
	if tables == 0 {
		if _, err := db.Exec(versionTableSQL); err != nil {
			return err
		}
	}
	var revisions []string
	if err := db.Select(&revisions, "SELECT version_num FROM alembic_version"); err != nil {
		return err
	}
	start := 0
	switch len(revisions) {
	case 0:
	case 1:
		index := -1
		for i, candidate := range migrations {
			if candidate.revision == revisions[0] {
				index = i
			}
		}
		if index < 0 {
			return fmt.Errorf("Can't locate revision identified by '%s'", revisions[0])
		}
		start = index + 1
	default:
		return fmt.Errorf("database has multiple revisions: %s", strings.Join(revisions, ", "))
	}
	for i := start; i <= end; i++ {
		if err := applyMigration(ctx, db, migrations[i], i); err != nil {
			return fmt.Errorf("revision %s: %w", migrations[i].revision, err)
		}
	}
	return nil
}

func applyMigration(ctx context.Context, db *sqlx.DB, step migration, index int) (err error) {
	script, err := migrationFiles.ReadFile(step.file)
	if err != nil {
		return err
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, string(script)); err != nil {
		return err
	}
	if index == 0 {
		_, err = tx.ExecContext(ctx, "INSERT INTO alembic_version (version_num) VALUES (?)", step.revision)
	} else {
		_, err = tx.ExecContext(ctx, "UPDATE alembic_version SET version_num = ? WHERE alembic_version.version_num = ?", step.revision, migrations[index-1].revision)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func verifyMigratedDatabase(path string) error {
	db, err := singleConnection(databaseURI(path, url.Values{"_pragma": {fmt.Sprintf("busy_timeout(%d)", SQLiteBusyTimeoutMS)}}))
	if err != nil {
		return err
	}
	defer db.Close()
	verifyErr := func(cause error) error {
		return wrapError(kindDatabaseMigration, fmt.Sprintf("failed to verify migrated subagent database %s: %v", path, cause), cause)
	}
	var quickCheck string
	if err := db.Get(&quickCheck, "PRAGMA quick_check"); err != nil {
		return verifyErr(err)
	}
	var revisions []string
	if err := db.Select(&revisions, "SELECT version_num FROM alembic_version ORDER BY version_num"); err != nil {
		return verifyErr(err)
	}
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		return verifyErr(err)
	}
	violations := rows.Next()
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return verifyErr(err)
	}
	if !strings.EqualFold(quickCheck, "ok") {
		return newError(kindDatabaseMigration, fmt.Sprintf("subagent database failed SQLite quick_check after migration: %s: %s", path, quickCheck))
	}
	if len(revisions) != 1 || revisions[0] != HeadRevision {
		return newError(kindDatabaseMigration, fmt.Sprintf("subagent database %s is at %v, expected [%s]", path, revisions, HeadRevision))
	}
	if violations {
		return newError(kindDatabaseMigration, fmt.Sprintf("subagent database %s contains foreign-key violations", path))
	}
	return nil
}

// crossProcessLock serializes migrations across extension processes.
func crossProcessLock(path string) (func(), error) {
	lockPath := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".migration.lock")
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}
