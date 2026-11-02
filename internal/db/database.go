package db

import (
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

	"github.com/jingkaihe/kodelet-subagent/internal/paths"
)

// Filename is the SQLite file created in the extension data directory.
const Filename = "subagents.sqlite"

// SQLiteBusyTimeoutMS bounds how long a connection waits for SQLite locks.
const SQLiteBusyTimeoutMS = 5_000

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
	_, err := migrate(path, "")
	return err
}

// migrate is Migrate that first imports source, when set, if path does not
// exist yet. It reports whether it imported.
func migrate(path, source string) (bool, error) {
	resolved := paths.Resolve(paths.ExpandUser(path))
	if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
		return false, err
	}
	lock := initializationLock(resolved)
	lock.Lock()
	defer lock.Unlock()
	unlock, err := crossProcessLock(resolved)
	if err != nil {
		return false, err
	}
	defer unlock()
	imported := false
	if source != "" {
		if imported, err = importLocked(paths.Resolve(paths.ExpandUser(source)), resolved); err != nil {
			return false, err
		}
	}
	return imported, bootstrapLocked(resolved)
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
