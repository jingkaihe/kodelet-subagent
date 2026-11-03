package db

import (
	"context"
	"embed"
	"fmt"
	"net/url"
	"strings"

	"github.com/jmoiron/sqlx"
)

// HeadRevision is the newest packaged schema revision.
const HeadRevision = "0003_mailbox"

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
	{revision: "0003_mailbox", file: "migrations/0003_mailbox.sql"},
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
			return fmt.Errorf("can't locate revision identified by '%s'", revisions[0])
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

func hasForeignKeyViolations(db *sqlx.DB) (bool, error) {
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	violations := rows.Next()
	return violations, rows.Err()
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
	violations, err := hasForeignKeyViolations(db)
	if err != nil {
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
