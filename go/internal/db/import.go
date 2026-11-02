package db

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
)

// importedClaimMessage explains runs that were active in an imported
// database. The installation that wrote it is assumed to have stopped, so none
// of its claims can still be live.
const importedClaimMessage = "agent interrupted when its database was imported"

// importLocked copies source into target when target does not exist yet. It
// runs under target's migration locks. The copy is staged beside target,
// upgraded, verified, and only then renamed into place, so a failed import
// leaves no target behind. Source is only read.
func importLocked(source, target string) (bool, error) {
	if source == target {
		return false, nil
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	info, err := os.Stat(source)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, importError(source, target, err)
	}
	if !info.Mode().IsRegular() {
		return false, importError(source, target, errors.New("not a file"))
	}
	hasVersion, hasSchema, err := inspectDatabase(source)
	if err != nil {
		return false, importError(source, target, err)
	}
	if !hasVersion && !hasSchema {
		return false, nil
	}
	if !hasVersion {
		return false, importError(source, target, errors.New("database has no Alembic metadata"))
	}
	staging := target + ".import"
	removeStaging(staging)
	if err := stageImport(source, staging); err != nil {
		removeStaging(staging)
		return false, importError(source, target, err)
	}
	if err := os.Rename(staging, target); err != nil {
		removeStaging(staging)
		return false, importError(source, target, err)
	}
	if err := syncPath(filepath.Dir(target)); err != nil {
		return true, importError(source, target, err)
	}
	return true, nil
}

func importError(source, target string, cause error) error {
	return wrapError(kindDatabaseMigration, fmt.Sprintf(
		"failed to import subagent database %s into %s: %v; move it aside to start with an empty database",
		source, target, cause,
	), cause)
}

// stageImport writes an upgraded copy of source to staging. VACUUM INTO reads
// a consistent snapshot, including any write-ahead log a stopped process left
// behind, and writes a rollback-journal database that is safe to rename.
func stageImport(source, staging string) error {
	query := url.Values{"mode": {"ro"}, "_pragma": {fmt.Sprintf("busy_timeout(%d)", SQLiteBusyTimeoutMS)}}
	src, err := singleConnection(databaseURI(source, query))
	if err != nil {
		return err
	}
	_, err = src.Exec("VACUUM INTO ?", staging)
	if err := errors.Join(err, src.Close()); err != nil {
		return err
	}
	if err := runMigrations(staging); err != nil {
		return err
	}
	if err := releaseImportedClaims(staging); err != nil {
		return err
	}
	if err := verifyMigratedDatabase(staging); err != nil {
		return err
	}
	return syncPath(staging)
}

// releaseImportedClaims ends every active claim in the staged copy.
func releaseImportedClaims(path string) error {
	db, err := singleConnection(databaseURI(path, connectionParameters()))
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	if _, err := reconcileLeasesTx(tx, math.MaxFloat64, SystemClock(), importedClaimMessage); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func removeStaging(staging string) {
	for _, path := range []string{staging, staging + "-journal"} {
		_ = os.Remove(path)
	}
}

func syncPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
