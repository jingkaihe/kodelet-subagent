package subagent

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
)

// LegacyDataDirName is the data directory Kodelet assigned the Python
// extension, which was installed as the jingkaihe@kodelet-subagent plugin.
const LegacyDataDirName = "jingkaihe@kodelet-subagent_subagent"

// legacyDatabasePath returns the Python extension's database for path, a
// database in this extension's data directory. Kodelet keeps every
// extension's data directory under one root.
func legacyDatabasePath(path string) string {
	return filepath.Join(filepath.Dir(filepath.Dir(path)), LegacyDataDirName, db.Filename)
}

// newDefaultStore opens path, importing the Python extension's agents the
// first time this extension uses its data directory.
func newDefaultStore(path, runtimeID string) Store {
	return db.NewStore(path, runtimeID, nil, db.ImportFrom(legacyDatabasePath(path), reportImport))
}

func reportImport(source, target string) {
	_, _ = fmt.Fprintf(os.Stderr, "subagent: imported agents from %s into %s\n", source, target)
}
