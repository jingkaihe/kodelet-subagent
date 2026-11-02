package main

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/paths"
	"github.com/jingkaihe/kodelet-subagent/internal/version"
)

func TestMigrateCommandResolvesPathAndReportsSuccess(t *testing.T) {
	root := paths.Resolve(t.TempDir())
	t.Chdir(root)
	var stdout, stderr bytes.Buffer
	require.Zero(t, run([]string{"migrate", "data/subagents.sqlite"}, &stdout, &stderr), stderr.String())
	database := filepath.Join(root, "data", db.Filename)
	require.Equal(t, "Migrated "+database+"\n", stdout.String())
	require.FileExists(t, database)
}

func TestMigrateCommandRequiresOneDatabase(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 1, run([]string{"migrate"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "usage: kodelet-extension-subagent migrate DATABASE")
}

func TestVersionAndHelpCommands(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Zero(t, run([]string{"version"}, &stdout, &stderr))
	require.Equal(t, version.Version+"\n", stdout.String())
	stdout.Reset()
	require.Zero(t, run([]string{"--help"}, &stdout, &stderr))
	require.Contains(t, stdout.String(), "Without a command, serve the Kodelet extension protocol")
}

func TestUnknownCommandIsRejected(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 2, run([]string{"install"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), `unknown command "install"`)
}
