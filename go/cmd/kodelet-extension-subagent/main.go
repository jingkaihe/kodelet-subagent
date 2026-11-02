// Command kodelet-extension-subagent is the Kodelet extension for durable,
// named background agents. Run without arguments, it serves the extension
// protocol over stdio; subcommands install and maintain it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/install"
	"github.com/jingkaihe/kodelet-subagent/internal/paths"
	"github.com/jingkaihe/kodelet-subagent/internal/subagent"
	"github.com/jingkaihe/kodelet-subagent/internal/version"
)

// drainTimeout bounds how long the process waits, after the host disconnects,
// for owned agents to stop and release their claims.
const drainTimeout = 30 * time.Second

const usage = `Usage: kodelet-extension-subagent [command]

Without a command, serve the Kodelet extension protocol over stdio.

Commands:
  install [--home DIR]   Install this binary as the jingkaihe@kodelet-subagent plugin
  migrate DATABASE       Upgrade a subagent SQLite database to the latest revision
  version                Print the version
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return serve(stderr)
	}
	var err error
	switch args[0] {
	case "serve":
		return serve(stderr)
	case "install":
		err = installCommand(args[1:], stdout)
	case "migrate":
		err = migrateCommand(args[1:], stdout)
	case "version", "--version":
		_, err = fmt.Fprintln(stdout, version.Version)
	case "help", "-h", "--help":
		_, err = fmt.Fprint(stdout, usage)
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "kodelet-extension-subagent: %v\n", err)
		return 1
	}
	return 0
}

func serve(stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	app := subagent.NewApplication(nil)
	if err := app.Run(ctx, drainTimeout); err != nil && !errors.Is(err, context.Canceled) {
		_, _ = fmt.Fprintf(stderr, "kodelet-extension-subagent: %v\n", err)
		return 1
	}
	return 0
}

func installCommand(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("install", flag.ContinueOnError)
	home := flags.String("home", "", "override the home directory used for installation")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *home == "" {
		var err error
		if *home, err = os.UserHomeDir(); err != nil {
			return err
		}
	}
	source, err := os.Executable()
	if err != nil {
		return err
	}
	if resolved, err := filepath.EvalSymlinks(source); err == nil {
		source = resolved
	}
	installed, err := install.Plugin(*home, source)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "Installed Kodelet subagent extension at %s\n", installed)
	return err
}

func migrateCommand(args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: kodelet-extension-subagent migrate DATABASE")
	}
	database := paths.Resolve(paths.ExpandUser(args[0]))
	if err := db.Migrate(database); err != nil {
		return err
	}
	_, err := fmt.Fprintf(stdout, "Migrated %s\n", database)
	return err
}
