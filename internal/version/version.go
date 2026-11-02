// Package version reports the extension version.
package version

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var raw string

// Version is the release version, maintained in the VERSION file.
var Version = strings.TrimSpace(raw)
