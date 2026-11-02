// Package paths resolves user-supplied filesystem paths the way the original
// Python extension did: symlinks are resolved before ".." is applied.
package paths

import (
	"os"
	"path/filepath"
	"strings"
)

const maxSymlinkHops = 255

// ExpandUser replaces a leading "~" with the current user's home directory.
func ExpandUser(path string) string {
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	return filepath.Join(home, strings.TrimPrefix(path, "~"))
}

// Resolve returns an absolute path with symlinks resolved as far as the
// path exists. Unlike filepath.Clean, ".." applies to the resolved parent, and
// missing trailing components are appended without requiring them to exist.
func Resolve(path string) string {
	if !filepath.IsAbs(path) {
		if cwd, err := os.Getwd(); err == nil {
			path = cwd + string(filepath.Separator) + path
		}
	}
	hops := 0
	return resolveFrom(string(filepath.Separator), path, &hops)
}

func resolveFrom(base, path string, hops *int) string {
	resolved := base
	components := strings.Split(path, string(filepath.Separator))
	for index, component := range components {
		switch component {
		case "", ".":
			continue
		case "..":
			resolved = filepath.Dir(resolved)
			continue
		}
		next := filepath.Join(resolved, component)
		info, err := os.Lstat(next)
		if err != nil {
			// The remainder does not exist; keep it lexical.
			rest := strings.Join(components[index+1:], string(filepath.Separator))
			return filepath.Clean(filepath.Join(next, rest))
		}
		if info.Mode()&os.ModeSymlink == 0 {
			resolved = next
			continue
		}
		*hops++
		target, err := os.Readlink(next)
		if err != nil || *hops > maxSymlinkHops {
			resolved = next
			continue
		}
		if filepath.IsAbs(target) {
			resolved = resolveFrom(string(filepath.Separator), target, hops)
		} else {
			resolved = resolveFrom(resolved, target, hops)
		}
	}
	return resolved
}
