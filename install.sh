#!/usr/bin/env bash
# Install kodelet-extension-subagent from GitHub Releases:
#
#   curl -fsSL https://raw.githubusercontent.com/jingkaihe/kodelet-subagent/main/install.sh | bash
#
# Everything runs from main, called on the last line, so a partially
# downloaded script does nothing.
set -euo pipefail

REPO="jingkaihe/kodelet-subagent"
BINARY="kodelet-extension-subagent"
GITHUB_URL="https://github.com"
GITHUB_API_URL="https://api.github.com"
CURL_PROTOCOLS="=https"

VERSION="${KODELET_SUBAGENT_VERSION:-latest}"
INSTALL_DIR="${KODELET_SUBAGENT_INSTALL_DIR:-$HOME/.local}"

OS=""
ARCH=""
TAG=""
TEMP_DIR=""
STAGED=""

usage() {
	cat <<'EOF'
Usage: install.sh [-h|--help]

Installs kodelet-extension-subagent, the Kodelet subagent extension, from
GitHub Releases after verifying it against the release's checksums.txt.

Environment:
  KODELET_SUBAGENT_VERSION      Version to install, such as 0.7.0 (default: latest)
  KODELET_SUBAGENT_INSTALL_DIR  Install prefix; the binary goes in its bin
                                directory (default: ~/.local)
  GH_TOKEN, GITHUB_TOKEN        GitHub token for looking up the latest version.
                                Without one, `gh auth token` is used when the
                                GitHub CLI is signed in.
EOF
}

info() {
	printf '%s\n' "$*"
}

warn() {
	printf 'warning: %s\n' "$*" >&2
}

fail() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ -n "$STAGED" ]; then
		rm -f "$STAGED"
	fi
	if [ -n "$TEMP_DIR" ]; then
		rm -rf "$TEMP_DIR"
	fi
}

curl_get() {
	curl --proto "$CURL_PROTOCOLS" --tlsv1.2 -sSL --retry 3 "$@"
}

require_tools() {
	local tool
	for tool in curl uname mktemp awk sed; do
		command -v "$tool" >/dev/null 2>&1 || fail "missing required command: $tool"
	done
	if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
		fail "missing sha256sum or shasum, which are needed to verify the download"
	fi
}

detect_platform() {
	case "$(uname -s)" in
	Linux) OS="linux" ;;
	Darwin) OS="darwin" ;;
	*) fail "unsupported operating system $(uname -s); builds are published for Linux and macOS" ;;
	esac
	case "$(uname -m)" in
	x86_64 | amd64) ARCH="amd64" ;;
	arm64 | aarch64) ARCH="arm64" ;;
	*) fail "unsupported architecture $(uname -m); builds are published for amd64 and arm64" ;;
	esac
}

# github_token prints a token for the GitHub API, if one is available. It is
# only ever sent to the API, never to release downloads.
github_token() {
	if [ -n "${GH_TOKEN:-}" ]; then
		printf '%s' "$GH_TOKEN"
	elif [ -n "${GITHUB_TOKEN:-}" ]; then
		printf '%s' "$GITHUB_TOKEN"
	elif command -v gh >/dev/null 2>&1; then
		gh auth token --hostname github.com 2>/dev/null || true
	fi
}

valid_tag() {
	case "$1" in
	v[0-9]*) [ "${1#*[!A-Za-z0-9._-]}" = "$1" ] ;;
	*) return 1 ;;
	esac
}

# latest_tag_from_api asks the GitHub API for the latest release. Headers go
# through stdin so the token never appears in the process list.
latest_tag_from_api() {
	local token headers response
	token="$(github_token)"
	headers="Accept: application/vnd.github+json"
	if [ -n "$token" ]; then
		headers="$headers"$'\n'"Authorization: Bearer $token"
	fi
	response="$(printf '%s\n' "$headers" | curl_get -f -H @- "$GITHUB_API_URL/repos/$REPO/releases/latest" 2>/dev/null)" || return 1
	printf '%s\n' "$response" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'
}

# latest_tag_from_redirect follows github.com's redirect from releases/latest
# to releases/tag/<tag>, which does not count against API rate limits.
latest_tag_from_redirect() {
	local effective
	effective="$(curl_get -f -I -o /dev/null -w '%{url_effective}' "$GITHUB_URL/$REPO/releases/latest" 2>/dev/null)" || return 1
	case "$effective" in
	*/releases/tag/*) printf '%s\n' "${effective##*/releases/tag/}" ;;
	*) return 1 ;;
	esac
}

resolve_tag() {
	if [ "$VERSION" != "latest" ]; then
		TAG="v${VERSION#v}"
		valid_tag "$TAG" || fail "invalid version: $VERSION"
		return
	fi
	TAG="$(latest_tag_from_api || true)"
	TAG="${TAG%%$'\n'*}"
	if ! valid_tag "$TAG"; then
		TAG="$(latest_tag_from_redirect || true)"
	fi
	valid_tag "$TAG" || fail "could not find the latest release of $REPO"
}

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{ print $1 }'
	else
		shasum -a 256 "$1" | awk '{ print $1 }'
	fi
}

# download fetches name from the release into TEMP_DIR.
download() {
	local name="$1" url status
	url="$GITHUB_URL/$REPO/releases/download/$TAG/$name"
	status="$(curl_get -o "$TEMP_DIR/$name" -w '%{http_code}' "$url")" || fail "could not download $url"
	case "$status" in
	200) ;;
	404) fail "$TAG has no $name; Go builds are published from v0.7.0 for linux and darwin on amd64 and arm64" ;;
	*) fail "could not download $url (HTTP $status)" ;;
	esac
}

verify() {
	local name="$1" expected actual
	expected="$(awk -v name="$name" '$2 == name || $2 == "*" name { print $1; exit }' "$TEMP_DIR/checksums.txt")"
	[ -n "$expected" ] || fail "checksums.txt in $TAG has no entry for $name"
	actual="$(sha256 "$TEMP_DIR/$name")"
	[ "$actual" = "$expected" ] || fail "checksum mismatch for $name: expected $expected, got $actual"
}

# install_binary stages the binary beside its destination and renames it into
# place, which replaces a copy that is currently running without touching it.
install_binary() {
	local asset="$1" bin_dir dest previous installed
	bin_dir="$INSTALL_DIR/bin"
	dest="$bin_dir/$BINARY"
	mkdir -p "$bin_dir"
	STAGED="$bin_dir/.$BINARY.install.$$"
	cp "$TEMP_DIR/$asset" "$STAGED"
	chmod 755 "$STAGED"
	installed="$("$STAGED" version 2>/dev/null)" || fail "the downloaded $asset does not run on this machine"
	[ "$installed" = "${TAG#v}" ] || fail "the downloaded $asset reports version $installed, not ${TAG#v}"
	previous=""
	if [ -x "$dest" ]; then
		previous="$("$dest" version 2>/dev/null || echo unknown)"
	fi
	mv -f "$STAGED" "$dest"
	STAGED=""
	if [ -n "$previous" ] && [ "$previous" != "$installed" ]; then
		info "Upgraded $dest from $previous to $installed"
	elif [ -n "$previous" ]; then
		info "Reinstalled $installed at $dest"
	else
		info "Installed $installed at $dest"
	fi
}

check_environment() {
	local bin_dir="$INSTALL_DIR/bin" found legacy
	case ":$PATH:" in
	*":$bin_dir:"*)
		found="$(command -v "$BINARY" || true)"
		if [ -n "$found" ] && [ "$found" != "$bin_dir/$BINARY" ]; then
			warn "$found comes first on PATH, so Kodelet will run it instead; remove it or reorder PATH"
		fi
		;;
	*) warn "$bin_dir is not on PATH; add it to the PATH of the Kodelet runner so it can discover the extension" ;;
	esac
	legacy="$HOME/.kodelet/plugins/jingkaihe@kodelet-subagent"
	if [ -d "$legacy" ]; then
		warn "the Python plugin is still installed at $legacy"
		warn "Kodelet will not start two extensions with the same tools: stop the runner, run"
		warn "  rm -rf '$legacy'"
		warn "and start it again. Its agents are imported on first use."
	fi
}

main() {
	local asset
	case "${1:-}" in
	"") ;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage >&2
		fail "unknown argument: $1"
		;;
	esac

	require_tools
	detect_platform
	resolve_tag
	asset="$BINARY-$OS-$ARCH"
	info "Installing $BINARY $TAG for $OS/$ARCH"

	trap cleanup EXIT
	TEMP_DIR="$(mktemp -d)"
	download "$asset"
	download checksums.txt
	verify "$asset"
	install_binary "$asset"
	check_environment
	info "Restart the Kodelet runner to load it, then check: kodelet extension inspect subagent"
}

main "$@"
