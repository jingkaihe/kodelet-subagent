package db

import "errors"

// Sentinel errors matched with errors.Is.
var (
	ErrAgentNotFound       = errors.New("agent not found")
	ErrAgentConflict       = errors.New("agent conflict")
	ErrAgentLimit          = errors.New("agent limit reached")
	ErrLeaseLost           = errors.New("agent lease lost")
	ErrDatabaseBootstrap   = errors.New("subagent database bootstrap failed")
	ErrUnsupportedDatabase = errors.New("unsupported subagent database")
	ErrDatabaseMigration   = errors.New("subagent database migration failed")
)

type errorKind int

const (
	kindNotFound errorKind = iota + 1
	kindConflict
	kindLimit
	kindLeaseLost
	kindUnsupportedDatabase
	kindDatabaseMigration
)

// Error is a store error with a model-facing message.
type Error struct {
	kind    errorKind
	message string
	cause   error
}

func newError(kind errorKind, message string) *Error {
	return &Error{kind: kind, message: message}
}

func wrapError(kind errorKind, message string, cause error) *Error {
	return &Error{kind: kind, message: message, cause: cause}
}

func (e *Error) Error() string { return e.message }

// Unwrap returns the underlying cause, if any.
func (e *Error) Unwrap() error { return e.cause }

// Is matches the package sentinel errors.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrAgentNotFound:
		return e.kind == kindNotFound
	case ErrAgentConflict:
		return e.kind == kindConflict
	case ErrAgentLimit:
		return e.kind == kindLimit
	case ErrLeaseLost:
		return e.kind == kindLeaseLost
	case ErrUnsupportedDatabase:
		return e.kind == kindUnsupportedDatabase
	case ErrDatabaseMigration:
		return e.kind == kindDatabaseMigration
	case ErrDatabaseBootstrap:
		return e.kind == kindUnsupportedDatabase || e.kind == kindDatabaseMigration
	default:
		return false
	}
}

// SQLite primary result codes inspected by callers.
const (
	sqliteBusy       = 5
	sqliteLocked     = 6
	sqliteConstraint = 19
)

func sqliteCode(err error) (int, bool) {
	var coded interface{ Code() int }
	if !errors.As(err, &coded) {
		return 0, false
	}
	return coded.Code() & 0xff, true
}

// IsRetryable reports whether err is a transient SQLite lock or busy error.
func IsRetryable(err error) bool {
	code, ok := sqliteCode(err)
	return ok && (code == sqliteBusy || code == sqliteLocked)
}

// IsDefinitive reports whether err proves a worker no longer owns its claim.
func IsDefinitive(err error) bool {
	return errors.Is(err, ErrAgentNotFound) || errors.Is(err, ErrAgentConflict) || errors.Is(err, ErrLeaseLost)
}

func isConstraintViolation(err error) bool {
	code, ok := sqliteCode(err)
	return ok && code == sqliteConstraint
}
