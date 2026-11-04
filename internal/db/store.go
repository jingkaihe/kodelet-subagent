// Package db stores durable background-agent state in SQLite.
//
// Runtime state transitions use explicit IMMEDIATE transactions. Every
// worker-side update is fenced by the lease token, generation, runtime ID, and
// lease expiry recorded when the run was claimed.
package db

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/url"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/jingkaihe/kodelet-subagent/internal/paths"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// Store limits and lease settings.
const (
	DefaultListLimit               = 64
	MaxListLimit                   = 256
	MaxActiveAgentsPerConversation = 3
	MaxActiveAgentsTotal           = 8
	LeaseDurationSeconds           = 60.0
	MaxSteeringMessageLength       = 10_000
)

// Clock returns the current time as epoch seconds.
type Clock func() float64

// SystemClock reads the wall clock.
func SystemClock() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// Store persists agents for one SQLite database on behalf of one runtime.
//
// Operations accept a context so callers can abandon work that has not
// started: a canceled context is reported before a transaction begins, but a
// transaction that has begun always runs to completion.
type Store struct {
	path           string
	runtimeID      string
	clock          Clock
	mailboxEnabled bool

	importSource string
	onImport     func(source, target string)

	mu     sync.Mutex
	db     *sqlx.DB
	readMu sync.Mutex
	readDB *sqlx.DB
}

// StoreOption configures NewStore.
type StoreOption func(*Store)

// ImportFrom makes a store whose database does not exist yet start from a
// copy of source, a database written by an earlier installation. notify, when
// set, is called after an import.
func ImportFrom(source string, notify func(source, target string)) StoreOption {
	return func(s *Store) {
		s.importSource = source
		s.onImport = notify
	}
}

// NewStore returns a store for the database at path. Relative paths are
// resolved against the current directory immediately.
func NewStore(path, runtimeID string, clock Clock, opts ...StoreOption) *Store {
	if clock == nil {
		clock = SystemClock
	}
	store := &Store{path: paths.Resolve(paths.ExpandUser(path)), runtimeID: runtimeID, clock: clock}
	for _, opt := range opts {
		opt(store)
	}
	return store
}

// Path returns the resolved database path.
func (s *Store) Path() string { return s.path }

// RuntimeID returns the runtime that owns leases created by this store.
func (s *Store) RuntimeID() string { return s.runtimeID }

// Now returns the store clock's current time.
func (s *Store) Now() float64 { return s.clock() }

// Close releases the database connection pool.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readMu.Lock()
	defer s.readMu.Unlock()
	var err error
	if s.db != nil {
		err = s.db.Close()
		s.db = nil
	}
	if s.readDB != nil {
		err = errors.Join(err, s.readDB.Close())
		s.readDB = nil
	}
	return err
}

// readDatabase never initializes or migrates a database. Its deferred,
// read-only transactions do not compete with lease writers in WAL mode.
// Fail fast on SQLite locks rather than outliving a hook's context deadline.
func (s *Store) readDatabase() (*sqlx.DB, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if s.readDB == nil {
		var err error
		s.readDB, err = sqlx.Open("sqlite", databaseURI(s.path, url.Values{
			"mode": {"ro"}, "_pragma": {"busy_timeout(0)"},
		}))
		if err != nil {
			return nil, err
		}
	}
	return s.readDB, nil
}

func (s *Store) database() (*sqlx.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		return s.db, nil
	}
	imported, err := migrate(s.path, s.importSource)
	if err != nil {
		return nil, err
	}
	if imported && s.onImport != nil {
		s.onImport(s.importSource, s.path)
	}
	db, err := Open(s.path)
	if err != nil {
		return nil, err
	}
	s.db = db
	return db, nil
}

// Initialize migrates the database once and reconciles expired claims.
func (s *Store) Initialize(ctx context.Context) error {
	_, err := s.ReconcileExpired(ctx)
	return err
}

func (s *Store) transaction(ctx context.Context, fn func(tx *sqlx.Tx, now float64) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	background := context.Background()
	tx, err := db.BeginTxx(background, nil)
	if err != nil {
		return err
	}
	if err := fn(tx, s.Now()); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

type agentRow struct {
	ID                  string   `db:"id"`
	Name                string   `db:"name"`
	OwnerConversationID string   `db:"owner_conversation_id"`
	ChildConversationID *string  `db:"child_conversation_id"`
	ContextMode         string   `db:"context_mode"`
	CWD                 string   `db:"cwd"`
	Status              string   `db:"status"`
	ActiveRunID         string   `db:"active_run_id"`
	Generation          int      `db:"generation"`
	LeaseRuntimeID      *string  `db:"lease_runtime_id"`
	LeaseToken          *string  `db:"lease_token"`
	LeaseExpiresAt      *float64 `db:"lease_expires_at"`
	CreatedAt           float64  `db:"created_at"`
	UpdatedAt           float64  `db:"updated_at"`
}

type runRow struct {
	ID          string   `db:"id"`
	AgentID     string   `db:"agent_id"`
	Generation  int      `db:"generation"`
	LeaseToken  string   `db:"lease_token"`
	Task        string   `db:"task"`
	Status      string   `db:"status"`
	Result      *string  `db:"result"`
	Error       *string  `db:"error"`
	CreatedAt   float64  `db:"created_at"`
	StartedAt   *float64 `db:"started_at"`
	CompletedAt *float64 `db:"completed_at"`
	UpdatedAt   float64  `db:"updated_at"`
}

type activeRow struct {
	ID          string `db:"id"`
	ActiveRunID string `db:"active_run_id"`
	Generation  int    `db:"generation"`
	Status      string `db:"status"`
}

func newID(prefix string) string {
	var data [16]byte
	_, _ = rand.Read(data[:])
	data[6] = (data[6] & 0x0f) | 0x40
	data[8] = (data[8] & 0x3f) | 0x80
	return prefix + hex.EncodeToString(data[:])
}

func newToken() string {
	var data [32]byte
	_, _ = rand.Read(data[:])
	return hex.EncodeToString(data[:])
}

func equalStrings(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func agentRowTx(tx *sqlx.Tx, agentID string) (agentRow, error) {
	var row agentRow
	err := tx.Get(&row, `SELECT * FROM agents WHERE id = ?`, agentID)
	if errors.Is(err, sql.ErrNoRows) {
		return row, newError(kindNotFound, "agent not found: "+agentID)
	}
	return row, err
}

func runRowTx(tx *sqlx.Tx, agentID, runID string) (runRow, error) {
	var row runRow
	err := tx.Get(&row, `SELECT * FROM runs WHERE id = ? AND agent_id = ?`, runID, agentID)
	if errors.Is(err, sql.ErrNoRows) {
		return row, newError(kindNotFound, "agent run not found: "+runID)
	}
	return row, err
}

func fetchAgentTx(tx *sqlx.Tx, agentID, runID, ownerID string) (types.Agent, error) {
	agent, err := agentRowTx(tx, agentID)
	if err != nil {
		return types.Agent{}, err
	}
	if ownerID != "" && agent.OwnerConversationID != ownerID {
		return types.Agent{}, newError(kindNotFound, "agent not found: "+agentID)
	}
	if runID == "" {
		runID = agent.ActiveRunID
	}
	run, err := runRowTx(tx, agentID, runID)
	if err != nil {
		return types.Agent{}, err
	}
	return types.Agent{
		ID:                  agent.ID,
		Name:                agent.Name,
		OwnerConversationID: agent.OwnerConversationID,
		ConversationID:      agent.ChildConversationID,
		ContextMode:         types.ContextMode(agent.ContextMode),
		CWD:                 agent.CWD,
		Status:              types.AgentStatus(agent.Status),
		Generation:          agent.Generation,
		CreatedAt:           agent.CreatedAt,
		UpdatedAt:           agent.UpdatedAt,
		Run: types.Run{
			ID:          run.ID,
			Generation:  run.Generation,
			Task:        run.Task,
			Status:      types.RunStatus(run.Status),
			Result:      run.Result,
			Error:       run.Error,
			CreatedAt:   run.CreatedAt,
			StartedAt:   run.StartedAt,
			CompletedAt: run.CompletedAt,
			UpdatedAt:   run.UpdatedAt,
		},
	}, nil
}
