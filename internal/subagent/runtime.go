package subagent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/paths"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// Environment variables read by the extension.
const (
	// RecursionGuardEnv marks extension processes hosted by a child agent.
	RecursionGuardEnv = "KODELET_SUBAGENT_EXTENSION_CHILD"
	// ProfileEnv selects the model profile for fresh agents.
	ProfileEnv = "KODELET_SUBAGENT_PROFILE"
)

// AgentToolNames lists the tools disabled for child agents.
var AgentToolNames = []string{
	"spawn_agent",
	"wait_agent",
	"list_agents",
	"followup_agent",
	"steer_agent",
	"cancel_agent",
}

const stoppedSessionMessage = "agent interrupted because the extension session stopped"

var errShuttingDown = errors.New("the extension session is shutting down")

// Timing holds the runtime's timeouts and retry intervals.
type Timing struct {
	AgentTimeout                       time.Duration
	AgentStartTimeout                  time.Duration
	CancelCleanupTimeout               time.Duration
	HeartbeatInterval                  time.Duration
	MinHeartbeatInterval               time.Duration
	HeartbeatRetryMax                  time.Duration
	WorkerUpdateRetryInitial           time.Duration
	WorkerUpdateRetryMax               time.Duration
	BackgroundLeaseReleaseRetryInitial time.Duration
	BackgroundLeaseReleaseRetryMax     time.Duration
	ClientCloseRetryInitial            time.Duration
	ClientCloseRetryMax                time.Duration
	SteeringPoll                       time.Duration
	SteeringRetry                      time.Duration
	// HostRequestTimeout bounds widget and lease requests made after the
	// originating call has returned.
	HostRequestTimeout time.Duration
}

// DefaultTiming returns the production timing.
func DefaultTiming() Timing {
	return Timing{
		AgentTimeout:                       time.Hour - 10*time.Second,
		AgentStartTimeout:                  time.Minute,
		CancelCleanupTimeout:               10 * time.Second,
		HeartbeatInterval:                  20 * time.Second,
		MinHeartbeatInterval:               time.Second,
		HeartbeatRetryMax:                  2 * time.Second,
		WorkerUpdateRetryInitial:           100 * time.Millisecond,
		WorkerUpdateRetryMax:               time.Second,
		BackgroundLeaseReleaseRetryInitial: 100 * time.Millisecond,
		BackgroundLeaseReleaseRetryMax:     2 * time.Second,
		ClientCloseRetryInitial:            100 * time.Millisecond,
		ClientCloseRetryMax:                2 * time.Second,
		SteeringPoll:                       100 * time.Millisecond,
		SteeringRetry:                      250 * time.Millisecond,
		HostRequestTimeout:                 30 * time.Second,
	}
}

func (t Timing) heartbeatInterval() time.Duration {
	return max(t.MinHeartbeatInterval, t.HeartbeatInterval)
}

// RuntimeOptions configures NewRuntime.
type RuntimeOptions struct {
	// RuntimeID identifies this process's leases; it defaults to a random ID.
	RuntimeID string
	// ClientFactory defaults to DefaultClientFactory.
	ClientFactory ClientFactory
	// Timing defaults to DefaultTiming.
	Timing *Timing
	// NewStore opens the store for a database path. The default store imports
	// the Python extension's database on first use.
	NewStore func(path, runtimeID string) Store
}

// Runtime owns mutable extension-process state and background-agent
// orchestration.
//
// liveRuns is only the current-generation lookup for an agent; ownedRuns
// retains every generation by run ID until its cleanup completes.
type Runtime struct {
	runtimeID     string
	clientFactory ClientFactory
	newStore      func(path, runtimeID string) Store
	timing        Timing

	mu           sync.Mutex
	stores       map[string]Store
	liveRuns     map[runKey]*LiveRun
	ownedRuns    map[runKey]*LiveRun
	setupTasks   map[*task]struct{}
	cleanupTasks map[*task]struct{}
	reservations map[chan struct{}]struct{}
	widgetLocks  map[runKey]*sync.Mutex
	shuttingDown bool

	shutdownMu sync.Mutex
	shutdownOp *task
}

// NewRuntime returns an empty runtime.
func NewRuntime(opts RuntimeOptions) *Runtime {
	runtimeID := opts.RuntimeID
	if runtimeID == "" {
		var data [16]byte
		_, _ = rand.Read(data[:])
		runtimeID = "runtime_" + hex.EncodeToString(data[:])
	}
	clientFactory := opts.ClientFactory
	if clientFactory == nil {
		clientFactory = DefaultClientFactory
	}
	timing := DefaultTiming()
	if opts.Timing != nil {
		timing = *opts.Timing
	}
	newStore := opts.NewStore
	if newStore == nil {
		newStore = newDefaultStore
	}
	return &Runtime{
		runtimeID:     runtimeID,
		clientFactory: clientFactory,
		newStore:      newStore,
		timing:        timing,
		stores:        map[string]Store{},
		liveRuns:      map[runKey]*LiveRun{},
		ownedRuns:     map[runKey]*LiveRun{},
		setupTasks:    map[*task]struct{}{},
		cleanupTasks:  map[*task]struct{}{},
		reservations:  map[chan struct{}]struct{}{},
		widgetLocks:   map[runKey]*sync.Mutex{},
	}
}

// RuntimeID returns the ID recorded on this runtime's leases.
func (r *Runtime) RuntimeID() string { return r.runtimeID }

// ResetForInitialize lets a newly initialized extension session accept work.
func (r *Runtime) ResetForInitialize() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shuttingDown = false
}

func (r *Runtime) ensureAccepting() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shuttingDown {
		return errShuttingDown
	}
	return nil
}

func databasePath(call Call) string {
	return filepath.Join(paths.Resolve(call.DataDir()), db.Filename)
}

// storeForCall returns the initialized store for the call's data directory.
func (r *Runtime) storeForCall(ctx context.Context, call Call) (Store, error) {
	path := databasePath(call)
	r.mu.Lock()
	store := r.stores[path]
	if store == nil {
		store = r.newStore(path, r.runtimeID)
		r.stores[path] = store
	}
	r.mu.Unlock()
	if err := store.Initialize(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

func (r *Runtime) liveRun(store Store, agentID string) *LiveRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.liveRuns[runKey{path: store.Path(), id: agentID}]
}

func (r *Runtime) ownedRun(store Store, runID string) *LiveRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ownedRuns[runKey{path: store.Path(), id: runID}]
}

func (r *Runtime) widgetLock(store Store, ownerID string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := runKey{path: store.Path(), id: ownerID}
	lock := r.widgetLocks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		r.widgetLocks[key] = lock
	}
	return lock
}

// syncAgentWidget renders the owner's agents into the persistent widget.
func (r *Runtime) syncAgentWidget(ctx context.Context, ui WidgetUI, store Store, ownerID string) error {
	if ui == nil {
		return nil
	}
	lock := r.widgetLock(store, ownerID)
	lock.Lock()
	defer lock.Unlock()
	agents, err := store.List(ctx, ownerID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, r.timing.HostRequestTimeout)
	defer cancel()
	if len(agents) == 0 {
		return ui.RemoveWidget(ctx, WidgetID)
	}
	return ui.SetWidget(ctx, WidgetID, agentWidgetLines(agents), extension.WidgetAboveComposer)
}

func (r *Runtime) safeSyncAgentWidget(ctx context.Context, ui WidgetUI, store Store, ownerID string) {
	_ = r.syncAgentWidget(ctx, ui, store, ownerID)
}

func (r *Runtime) liveRunFromClaim(claim types.Claim, taskText string, store Store) *LiveRun {
	return newLiveRun(claim, taskText, store)
}
