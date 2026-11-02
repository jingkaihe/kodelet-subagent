package subagent

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/jingkaihe/kodelet/sdk/go/taskprogress"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/paths"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

type fixture struct {
	t       *testing.T
	ctx     context.Context
	root    string
	cwd     string
	dataDir string
	acp     *fakeACP
	timing  Timing
	runtime *Runtime
	app     *Application
}

// newFixture builds an isolated application backed by fake ACP clients.
func newFixture(t *testing.T, tune ...func(*Timing)) *fixture {
	t.Helper()
	t.Setenv(RecursionGuardEnv, "0")
	t.Setenv(ProfileEnv, "")
	root := paths.Resolve(t.TempDir())
	f := &fixture{
		t:       t,
		ctx:     context.Background(),
		root:    root,
		cwd:     filepath.Join(root, "workspace"),
		dataDir: filepath.Join(root, "extension-data"),
		acp:     newFakeACP(),
		timing:  DefaultTiming(),
	}
	require.NoError(t, os.Mkdir(f.cwd, 0o755))
	require.NoError(t, os.Mkdir(f.dataDir, 0o755))
	for _, apply := range tune {
		apply(&f.timing)
	}
	f.runtime = f.newRuntime("")
	f.app = NewApplication(f.runtime)
	t.Cleanup(f.close)
	return f
}

func (f *fixture) newRuntime(runtimeID string) *Runtime {
	timing := f.timing
	return NewRuntime(RuntimeOptions{RuntimeID: runtimeID, ClientFactory: f.acp.factory, Timing: &timing})
}

// close releases every blocked fake and drains the runtimes.
func (f *fixture) close() {
	f.acp.releaseAll()
	f.runtime.mu.Lock()
	var leases []*fakeLease
	for _, live := range f.runtime.ownedRuns {
		if lease, ok := live.BackgroundLease().(*fakeLease); ok {
			leases = append(leases, lease)
		}
	}
	f.runtime.mu.Unlock()
	for _, lease := range leases {
		lease.closeRelease.set()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.runtime.Shutdown(ctx); err != nil {
		f.t.Errorf("runtime did not shut down: %v", err)
	}
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	for _, store := range f.runtime.stores {
		if closer, ok := store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
}

type callOption func(*fakeCall)

func invokedBy(value string) callOption   { return func(c *fakeCall) { c.info.InvokedBy = value } }
func withProfile(value string) callOption { return func(c *fakeCall) { c.info.Profile = value } }
func blockFork() callOption               { return func(c *fakeCall) { c.blockFork = true } }
func blockAcquire() callOption            { return func(c *fakeCall) { c.blockAcquire = true } }
func blockRelease() callOption            { return func(c *fakeCall) { c.blockRelease = true } }

func (f *fixture) call(owner string, opts ...callOption) *fakeCall {
	call := &fakeCall{
		info:           extension.CallInfo{ConversationID: owner, CWD: f.cwd},
		dataDir:        f.dataDir,
		ui:             newFakeUI(),
		forkStarted:    newEvent(),
		forkRelease:    newEvent(),
		acquireStarted: newEvent(),
		acquireRelease: newEvent(),
	}
	for _, apply := range opts {
		apply(call)
	}
	return call
}

func (f *fixture) store(call *fakeCall) Store {
	f.t.Helper()
	store, err := f.runtime.storeForCall(f.ctx, call)
	require.NoError(f.t, err)
	return store
}

// hookStore installs hooks around the store used for call's data directory.
func (f *fixture) hookStore(call *fakeCall, hooks *hookedStore) *hookedStore {
	hooks.Store = f.store(call)
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	f.runtime.stores[hooks.Store.Path()] = hooks
	return hooks
}

// standaloneStore opens a database outside the runtime under name.
func (f *fixture) standaloneStore(name, runtimeID string, clock db.Clock) *db.Store {
	f.t.Helper()
	store := db.NewStore(filepath.Join(f.root, name, db.Filename), runtimeID, clock)
	f.t.Cleanup(func() { _ = store.Close() })
	require.NoError(f.t, store.Initialize(f.ctx))
	return store
}

func (f *fixture) get(store Store, owner, agentID string) types.Agent {
	f.t.Helper()
	agent, err := store.Get(f.ctx, owner, agentID, "")
	require.NoError(f.t, err)
	return agent
}

func (f *fixture) waitForStatus(store Store, owner, agentID string, statuses ...types.RunStatus) types.Agent {
	f.t.Helper()
	var agent types.Agent
	waitUntil(f.t, func() bool {
		agent = f.get(store, owner, agentID)
		return slices.Contains(statuses, agent.Run.Status)
	})
	return agent
}

func (f *fixture) steeringRows(store Store) [][3]any {
	f.t.Helper()
	database, err := db.Open(store.Path())
	require.NoError(f.t, err)
	defer database.Close()
	var rows []struct {
		RunID      string `db:"run_id"`
		Generation int    `db:"generation"`
		Message    string `db:"message"`
	}
	require.NoError(f.t, database.Select(&rows, "SELECT run_id, generation, message FROM steering_messages ORDER BY id"))
	result := make([][3]any, 0, len(rows))
	for _, row := range rows {
		result = append(result, [3]any{row.RunID, row.Generation, row.Message})
	}
	return result
}

func (f *fixture) ownedCount() int {
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	return len(f.runtime.ownedRuns)
}

func (f *fixture) liveCount() int {
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	return len(f.runtime.liveRuns)
}

func (f *fixture) cleanupCount() int {
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	return len(f.runtime.cleanupTasks)
}

func (f *fixture) setupCount() int {
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	return len(f.runtime.setupTasks)
}

// Tool call helpers.

func (f *fixture) spawn(call *fakeCall, input SpawnAgentInput) extension.ToolResult {
	if input.ContextMode == "" {
		input.ContextMode = types.ContextFork
	}
	return f.app.SpawnAgent(f.ctx, input, call)
}

func (f *fixture) wait(call *fakeCall, agentID string, timeoutMS int) extension.ToolResult {
	return f.app.WaitAgent(f.ctx, WaitAgentInput{AgentID: agentID, TimeoutMS: timeoutMS}, call)
}

func (f *fixture) followup(call *fakeCall, agentID, task string) extension.ToolResult {
	return f.app.FollowupAgent(f.ctx, FollowupAgentInput{AgentID: agentID, Task: task}, call)
}

func (f *fixture) cancel(call *fakeCall, agentID string) extension.ToolResult {
	return f.app.CancelAgent(f.ctx, CancelAgentInput{AgentID: agentID}, call)
}

func (f *fixture) steer(call *fakeCall, agentID, message string) extension.ToolResult {
	return f.app.SteerAgent(f.ctx, SteerAgentInput{AgentID: agentID, Message: message}, call)
}

func (f *fixture) list(call *fakeCall) extension.ToolResult {
	return f.app.ListAgents(f.ctx, ListAgentsInput{}, call)
}

// Result accessors.

func str(result extension.ToolResult, key string) string {
	value, _ := result.Data[key].(string)
	return value
}

func agentID(t *testing.T, result extension.ToolResult) string {
	t.Helper()
	require.Empty(t, result.Error, result.Content)
	id := str(result, "agent_id")
	require.NotEmpty(t, id)
	return id
}

func presentationOf(result extension.ToolResult) extension.Presentation {
	value, _ := result.Data["presentation"].(extension.Presentation)
	return value
}

func taskRunOf(t *testing.T, result extension.ToolResult) taskprogress.Snapshot {
	t.Helper()
	value, ok := result.Data["taskRun"].(taskprogress.Snapshot)
	require.True(t, ok, "result has no taskRun")
	return value
}

func activities(snapshot taskprogress.Snapshot) map[string]taskprogress.Activity {
	byID := make(map[string]taskprogress.Activity, len(snapshot.Activities))
	for _, activity := range snapshot.Activities {
		byID[activity.ID] = activity
	}
	return byID
}
