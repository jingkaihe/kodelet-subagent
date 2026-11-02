package db

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/paths"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

type mutableClock struct {
	mu    sync.Mutex
	value float64
}

func newMutableClock() *mutableClock { return &mutableClock{value: 1_000} }

func (c *mutableClock) now() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

func (c *mutableClock) advance(seconds float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += seconds
}

func newTestStore(t *testing.T, path, runtimeID string, clock Clock) *Store {
	t.Helper()
	store := NewStore(path, runtimeID, clock)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Initialize(context.Background()))
	return store
}

//go:fix inline

func steeringRows(t *testing.T, path string) [][3]any {
	t.Helper()
	var rows []struct {
		RunID      string `db:"run_id"`
		Generation int    `db:"generation"`
		Message    string `db:"message"`
	}
	require.NoError(t, rawDB(t, path).Select(&rows, "SELECT run_id, generation, message FROM steering_messages ORDER BY id"))
	result := make([][3]any, 0, len(rows))
	for _, row := range rows {
		result = append(result, [3]any{row.RunID, row.Generation, row.Message})
	}
	return result
}

func TestStoreResolvesRelativeDatabasePathAtConstruction(t *testing.T) {
	root := paths.Resolve(t.TempDir())
	original := filepath.Join(root, "original")
	later := filepath.Join(root, "later")
	require.NoError(t, os.Mkdir(original, 0o755))
	require.NoError(t, os.Mkdir(later, 0o755))
	t.Chdir(original)

	store := newTestStore(t, Filename, "runtime-relative-path", nil)
	expected := filepath.Join(original, Filename)
	require.Equal(t, expected, store.Path())

	t.Chdir(later)
	ctx := context.Background()
	claim, err := store.Create(ctx, "owner", "stable-path", "verify stable database ownership", later, types.ContextFresh)
	require.NoError(t, err)
	persisted, err := store.Get(ctx, "owner", claim.Agent.ID, "")
	require.NoError(t, err)
	require.Equal(t, "stable-path", persisted.Name)
	require.FileExists(t, expected)
	require.NoFileExists(t, filepath.Join(later, Filename))
}

func TestSchemaWALAndRecordsSurviveStoreReconstruction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newTestStore(t, filepath.Join(dir, Filename), "runtime-original", nil)
	claim, err := store.Create(ctx, "owner-1", "persistence-worker", "persist this task", dir, types.ContextFresh)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	db := rawDB(t, store.Path())
	var journalMode, revision string
	require.NoError(t, db.Get(&journalMode, "PRAGMA journal_mode"))
	require.NoError(t, db.Get(&revision, "SELECT version_num FROM alembic_version"))
	require.Equal(t, "wal", journalMode)
	require.Equal(t, HeadRevision, revision)

	reconstructed := newTestStore(t, store.Path(), "runtime-reconstructed", nil)
	persisted, err := reconstructed.Get(ctx, "owner-1", claim.Agent.ID, claim.Lease.RunID)
	require.NoError(t, err)
	require.Equal(t, "persist this task", persisted.Run.Task)
	require.Equal(t, "persistence-worker", persisted.Name)
	require.Equal(t, types.AgentStarting, persisted.Status)
}

func TestAgentNamesAreCanonicalAndUniquePerOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newTestStore(t, filepath.Join(dir, "agent-names", Filename), "runtime-names", nil)
	first, err := store.Create(ctx, "owner", "research-agent", "first task", dir, types.ContextFresh)
	require.NoError(t, err)
	require.Equal(t, "research-agent", first.Agent.Name)

	_, err = store.Create(ctx, "owner", "research-agent", "duplicate task", dir, types.ContextFresh)
	require.ErrorIs(t, err, ErrAgentConflict)
	require.ErrorContains(t, err, "agent name already exists")

	other, err := store.Create(ctx, "other-owner", "research-agent", "other task", dir, types.ContextFresh)
	require.NoError(t, err)
	require.Equal(t, "research-agent", other.Agent.Name)

	for _, invalid := range []string{
		"Research-agent", "research agent", "research_agent", "one-two-three-four",
		"-reviewer", "reviewer-", "reviewer--one", "1-reviewer",
	} {
		_, err := store.Create(ctx, "owner", invalid, "invalid task", dir, types.ContextFresh)
		require.ErrorContains(t, err, "name must contain", invalid)
	}
}

func TestStoreLimitsAreAtomicPerOwnerAndGlobally(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	run := func(path string, count int, owner func(int) string) []error {
		stores := make([]*Store, count)
		for i := range stores {
			stores[i] = newTestStore(t, path, "runtime-"+string(rune('a'+i)), nil)
		}
		errs := make([]error, count)
		var wg sync.WaitGroup
		for i, store := range stores {
			wg.Go(func() {
				_, errs[i] = store.Create(ctx, owner(i), "agent-"+string(rune('a'+i)), "task", dir, types.ContextFresh)
			})
		}
		wg.Wait()
		return errs
	}
	failures := func(errs []error) []error {
		var failed []error
		for _, err := range errs {
			if err != nil {
				failed = append(failed, err)
			}
		}
		return failed
	}

	ownerErrs := failures(run(filepath.Join(dir, "owner-limit", Filename), 4, func(int) string { return "one-owner" }))
	require.Len(t, ownerErrs, 1)
	require.ErrorIs(t, ownerErrs[0], ErrAgentLimit)

	totalErrs := failures(run(filepath.Join(dir, "total-limit", Filename), 9, func(i int) string {
		return "owner-" + string(rune('0'+i/3))
	}))
	require.Len(t, totalErrs, 1)
	require.ErrorIs(t, totalErrs[0], ErrAgentLimit)
	require.ErrorContains(t, totalErrs[0], "extension")
}

func TestExpiryReconciliationAndGenerationRuntimeFencing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := newMutableClock()
	store := newTestStore(t, filepath.Join(dir, "leases", Filename), "runtime-a", clock.now)
	claim, err := store.Create(ctx, "owner", "lease-worker", "work", dir, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(ctx, claim.Lease, "child-lease")
	require.NoError(t, err)
	clock.advance(LeaseDurationSeconds + 0.1)
	reconciled, err := store.ReconcileExpired(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, reconciled)
	expired, err := store.Get(ctx, "owner", claim.Agent.ID, "")
	require.NoError(t, err)
	require.Equal(t, types.AgentInterrupted, expired.Status)
	require.Equal(t, types.RunInterrupted, expired.Run.Status)
	require.Contains(t, *expired.Run.Error, "lease expired")

	resumed, err := store.Claim(ctx, "owner", claim.Agent.ID, "retry")
	require.NoError(t, err)
	_, err = store.Heartbeat(ctx, claim.Lease)
	require.ErrorIs(t, err, ErrLeaseLost)

	second := newTestStore(t, store.Path(), "runtime-b", clock.now)
	_, err = second.Heartbeat(ctx, resumed.Lease.WithRuntimeID("runtime-b"))
	require.ErrorIs(t, err, ErrLeaseLost)
}

func TestCancelingLeaseExpiryRecoversToResumableCanceled(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := newMutableClock()
	owner := newTestStore(t, filepath.Join(dir, "cancel-expiry", Filename), "runtime-owner", clock.now)
	claim, err := owner.Create(ctx, "owner", "cancel-expiry-worker", "work", dir, types.ContextFresh)
	require.NoError(t, err)
	_, err = owner.MarkRunning(ctx, claim.Lease, "child-cancel-expiry")
	require.NoError(t, err)
	other := newTestStore(t, owner.Path(), "runtime-other", clock.now)

	canceling, err := other.Cancel(ctx, "owner", claim.Agent.ID)
	require.NoError(t, err)
	require.Equal(t, types.AgentCanceling, canceling.Status)
	require.Equal(t, types.RunCanceled, canceling.Run.Status)
	_, err = other.Claim(ctx, "owner", claim.Agent.ID, "too early")
	require.ErrorIs(t, err, ErrAgentConflict)
	require.ErrorContains(t, err, "agent is canceling")

	clock.advance(LeaseDurationSeconds - 10)
	renewed, err := owner.HeartbeatCanceling(ctx, claim.Lease)
	require.NoError(t, err)
	require.Equal(t, clock.now()+LeaseDurationSeconds, renewed)
	clock.advance(11)
	reconciled, err := other.ReconcileExpired(ctx)
	require.NoError(t, err)
	require.Zero(t, reconciled)
	still, err := other.Get(ctx, "owner", claim.Agent.ID, "")
	require.NoError(t, err)
	require.Equal(t, types.AgentCanceling, still.Status)

	clock.advance(LeaseDurationSeconds - 10 + 0.1)
	reconciled, err = other.ReconcileExpired(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, reconciled)
	canceled, err := other.Get(ctx, "owner", claim.Agent.ID, "")
	require.NoError(t, err)
	require.Equal(t, types.AgentCanceled, canceled.Status)
	resumed, err := other.Claim(ctx, "owner", claim.Agent.ID, "resume after recovery")
	require.NoError(t, err)
	require.Equal(t, 2, resumed.Agent.Generation)
	completed, err := owner.CompleteCancel(ctx, claim.Lease)
	require.NoError(t, err)
	require.False(t, completed)
	_, err = owner.Heartbeat(ctx, claim.Lease)
	require.ErrorIs(t, err, ErrLeaseLost)
}

func TestInterruptRuntimeOnlyTouchesItsOwnRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "session-end", Filename)
	current := newTestStore(t, path, "current-runtime", nil)
	other := newTestStore(t, path, "other-runtime", nil)
	currentClaim, err := current.Create(ctx, "current-owner", "current-worker", "current task", dir, types.ContextFresh)
	require.NoError(t, err)
	otherClaim, err := other.Create(ctx, "other-owner", "other-worker", "other task", dir, types.ContextFresh)
	require.NoError(t, err)

	interrupted, err := current.InterruptRuntime(ctx, "agent interrupted because the extension session stopped")
	require.NoError(t, err)
	require.Equal(t, 1, interrupted)

	currentRecord, err := current.Get(ctx, "current-owner", currentClaim.Agent.ID, "")
	require.NoError(t, err)
	otherRecord, err := current.Get(ctx, "other-owner", otherClaim.Agent.ID, "")
	require.NoError(t, err)
	require.Equal(t, types.AgentInterrupted, currentRecord.Status)
	require.Equal(t, types.RunInterrupted, currentRecord.Run.Status)
	require.Equal(t, types.AgentStarting, otherRecord.Status)
	require.Equal(t, types.RunStarting, otherRecord.Run.Status)
}

func TestSteeringIsRunScopedAndCarriedToFollowups(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newTestStore(t, filepath.Join(dir, "steering", Filename), "runtime-steering", nil)
	claim, err := store.Create(ctx, "owner", "parser-worker", "long task", dir, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(ctx, claim.Lease, "child-steering")
	require.NoError(t, err)

	first, err := store.EnqueueSteering(ctx, "owner", claim.Agent.ID, "focus on the parser")
	require.NoError(t, err)
	require.Equal(t, types.EnqueueSteeringResult{Accepted: true}, first)
	second, err := store.EnqueueSteering(ctx, "owner", claim.Agent.ID, "  and the lexer  ")
	require.NoError(t, err)
	require.Equal(t, types.EnqueueSteeringResult{Accepted: true, AlreadyPending: true}, second)
	require.Equal(t, [][3]any{
		{claim.Lease.RunID, 1, "focus on the parser"},
		{claim.Lease.RunID, 1, "and the lexer"},
	}, steeringRows(t, store.Path()))

	next, err := store.NextSteering(ctx, claim.Lease)
	require.NoError(t, err)
	require.Equal(t, "focus on the parser", next.Message)
	acknowledged, err := store.AcknowledgeSteering(ctx, claim.Lease, next.ID)
	require.NoError(t, err)
	require.True(t, acknowledged)

	terminal, err := store.Terminal(ctx, claim.Lease, types.TerminalIdle, types.TerminalUpdate{
		ConversationID: new("child-steering"), Result: new("done"),
	})
	require.NoError(t, err)
	require.Equal(t, types.RunCompleted, terminal.Run.Status)
	require.Equal(t, [][3]any{{claim.Lease.RunID, 1, "and the lexer"}}, steeringRows(t, store.Path()))

	_, err = store.EnqueueSteering(ctx, "owner", claim.Agent.ID, "too late")
	require.ErrorIs(t, err, ErrAgentConflict)
	require.ErrorContains(t, err, "not currently running")

	followup, err := store.Claim(ctx, "owner", claim.Agent.ID, "follow-up task")
	require.NoError(t, err)
	require.Equal(t, [][3]any{{followup.Lease.RunID, 2, "and the lexer"}}, steeringRows(t, store.Path()))
	_, err = store.MarkRunning(ctx, followup.Lease, "child-steering")
	require.NoError(t, err)
	carried, err := store.NextSteering(ctx, followup.Lease)
	require.NoError(t, err)
	require.Equal(t, "and the lexer", carried.Message)
	acknowledged, err = store.AcknowledgeSteering(ctx, followup.Lease, carried.ID)
	require.NoError(t, err)
	require.True(t, acknowledged)
	require.Empty(t, steeringRows(t, store.Path()))
	none, err := store.NextSteering(ctx, followup.Lease)
	require.NoError(t, err)
	require.Nil(t, none)

	_, err = store.EnqueueSteering(ctx, "owner", claim.Agent.ID, "   ")
	require.ErrorContains(t, err, "steering message is required")
}

func TestCancelDiscardsPendingSteeringAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newTestStore(t, filepath.Join(dir, Filename), "runtime-cancel", nil)
	claim, err := store.Create(ctx, "owner", "cancel-worker", "task", dir, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(ctx, claim.Lease, "child-cancel")
	require.NoError(t, err)
	_, err = store.EnqueueSteering(ctx, "owner", claim.Agent.ID, "pending")
	require.NoError(t, err)

	canceling, err := store.Cancel(ctx, "owner", claim.Agent.ID)
	require.NoError(t, err)
	require.Equal(t, types.AgentCanceling, canceling.Status)
	require.Equal(t, "agent canceled by parent", *canceling.Run.Error)
	require.Empty(t, steeringRows(t, store.Path()))

	_, err = store.Heartbeat(ctx, claim.Lease)
	require.ErrorIs(t, err, ErrLeaseLost)
	_, err = store.HeartbeatCanceling(ctx, claim.Lease)
	require.NoError(t, err)
	_, err = store.AttachCancelingConversation(ctx, claim.Lease, "child-cancel")
	require.NoError(t, err)
	completed, err := store.CompleteCancel(ctx, claim.Lease)
	require.NoError(t, err)
	require.True(t, completed)
	completed, err = store.CompleteCancel(ctx, claim.Lease)
	require.NoError(t, err)
	require.True(t, completed)
	again, err := store.Cancel(ctx, "owner", claim.Agent.ID)
	require.NoError(t, err)
	require.Equal(t, types.AgentCanceled, again.Status)

	idle, err := store.Create(ctx, "owner", "idle-worker", "task", dir, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(ctx, idle.Lease, "child-idle")
	require.NoError(t, err)
	_, err = store.Terminal(ctx, idle.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("done")})
	require.NoError(t, err)
	canceled, err := store.Cancel(ctx, "owner", idle.Agent.ID)
	require.NoError(t, err)
	require.Equal(t, types.AgentCanceled, canceled.Status)
	require.Equal(t, types.RunCompleted, canceled.Run.Status)
}

func TestAbortDeletesOnlyUnstartedInitialReservations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newTestStore(t, filepath.Join(dir, Filename), "runtime-abort", nil)
	claim, err := store.Create(ctx, "owner", "abort-worker", "task", dir, types.ContextFork)
	require.NoError(t, err)
	deleted, err := store.Abort(ctx, claim.Lease)
	require.NoError(t, err)
	require.True(t, deleted)
	deleted, err = store.Abort(ctx, claim.Lease)
	require.NoError(t, err)
	require.False(t, deleted)
	agents, err := store.List(ctx, "owner")
	require.NoError(t, err)
	require.Empty(t, agents)

	started, err := store.Create(ctx, "owner", "started-worker", "task", dir, types.ContextFork)
	require.NoError(t, err)
	_, err = store.AttachConversation(ctx, started.Lease, "child-started")
	require.NoError(t, err)
	_, err = store.Abort(ctx, started.Lease)
	require.ErrorIs(t, err, ErrAgentConflict)

	other, err := store.Create(ctx, "owner", "other-worker", "task", dir, types.ContextFork)
	require.NoError(t, err)
	_, err = store.AttachConversation(ctx, other.Lease, "child-started")
	require.ErrorIs(t, err, ErrAgentConflict)
	require.ErrorContains(t, err, "already attached to another agent")
}

func TestTerminalIsIdempotentOnlyForIdenticalUpdates(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newTestStore(t, filepath.Join(dir, Filename), "runtime-retry", newMutableClock().now)
	claim, err := store.Create(ctx, "owner", "retry-worker", "retry update", dir, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.Terminal(ctx, claim.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("too early")})
	require.ErrorIs(t, err, ErrAgentConflict)
	require.ErrorContains(t, err, "agent must be running before becoming idle")
	_, err = store.MarkRunning(ctx, claim.Lease, "child-retry")
	require.NoError(t, err)

	update := types.TerminalUpdate{ConversationID: new("child-retry"), Result: new("final answer")}
	_, err = store.Terminal(ctx, claim.Lease, types.TerminalIdle, update)
	require.NoError(t, err)
	repeated, err := store.Terminal(ctx, claim.Lease, types.TerminalIdle, update)
	require.NoError(t, err)
	require.Equal(t, types.RunCompleted, repeated.Run.Status)
	require.Equal(t, "final answer", *repeated.Run.Result)
	_, err = store.Terminal(ctx, claim.Lease, types.TerminalIdle, types.TerminalUpdate{
		ConversationID: new("child-retry"), Result: new("different answer"),
	})
	require.ErrorIs(t, err, ErrLeaseLost)
}

func TestCanceledContextDoesNotStartTransactions(t *testing.T) {
	dir := t.TempDir()
	store := newTestStore(t, filepath.Join(dir, Filename), "runtime-context", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.Create(ctx, "owner", "never-created", "task", dir, types.ContextFresh)
	require.ErrorIs(t, err, context.Canceled)
	agents, err := store.List(context.Background(), "owner")
	require.NoError(t, err)
	require.Empty(t, agents)
}
