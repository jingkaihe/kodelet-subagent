package subagent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/agent"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// startShutdown runs Shutdown on its own goroutine.
func (f *fixture) startShutdown() <-chan error {
	done := make(chan error, 1)
	go func() { done <- f.runtime.Shutdown(context.Background()) }()
	return done
}

func requirePending(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("finished early")
	case <-time.After(50 * time.Millisecond):
	}
}

func requireDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(eventTimeout):
		t.Fatal("did not finish")
	}
}

func TestCompletedRunRemainsOwnedUntilBackgroundLeaseRelease(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1", blockRelease())
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "lease-cleanup-worker", Task: "finish before lease release", ContextMode: types.ContextFresh}))
	store := f.store(call)
	lease := call.backgroundLeases()[0]

	lease.closeStarted.wait(t)
	live := f.runtime.liveRun(store, id)
	require.NotNil(t, live)
	require.True(t, f.acp.client(t, 0).isClosed())
	f.runtime.mu.Lock()
	cleanup := live.cleanupTask
	_, tracked := f.runtime.cleanupTasks[cleanup]
	f.runtime.mu.Unlock()
	require.NotNil(t, cleanup)
	require.True(t, tracked)

	shutdown := f.startShutdown()
	requirePending(t, shutdown)
	require.NotNil(t, f.runtime.liveRun(store, id))

	lease.closeRelease.set()
	requireDone(t, shutdown)
	require.Equal(t, 1, lease.closes())
	require.Nil(t, f.runtime.liveRun(store, id))
	require.Zero(t, f.ownedCount())
	require.Zero(t, f.cleanupCount())
}

func TestShutdownDuringClientCloseStillReleasesBackgroundLease(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockClose = true })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "client-cleanup-worker", Task: "finish before client close", ContextMode: types.ContextFresh}))
	store := f.store(call)
	lease := call.backgroundLeases()[0]

	f.acp.closeStarted.wait(t)
	require.NotNil(t, f.runtime.liveRun(store, id))
	require.Zero(t, lease.closes())
	shutdown := f.startShutdown()
	requirePending(t, shutdown)
	require.NotNil(t, f.runtime.liveRun(store, id))

	f.acp.closeRelease.set()
	requireDone(t, shutdown)
	client := f.acp.client(t, 0)
	require.True(t, client.isClosed())
	require.Equal(t, 1, client.closes())
	require.Equal(t, 1, lease.closes())
	require.Nil(t, f.runtime.liveRun(store, id))
	require.Zero(t, f.ownedCount())
	require.Zero(t, f.cleanupCount())
}

func TestShutdownOwnsPriorRunDuringImmediateFollowup(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	terminalCommitted, terminalRelease := newEvent(), newEvent()
	defer terminalRelease.set()
	// firstRunID is written before the run is released and read only after.
	var firstRunID string
	var hooks *hookedStore
	hooks = f.hookStore(call, &hookedStore{
		terminal: func(ctx context.Context, lease *types.Lease, status types.TerminalStatus, update types.TerminalUpdate) (types.Agent, error) {
			record, err := hooks.Store.Terminal(ctx, lease, status, update)
			if err == nil && lease.RunID == firstRunID && status == types.TerminalIdle {
				terminalCommitted.set()
				if waitErr := terminalRelease.waitCtx(ctx); waitErr != nil {
					return record, waitErr
				}
			}
			return record, err
		},
	})
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	spawned := f.spawn(call, SpawnAgentInput{Name: "handoff-worker", Task: "first generation", ContextMode: types.ContextFresh})
	id := agentID(t, spawned)
	firstRunID = str(spawned, "run_id")
	f.acp.started().wait(t)
	store := Store(hooks)
	firstLive := f.runtime.liveRun(store, id)
	_, firstRunner := f.ownerTasks(firstLive)
	firstClient := f.acp.client(t, 0)
	firstLease := call.backgroundLeases()[0]

	f.acp.release().set()
	terminalCommitted.wait(t)
	require.Equal(t, types.RunCompleted, f.get(store, "owner-1", id).Run.Status)
	f.runtime.mu.Lock()
	require.Nil(t, firstLive.cleanupTask)
	f.runtime.mu.Unlock()

	f.acp.resetRun()
	followup := f.followup(call, id, "second generation")
	f.acp.started().wait(t)
	secondLive := f.runtime.liveRun(hooks, id)
	require.Equal(t, str(followup, "run_id"), secondLive.runID)
	require.NotSame(t, firstLive, secondLive)
	require.NotNil(t, f.runtime.ownedRun(hooks, firstRunID))
	require.NotNil(t, f.runtime.ownedRun(hooks, secondLive.runID))

	requireDone(t, f.startShutdown())
	require.True(t, firstRunner.finished())
	require.True(t, firstClient.isClosed())
	require.Equal(t, 1, firstLease.closes())
	require.Equal(t, 1, call.backgroundLeases()[1].closes())
	require.Zero(t, f.ownedCount())
}

func TestStartupTimeoutIsPersistedAsFailure(t *testing.T) {
	f := newFixture(t, func(t *Timing) { t.AgentStartTimeout = 20 * time.Millisecond })
	call := f.call("owner-1", withProfile("parent-profile"))
	f.acp.update(func(a *fakeACP) { a.blockStartup = true })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "slow-starter", Task: "never starts", ContextMode: types.ContextFresh}))
	f.acp.startupStarted.wait(t)
	store := f.store(call)
	failed := f.waitForStatus(store, "owner-1", id, types.RunFailed)
	require.Contains(t, *failed.Run.Error, "timed out while starting")
	waitUntil(t, func() bool { return call.backgroundLeases()[0].closes() == 1 })
	require.True(t, f.acp.client(t, 0).isClosed())
	require.Equal(t, "parent-profile", f.acp.client(t, 0).options(t).Profile)
	require.Nil(t, failed.ConversationID)

	// Without a saved conversation, retry resolves the current defaults.
	require.NoError(t, f.runtime.Shutdown(f.ctx))
	f.runtime = f.newRuntime("")
	f.app = NewApplication(f.runtime)
	f.acp.update(func(a *fakeACP) { a.blockStartup = false })
	call.setProfile("changed-parent-profile")
	t.Setenv(ProfileEnv, "changed-profile")
	require.Empty(t, f.followup(call, id, "retry startup").Error)
	require.Equal(t, "completed", str(f.wait(call, id, 1000), "status"))
	options := f.acp.client(t, -1).options(t)
	require.Empty(t, options.Resume)
	require.Equal(t, "changed-profile", options.Profile)
}

func TestBackgroundLeaseReleaseRetriesTransientFailures(t *testing.T) {
	f := newFixture(t, func(t *Timing) { t.BackgroundLeaseReleaseRetryInitial = time.Millisecond })
	call := f.call("owner-1")
	call.releaseFailures = 1
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "lease-releaser", Task: "retry lease release", ContextMode: types.ContextFresh}))
	f.wait(call, id, 1000)
	waitUntil(t, func() bool { return call.backgroundLeases()[0].closes() == 2 })
}

func TestTerminalRetryAndIdempotency(t *testing.T) {
	f := newFixture(t, func(t *Timing) {
		t.WorkerUpdateRetryInitial, t.WorkerUpdateRetryMax = time.Millisecond, 2*time.Millisecond
	})
	store := f.standaloneStore("terminal-retry", "runtime-retry", func() float64 { return 1_000 })
	claim, err := store.Create(f.ctx, "owner", "retry-worker", "retry update", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(f.ctx, claim.Lease, "child-retry")
	require.NoError(t, err)
	attempts := 0
	hooks := &hookedStore{Store: store}
	hooks.terminal = func(ctx context.Context, lease *types.Lease, status types.TerminalStatus, update types.TerminalUpdate) (types.Agent, error) {
		attempts++
		if attempts < 3 {
			return types.Agent{}, sqliteError{code: 5, message: "database is busy"}
		}
		return store.Terminal(ctx, lease, status, update)
	}
	live := f.runtime.liveRunFromClaim(claim, "retry update", hooks)
	live.setConversationID("child-retry")

	committed, err := f.runtime.safeWorkerTerminal(f.ctx, live, types.TerminalIdle, strPtr("final answer"), nil)
	require.NoError(t, err)
	require.True(t, committed)
	require.Equal(t, 3, attempts)

	update := types.TerminalUpdate{ConversationID: strPtr("child-retry"), Result: strPtr("final answer")}
	repeated, err := store.Terminal(f.ctx, claim.Lease, types.TerminalIdle, update)
	require.NoError(t, err)
	require.Equal(t, types.RunCompleted, repeated.Run.Status)
	require.Equal(t, "final answer", *repeated.Run.Result)
	update.Result = strPtr("different answer")
	_, err = store.Terminal(f.ctx, claim.Lease, types.TerminalIdle, update)
	require.Error(t, err)
}

func TestTerminalUpdateDoesNotRetryNonTransientStoreErrors(t *testing.T) {
	f := newFixture(t)
	store := f.standaloneStore("terminal-programming-error", "runtime-error", nil)
	claim, err := store.Create(f.ctx, "owner", "error-worker", "fail once", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(f.ctx, claim.Lease, "child-error")
	require.NoError(t, err)
	attempts := 0
	hooks := &hookedStore{Store: store}
	hooks.terminal = func(context.Context, *types.Lease, types.TerminalStatus, types.TerminalUpdate) (types.Agent, error) {
		attempts++
		return types.Agent{}, sqliteError{code: 1, message: "no such table: agents"}
	}
	live := f.runtime.liveRunFromClaim(claim, "fail once", hooks)
	live.setConversationID("child-error")

	committed, err := f.runtime.safeWorkerTerminal(f.ctx, live, types.TerminalIdle, strPtr("unreachable"), nil)
	require.NoError(t, err)
	require.False(t, committed)
	require.Equal(t, 1, attempts)
}

func TestCanceledReservationIsCompensatedAfterCommit(t *testing.T) {
	f := newFixture(t)
	store := f.standaloneStore("canceled-reservation", "runtime-cancel", nil)
	started, release := newEvent(), newEvent()
	ctx, cancel := context.WithCancel(f.ctx)
	result := make(chan error, 1)
	go func() {
		_, err := f.runtime.reserveClaim(ctx, func(ctx context.Context) (types.Claim, error) {
			started.set()
			<-release.ch
			return store.Create(ctx, "owner", "delayed-worker", "delayed", f.cwd, types.ContextFresh)
		}, "delayed", store, true)
		result <- err
	}()
	started.wait(t)
	cancel()
	release.set()
	require.ErrorIs(t, <-result, context.Canceled)
	agents, err := store.List(f.ctx, "owner")
	require.NoError(t, err)
	require.Empty(t, agents)
}

func TestShutdownCancelsAndAwaitsInflightAgentSetup(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1", blockFork(), blockRelease())
	spawning := f.startSpawn(call, SpawnAgentInput{Name: "setup-worker", Task: "block during setup"})
	call.forkStarted.wait(t)
	require.Equal(t, 1, f.setupCount())
	require.Len(t, call.backgroundLeases(), 1)
	lease := call.backgroundLeases()[0]

	shutdown := f.startShutdown()
	lease.closeStarted.wait(t)
	requirePending(t, shutdown)
	require.Equal(t, 1, f.setupCount())

	lease.closeRelease.set()
	requireDone(t, shutdown)
	require.Contains(t, receive(t, spawning).Error, setupCanceledMessage)
	require.Zero(t, f.setupCount())
	require.Zero(t, f.liveCount())
	require.Zero(t, f.ownedCount())
	agents, err := f.store(call).List(f.ctx, "owner-1")
	require.NoError(t, err)
	require.Empty(t, agents)
	require.Equal(t, 1, lease.closes())
}

func TestShutdownBarrierCompensatesConcurrentReservation(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	store := f.store(call)
	started, release := newEvent(), newEvent()
	reservation := make(chan error, 1)
	go func() {
		_, err := f.runtime.reserveClaim(f.ctx, func(ctx context.Context) (types.Claim, error) {
			started.set()
			<-release.ch
			return store.Create(ctx, "owner-1", "shutdown-worker", "during shutdown", f.cwd, types.ContextFresh)
		}, "during shutdown", store, true)
		reservation <- err
	}()
	started.wait(t)
	shutdown := make(chan error, 1)
	go func() { shutdown <- f.app.interruptLiveAgents(f.ctx) }()
	requirePending(t, shutdown)
	release.set()
	requireDone(t, shutdown)
	require.ErrorContains(t, <-reservation, "shutting down")
	agents, err := store.List(f.ctx, "owner-1")
	require.NoError(t, err)
	require.Empty(t, agents)
}

func TestTransportFailureCleanupCanBeCanceledBeforeClientClose(t *testing.T) {
	f := newFixture(t, cancelCleanupTimeout(10*time.Millisecond), func(t *Timing) {
		t.HeartbeatInterval, t.MinHeartbeatInterval = 10*time.Millisecond, time.Millisecond
	})
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) {
		a.runFailure = errors.New("transport unavailable")
		a.blockClose = true
	})
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "release-agent", Task: "must actually stop"}))
	lease := call.backgroundLeases()[0]
	// A cancel can arrive after transport failure has begun draining.
	f.acp.closeStarted.wait(t)
	result := f.cancel(call, id)
	require.Equal(t, "canceling", str(result, "agent_status"))
	store := f.store(call)
	require.Equal(t, types.AgentCanceling, f.get(store, "owner-1", id).Status)
	require.NotZero(t, f.ownedCount())
	require.False(t, f.acp.client(t, 0).isClosed())
	require.Zero(t, lease.closes())
	live := f.runtime.liveRun(store, id)
	expiresAt := live.lease.ExpiresAt()
	waitUntil(t, func() bool { return live.lease.ExpiresAt() > expiresAt })

	f.acp.closeRelease.set()
	waitUntil(t, func() bool { return f.ownedCount() == 0 })
	require.Equal(t, 1, lease.closes())
	require.Equal(t, types.AgentCanceled, f.get(store, "owner-1", id).Status)
}

func TestTransportFailureRetainsActiveClaimUntilClientClose(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) {
		a.runFailure = errors.New("transport unavailable")
		a.blockClose = true
	})
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "read-failure-agent", Task: "still running"}))
	f.acp.closeStarted.wait(t)
	store := f.store(call)
	require.Equal(t, types.AgentRunning, f.get(store, "owner-1", id).Status)
	require.False(t, f.acp.client(t, 0).isClosed())
	require.Zero(t, call.backgroundLeases()[0].closes())
	require.NotZero(t, f.ownedCount())
	require.NotEmpty(t, f.followup(call, id, "not yet").Error)
	require.Len(t, f.acp.instances(), 1)
	require.Equal(t, 1, f.get(store, "owner-1", id).Run.Generation)

	f.acp.closeRelease.set()
	failed := f.waitForStatus(store, "owner-1", id, types.RunFailed)
	require.Contains(t, *failed.Run.Error, "transport unavailable")
}

func TestSteeringPumpDeliversRetriesAndAcknowledges(t *testing.T) {
	f := newFixture(t, func(t *Timing) { t.SteeringPoll, t.SteeringRetry = 10*time.Millisecond, 10*time.Millisecond })
	store := f.standaloneStore("steering", "runtime-steering", nil)
	claim, err := store.Create(f.ctx, "owner", "parser-worker", "long task", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(f.ctx, claim.Lease, "child-steering")
	require.NoError(t, err)
	first, err := store.EnqueueSteering(f.ctx, "owner", claim.Agent.ID, "focus on the parser")
	require.NoError(t, err)
	require.Equal(t, types.EnqueueSteeringResult{Accepted: true}, first)
	require.Equal(t, [][3]any{{claim.Lease.RunID, claim.Lease.Generation, "focus on the parser"}}, f.steeringRows(store))

	client := f.acp.factory("kodelet", f.cwd, nil).(*fakeClient)
	created, err := client.CreateSession(f.ctx, agent.SessionOptions{Resume: "child-steering"})
	require.NoError(t, err)
	session := created.(*fakeSession)
	live := f.runtime.liveRunFromClaim(claim, "long task", store)
	live.setConversationID(session.ID())
	f.acp.update(func(a *fakeACP) {
		a.steerOutcomes = []agent.SteeringOutcome{agent.SteeringFailed, agent.SteeringInjected}
	})

	pump := goTask(func(ctx context.Context) { f.runtime.steeringPump(ctx, live, session) })
	session.steerReceived.wait(t)
	waitUntil(t, func() bool { return len(f.steeringRows(store)) == 0 })
	stopTask(pump)
	require.Equal(t, []string{"focus on the parser", "focus on the parser"}, session.steers())
	next, err := store.NextSteering(f.ctx, claim.Lease)
	require.NoError(t, err)
	require.Nil(t, next)
}

func TestHeartbeatLossStopsTheRunWithTheLeaseError(t *testing.T) {
	f := newFixture(t, func(t *Timing) { t.HeartbeatInterval, t.MinHeartbeatInterval = 10*time.Millisecond, time.Millisecond })
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "lease-loser", Task: "lose the lease", ContextMode: types.ContextFresh}))
	f.acp.started().wait(t)
	store := f.store(call)
	// Interrupting the claimed row fences out the worker that still holds it.
	_, err := store.InterruptRuntime(f.ctx, "simulated takeover")
	require.NoError(t, err)

	interrupted := f.waitForStatus(store, "owner-1", id, types.RunInterrupted)
	require.Equal(t, "simulated takeover", *interrupted.Run.Error)
	waitUntil(t, func() bool { return f.ownedCount() == 0 })
	require.True(t, f.acp.client(t, 0).isClosed())
	require.Equal(t, 1, call.backgroundLeases()[0].closes())
}
