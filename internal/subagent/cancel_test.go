package subagent

import (
	"context"
	"testing"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/agent"
	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func cancelCleanupTimeout(d time.Duration) func(*Timing) {
	return func(t *Timing) { t.CancelCleanupTimeout = d }
}

// startSpawn runs spawn_agent on its own goroutine.
func (f *fixture) startSpawn(call *fakeCall, input SpawnAgentInput) <-chan extension.ToolResult {
	result := make(chan extension.ToolResult, 1)
	go func() { result <- f.spawn(call, input) }()
	return result
}

func (f *fixture) ownerTasks(live *LiveRun) (setup, runner *task) {
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	return live.setupTask, live.runnerTask
}

func TestACPCancellationRetainsClaimUntilCloseAndLabelsPartialOutput(t *testing.T) {
	for _, partial := range []string{"", "partial answer before cancellation"} {
		t.Run(partial, func(t *testing.T) {
			f := newFixture(t)
			call := f.call("owner-1")
			f.acp.update(func(a *fakeACP) {
				a.blockRuns, a.blockClose = true, true
				a.runResponse = &agent.Response{Content: partial, StopReason: "cancelled"}
			})
			id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "canceled-worker", Task: "inspect"}))
			f.acp.started().wait(t)
			store := f.store(call)
			live := f.runtime.liveRun(store, id)
			_, runner := f.ownerTasks(live)
			f.acp.release().set()
			f.acp.closeStarted.wait(t)

			require.Equal(t, types.RunRunning, f.get(store, "owner-1", id).Run.Status)
			require.Same(t, live, f.runtime.liveRun(store, id))
			require.NotEmpty(t, f.followup(call, id, "too early").Error)
			f.acp.closeRelease.set()
			<-runner.done

			result := f.wait(call, id, 0)
			require.Equal(t, "interrupted", str(result, "status"))
			require.Contains(t, result.Error, "canceled by kodelet")
			require.NotContains(t, result.Error, "empty response")
			require.NotContains(t, result.Data, "result")
			if partial != "" {
				require.Equal(t, partial, str(result, "partial_result"))
				require.Contains(t, result.Content, "Partial output (agent did not complete):\n"+partial)
			} else {
				require.NotContains(t, result.Data, "partial_result")
			}
			require.True(t, f.acp.client(t, 0).isClosed())
			require.Nil(t, f.runtime.liveRun(store, id))
		})
	}
}

func TestCancelPersistsCleanup(t *testing.T) {
	f := newFixture(t, cancelCleanupTimeout(10*time.Millisecond))
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns, a.blockClose = true, true })
	spawned := f.spawn(call, SpawnAgentInput{Name: "cancelable-worker", Task: "cancel me", ContextMode: types.ContextFresh})
	leases := call.backgroundLeases()
	require.Len(t, leases, 1)
	require.Zero(t, leases[0].closes())
	id := agentID(t, spawned)
	firstRunID := str(spawned, "run_id")
	f.acp.started().wait(t)
	store := f.store(call)
	running := f.waitForStatus(store, "owner-1", id, types.RunRunning)
	childID := *running.ConversationID
	_, err := store.EnqueueSteering(f.ctx, "owner-1", id, "pending")
	require.NoError(t, err)

	canceled := f.cancel(call, id)
	blocked := f.followup(call, id, "conclude after cancellation")
	require.Contains(t, canceled.Content, "cleanup is still finishing")
	require.Equal(t, presentation("Cancel cancelable-worker",
		"Cancellation saved; worker cleanup is still finishing. Retry followup_agent after cleanup completes."),
		presentationOf(canceled))
	require.Contains(t, blocked.Error, "cancellation is still in progress")
	require.Len(t, f.acp.instances(), 1)
	require.NotContains(t, presentationOf(canceled).Body, id)
	persisted := f.get(store, "owner-1", id)
	require.Equal(t, types.AgentCanceling, persisted.Status)
	require.Equal(t, types.RunCanceled, persisted.Run.Status)
	require.Equal(t, 1, persisted.Generation)
	require.Empty(t, f.steeringRows(store))
	require.Contains(t, call.ui.text(WidgetID), "1 canceling")
	require.Contains(t, f.wait(call, id, 0).Content, "still canceling")

	client := f.acp.client(t, 0)
	session := client.session(t)
	require.False(t, client.isClosed())
	f.acp.closeStarted.wait(t)
	f.acp.closeRelease.set()
	waitUntil(t, func() bool { return f.runtime.liveRun(store, id) == nil })
	require.Equal(t, types.AgentCanceled, f.get(store, "owner-1", id).Status)
	require.True(t, client.isClosed())
	require.Equal(t, 1, client.closes())
	require.Equal(t, 1, session.closes())
	waitUntil(t, func() bool { return leases[0].closes() == 1 && f.ownedCount() == 0 })

	again := f.cancel(call, id)
	require.Equal(t, presentation("Cancel cancelable-worker", "Canceled. Use followup_agent to resume this agent later."), presentationOf(again))
	require.NotContains(t, again.Content, id)

	f.acp.resetRun()
	followup := f.followup(call, id, "conclude after cancellation")
	f.acp.started().wait(t)
	require.Equal(t, float64(2), toFloat(followup.Data["generation"]))
	require.NotEqual(t, firstRunID, str(followup, "run_id"))
	require.Equal(t, childID, f.acp.client(t, 1).options(t).Resume)
	historical, err := store.Get(f.ctx, "owner-1", id, firstRunID)
	require.NoError(t, err)
	require.Equal(t, types.RunCanceled, historical.Run.Status)
	f.acp.release().set()
	require.Equal(t, "result for conclude after cancellation", f.wait(call, id, 1000).Content)
}

func TestCancelBecomesResumableBeforeBackgroundLeaseRelease(t *testing.T) {
	f := newFixture(t, cancelCleanupTimeout(300*time.Millisecond))
	call := f.call("owner-1", blockRelease())
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "cancel-lease-worker", Task: "cancel before lease release", ContextMode: types.ContextFresh}))
	f.acp.started().wait(t)
	store := f.store(call)
	lease := call.backgroundLeases()[0]

	canceled := f.cancel(call, id)
	lease.closeStarted.wait(t)
	require.Contains(t, canceled.Content, "it can now be resumed")
	require.Equal(t, types.AgentCanceled, f.get(store, "owner-1", id).Status)
	require.NotZero(t, f.ownedCount())

	call.mu.Lock()
	call.blockRelease = false
	call.mu.Unlock()
	f.acp.resetRun()
	followup := f.followup(call, id, "resume while old lease closes")
	f.acp.started().wait(t)
	require.Equal(t, float64(2), toFloat(followup.Data["generation"]))
	lease.closeRelease.set()
	f.acp.release().set()
	require.Equal(t, "result for resume while old lease closes", f.wait(call, id, 1000).Content)
}

func TestCancelRetriesClientCloseBeforeBecomingResumable(t *testing.T) {
	f := newFixture(t, cancelCleanupTimeout(10*time.Millisecond), func(t *Timing) {
		t.ClientCloseRetryInitial, t.ClientCloseRetryMax = 10*time.Millisecond, 10*time.Millisecond
	})
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns, a.closeFailures = true, 100 })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "cancel-close-worker", Task: "retry close", ContextMode: types.ContextFresh}))
	f.acp.started().wait(t)
	store := f.store(call)

	canceled := f.cancel(call, id)
	require.Contains(t, canceled.Content, "worker cleanup is still finishing")
	require.Equal(t, types.AgentCanceling, f.get(store, "owner-1", id).Status)
	require.NotZero(t, f.ownedCount())
	require.Contains(t, f.followup(call, id, "too early").Error, "cancellation is still in progress")

	f.acp.update(func(a *fakeACP) { a.closeFailures = 0 })
	waitUntil(t, func() bool { return f.get(store, "owner-1", id).Status == types.AgentCanceled })
	client := f.acp.client(t, 0)
	require.True(t, client.isClosed())
	require.Greater(t, client.closes(), 1)
}

func TestCancelingBlocksCrossRuntimeFollowupUntilOwnerCleanup(t *testing.T) {
	f := newFixture(t, func(t *Timing) { t.HeartbeatInterval, t.MinHeartbeatInterval = 10*time.Millisecond, time.Millisecond })
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns, a.blockClose = true, true })
	spawned := f.spawn(call, SpawnAgentInput{Name: "shared-runtime-worker", Task: "first generation", ContextMode: types.ContextFresh})
	id := agentID(t, spawned)
	firstRunID := str(spawned, "run_id")
	f.acp.started().wait(t)
	ownerStore := f.store(call)

	otherRuntime := f.newRuntime("other-runtime")
	other := NewApplication(otherRuntime)
	t.Cleanup(func() { _ = otherRuntime.Shutdown(context.Background()) })
	canceled := other.CancelAgent(f.ctx, CancelAgentInput{AgentID: id}, call)
	require.Contains(t, canceled.Content, "worker cleanup is still finishing")
	blocked := other.FollowupAgent(f.ctx, FollowupAgentInput{AgentID: id, Task: "second generation"}, call)
	require.Contains(t, blocked.Error, "agent cancellation is still in progress")
	require.Len(t, f.acp.instances(), 1)

	f.acp.closeStarted.wait(t)
	canceling := f.get(ownerStore, "owner-1", id)
	require.Equal(t, types.AgentCanceling, canceling.Status)
	require.Equal(t, types.RunCanceled, canceling.Run.Status)
	f.acp.closeRelease.set()
	waitUntil(t, func() bool { return f.get(ownerStore, "owner-1", id).Status == types.AgentCanceled })
	historical, err := ownerStore.Get(f.ctx, "owner-1", id, firstRunID)
	require.NoError(t, err)
	require.Equal(t, types.RunCanceled, historical.Run.Status)

	f.acp.resetRun()
	followup := other.FollowupAgent(f.ctx, FollowupAgentInput{AgentID: id, Task: "second generation"}, call)
	f.acp.started().wait(t)
	require.Equal(t, float64(2), toFloat(followup.Data["generation"]))
	require.NotEqual(t, firstRunID, str(followup, "run_id"))
	f.acp.release().set()
	completed := other.WaitAgent(f.ctx, WaitAgentInput{AgentID: id, TimeoutMS: 1000}, call)
	require.Equal(t, "result for second generation", completed.Content)
	require.NoError(t, otherRuntime.Shutdown(f.ctx))
}

func TestCancelOwnsPrelaunchForkSetupBeforeResuming(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1", blockFork())
	spawning := f.startSpawn(call, SpawnAgentInput{Name: "cancel-setup-worker", Task: "first attempt"})
	call.forkStarted.wait(t)
	store := f.store(call)
	reserved, err := store.List(f.ctx, "owner-1")
	require.NoError(t, err)
	require.Len(t, reserved, 1)
	setup, runner := f.ownerTasks(f.runtime.liveRun(store, reserved[0].ID))
	require.NotNil(t, setup)
	require.Nil(t, runner)

	canceled := f.cancel(call, reserved[0].ID)
	require.Contains(t, canceled.Content, "Use followup_agent to resume it later")
	require.Contains(t, receive(t, spawning).Error, setupCanceledMessage)
	persisted := f.get(store, "owner-1", reserved[0].ID)
	require.Equal(t, types.AgentCanceled, persisted.Status)
	require.Equal(t, types.RunCanceled, persisted.Run.Status)
	require.Nil(t, persisted.ConversationID)
	require.Empty(t, f.acp.instances())
	require.Zero(t, f.liveCount())
	require.Zero(t, f.ownedCount())

	call.mu.Lock()
	call.blockFork = false
	call.mu.Unlock()
	followup := f.followup(call, reserved[0].ID, "retry after setup cancellation")
	require.Equal(t, "child-owner-1-2", str(followup, "conversation_id"))
	require.Equal(t, "result for retry after setup cancellation", f.wait(call, reserved[0].ID, 1000).Content)
}

func TestCancelPreservesForkCreatedBeforeAttachment(t *testing.T) {
	f := newFixture(t, cancelCleanupTimeout(10*time.Millisecond))
	call := f.call("owner-1")
	attachStarted, attachRelease := newEvent(), newEvent()
	cancelAttachStarted, cancelAttachRelease := newEvent(), newEvent()
	var hooks *hookedStore
	hooks = f.hookStore(call, &hookedStore{
		attachConversation: func(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error) {
			attachStarted.set()
			if err := attachRelease.waitCtx(ctx); err != nil {
				return types.Agent{}, err
			}
			return hooks.Store.AttachConversation(ctx, lease, conversationID)
		},
		attachCancelingConversation: func(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error) {
			cancelAttachStarted.set()
			if err := cancelAttachRelease.waitCtx(ctx); err != nil {
				return types.Agent{}, err
			}
			return hooks.Store.AttachCancelingConversation(ctx, lease, conversationID)
		},
	})

	spawning := f.startSpawn(call, SpawnAgentInput{Name: "cancel-fork-worker", Task: "cancel after fork"})
	attachStarted.wait(t)
	reserved, err := hooks.List(f.ctx, "owner-1")
	require.NoError(t, err)
	require.Len(t, reserved, 1)
	id := reserved[0].ID
	canceled := f.cancel(call, id)
	cancelAttachStarted.wait(t)
	require.Contains(t, canceled.Content, "worker cleanup is still finishing")
	require.Contains(t, f.followup(call, id, "too early").Error, "cancellation is still in progress")
	cancelAttachRelease.set()
	require.NotEmpty(t, receive(t, spawning).Error)

	waitUntil(t, func() bool { return f.get(hooks, "owner-1", id).Status == types.AgentCanceled })
	persisted := f.get(hooks, "owner-1", id)
	require.Equal(t, "child-owner-1-1", *persisted.ConversationID)
	require.Equal(t, []string{"cancel-fork-worker"}, call.forks())

	followup := f.followup(call, id, "resume preserved fork")
	require.Equal(t, "child-owner-1-1", str(followup, "conversation_id"))
	require.Equal(t, []string{"cancel-fork-worker"}, call.forks())
	require.Equal(t, "result for resume preserved fork", f.wait(call, id, 1000).Content)
}

func TestTerminalCancelResponseReloadsConcurrentGeneration(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "cancel-response-worker", Task: "initial task", ContextMode: types.ContextFresh}))
	f.wait(call, id, 1000)
	var raced types.Claim
	var hooks *hookedStore
	hooks = f.hookStore(call, &hookedStore{
		cancel: func(ctx context.Context, ownerID, agentID string) (types.Agent, error) {
			canceled, err := hooks.Store.Cancel(ctx, ownerID, agentID)
			if err != nil {
				return canceled, err
			}
			raced, err = hooks.Store.Claim(ctx, ownerID, agentID, "concurrent follow-up")
			return canceled, err
		},
	})

	canceled := f.cancel(call, id)
	require.Contains(t, canceled.Content, "agent has since changed generation")
	require.Equal(t, raced.Lease.RunID, str(canceled, "run_id"))
	_, err := hooks.Terminal(f.ctx, raced.Lease, types.TerminalInterrupted, types.TerminalUpdate{Error: new("test cleanup")})
	require.NoError(t, err)
}

func TestCancelOwnsBlockedBackgroundAcquisition(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1", blockAcquire())
	// Database migrations are setup, not part of the acquisition deadline.
	store := f.store(call)
	spawning := f.startSpawn(call, SpawnAgentInput{Name: "cancel-acquire-worker", Task: "wait for background ownership", ContextMode: types.ContextFresh})
	call.acquireStarted.wait(t)
	reserved, err := store.List(f.ctx, "owner-1")
	require.NoError(t, err)
	require.Len(t, reserved, 1)

	canceled := f.cancel(call, reserved[0].ID)
	require.Contains(t, canceled.Content, "Use followup_agent to resume it later")
	require.Contains(t, receive(t, spawning).Error, setupCanceledMessage)
	persisted := f.get(store, "owner-1", reserved[0].ID)
	require.Equal(t, types.AgentCanceled, persisted.Status)
	require.Equal(t, types.RunCanceled, persisted.Run.Status)
	require.Empty(t, call.backgroundLeases())
	require.Empty(t, f.acp.instances())
	require.Zero(t, f.liveCount())
	require.Zero(t, f.ownedCount())
}

func TestSteeringPromptRequiredPreservesQueueForFollowup(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) {
		a.blockRuns = true
		a.steerOutcomes = []agent.SteeringOutcome{agent.SteeringPromptRequired}
	})
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "queued-agent", Task: "first"}))
	f.acp.started().wait(t)
	first := f.acp.client(t, 0).session(t)
	require.Empty(t, f.steer(call, id, "retain this guidance").Error)
	store := f.store(call)
	first.steerReceived.wait(t)
	require.Len(t, f.steeringRows(store), 1)
	f.acp.release().set()
	f.wait(call, id, 1000)

	f.acp.resetRun()
	f.followup(call, id, "continue")
	f.acp.started().wait(t)
	waitUntil(t, func() bool { return len(f.steeringRows(store)) == 0 })
	require.Equal(t, []string{"retain this guidance"}, first.steers())
	require.Equal(t, []string{"retain this guidance"}, f.acp.client(t, -1).session(t).steers())
	f.cancel(call, id)
}

func toFloat(value any) float64 {
	switch typed := value.(type) {
	case int:
		return float64(typed)
	case float64:
		return typed
	default:
		return -1
	}
}
