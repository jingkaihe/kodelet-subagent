package subagent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/agent"
	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/jingkaihe/kodelet/sdk/go/taskprogress"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// startWait runs wait_agent on its own goroutine.
func (f *fixture) startWait(ctx context.Context, call *fakeCall, agentID string, timeoutMS int) <-chan extension.ToolResult {
	result := make(chan extension.ToolResult, 1)
	go func() { result <- f.app.WaitAgent(ctx, WaitAgentInput{AgentID: agentID, TimeoutMS: timeoutMS}, call) }()
	return result
}

func receive(t *testing.T, results <-chan extension.ToolResult) extension.ToolResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(eventTimeout):
		t.Fatal("timed out waiting for wait_agent")
		return extension.ToolResult{}
	}
}

func firstTaskRun(t *testing.T, call *fakeCall) taskprogress.Snapshot {
	t.Helper()
	updates := call.toolUpdates()
	require.NotEmpty(t, updates)
	snapshot, ok := updates[0].taskRun()
	require.True(t, ok)
	return snapshot
}

func TestWaitKeepsTheRunCapturedWhenFollowupStarts(t *testing.T) {
	f := newFixture(t)
	f.app.waitPoll = 200 * time.Millisecond
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	spawned := f.spawn(call, SpawnAgentInput{Name: "run-capture", Task: "first pass", ContextMode: types.ContextFresh})
	id := agentID(t, spawned)
	firstRunID := str(spawned, "run_id")
	f.acp.started().wait(t)
	store := f.store(call)

	waiting := f.startWait(f.ctx, call, id, 1000)
	waitUntil(t, func() bool { return len(call.toolUpdates()) > 0 })
	first := f.acp.client(t, 0).session(t)
	first.toolCall("first-call", "bash", map[string]any{})
	first.toolResult("first-call", "failed", "")
	f.acp.release().set()
	f.waitForStatus(store, "owner-1", id, types.RunCompleted)

	f.acp.resetRun()
	followup := f.followup(call, id, "second pass")
	f.acp.started().wait(t)
	f.acp.client(t, -1).session(t).toolCall("second-call", "file_read", map[string]any{})
	result := receive(t, waiting)

	require.Equal(t, "result for first pass", result.Content)
	require.Equal(t, firstRunID, str(result, "run_id"))
	require.NotEqual(t, firstRunID, str(followup, "run_id"))
	taskRun := taskRunOf(t, result)
	require.Len(t, taskRun.Activities, 1)
	require.Equal(t, "first-call", taskRun.Activities[0].ID)
	require.Equal(t, 1, taskRun.Counts.Failed)

	f.acp.release().set()
	require.Equal(t, "result for second pass", f.wait(call, id, 1000).Content)
}

func TestWaitStreamsLiveSubagentTaskProgress(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "progress-inspector", Task: "inspect progress", ContextMode: types.ContextFresh}))
	f.acp.started().wait(t)
	session := f.acp.client(t, 0).session(t)
	waiting := f.startWait(f.ctx, call, id, 1000)

	waitUntil(t, func() bool { return session.listeners() > 0 })
	session.toolCall("bash-call", "bash", map[string]any{"command": "uv run pytest", "description": "Run tests"})
	session.toolCall("search-call", "grep_tool", map[string]any{"pattern": "progress", "path": f.cwd + "/src"})
	session.toolUpdate("bash-call", "collecting tests\n3 tests collected")
	waitUntil(t, func() bool {
		for _, update := range call.toolUpdates() {
			snapshot, ok := update.taskRun()
			if ok && snapshot.Counts.Running == 2 && activities(snapshot)["bash-call"].Preview == "3 tests collected" {
				return true
			}
		}
		return false
	})
	session.toolResult("bash-call", "failed", "test assertion failed")
	session.toolResult("search-call", "completed", "src/ui.py:18:progress")
	session.emit(agent.EventAssistantMessageDelta, agent.AssistantMessageDelta{DeltaContent: "Here is the summary"})
	waitUntil(t, func() bool {
		for _, update := range call.toolUpdates() {
			if snapshot, ok := update.taskRun(); ok && snapshot.Phase == "responding" {
				return true
			}
		}
		return false
	})
	f.acp.release().set()
	completed := receive(t, waiting)

	require.Equal(t, "result for inspect progress", completed.Content)
	taskRun := taskRunOf(t, completed)
	require.Equal(t, "subagent", taskRun.Kind)
	require.Equal(t, "Wait for progress-inspector", taskRun.Title)
	require.Equal(t, "completed", taskRun.Status)
	byID := activities(taskRun)
	require.Equal(t, taskprogress.Activity{
		ID: "bash-call", Sequence: 1, Kind: "bash", Label: "Bash: Run tests", Detail: "run tests",
		Status: "failed", Preview: "test assertion failed",
	}, byID["bash-call"])
	require.Equal(t, taskprogress.Activity{
		ID: "search-call", Sequence: 2, Kind: "grep_tool", Label: `Search "progress" in src`, Detail: "searching src",
		Status: "succeeded",
	}, byID["search-call"])
	require.Equal(t, taskprogress.Counts{Succeeded: 1, Failed: 1}, taskRun.Counts)
	require.Zero(t, session.listeners())
}

func TestWaitTimeoutKeepsOnlyRuntimeProgressAndStopsPublishing(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "long-runner", Task: "keep working", ContextMode: types.ContextFresh}))
	f.acp.started().wait(t)
	session := f.acp.client(t, 0).session(t)
	waiting := f.startWait(f.ctx, call, id, 150)
	waitUntil(t, func() bool { return session.listeners() > 0 })
	session.toolCall("pending-call", "bash", map[string]any{"description": "Run slow tests"})
	result := receive(t, waiting)

	require.Equal(t, "running", str(result, "status"))
	taskRun := taskRunOf(t, result)
	require.Equal(t, "Wait for long-runner", taskRun.Title)
	require.Equal(t, "running", taskRun.Status)
	require.Equal(t, 1, taskRun.Counts.Running)
	require.Zero(t, taskRun.Counts.Succeeded)
	require.NotEmpty(t, call.toolUpdates())
	require.False(t, f.acp.client(t, 0).isClosed())
	require.Equal(t, 1, session.listeners())
	updateCount := len(call.toolUpdates())
	session.toolUpdate("pending-call", "still running")

	next := f.call("owner-1")
	waitingAgain := f.startWait(f.ctx, next, id, 1000)
	waitUntil(t, func() bool { return len(next.toolUpdates()) > 0 })
	require.Equal(t, "still running", firstTaskRun(t, next).Activities[0].Preview)
	f.acp.release().set()
	require.Equal(t, "result for keep working", receive(t, waitingAgain).Content)
	require.Len(t, call.toolUpdates(), updateCount)
	require.Zero(t, session.listeners())
}

func TestCancelingOneWaitDetachesWithoutInterruptingAgentOrOtherWait(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "shared-progress", Task: "keep working"}))
	f.acp.started().wait(t)
	session := f.acp.client(t, 0).session(t)
	firstCall, secondCall := f.call("owner-1"), f.call("owner-1")
	firstCtx, cancelFirst := context.WithCancel(f.ctx)
	defer cancelFirst()
	firstWait := f.startWait(firstCtx, firstCall, id, 1000)
	secondWait := f.startWait(f.ctx, secondCall, id, 1000)

	waitUntil(t, func() bool { return len(firstCall.toolUpdates()) > 0 && len(secondCall.toolUpdates()) > 0 })
	require.Equal(t, 1, session.listeners())
	cancelFirst()
	receive(t, firstWait)
	canceledUpdates := len(firstCall.toolUpdates())
	require.Equal(t, 1, session.listeners())
	require.False(t, f.acp.client(t, 0).isClosed())
	session.toolCall("still-working", "file_read", nil)
	session.toolResult("still-working", "completed", "")
	f.acp.release().set()
	completed := receive(t, secondWait)
	require.Equal(t, "result for keep working", completed.Content)
	require.Equal(t, 1, taskRunOf(t, completed).Counts.Succeeded)
	require.Len(t, firstCall.toolUpdates(), canceledUpdates)
	require.Zero(t, session.listeners())
}

func TestLateWaitSeesStartupFailuresAndActiveToolProgress(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "startup-progress", Task: "inspect startup"}))
	f.acp.started().wait(t)
	session := f.acp.client(t, 0).session(t)
	require.Equal(t, 1, session.listeners())
	require.Empty(t, call.toolUpdates())
	for _, callID := range []string{"early-failure", "still-running"} {
		session.toolCall(callID, "file_read", nil)
	}
	session.toolResult("early-failure", "failed", "missing file")
	session.toolUpdate("still-running", "reading source")
	waiting := f.startWait(f.ctx, call, id, 1000)

	waitUntil(t, func() bool { return len(call.toolUpdates()) > 0 })
	initial := firstTaskRun(t, call)
	require.Equal(t, taskprogress.Counts{Failed: 1, Running: 1}, initial.Counts)
	byID := activities(initial)
	require.Equal(t, "missing file", byID["early-failure"].Preview)
	require.Equal(t, "reading source", byID["still-running"].Preview)
	session.toolResult("still-running", "failed", "read error")
	f.acp.release().set()
	result := receive(t, waiting)
	require.Equal(t, taskprogress.Counts{Failed: 2}, taskRunOf(t, result).Counts)
	require.Zero(t, session.listeners())
}

func TestLateWaitActivityHistoryUsesSDKBounds(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "bounded-progress", Task: "inspect many files"}))
	f.acp.started().wait(t)
	session := f.acp.client(t, 0).session(t)
	for _, batch := range []struct {
		status string
		count  int
	}{{"completed", 12}, {"failed", 7}, {"running", 10}} {
		for index := range batch.count {
			callID := fmt.Sprintf("%s-%d", batch.status, index)
			session.toolCall(callID, "file_read", nil)
			if batch.status != "running" {
				session.toolResult(callID, batch.status, "")
			}
		}
	}
	snapshot := taskRunOf(t, f.wait(call, id, 50))
	require.Equal(t, taskprogress.Counts{Succeeded: 12, Failed: 7, Running: 10}, snapshot.Counts)
	require.Len(t, snapshot.Activities, 14)
	require.Equal(t, 9, snapshot.OmittedSucceeded)
	require.Equal(t, 4, snapshot.OmittedFailed)
	require.Equal(t, 2, snapshot.OmittedRunning)
	var failed []string
	for _, activity := range snapshot.Activities {
		if activity.Status == "failed" {
			failed = append(failed, activity.ID)
		}
	}
	require.Equal(t, []string{"failed-4", "failed-5", "failed-6"}, failed)
}

func TestCancelWaitDuringFinalWidgetSyncLeavesNoWaiterResources(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(fmt.Sprintf("completed=%t", completed), func(t *testing.T) {
			f := newFixture(t)
			call := f.call("owner-1")
			f.acp.update(func(a *fakeACP) { a.blockRuns = true })
			id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "widget-cancellation", Task: "inspect"}))
			f.acp.started().wait(t)
			session := f.acp.client(t, 0).session(t)
			live := f.runtime.liveRun(f.store(call), id)
			require.NotNil(t, live)
			runner := live.runnerTask

			waiter := f.call("owner-1")
			waiter.ui.block, waiter.ui.blockStarted = newEvent(), newEvent()
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			timeout := 50
			if completed {
				timeout = 1000
			}
			waiting := f.startWait(ctx, waiter, id, timeout)
			waitUntil(t, func() bool { return len(waiter.toolUpdates()) > 0 })
			if completed {
				f.acp.release().set()
			}
			waiter.ui.blockStarted.wait(t)
			cancel()
			receive(t, waiting)
			updateCount := len(waiter.toolUpdates())
			expectedListeners := 1
			if completed {
				expectedListeners = 0
			}
			require.Equal(t, expectedListeners, session.listeners())
			session.emit(agent.EventAssistantMessageDelta, agent.AssistantMessageDelta{DeltaContent: "late text"})
			require.Len(t, waiter.toolUpdates(), updateCount)

			waiter.ui.block.set()
			f.acp.release().set()
			select {
			case <-runner.done:
			case <-time.After(eventTimeout):
				t.Fatal("runner did not finish")
			}
			require.Zero(t, session.listeners())
		})
	}
}
