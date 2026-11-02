package subagent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func strPtr(value string) *string { return &value }

func TestSpawnForkAndFreshWaitAndListOwnership(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	f.acp.update(func(a *fakeACP) { a.blockRuns = true })

	forked := f.spawn(call, SpawnAgentInput{Name: "authentication-inspector", Task: "inspect authentication"})
	forkedID := agentID(t, forked)
	require.Equal(t, "authentication-inspector", str(forked, "name"))
	require.Contains(t, forked.Content, "authentication-inspector")
	require.Equal(t, presentation("Spawn authentication-inspector", "inspect authentication"), presentationOf(forked))
	leases := call.backgroundLeases()
	require.Len(t, leases, 1)
	require.Contains(t, leases[0].description, forkedID)
	require.Zero(t, leases[0].closes())

	f.acp.started().wait(t)
	store := f.store(call)
	running := f.waitForStatus(store, "owner-1", forkedID, types.RunRunning)
	require.Equal(t, "child-owner-1-1", *running.ConversationID)
	require.Equal(t, types.ContextFork, running.ContextMode)
	require.Equal(t, []string{"authentication-inspector"}, call.forks())
	require.Contains(t, forked.Content, *running.ConversationID)
	client := f.acp.client(t, 0)
	options := client.options(t)
	require.Equal(t, *running.ConversationID, options.Resume)
	require.Nil(t, options.Extensions)
	require.Empty(t, options.ParentConversationID)
	require.Equal(t, "kodelet", client.command)
	require.Equal(t, "1", client.env[RecursionGuardEnv])
	waitUntil(t, func() bool { return containsAll(call.ui.text(WidgetID), "1 active", "authentication-inspector") })

	f.acp.release().set()
	completed := f.wait(call, forkedID, 1000)
	require.Equal(t, "result for inspect authentication", completed.Content)
	require.Equal(t, "completed", str(completed, "status"))
	require.Equal(t, presentation("Wait for authentication-inspector"), presentationOf(completed))
	require.Contains(t, call.ui.text(WidgetID), "1 completed")
	waitUntil(t, func() bool { return leases[0].closes() == 1 })

	f.acp.update(func(a *fakeACP) { a.blockRuns = false })
	fresh := f.spawn(call, SpawnAgentInput{Name: "independent-reviewer", Task: "independent review", ContextMode: types.ContextFresh})
	freshID := agentID(t, fresh)
	freshResult := f.wait(call, freshID, 1000)
	require.Equal(t, "result for independent review", freshResult.Content)
	freshOptions := f.acp.client(t, 1).options(t)
	require.Empty(t, freshOptions.Resume)
	require.Equal(t, "owner-1", freshOptions.ParentConversationID)
	require.Equal(t, "fresh", str(freshResult, "context_mode"))
	leases = call.backgroundLeases()
	require.Len(t, leases, 2)
	waitUntil(t, func() bool { return leases[1].closes() == 1 })

	listed := f.list(call)
	require.Len(t, listed.Data["agents"], 2)
	for _, expected := range []string{"authentication-inspector", "independent-reviewer", *running.ConversationID, str(freshResult, "conversation_id")} {
		require.Contains(t, listed.Content, expected)
	}
	listPresentation := presentationOf(listed)
	require.Equal(t, "List agents", listPresentation.Summary)
	require.Equal(t, "markdown", listPresentation.Format)
	for _, expected := range []string{
		"**authentication-inspector** — completed", "inspect authentication",
		"**independent-reviewer** — completed", "independent review",
	} {
		require.Contains(t, listPresentation.Body, expected)
	}
	require.NotContains(t, listPresentation.Body, forkedID)
	require.NotContains(t, listPresentation.Body, *running.ConversationID)

	other := f.call("owner-2")
	otherList := f.list(other)
	require.Empty(t, otherList.Data["agents"])
	require.Equal(t, presentation("List agents", "No background agents."), presentationOf(otherList))
	hidden := f.wait(other, forkedID, 0)
	require.Contains(t, hidden.Error, "agent not found")
	require.Equal(t, presentation("Wait for agent"), presentationOf(hidden))

	restored := f.call("owner-1")
	f.app.restoreAgentWidget(f.ctx, restored)
	require.True(t, containsAll(restored.ui.text(WidgetID), "2 completed", "independent-reviewer"))
}

func TestWaitCurrentRunAndFollowupResumesChild(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "iterative-reviewer", Task: "first pass"}))
	first := f.wait(call, id, 1000)
	childID := str(first, "conversation_id")

	f.acp.update(func(a *fakeACP) { a.blockRuns = true })
	f.acp.resetRun()
	followup := f.followup(call, id, "second pass")
	require.Contains(t, followup.Content, "Follow-up task sent:\nsecond pass")
	require.Equal(t, "second pass", str(followup, "task"))
	require.Equal(t, presentation("Follow up iterative-reviewer", "second pass"), presentationOf(followup))
	secondRunID := str(followup, "run_id")
	f.acp.started().wait(t)
	require.Equal(t, childID, f.acp.client(t, 1).options(t).Resume)

	current := f.wait(call, id, 0)
	require.Equal(t, secondRunID, str(current, "run_id"))
	require.Equal(t, "running", str(current, "status"))
	require.Equal(t, presentation("Wait for iterative-reviewer"), presentationOf(current))
	f.acp.release().set()
	require.Equal(t, "result for second pass", f.wait(call, id, 1000).Content)
}

func TestFreshProfileSelectionPrecedence(t *testing.T) {
	cases := []struct {
		name, parent, configured, expected string
		unsetConfigured                    bool
	}{
		{name: "parent", parent: "parent-profile", unsetConfigured: true, expected: "parent-profile"},
		{name: "unnamed", unsetConfigured: true},
		{name: "blank-profiles", parent: "  ", configured: "  "},
		{name: "base", parent: "default", unsetConfigured: true, expected: "default"},
		{name: "configured", parent: "parent-profile", configured: "configured-profile", expected: "configured-profile"},
		{name: "configured-base", parent: "parent-profile", configured: "default", expected: "default"},
		{name: "blank-env", parent: "parent-profile", configured: "  ", expected: "parent-profile"},
		{name: "trim-env", parent: "parent-profile", configured: " configured-profile ", expected: "configured-profile"},
	}
	f := newFixture(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.unsetConfigured {
				require.NoError(t, os.Unsetenv(ProfileEnv))
			} else {
				t.Setenv(ProfileEnv, tc.configured)
			}
			call := f.call("owner-1", withProfile(tc.parent))
			spawned := f.spawn(call, SpawnAgentInput{Name: tc.name, Task: "inspect", ContextMode: types.ContextFresh})
			completed := f.wait(call, agentID(t, spawned), 1000)
			require.Equal(t, "completed", str(completed, "status"))
			options := f.acp.client(t, -1).options(t)
			require.Equal(t, tc.expected, options.Profile)
			require.Empty(t, options.Resume)
		})
	}
}

func TestForkIgnoresConfiguredFreshProfile(t *testing.T) {
	f := newFixture(t)
	t.Setenv(ProfileEnv, "configured-profile")
	call := f.call("owner-1", withProfile("parent-profile"))
	completed := f.wait(call, agentID(t, f.spawn(call, SpawnAgentInput{Name: "forked", Task: "inspect"})), 1000)
	require.Equal(t, "completed", str(completed, "status"))
	options := f.acp.client(t, 0).options(t)
	require.Equal(t, str(completed, "conversation_id"), options.Resume)
	require.Empty(t, options.Profile)
	require.Equal(t, []string{"forked"}, call.forks())
}

func TestFollowupReforksWhenInterruptedBeforeAttachment(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	store := f.store(call)
	initial, err := store.Create(f.ctx, "owner-1", "setup-agent", "initial setup", f.cwd, types.ContextFork)
	require.NoError(t, err)
	interrupted, err := store.Terminal(f.ctx, initial.Lease, types.TerminalInterrupted, types.TerminalUpdate{Error: strPtr("setup interrupted")})
	require.NoError(t, err)
	require.Nil(t, interrupted.ConversationID)

	followup := f.followup(call, initial.Agent.ID, "retry setup")
	require.Equal(t, []string{"setup-agent"}, call.forks())
	require.Equal(t, "child-owner-1-1", str(followup, "conversation_id"))
	require.Equal(t, "result for retry setup", f.wait(call, initial.Agent.ID, 1000).Content)
}

func TestSteerResultExposesTheNormalizedQueuedMessage(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	store := f.store(call)
	claim, err := store.Create(f.ctx, "owner-1", "steerable-worker", "long task", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(f.ctx, claim.Lease, "child-steerable")
	require.NoError(t, err)

	result := f.steer(call, claim.Agent.ID, "  focus on the parser  ")
	require.Contains(t, result.Content, "Steering message queued for delivery:\nfocus on the parser")
	require.Contains(t, result.Content, "steerable-worker")
	require.Equal(t, "steerable-worker", str(result, "name"))
	require.Equal(t, "focus on the parser", str(result, "message"))
	require.Equal(t, presentation("Steer steerable-worker", "focus on the parser"), presentationOf(result))
	require.Equal(t, true, result.Data["accepted"])
	require.Equal(t, [][3]any{{claim.Lease.RunID, claim.Lease.Generation, "focus on the parser"}}, f.steeringRows(store))
}

func TestLegacyAgentCanResumeWithoutDelegationMetadata(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	store := f.store(call)
	claim, err := store.Create(f.ctx, "owner-1", "legacy-agent", "old", f.cwd, types.ContextFork)
	require.NoError(t, err)
	_, err = store.MarkRunning(f.ctx, claim.Lease, "legacy-conversation")
	require.NoError(t, err)
	_, err = store.Terminal(f.ctx, claim.Lease, types.TerminalIdle, types.TerminalUpdate{Result: strPtr("old result")})
	require.NoError(t, err)

	result := f.followup(call, claim.Agent.ID, "resume")
	require.Empty(t, result.Error)
	require.Equal(t, "result for resume", f.wait(call, claim.Agent.ID, 1000).Content)
	agent := f.get(store, "owner-1", claim.Agent.ID)
	require.Equal(t, "legacy-conversation", *agent.ConversationID)
	require.Equal(t, types.AgentIdle, agent.Status)
	require.Equal(t, 2, agent.Run.Generation)
	require.Equal(t, "legacy-conversation", f.acp.client(t, 0).options(t).Resume)
	require.Empty(t, call.forks())
}

func TestForkCWDRejectsDifferentDirectoriesBeforeSetup(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	outside := filepath.Join(f.root, "outside")
	require.NoError(t, os.Mkdir(outside, 0o755))
	require.NoError(t, os.Symlink(outside, filepath.Join(f.cwd, "escape")))
	require.NoError(t, os.Mkdir(filepath.Join(f.cwd, "nested"), 0o755))
	for _, cwd := range []string{outside, "../outside", "escape", "nested"} {
		result := f.spawn(call, SpawnAgentInput{Name: "forked-agent", Task: "work", CWD: strPtr(cwd)})
		require.Equal(t, "spawn_agent failed: context_mode='fork' requires the current workspace cwd", result.Error, cwd)
	}
	require.Empty(t, f.runtime.stores)
	require.Empty(t, call.forks())
	require.Empty(t, call.backgroundLeases())
	require.Empty(t, f.acp.instances())
}

func TestForkCWDAcceptsPathsResolvingToCurrentWorkspace(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	alias := filepath.Join(f.root, "workspace-alias")
	require.NoError(t, os.Symlink(f.cwd, alias))
	for index, cwd := range []*string{nil, strPtr(f.cwd), strPtr("."), strPtr("../workspace"), strPtr(alias)} {
		names := []string{"forked-agent", "forked-agent-one", "forked-agent-two", "forked-agent-three", "forked-agent-four"}
		result := f.spawn(call, SpawnAgentInput{Name: names[index], Task: "work", CWD: cwd})
		require.Equal(t, "result for work", f.wait(call, agentID(t, result), 1000).Content)
		options := f.acp.client(t, -1).options(t)
		require.Equal(t, f.cwd, options.CWD)
		require.NotEmpty(t, options.Resume)
	}
}

func TestFreshCWDAcceptsAbsoluteRelativeAndSymlinkSiblingDirectories(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	outside := filepath.Join(f.root, "outside")
	require.NoError(t, os.Mkdir(outside, 0o755))
	require.NoError(t, os.Symlink(outside, filepath.Join(f.cwd, "escape")))
	for index, cwd := range []string{outside, "../outside", "escape"} {
		names := []string{"outside-agent", "outside-agent-one", "outside-agent-two"}
		result := f.spawn(call, SpawnAgentInput{Name: names[index], Task: "work", CWD: strPtr(cwd), ContextMode: types.ContextFresh})
		require.Equal(t, "result for work", f.wait(call, agentID(t, result), 1000).Content)
		options := f.acp.client(t, -1).options(t)
		require.Equal(t, outside, options.CWD)
		require.Empty(t, options.Resume)
	}
	require.Empty(t, call.forks())
}

func TestSpawnRejectsMissingAndNonDirectoryCWD(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	file := filepath.Join(f.root, "file")
	require.NoError(t, os.WriteFile(file, nil, 0o644))
	cases := map[string]string{
		"  ":      "spawn_agent failed: cwd must be a non-empty string when provided",
		"missing": "spawn_agent failed: cwd does not exist: " + filepath.Join(f.cwd, "missing"),
		file:      "spawn_agent failed: cwd is not a directory: " + file,
	}
	for cwd, expected := range cases {
		result := f.spawn(call, SpawnAgentInput{Name: "fresh-agent", Task: "work", CWD: strPtr(cwd), ContextMode: types.ContextFresh})
		require.Equal(t, expected, result.Error)
	}
}

func TestFollowupPreservesLegacySiblingCWD(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	store := f.store(call)
	outside := filepath.Join(f.root, "legacy-workspace")
	require.NoError(t, os.Mkdir(outside, 0o755))
	claim, err := store.Create(f.ctx, "owner-1", "legacy-outside", "old task", outside, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(f.ctx, claim.Lease, "old-conversation")
	require.NoError(t, err)
	_, err = store.Terminal(f.ctx, claim.Lease, types.TerminalIdle, types.TerminalUpdate{Result: strPtr("old output")})
	require.NoError(t, err)

	require.Empty(t, f.followup(call, claim.Agent.ID, "continue").Error)
	require.Equal(t, "result for continue", f.wait(call, claim.Agent.ID, 1000).Content)
	stored := f.get(store, "owner-1", claim.Agent.ID)
	require.Equal(t, 2, stored.Run.Generation)
	require.Equal(t, types.AgentIdle, stored.Status)
	options := f.acp.client(t, 0).options(t)
	require.Equal(t, outside, options.CWD)
	require.Equal(t, "old-conversation", options.Resume)
}

func TestToolInputValidationErrors(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1")
	invalid := f.spawn(call, SpawnAgentInput{Name: "Bad Name", Task: "work"})
	require.Contains(t, invalid.Error, "name must contain")
	require.Equal(t, presentation("Spawn agent"), presentationOf(invalid))
	blank := f.spawn(call, SpawnAgentInput{Name: "worker", Task: "   "})
	require.Equal(t, "task is required and must be a non-empty string", blank.Error)
	require.Equal(t, presentation("Spawn worker"), presentationOf(blank))
	followup := f.followup(call, "agent", " ")
	require.Equal(t, "task is required and must be a non-empty string", followup.Error)
	steer := f.steer(call, "missing", "focus")
	require.Equal(t, "agent not found", steer.Error)
}

func TestSessionEndInterruptsOnlyCurrentRuntimeRows(t *testing.T) {
	f := newFixture(t)
	path := filepath.Join(f.root, "session-end")
	current := f.standaloneStore("session-end", f.runtime.RuntimeID(), nil)
	other := f.standaloneStore("session-end", "other-runtime", nil)
	require.Equal(t, filepath.Join(path, "subagents.sqlite"), current.Path())
	currentClaim, err := current.Create(f.ctx, "current-owner", "current-worker", "current task", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	otherClaim, err := other.Create(f.ctx, "other-owner", "other-worker", "other task", f.cwd, types.ContextFresh)
	require.NoError(t, err)
	f.runtime.mu.Lock()
	f.runtime.stores[current.Path()] = current
	f.runtime.mu.Unlock()

	require.NoError(t, f.app.interruptLiveAgents(f.ctx))

	currentRecord := f.get(current, "current-owner", currentClaim.Agent.ID)
	otherRecord := f.get(current, "other-owner", otherClaim.Agent.ID)
	require.Equal(t, types.AgentInterrupted, currentRecord.Status)
	require.Equal(t, types.RunInterrupted, currentRecord.Run.Status)
	require.Equal(t, types.AgentStarting, otherRecord.Status)
	require.Equal(t, types.RunStarting, otherRecord.Run.Status)
}

func TestChildCallsAreRejectedAndRecursiveToolsDisabled(t *testing.T) {
	f := newFixture(t)
	child := f.call("owner-1", invokedBy("spawn_agent"))
	require.True(t, isAgentChild(child))
	require.False(t, isAgentChild(f.call("owner-1", invokedBy("main"))))
	require.False(t, isAgentChild(f.call("owner-1")))
	require.Equal(t, &extension.ToolListPatch{Disable: AgentToolNames}, f.app.disableRecursiveAgents(child).Tools)
	require.Nil(t, f.app.disableRecursiveAgents(f.call("owner-1")).Tools)

	nested := f.spawn(child, SpawnAgentInput{Name: "nested-worker", Task: "nested work"})
	require.Contains(t, nested.Error, "only available to the main agent")
	require.Equal(t, presentation("Spawn agent"), presentationOf(nested))
	for _, result := range []extension.ToolResult{
		f.wait(child, "agent", 0), f.list(child), f.followup(child, "agent", "task"),
		f.steer(child, "agent", "message"), f.cancel(child, "agent"),
	} {
		require.Equal(t, childToolErrorMessage, result.Error)
	}

	t.Setenv(RecursionGuardEnv, "1")
	require.True(t, isAgentChild(f.call("owner-1")))
}

func containsAll(text string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(text, part) {
			return false
		}
	}
	return true
}
