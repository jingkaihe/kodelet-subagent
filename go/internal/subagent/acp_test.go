package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/jingkaihe/kodelet/sdk/go/extension/extensiontest"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/paths"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// fakeACPFactory starts this test binary as kodelet acp through the real SDK client.
func fakeACPFactory(t *testing.T, mode string, extra map[string]string) ClientFactory {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	return func(_, cwd string, env map[string]string) AgentClient {
		merged := maps.Clone(env)
		merged[fakeACPModeEnv] = mode
		maps.Copy(merged, extra)
		return DefaultClientFactory(executable, cwd, merged)
	}
}

func childReaped(pidFile string) bool {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return false
	}
	return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
}

// reapCheckingLease records whether the ACP child had exited when released.
type reapCheckingLease struct {
	pidFile string

	mu     sync.Mutex
	closes int
	reaped bool
}

func (l *reapCheckingLease) Close(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closes++
	l.reaped = childReaped(l.pidFile)
	return nil
}

func (l *reapCheckingLease) released() (int, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closes, l.reaped
}

type acpCase struct {
	t       *testing.T
	ctx     context.Context
	dir     string
	pidFile string
	runtime *Runtime
	store   *db.Store
	lease   *reapCheckingLease
	live    *LiveRun
}

// newACPCase reserves a fresh agent run owned by a runtime whose clients
// talk to the fake ACP process in mode.
func newACPCase(t *testing.T, mode string, extra map[string]string) *acpCase {
	t.Helper()
	dir := paths.Resolve(t.TempDir())
	c := &acpCase{t: t, ctx: context.Background(), dir: dir, pidFile: filepath.Join(dir, "pid")}
	env := map[string]string{fakeACPPIDEnv: c.pidFile}
	maps.Copy(env, extra)
	c.runtime = NewRuntime(RuntimeOptions{ClientFactory: fakeACPFactory(t, mode, env)})
	c.store = db.NewStore(filepath.Join(dir, "agents.sqlite"), c.runtime.RuntimeID(), nil)
	t.Cleanup(func() { _ = c.store.Close() })
	require.NoError(t, c.store.Initialize(c.ctx))
	claim, err := c.store.Create(c.ctx, "parent", "worker", "continue", dir, types.ContextFresh)
	require.NoError(t, err)
	c.live = c.runtime.liveRunFromClaim(claim, "continue", c.store)
	c.lease = &reapCheckingLease{pidFile: c.pidFile}
	c.live.setBackgroundLease(c.lease)
	return c
}

func (c *acpCase) resume() {
	c.t.Helper()
	_, err := c.store.AttachConversation(c.ctx, c.live.lease, "saved-child")
	require.NoError(c.t, err)
	c.live.setConversationID("saved-child")
}

func (c *acpCase) runToCompletion() types.Agent {
	c.t.Helper()
	c.runtime.launchLiveRun(c.live, nil)
	_, runner := c.ownerTasks()
	select {
	case <-runner.done:
	case <-time.After(10 * time.Second):
		c.t.Fatal("runner did not finish")
	}
	return c.finish()
}

func (c *acpCase) ownerTasks() (setup, runner *task) {
	c.runtime.mu.Lock()
	defer c.runtime.mu.Unlock()
	return c.live.setupTask, c.live.runnerTask
}

// finish shuts the runtime down and checks that every resource was released.
func (c *acpCase) finish() types.Agent {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	require.NoError(c.t, c.runtime.Shutdown(ctx))
	c.runtime.mu.Lock()
	require.Empty(c.t, c.runtime.liveRuns)
	require.Empty(c.t, c.runtime.ownedRuns)
	require.Empty(c.t, c.runtime.cleanupTasks)
	c.runtime.mu.Unlock()
	require.True(c.t, childReaped(c.pidFile))
	record, err := c.store.Get(c.ctx, "parent", c.live.agentID, "")
	require.NoError(c.t, err)
	return record
}

func (c *acpCase) requireLeaseReleasedAfterExit() {
	c.t.Helper()
	closes, reaped := c.lease.released()
	require.Equal(c.t, 1, closes)
	require.True(c.t, reaped, "lease released before the ACP child exited")
}

func readRequests(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	var requests []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var request map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &request))
		requests = append(requests, request)
	}
	return requests
}

func methods(requests []map[string]any) []string {
	names := make([]string, 0, len(requests))
	for _, request := range requests {
		names = append(names, request["method"].(string))
	}
	return names
}

func TestRuntimeReleasesOwnershipAndLeaseOnlyAfterProcessExit(t *testing.T) {
	expected := map[string]types.AgentStatus{
		"normal":       types.AgentIdle,
		"exit-on-load": types.AgentFailed,
		"ignore-term":  types.AgentCanceled,
	}
	for mode, status := range expected {
		t.Run(mode, func(t *testing.T) {
			requests := filepath.Join(t.TempDir(), "requests")
			c := newACPCase(t, mode, map[string]string{fakeACPBytesEnv: "16", fakeACPRequestsEnv: requests})
			c.resume()
			var record types.Agent
			if mode == "ignore-term" {
				c.runtime.launchLiveRun(c.live, nil)
				waitUntil(t, func() bool { return slices.Contains(methods(readRequests(t, requests)), "session/load") })
				_, err := c.store.Cancel(c.ctx, "parent", c.live.agentID)
				require.NoError(t, err)
				require.True(t, c.runtime.cancelLiveRun(c.ctx, c.store, c.live.agentID, c.live.runID, 5*time.Second))
				record = c.finish()
			} else {
				record = c.runToCompletion()
			}
			require.Equal(t, status, record.Status)
			if mode == "exit-on-load" {
				require.Contains(t, *record.Run.Error, "agent failed")
			}
			c.requireLeaseReleasedAfterExit()
		})
	}
}

func TestFreshRecursionGuardFailsClosedWithoutSupportedACPExtensions(t *testing.T) {
	for _, mode := range []string{"no-extensions", "unsupported-extensions"} {
		for _, resume := range []bool{false, true} {
			t.Run(mode+"/resume="+strconv.FormatBool(resume), func(t *testing.T) {
				requests := filepath.Join(t.TempDir(), "requests")
				c := newACPCase(t, mode, map[string]string{fakeACPRequestsEnv: requests})
				if resume {
					c.resume()
				}
				record := c.runToCompletion()
				require.Equal(t, types.RunFailed, record.Run.Status)
				require.Contains(t, *record.Run.Error, "sessionExtensions version 1")
				require.Equal(t, []string{"initialize"}, methods(readRequests(t, requests)))
				c.requireLeaseReleasedAfterExit()
			})
		}
	}
}

func TestFreshProfileAndParentRelationshipAreSentOnlyOnCreation(t *testing.T) {
	for _, resume := range []bool{false, true} {
		for _, mode := range []string{"normal", "no-hierarchy"} {
			for _, profile := range []string{"", "reviewer", "default"} {
				name := mode + "/resume=" + strconv.FormatBool(resume) + "/profile=" + profile
				t.Run(name, func(t *testing.T) {
					scratch := t.TempDir()
					requests, args := filepath.Join(scratch, "requests"), filepath.Join(scratch, "args")
					c := newACPCase(t, mode, map[string]string{fakeACPRequestsEnv: requests, fakeACPArgsEnv: args, fakeACPBytesEnv: "16"})
					c.live.setProfile(profile)
					if resume {
						c.resume()
					}
					record := c.runToCompletion()

					expectedArgs := []string{"acp"}
					if profile != "" && !resume {
						expectedArgs = append(expectedArgs, "--profile="+profile)
					}
					data, err := os.ReadFile(args)
					require.NoError(t, err)
					var actualArgs []string
					require.NoError(t, json.Unmarshal(data, &actualArgs))
					require.Equal(t, expectedArgs, actualArgs)

					sent := readRequests(t, requests)
					if mode == "no-hierarchy" && !resume {
						require.Equal(t, types.RunFailed, record.Run.Status)
						require.Contains(t, *record.Run.Error, "conversationHierarchy version 1")
						require.Equal(t, []string{"initialize"}, methods(sent))
						return
					}
					require.Equal(t, types.RunCompleted, record.Run.Status)
					session := sent[1]
					expectedMethod := "session/new"
					if resume {
						expectedMethod = "session/load"
					}
					require.Equal(t, expectedMethod, session["method"])
					meta := session["params"].(map[string]any)["_meta"].(map[string]any)
					require.Equal(t, map[string]any{"version": float64(1), "extensionIds": []any{"inline-1"}}, meta["sessionExtensions"])
					if resume {
						require.NotContains(t, meta, "conversationHierarchy")
					} else {
						require.Equal(t, map[string]any{"version": float64(1), "parentConversationId": "parent"}, meta["conversationHierarchy"])
					}
				})
			}
		}
	}
}

func TestRuntimePreservesRealSDKCancelledStopReasonAndPartialOutput(t *testing.T) {
	for _, size := range []int{0, 31} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			c := newACPCase(t, "cancelled", map[string]string{fakeACPBytesEnv: strconv.Itoa(size)})
			record := c.runToCompletion()
			require.Equal(t, types.RunInterrupted, record.Run.Status)
			require.Equal(t, "agent canceled by kodelet", *record.Run.Error)
			if size == 0 {
				require.Nil(t, record.Run.Result)
			} else {
				require.Equal(t, strings.Repeat("p", size), *record.Run.Result)
			}
			require.Equal(t, "failed", c.live.progress.Snapshot().Status)
		})
	}
}

// namedForkHost is the host half of the tool context protocol: named forks,
// background leases, and tool progress.
type namedForkHost struct {
	dir string

	mu       sync.Mutex
	forks    []string
	acquired int
	released []string
	updates  []map[string]any
	problems []string
}

func (h *namedForkHost) handle(_ context.Context, call extensiontest.HostCall) (any, error) {
	var params map[string]any
	_ = call.DecodeParams(&params)
	h.mu.Lock()
	defer h.mu.Unlock()
	switch call.Method {
	case "kodelet.conversation.fork":
		if params["asChild"] != true {
			h.problems = append(h.problems, "fork was not requested as a child")
		}
		h.forks = append(h.forks, params["name"].(string))
		return map[string]any{"conversationId": "named-child"}, nil
	case "kodelet.runtime.background.acquire":
		h.acquired++
		return map[string]any{"leaseId": strconv.Itoa(h.acquired)}, nil
	case "kodelet.runtime.background.release":
		leaseID := params["leaseId"].(string)
		if !childReaped(filepath.Join(h.dir, "pid-"+leaseID)) {
			h.problems = append(h.problems, "lease "+leaseID+" released before its ACP child exited")
		}
		h.released = append(h.released, leaseID)
		return map[string]any{}, nil
	case "kodelet.tool.update":
		if data, ok := params["data"].(map[string]any); ok {
			h.updates = append(h.updates, data)
		}
		return map[string]any{}, nil
	default:
		return nil, &extension.RPCError{Code: -32601, Message: "method not found: " + call.Method}
	}
}

func (h *namedForkHost) responding() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, update := range h.updates {
		if taskRun, ok := update["taskRun"].(map[string]any); ok && taskRun["phase"] == "responding" {
			return true
		}
	}
	return false
}

func TestNamedForkStreamingSteeringResumeAndCancelUseRealSDK(t *testing.T) {
	t.Setenv(RecursionGuardEnv, "0")
	t.Setenv(ProfileEnv, "")
	ctx := context.Background()
	dir := paths.Resolve(t.TempDir())
	workspace := filepath.Join(dir, "workspace")
	require.NoError(t, os.Mkdir(workspace, 0o755))
	requests, finish := filepath.Join(dir, "requests"), filepath.Join(dir, "finish")
	executable, err := os.Executable()
	require.NoError(t, err)

	var mu sync.Mutex
	var clients int
	var problems []string
	factory := func(command, cwd string, env map[string]string) AgentClient {
		mu.Lock()
		defer mu.Unlock()
		if command != "kodelet" || cwd != workspace || env[RecursionGuardEnv] != "1" {
			problems = append(problems, "unexpected client: "+command+" "+cwd)
		}
		clients++
		merged := maps.Clone(env)
		maps.Copy(merged, map[string]string{
			fakeACPModeEnv:     "interactive",
			fakeACPRequestsEnv: requests,
			fakeACPPIDEnv:      filepath.Join(dir, "pid-"+strconv.Itoa(clients)),
			fakeACPFinishEnv:   finish,
		})
		return DefaultClientFactory(executable, cwd, merged)
	}
	timing := DefaultTiming()
	timing.SteeringPoll, timing.SteeringRetry = 20*time.Millisecond, 20*time.Millisecond
	runtime := NewRuntime(RuntimeOptions{ClientFactory: factory, Timing: &timing})
	app := NewApplication(runtime)
	host := &namedForkHost{dir: dir}
	h := extensiontest.New(app.Extension(), &extensiontest.Options{Host: host.handle})
	params := extensiontest.DefaultInitializeParams()
	params.Extension.DataDir = filepath.Join(dir, "data")
	params.Capabilities["runtime"] = map[string]any{"backgroundTasks": true}
	params.Capabilities["conversations"] = map[string]any{"fork": true, "hierarchy": true}
	_, err = h.Initialize(ctx, &params)
	require.NoError(t, err)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		require.NoError(t, runtime.Shutdown(shutdownCtx))
		h.Close()
	}()
	call := extensiontest.WithCall(extension.CallInfo{ConversationID: "parent", CWD: workspace})
	executeTool := func(name string, input map[string]any) extension.ToolResult {
		result, err := h.ExecuteTool(ctx, name, input, call)
		require.NoError(t, err)
		require.Empty(t, result.Error, result.Content)
		return result
	}

	spawned := executeTool("spawn_agent", map[string]any{"name": "named-reviewer", "task": "inspect"})
	id := spawned.Data["agent_id"].(string)
	require.Equal(t, "named-child", spawned.Data["conversation_id"])
	waiting := make(chan extension.ToolResult, 1)
	go func() {
		result, _ := h.ExecuteTool(ctx, "wait_agent", map[string]any{"agent_id": id, "timeout_ms": 5000}, call)
		waiting <- result
	}()
	store, err := runtime.storeForCall(ctx, &fakeCall{dataDir: params.Extension.DataDir})
	require.NoError(t, err)
	// Steering requires the persisted running state, not merely an ACP session.
	waitUntil(t, func() bool {
		agent, err := store.Get(ctx, "parent", id, "")
		return err == nil && agent.Status == types.AgentRunning
	})
	executeTool("steer_agent", map[string]any{"agent_id": id, "message": "inspect source"})
	waitUntil(t, host.responding)
	executeTool("steer_agent", map[string]any{"agent_id": id, "message": "finish"})
	// Complete only after durable acknowledgement, so the guidance is not
	// carried into a follow-up.
	live := runtime.liveRun(store, id)
	waitUntil(t, func() bool {
		next, err := store.NextSteering(ctx, live.lease)
		return err == nil && next == nil
	})
	require.NoError(t, os.WriteFile(finish, nil, 0o644))
	completed := receive(t, waiting)
	require.Equal(t, "Inspecting: complete", completed.Content)
	taskRun := completed.Data["taskRun"].(map[string]any)
	require.Equal(t, map[string]any{"succeeded": float64(1), "failed": float64(0), "running": float64(0)}, taskRun["counts"])
	activities := taskRun["activities"].([]any)
	require.Len(t, activities, 1)
	require.Equal(t, "file_read", activities[0].(map[string]any)["kind"])
	require.Equal(t, "succeeded", activities[0].(map[string]any)["status"])

	followed := executeTool("followup_agent", map[string]any{"agent_id": id, "task": "second pass"})
	require.Equal(t, "named-child", followed.Data["conversation_id"])
	waitUntil(t, func() bool {
		count := 0
		for _, method := range methods(readRequests(t, requests)) {
			if method == "session/prompt" {
				count++
			}
		}
		return count >= 2
	})
	canceled := executeTool("cancel_agent", map[string]any{"agent_id": id})
	require.Equal(t, "canceled", canceled.Data["agent_status"])
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, runtime.Shutdown(shutdownCtx))

	host.mu.Lock()
	defer host.mu.Unlock()
	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, problems)
	require.Empty(t, host.problems)
	require.Equal(t, []string{"named-reviewer"}, host.forks)
	released := slices.Clone(host.released)
	slices.Sort(released)
	require.Equal(t, []string{"1", "2"}, released)
	require.Equal(t, 2, clients)
	runtime.mu.Lock()
	require.Empty(t, runtime.ownedRuns)
	runtime.mu.Unlock()

	sent := readRequests(t, requests)
	var loads, steering []map[string]any
	for _, request := range sent {
		params, _ := request["params"].(map[string]any)
		switch request["method"] {
		case "session/load":
			loads = append(loads, params)
		case "_session/steering":
			steering = append(steering, params)
		case "session/new":
			t.Fatal("named forks must resume, not create, a session")
		}
	}
	expectedLoad := map[string]any{"sessionId": "named-child", "cwd": workspace}
	require.Equal(t, []map[string]any{expectedLoad, expectedLoad}, loads)
	texts := make([]string, 0, len(steering))
	for _, request := range steering {
		texts = append(texts, request["prompt"].([]any)[0].(map[string]any)["text"].(string))
		meta := request["_meta"].(map[string]any)["steering"].(map[string]any)
		require.Equal(t, "promptRequired", meta["idleBehavior"])
	}
	require.Equal(t, []string{"inspect source", "finish"}, texts)
}
