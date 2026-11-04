package subagent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/jingkaihe/kodelet/sdk/go/extension/extensiontest"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func sdkHost(_ context.Context, call extensiontest.HostCall) (any, error) {
	switch call.Method {
	case "kodelet.runtime.background.acquire":
		return map[string]any{"leaseId": "lease-1"}, nil
	case "kodelet.runtime.background.release", "kodelet.tool.update":
		return map[string]any{}, nil
	case "kodelet.conversation.fork":
		return map[string]any{"conversationId": "child-sdk"}, nil
	default:
		return nil, &extension.RPCError{Code: -32601, Message: "method not found: " + call.Method}
	}
}

func (f *fixture) harness(t *testing.T) *extensiontest.Harness {
	t.Helper()
	h := extensiontest.New(f.app.Extension(), &extensiontest.Options{Host: sdkHost})
	t.Cleanup(h.Close)
	params := extensiontest.DefaultInitializeParams()
	params.Extension.CWD = f.cwd
	params.Extension.DataDir = f.dataDir
	params.Capabilities["runtime"] = map[string]any{"backgroundTasks": true}
	params.Capabilities["conversations"] = map[string]any{"fork": true, "hierarchy": true}
	_, err := h.Initialize(f.ctx, &params)
	require.NoError(t, err)
	return h
}

func owner(conversation string) extensiontest.CallOption {
	return extensiontest.WithCall(extension.CallInfo{ConversationID: conversation})
}

func execute(t *testing.T, h *extensiontest.Harness, name string, input map[string]any) extension.ToolResult {
	t.Helper()
	result, err := h.ExecuteTool(context.Background(), name, input, owner("owner-1"))
	require.NoError(t, err)
	require.Empty(t, result.Error, result.Content)
	assertStructuredResult(t, result, name)
	return result
}

// assertStructuredResult checks structured content against the advertised
// output schema and the metadata copy that omits nulls.
func assertStructuredResult(t *testing.T, result extension.ToolResult, name string) map[string]any {
	t.Helper()
	payload, ok := result.StructuredContent.(map[string]any)
	require.True(t, ok, "structured content is %T", result.StructuredContent)
	schema := resolveSchema(t, toolSchemas[name].output)
	require.NoError(t, schema.Validate(payload))
	properties := toolSchemas[name].output["properties"].(map[string]any)
	for key := range payload {
		require.Contains(t, properties, key)
	}
	require.NotContains(t, payload, "presentation")
	require.NotContains(t, payload, "taskRun")
	require.Contains(t, result.Data, "presentation")
	metadata := map[string]any{}
	for key, value := range result.Data {
		if key != "presentation" && key != "taskRun" {
			metadata[key] = value
		}
	}
	require.Equal(t, withoutNulls(payload), metadata)
	return payload
}

func resolveSchema(t *testing.T, document map[string]any) *jsonschema.Resolved {
	t.Helper()
	data, err := json.Marshal(document)
	require.NoError(t, err)
	var schema jsonschema.Schema
	require.NoError(t, json.Unmarshal(data, &schema))
	resolved, err := schema.Resolve(nil)
	require.NoError(t, err)
	return resolved
}

func TestSDKStructuredResultsAcrossAgentLifecycle(t *testing.T) {
	f := newFixture(t)
	h := f.harness(t)
	f.acp.update(func(a *fakeACP) { a.blockStartup, a.blockRuns = true, true })

	empty := execute(t, h, "list_agents", map[string]any{})
	require.Equal(t, map[string]any{"agents": []any{}}, empty.StructuredContent)
	spawned := execute(t, h, "spawn_agent", map[string]any{"name": "sdk-worker", "task": "first task", "context_mode": "fresh"})
	payload := spawned.StructuredContent.(map[string]any)
	id := payload["agent_id"].(string)
	require.Equal(t, "starting", payload["status"])
	for _, field := range []string{"conversation_id", "started_at", "completed_at"} {
		require.Contains(t, payload, field)
		require.Nil(t, payload[field])
	}
	listed := execute(t, h, "list_agents", map[string]any{})
	require.Equal(t, map[string]any{"agents": []any{payload}}, listed.StructuredContent)

	f.acp.startupRelease.set()
	f.acp.started().wait(t)
	running := execute(t, h, "wait_agent", map[string]any{"agent_id": id, "timeout_ms": 1})
	require.Equal(t, "running", running.StructuredContent.(map[string]any)["status"])
	require.Contains(t, running.Data, "taskRun")
	steered := execute(t, h, "steer_agent", map[string]any{"agent_id": id, "message": "focus here"})
	require.Equal(t, map[string]any{
		"agent_id": id, "name": "sdk-worker", "message": "focus here", "accepted": true, "alreadyPending": false,
	}, steered.StructuredContent)
	canceled := execute(t, h, "cancel_agent", map[string]any{"agent_id": id})
	require.Equal(t, "canceled", canceled.StructuredContent.(map[string]any)["agent_status"])
	followed := execute(t, h, "followup_agent", map[string]any{"agent_id": id, "task": "next task"})
	require.Equal(t, float64(2), followed.StructuredContent.(map[string]any)["generation"])
	f.acp.release().set()
	completed := execute(t, h, "wait_agent", map[string]any{"agent_id": id, "timeout_ms": 1000.0})
	structured := completed.StructuredContent.(map[string]any)
	require.Equal(t, "completed", structured["status"])
	require.Equal(t, "result for next task", structured["result"])
	require.Equal(t, "result for next task", completed.Content)
	require.NotContains(t, structured, "partial_result")
}

func TestSDKWaitPreservesTerminalSnapshotsAndPartialResults(t *testing.T) {
	f := newFixture(t)
	h := f.harness(t)
	store := f.store(f.call("owner-1"))
	for _, status := range []string{"failed", "interrupted", "canceling", "canceled"} {
		t.Run(status, func(t *testing.T) {
			claim, err := store.Create(f.ctx, "owner-1", status, "inspect", f.cwd, types.ContextFresh)
			require.NoError(t, err)
			_, err = store.MarkRunning(f.ctx, claim.Lease, "child-"+status)
			require.NoError(t, err)
			switch status {
			case "failed", "interrupted":
				_, err = store.Terminal(f.ctx, claim.Lease, types.TerminalStatus(status), types.TerminalUpdate{
					Error: new("worker stopped"), Result: new("partial answer"),
				})
				require.NoError(t, err)
			default:
				_, err = store.Cancel(f.ctx, "owner-1", claim.Agent.ID)
				require.NoError(t, err)
				if status == "canceled" {
					_, err = store.CompleteCancel(f.ctx, claim.Lease)
					require.NoError(t, err)
				}
			}
			result, err := h.ExecuteTool(f.ctx, "wait_agent", map[string]any{"agent_id": claim.Agent.ID, "timeout_ms": 0}, owner("owner-1"))
			require.NoError(t, err)
			payload := assertStructuredResult(t, result, "wait_agent")
			require.Equal(t, status, payload["agent_status"])
			require.NotContains(t, payload, "result")
			if status == "failed" || status == "interrupted" {
				require.Contains(t, result.Error, "worker stopped")
				require.Equal(t, "partial answer", payload["partial_result"])
				require.Equal(t, "worker stopped", payload["error"])
			} else {
				require.Empty(t, result.Error)
				require.NotContains(t, payload, "partial_result")
			}
			if status == "interrupted" {
				require.Contains(t, result.Content, "Partial output (agent did not complete)")
			}
		})
	}
}

func TestSDKToolErrorsHaveNullStructuredContent(t *testing.T) {
	f := newFixture(t)
	h := f.harness(t)
	cases := []struct {
		name  string
		input map[string]any
	}{
		{"spawn_agent", map[string]any{"name": "sdk-worker", "task": " "}},
		{"wait_agent", map[string]any{"agent_id": "missing"}},
		{"list_agents", map[string]any{}},
		{"followup_agent", map[string]any{"agent_id": "missing", "task": "retry"}},
		{"steer_agent", map[string]any{"agent_id": "missing", "message": "focus"}},
		{"cancel_agent", map[string]any{"agent_id": "missing"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conversation := "owner-1"
			if tc.name == "list_agents" {
				conversation = ""
			}
			result, err := h.ExecuteTool(f.ctx, tc.name, tc.input, owner(conversation))
			require.NoError(t, err)
			require.NotEmpty(t, result.Error)
			require.Equal(t, result.Error, result.Content)
			require.Nil(t, result.StructuredContent)
			require.Len(t, result.Data, 1)
			require.Contains(t, result.Data, "presentation")
		})
	}
}

func TestSDKRejectsInputThatViolatesTheSchema(t *testing.T) {
	f := newFixture(t)
	h := f.harness(t)
	for name, input := range map[string]map[string]any{
		"spawn_agent":    {"name": "Bad Name", "task": "work"},
		"wait_agent":     {"agent_id": "agent", "timeout_ms": 300_001},
		"steer_agent":    {"agent_id": "", "message": "focus"},
		"followup_agent": {"agent_id": "agent"},
	} {
		_, err := h.ExecuteTool(f.ctx, name, input, owner("owner-1"))
		require.ErrorContains(t, err, "invalid input for tool "+name)
	}
	_, err := h.ExecuteTool(f.ctx, "spawn_agent", map[string]any{"name": "worker", "task": "work", "context_mode": "later"}, owner("owner-1"))
	require.ErrorContains(t, err, "invalid input for tool spawn_agent")
}

// connectionPeer drives a Connection the way the Kodelet host does.
type connectionPeer struct {
	conn *Connection

	mu   sync.Mutex
	sent []map[string]any
}

func newConnectionPeer(app *Application) *connectionPeer {
	peer := &connectionPeer{}
	peer.conn = app.NewConnection(extension.TransportFunc(func(_ context.Context, message []byte) error {
		var decoded map[string]any
		if err := json.Unmarshal(message, &decoded); err != nil {
			return err
		}
		peer.mu.Lock()
		defer peer.mu.Unlock()
		peer.sent = append(peer.sent, decoded)
		return nil
	}), &extension.RuntimeOptions{LogOutput: io.Discard})
	return peer
}

func (p *connectionPeer) initialize(t *testing.T, id int, capabilities map[string]any) map[string]any {
	t.Helper()
	response := p.initializeConfig(t, id, capabilities, nil)
	require.NotContains(t, response, "error")
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	return result
}

func (p *connectionPeer) initializeConfig(t *testing.T, id int, capabilities map[string]any, config any) map[string]any {
	t.Helper()
	request, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": "extension.initialize",
		"params": map[string]any{
			"protocolVersion": extension.ProtocolVersion,
			"extension":       map[string]any{"id": "subagent", "config": config},
			"capabilities":    capabilities,
		},
	})
	require.NoError(t, err)
	require.NoError(t, p.conn.Receive(request))
	var result map[string]any
	waitUntil(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, message := range p.sent {
			if message["id"] == float64(id) {
				result = message
				return true
			}
		}
		return false
	})
	require.NotNil(t, result)
	return result
}

func toolNames(manifest map[string]any) []string {
	tools, _ := manifest["tools"].([]any)
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.(map[string]any)["name"].(string))
	}
	return names
}

func TestCapabilityGateAndMainOnlyRecursionGuards(t *testing.T) {
	t.Setenv(MailboxEnv, "")
	f := newFixture(t)
	peer := newConnectionPeer(f.app)
	defer peer.conn.Close(nil)
	enabled := peer.initialize(t, 1, map[string]any{"runtime": map[string]any{"backgroundTasks": true}})
	require.Equal(t, AgentToolNames, toolNames(enabled))
	tools := map[string]map[string]any{}
	for _, tool := range enabled["tools"].([]any) {
		tools[tool.(map[string]any)["name"].(string)] = tool.(map[string]any)
	}
	agentOutput := toolSchemas["spawn_agent"].output
	for _, name := range []string{"spawn_agent", "wait_agent", "followup_agent", "cancel_agent"} {
		require.Equal(t, agentOutput, tools[name]["outputSchema"])
	}
	listOutput := tools["list_agents"]["outputSchema"].(map[string]any)
	require.NotContains(t, listOutput, "$defs")
	require.Equal(t, agentOutput, listOutput["properties"].(map[string]any)["agents"].(map[string]any)["items"])
	require.ElementsMatch(t, []string{"agent_id", "timeout_ms"}, keys(tools["wait_agent"]["inputSchema"].(map[string]any)["properties"]))
	spawnInput := tools["spawn_agent"]["inputSchema"].(map[string]any)
	require.ElementsMatch(t, []any{"name", "task"}, spawnInput["required"])
	require.ElementsMatch(t, []string{"name", "task", "cwd", "context_mode"}, keys(spawnInput["properties"]))
	require.Equal(t, []any{"agent.init", "session.start", "agent.start", "session.end"}, events(enabled))

	for index, capabilities := range []map[string]any{
		{"runtime": map[string]any{"backgroundTasks": false}},
		{"runtime": map[string]any{}},
		{},
	} {
		require.Empty(t, toolNames(peer.initialize(t, index+2, capabilities)))
	}

	t.Setenv(RecursionGuardEnv, "1")
	child := newConnectionPeer(NewApplication(f.newRuntime("child-runtime")))
	defer child.conn.Close(nil)
	childManifest := child.initialize(t, 1, map[string]any{"runtime": map[string]any{"backgroundTasks": true}})
	require.Empty(t, toolNames(childManifest))
	require.Equal(t, events(enabled), events(childManifest))
}

func TestInitializeLetsTheRuntimeAcceptWorkAfterShutdown(t *testing.T) {
	f := newFixture(t)
	require.NoError(t, f.runtime.Shutdown(f.ctx))
	require.ErrorIs(t, f.runtime.ensureAccepting(), errShuttingDown)
	peer := newConnectionPeer(f.app)
	defer peer.conn.Close(nil)
	peer.initialize(t, 1, map[string]any{"runtime": map[string]any{"backgroundTasks": true}})
	require.NoError(t, f.runtime.ensureAccepting())
}

func keys(value any) []string {
	record, _ := value.(map[string]any)
	names := make([]string, 0, len(record))
	for name := range record {
		names = append(names, name)
	}
	return names
}

func events(manifest map[string]any) []any {
	subscriptions, _ := manifest["subscriptions"].([]any)
	names := make([]any, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		names = append(names, subscription.(map[string]any)["event"])
	}
	return names
}

func TestFreshRunsAndFollowupsDisableOnlySubagentControlsWithInlineHook(t *testing.T) {
	f := newFixture(t)
	call := f.call("owner-1", invokedBy("main"), withProfile("parent-profile"))
	require.False(t, isAgentChild(call))
	id := agentID(t, f.spawn(call, SpawnAgentInput{Name: "fresh-guard", Task: "inspect", ContextMode: types.ContextFresh}))
	conversationID := ""
	for generation := 1; generation <= 2; generation++ {
		if generation == 2 {
			call.setProfile("changed-parent-profile")
			t.Setenv(ProfileEnv, "changed-configured-profile")
			require.Empty(t, f.followup(call, id, "continue").Error)
		}
		completed := f.wait(call, id, 1000)
		require.Equal(t, "completed", str(completed, "status"))
		require.Equal(t, generation, completed.Data["generation"])
		options := f.acp.client(t, generation-1).options(t)
		require.Equal(t, conversationID, options.Resume)
		if generation == 1 {
			require.Equal(t, "owner-1", options.ParentConversationID)
			require.Equal(t, "parent-profile", options.Profile)
		} else {
			require.Empty(t, options.ParentConversationID)
			require.Empty(t, options.Profile)
		}
		conversationID = str(completed, "conversation_id")

		require.Len(t, options.Extensions, 1)
		h := extensiontest.New(options.Extensions[0], nil)
		manifest, err := h.Initialize(f.ctx, nil)
		require.NoError(t, err)
		require.Empty(t, manifest.Tools)
		require.Empty(t, manifest.Commands)
		require.Len(t, manifest.Subscriptions, 1)
		require.Equal(t, "agent.init", manifest.Subscriptions[0].Event)
		result, err := h.HandleEvent(f.ctx, "agent.init", map[string]any{"systemPrompt": "Preserve runner instructions"},
			extensiontest.WithCall(extension.CallInfo{InvokedBy: "main", CWD: f.cwd}))
		require.NoError(t, err)
		require.Equal(t, AgentToolNames, result.Tools.Disable)
		require.Empty(t, result.Tools.Enable)
		catalog := append(slices.Clone(AgentToolNames), "code_search", "file_read", "custom_tool")
		remaining := slices.DeleteFunc(catalog, func(name string) bool { return slices.Contains(result.Tools.Disable, name) })
		require.Equal(t, []string{"code_search", "file_read", "custom_tool"}, remaining)
		h.Close()
	}
	require.Empty(t, call.forks())
}

func TestServeSpeaksFramedJSONRPCAndShutsDownOnEOF(t *testing.T) {
	t.Setenv(MailboxEnv, "")
	f := newFixture(t)
	extensionReader, hostWriter := io.Pipe()
	hostReader, extensionWriter := io.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- f.app.Serve(f.ctx, extensionReader, extensionWriter)
		_ = extensionWriter.Close()
	}()

	request, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 7, "method": "extension.initialize",
		"params": map[string]any{"protocolVersion": extension.ProtocolVersion, "extension": map[string]any{"id": "subagent"}},
	})
	require.NoError(t, err)
	go func() {
		_ = (&frameWriter{writer: hostWriter}).write(request)
	}()
	payload, err := newFrameReader(hostReader).read()
	require.NoError(t, err)
	var response struct {
		ID     int            `json:"id"`
		Result map[string]any `json:"result"`
	}
	require.NoError(t, json.Unmarshal(payload, &response))
	require.Equal(t, 7, response.ID)
	require.Equal(t, "subagent", response.Result["name"])
	require.Empty(t, toolNames(response.Result))

	require.NoError(t, hostWriter.Close())
	go func() { _, _ = io.Copy(io.Discard, hostReader) }()
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(eventTimeout):
		t.Fatal("Serve did not return on EOF")
	}
	require.ErrorIs(t, f.runtime.ensureAccepting(), errShuttingDown)
}

func TestFrameReaderAcceptsLFHeadersAndRejectsMissingLength(t *testing.T) {
	frames := "Content-Length: 2\n\n{}" + "Content-Type: x\r\n\r\n{}"
	reader := newFrameReader(bytes.NewBufferString(frames))
	payload, err := reader.read()
	require.NoError(t, err)
	require.Equal(t, "{}", string(payload))
	_, err = reader.read()
	require.ErrorContains(t, err, "missing Content-Length")
}
