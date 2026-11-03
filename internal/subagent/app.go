package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"

	"github.com/jingkaihe/kodelet-subagent/internal/version"
)

// Tool descriptions shown to the model.
const (
	spawnAgentDescription = `Start a named Kodelet agent in the background and return immediately. Names
must contain one to three lowercase kebab-case words and be unique. By default,
the agent inherits the current conversation; set context_mode to fresh to start
without it. Fork mode requires cwd to match the current working directory.
Use followup_agent to wake an idle, failed, interrupted, or canceled agent.`

	waitAgentDescription = `Wait for a background agent's current run. Returns its final response when the
run finishes or its current status when the timeout expires. Set timeout_ms to
0 to check without waiting.`

	listAgentsDescription = `List background agents with their current status and latest run.`

	followupAgentDescription = `Give an idle, failed, interrupted, or canceled background agent a new task and
return immediately. The agent continues its existing conversation when
available.`

	steerAgentDescription = `Send guidance to a background agent that is currently running. The guidance is
delivered before its next model response, or kept for a later follow-up if the
current run ends first.`

	cancelAgentDescription = `Cancel a background agent. An active agent remains in the canceling state until
it fully stops. Once canceled, it can be resumed with followup_agent.`
)

// Tool timeouts and wait bounds.
const (
	spawnToolTimeout    = 60 * time.Second
	followupToolTimeout = 60 * time.Second
	waitToolTimeout     = 5*time.Minute + 10*time.Second
	listToolTimeout     = 10 * time.Second
	steerToolTimeout    = 15 * time.Second
	cancelToolTimeout   = 15 * time.Second
	defaultWaitMS       = 30_000
	waitPollInterval    = 100 * time.Millisecond
)

// Application binds the extension protocol handlers to one runtime.
type Application struct {
	runtime   *Runtime
	extension *extension.Extension
	waitPoll  time.Duration
}

// NewApplication registers the extension against runtime, or a new default
// runtime when nil. Child agents may use mailboxes but not lifecycle controls.
func NewApplication(runtime *Runtime) *Application {
	if runtime == nil {
		runtime = NewRuntime(RuntimeOptions{})
	}
	app := &Application{
		runtime:   runtime,
		extension: extension.New(extension.Metadata{Name: "subagent", Version: version.Version}),
		waitPoll:  waitPollInterval,
	}
	app.registerEvents()
	if os.Getenv(RecursionGuardEnv) != "1" {
		app.registerTools()
	}
	if runtime.mailboxEnabled {
		app.registerMailboxTools()
		app.registerMailboxEvents()
	}
	return app
}

// Extension returns the registered extension.
func (a *Application) Extension() *extension.Extension { return a.extension }

// Runtime returns the application's runtime.
func (a *Application) Runtime() *Runtime { return a.runtime }

func (a *Application) registerEvents() {
	extension.On(a.extension, extension.EventAgentInit, func(ctx context.Context, event extension.AgentInitEvent, ec *extension.EventContext) (extension.EventResult, error) {
		call := newSDKEventCall(ec)
		result := a.disableRecursiveAgents(call)
		a.noteMailboxTools(event, call)
		a.restoreMailboxWidget(ctx, call)
		return result, nil
	})
	extension.On(a.extension, extension.EventSessionStart, func(ctx context.Context, _ extension.SessionStartEvent, ec *extension.EventContext) (extension.EventResult, error) {
		a.restoreAgentWidget(ctx, newSDKEventCall(ec))
		a.restoreMailboxWidget(ctx, newSDKEventCall(ec))
		return extension.EventResult{}, nil
	})
	extension.On(a.extension, extension.EventAgentStart, func(ctx context.Context, _ extension.AgentStartEvent, ec *extension.EventContext) (extension.EventResult, error) {
		a.resetMailboxNotices(newSDKEventCall(ec))
		a.restoreAgentWidget(ctx, newSDKEventCall(ec))
		a.restoreMailboxWidget(ctx, newSDKEventCall(ec))
		return extension.EventResult{}, nil
	})
	extension.On(a.extension, extension.EventSessionEnd, func(ctx context.Context, _ extension.SessionEndEvent, _ *extension.EventContext) (extension.EventResult, error) {
		return extension.EventResult{}, a.interruptLiveAgents(ctx)
	})
}

func (a *Application) registerTools() {
	registerTool(a, "spawn_agent", spawnAgentDescription, spawnToolTimeout,
		func() SpawnAgentInput { return SpawnAgentInput{ContextMode: "fork"} }, a.SpawnAgent)
	registerTool(a, "wait_agent", waitAgentDescription, waitToolTimeout,
		func() WaitAgentInput { return WaitAgentInput{TimeoutMS: defaultWaitMS} }, a.WaitAgent)
	registerTool(a, "list_agents", listAgentsDescription, listToolTimeout,
		func() ListAgentsInput { return ListAgentsInput{} }, a.ListAgents)
	registerTool(a, "followup_agent", followupAgentDescription, followupToolTimeout,
		func() FollowupAgentInput { return FollowupAgentInput{} }, a.FollowupAgent)
	registerTool(a, "steer_agent", steerAgentDescription, steerToolTimeout,
		func() SteerAgentInput { return SteerAgentInput{} }, a.SteerAgent)
	registerTool(a, "cancel_agent", cancelAgentDescription, cancelToolTimeout,
		func() CancelAgentInput { return CancelAgentInput{} }, a.CancelAgent)
}

// registerTool advertises the tool's exact schemas and validates input
// against them before decoding it into In.
func registerTool[In any](a *Application, name, description string, timeout time.Duration, defaults func() In, handle func(context.Context, In, ToolCall) extension.ToolResult) {
	input, output := toolSchemas[name].clone()
	extension.RegisterTool(a.extension, extension.Tool[json.RawMessage]{
		Name:         name,
		Description:  description,
		InputSchema:  input,
		OutputSchema: output,
		Timeout:      timeout,
		Execute: func(ctx context.Context, raw json.RawMessage, tc *extension.ToolContext) (extension.ToolResult, error) {
			decoded, err := decodeInput(name, raw, defaults())
			if err != nil {
				return extension.ToolResult{}, fmt.Errorf("invalid input for tool %s: %w", name, err)
			}
			call := newSDKToolCall(tc)
			result := handle(ctx, decoded, call)
			a.appendMailboxNotice(ctx, call, &result)
			return result, nil
		},
	})
}

// isAgentChild reports whether the call comes from a background agent, which
// must not control agents of its own.
func isAgentChild(call Call) bool {
	if os.Getenv(RecursionGuardEnv) == "1" {
		return true
	}
	invokedBy := strings.TrimSpace(call.Info().InvokedBy)
	return invokedBy != "" && invokedBy != "main"
}

func ownerConversationID(call Call) (string, error) {
	owner := strings.TrimSpace(call.Info().ConversationID)
	if owner == "" {
		return "", errors.New("async agents require an active conversation ID")
	}
	return owner, nil
}

func (a *Application) disableRecursiveAgents(call Call) extension.EventResult {
	if isAgentChild(call) {
		return disabledAgentTools()
	}
	return extension.EventResult{}
}

// restoreAgentWidget redraws the widget when a session or run starts.
func (a *Application) restoreAgentWidget(ctx context.Context, call Call) {
	if isAgentChild(call) {
		return
	}
	owner := strings.TrimSpace(call.Info().ConversationID)
	if owner == "" {
		return
	}
	store, err := a.runtime.storeForCall(ctx, call)
	if err == nil {
		err = a.runtime.syncAgentWidget(ctx, call.UI(), store, owner)
	}
	if err != nil {
		call.Logger().Warn("failed to restore background-agent widget: " + err.Error())
	}
}

// interruptLiveAgents shuts the runtime down when the session ends. If the
// host stops waiting, shutdown continues and the process awaits it on exit.
func (a *Application) interruptLiveAgents(ctx context.Context) error {
	if err := a.runtime.Shutdown(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}
