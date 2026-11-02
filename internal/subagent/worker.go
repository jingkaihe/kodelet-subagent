package subagent

import (
	"context"
	"errors"
	"io/fs"
	"os/exec"
	"strings"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/agent"
	"github.com/jingkaihe/kodelet/sdk/go/extension"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// errTimedOut marks deadlines imposed by the runtime rather than the caller.
var errTimedOut = errors.New("timed out")

// freshAgentRecursionGuard disables subagent controls on the runner. Fresh
// conversations lack a persisted extension-fork initiator, and the ACP client
// environment does not reach the runner, so it is attached on every resume.
func freshAgentRecursionGuard() *extension.Extension {
	guard := extension.New(extension.Metadata{Name: "subagent-recursion-guard"})
	extension.On(guard, extension.EventAgentInit, func(context.Context, extension.AgentInitEvent, *extension.EventContext) (extension.EventResult, error) {
		return disabledAgentTools(), nil
	})
	return guard
}

func disabledAgentTools() extension.EventResult {
	return extension.EventResult{Tools: &extension.ToolListPatch{Disable: append([]string(nil), AgentToolNames...)}}
}

func agentFailureMessage(phase string, err error) string {
	var execErr *exec.Error
	var pathErr *fs.PathError
	switch {
	case errors.Is(err, errTimedOut) && phase == "starting":
		return "agent timed out while starting the kodelet session"
	case errors.Is(err, errTimedOut):
		return "agent timed out while waiting for kodelet to finish"
	case errors.As(err, &execErr), errors.As(err, &pathErr):
		return "Failed to execute kodelet: " + err.Error()
	default:
		return "agent failed: " + err.Error()
	}
}

// withTimeout runs fn under a deadline, reporting errTimedOut when the
// deadline, not the parent context, ended it.
func withTimeout[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	bounded, cancel := context.WithTimeoutCause(ctx, timeout, errTimedOut)
	defer cancel()
	value, err := fn(bounded)
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(bounded), errTimedOut) {
		return value, errTimedOut
	}
	return value, err
}

// runAgentJob runs one claimed generation in a child ACP session.
func (r *Runtime) runAgentJob(ctx context.Context, live *LiveRun, heartbeat *task) {
	if heartbeat == nil {
		heartbeat = r.startAgentHeartbeat(live)
	}
	var client AgentClient
	var steering *task
	var failure *workerFailure
	phase := "starting"
	err := func() error {
		if message := live.HeartbeatError(); message != "" {
			return errors.New(message)
		}
		client = r.clientFactory("kodelet", live.cwd, map[string]string{RecursionGuardEnv: "1"})
		live.setClient(client)
		session, err := r.createAgentSession(ctx, live, client)
		if err != nil {
			return err
		}
		phase = "running"
		steering = goTask(func(steeringCtx context.Context) { r.steeringPump(steeringCtx, live, session) })
		response, err := withTimeout(ctx, r.timing.AgentTimeout, func(runCtx context.Context) (*agent.Response, error) {
			return session.RunAndWait(runCtx, agent.RunOptions{Message: live.task})
		})
		if err != nil {
			return err
		}
		if err := context.Cause(ctx); ctx.Err() != nil {
			return err
		}
		stopTask(steering)
		steering = nil
		failure, err = r.completeAgentRun(ctx, live, response)
		return err
	}()
	if err != nil {
		stopTask(steering)
		steering = nil
		switch {
		case ctx.Err() == nil:
			live.setTerminalizing()
			failure = &workerFailure{status: types.TerminalFailed, message: agentFailureMessage(phase, err)}
		case !live.ParentCanceled():
			live.setTerminalizing()
			message := live.HeartbeatError()
			if message == "" {
				message = stoppedSessionMessage
			}
			failure = &workerFailure{status: types.TerminalInterrupted, message: message}
		}
	}
	cleanup := r.startLiveRunCleanup(live, client, steering, heartbeat, failure)
	<-cleanup.done
}

func (r *Runtime) createAgentSession(ctx context.Context, live *LiveRun, client AgentClient) (AgentSession, error) {
	conversationID := live.ConversationID()
	options := agent.SessionOptions{CWD: live.cwd}
	switch {
	case conversationID != "":
		options.Resume = conversationID
	case live.contextMode == types.ContextFresh:
		options.ParentConversationID = live.ownerConversationID
		options.Profile = live.Profile()
	}
	if live.contextMode == types.ContextFresh {
		options.Extensions = []*extension.Extension{freshAgentRecursionGuard()}
	}
	session, err := withTimeout(ctx, r.timing.AgentStartTimeout, func(startCtx context.Context) (AgentSession, error) {
		return client.CreateSession(startCtx, options)
	})
	if err != nil {
		return nil, err
	}
	live.setSession(session)
	// Attach before prompting, not when a potentially much later wait starts.
	live.progress.Attach(session)
	live.setConversationID(session.ID())
	if conversationID == "" {
		if _, err := live.store.AttachConversation(ctx, live.lease, session.ID()); err != nil {
			return nil, err
		}
	}
	if _, err := live.store.MarkRunning(ctx, live.lease, session.ID()); err != nil {
		return nil, err
	}
	r.safeSyncAgentWidget(ctx, live.UI(), live.store, live.ownerConversationID)
	return session, nil
}

// completeAgentRun records a finished turn. A canceled turn keeps the fenced
// claim until ACP cleanup finishes, and partial output never turns
// cancellation into success.
func (r *Runtime) completeAgentRun(ctx context.Context, live *LiveRun, response *agent.Response) (*workerFailure, error) {
	result := strings.TrimSpace(response.Content)
	live.setTerminalizing()
	if response.StopReason == "cancelled" {
		failure := &workerFailure{status: types.TerminalInterrupted, message: "agent canceled by kodelet"}
		if result != "" {
			failure.result = &result
		}
		return failure, nil
	}
	if result != "" {
		_, err := r.safeWorkerTerminal(ctx, live, types.TerminalIdle, &result, nil)
		return nil, err
	}
	message := "agent returned an empty response"
	_, err := r.safeWorkerTerminal(ctx, live, types.TerminalFailed, nil, &message)
	return nil, err
}
