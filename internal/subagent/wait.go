package subagent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/jingkaihe/kodelet/sdk/go/taskprogress"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// WaitAgent waits for an agent's current run, streaming its progress.
func (a *Application) WaitAgent(ctx context.Context, input WaitAgentInput, call ToolCall) extension.ToolResult {
	summary := "Wait for agent"
	if isAgentChild(call) {
		return childToolError(summary)
	}
	agent, store, progress, err := a.wait(ctx, strings.TrimSpace(input.AgentID), input.TimeoutMS, call, &summary)
	if errors.Is(err, db.ErrAgentNotFound) {
		return agentNotFound(summary)
	}
	if err != nil {
		return toolError("wait_agent failed: "+err.Error(), summary)
	}
	snapshot := publicSnapshot(agent, !agent.Run.Status.Active())
	snapshot["presentation"] = presentation(summary)
	if progress != nil {
		snapshot["taskRun"] = finishWaitProgress(ctx, progress, agent)
	}
	// Cancellation publishes its notice only after cleanup. A wait that still
	// reports canceling has not delivered the final outcome, so leave that notice
	// for a later wait. A historical run is independent of the current generation.
	canceling := agent.Status == types.AgentCanceling && agent.Run.Generation == agent.Generation
	if a.runtime.mailboxEnabled && !agent.Run.Status.Active() && !canceling {
		if err := store.AckTerminalMail(ctx, agent.OwnerConversationID, agent.ID, agent.Run.ID); err != nil {
			call.Logger().Warn("failed to acknowledge agent completion mail", "error", err.Error())
		}
	}
	a.runtime.safeSyncAgentWidget(ctx, call.UI(), store, agent.OwnerConversationID)
	return waitResult(agent, snapshot)
}

func (a *Application) wait(ctx context.Context, agentID string, timeoutMS int, call ToolCall, summary *string) (types.Agent, Store, *taskprogress.Progress, error) {
	owner, err := ownerConversationID(call)
	if err != nil {
		return types.Agent{}, nil, nil, err
	}
	store, err := a.runtime.storeForCall(ctx, call)
	if err != nil {
		return types.Agent{}, nil, nil, err
	}
	agent, err := store.Get(ctx, owner, agentID, "")
	if err != nil {
		return types.Agent{}, nil, nil, err
	}
	*summary = "Wait for " + agent.Name
	if !agent.Run.Status.Active() || timeoutMS <= 0 {
		return agent, store, nil, nil
	}
	agent, progress, err := a.waitForActiveRun(ctx, call, store, owner, agent, time.Duration(timeoutMS)*time.Millisecond)
	return agent, store, progress, err
}

// waitForActiveRun polls the selected run until it ends or the timeout
// expires. It forwards the run's own progress once this process owns it; for
// a run owned elsewhere, or still entering ownership, a snapshot-only
// fallback is used.
func (a *Application) waitForActiveRun(ctx context.Context, call ToolCall, store Store, owner string, agent types.Agent, timeout time.Duration) (types.Agent, *taskprogress.Progress, error) {
	progress := newAgentTaskProgress(agent)
	runID := agent.Run.ID
	deadline := time.Now().Add(timeout)
	tracking := false
	revision := -1
	for agent.Run.Status.Active() {
		if !tracking {
			// Hold this exact generation even after cleanup or a follow-up.
			if live := a.runtime.ownedRun(store, runID); live != nil {
				progress = live.progress
				tracking = true
			}
		}
		snapshot := progress.Snapshot()
		if snapshot.Revision != revision {
			publishTaskProgress(ctx, call, snapshot)
			revision = snapshot.Revision
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if err := sleep(ctx, min(a.waitPoll, remaining)); err != nil {
			return agent, progress, err
		}
		var err error
		if agent, err = store.Get(ctx, owner, agent.ID, runID); err != nil {
			return agent, progress, err
		}
	}
	return agent, progress, nil
}

func finishWaitProgress(ctx context.Context, progress *taskprogress.Progress, agent types.Agent) taskprogress.Snapshot {
	taskRun := progress.Snapshot()
	if agent.Run.Status.Active() || taskRun.Status != "running" {
		return taskRun
	}
	success := agent.Run.Status == types.RunCompleted
	message := ""
	if !success {
		message = errorOr(agent.Run.Error, "agent "+string(agent.Run.Status))
	}
	return progress.Finish(ctx, success, message)
}

func errorOr(value *string, fallback string) string {
	if value == nil || *value == "" {
		return fallback
	}
	return *value
}

func waitResult(agent types.Agent, snapshot map[string]any) extension.ToolResult {
	name := quoted(agent.Name)
	result := ""
	if agent.Run.Result != nil {
		result = *agent.Run.Result
	}
	switch {
	case agent.Run.Status.Active():
		return toolResult(fmt.Sprintf("Agent %s is still %s.", name, agent.Run.Status), snapshot, "")
	case agent.Run.Status == types.RunCompleted:
		return toolResult(result, snapshot, "")
	case agent.Run.Status == types.RunFailed:
		message := fmt.Sprintf("Agent %s failed: %s", name, errorOr(agent.Run.Error, "unknown error"))
		return toolResult(message, snapshot, message)
	case agent.Run.Status == types.RunInterrupted:
		message := fmt.Sprintf("Agent %s was interrupted: %s. Use followup_agent to resume it.",
			name, errorOr(agent.Run.Error, "worker stopped"))
		content := message
		if result != "" {
			content += "\n\nPartial output (agent did not complete):\n" + result
		}
		return toolResult(content, snapshot, message)
	case agent.Status == types.AgentCanceling && agent.Run.Generation == agent.Generation:
		return toolResult(fmt.Sprintf("Agent %s is still canceling. Retry followup_agent after cancellation finishes.", name), snapshot, "")
	default:
		return toolResult(fmt.Sprintf("Agent %s was canceled. Use followup_agent to resume it.", name), snapshot, "")
	}
}
