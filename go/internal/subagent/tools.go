package subagent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jingkaihe/kodelet/sdk/go/extension"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func conversationDetail(agent types.Agent, suffix string) string {
	if agent.ConversationID != nil {
		return " in conversation " + *agent.ConversationID + suffix
	}
	return " with fresh context" + suffix
}

// SpawnAgent starts a named agent in the background.
func (a *Application) SpawnAgent(ctx context.Context, input SpawnAgentInput, call ToolCall) extension.ToolResult {
	summary := "Spawn agent"
	if isAgentChild(call) {
		return childToolError(summary)
	}
	name, err := types.ValidateAgentName(input.Name)
	if err != nil {
		return toolError(err.Error(), summary)
	}
	summary = "Spawn " + name
	task := strings.TrimSpace(input.Task)
	if task == "" {
		return toolError("task is required and must be a non-empty string", summary)
	}
	claim, live, err := a.spawn(ctx, input, name, task, call)
	if err != nil {
		return toolError("spawn_agent failed: "+err.Error(), summary)
	}
	snapshot := publicSnapshot(claim.Agent, false)
	snapshot["presentation"] = presentation(summary, task)
	return toolResult(fmt.Sprintf(
		"Spawned agent %s (%s)%s. Continue with independent work and call wait_agent before relying on its result.",
		quoted(claim.Agent.Name), live.agentID, conversationDetail(claim.Agent, ""),
	), snapshot, "")
}

func (a *Application) spawn(ctx context.Context, input SpawnAgentInput, name, task string, call ToolCall) (types.Claim, *LiveRun, error) {
	owner, err := ownerConversationID(call)
	if err != nil {
		return types.Claim{}, nil, err
	}
	cwd, err := resolveAgentCWD(input.CWD, call.Info().CWD, input.ContextMode)
	if err != nil {
		return types.Claim{}, nil, err
	}
	store, err := a.runtime.storeForCall(ctx, call)
	if err != nil {
		return types.Claim{}, nil, err
	}
	claim, err := a.runtime.reserveClaim(ctx, func(ctx context.Context) (types.Claim, error) {
		return store.Create(ctx, owner, name, task, cwd, input.ContextMode)
	}, task, store, true)
	if err != nil {
		return types.Claim{}, nil, err
	}
	claim, live, err := a.runtime.prepareClaim(ctx, claim, task, store, call, true)
	if err != nil {
		return types.Claim{}, nil, err
	}
	a.runtime.safeSyncAgentWidget(ctx, call.UI(), store, owner)
	return claim, live, nil
}

// FollowupAgent gives an idle, failed, interrupted, or canceled agent a new task.
func (a *Application) FollowupAgent(ctx context.Context, input FollowupAgentInput, call ToolCall) extension.ToolResult {
	summary := "Follow up agent"
	if isAgentChild(call) {
		return childToolError(summary)
	}
	task := strings.TrimSpace(input.Task)
	if task == "" {
		return toolError("task is required and must be a non-empty string", summary)
	}
	claim, live, err := a.followup(ctx, strings.TrimSpace(input.AgentID), task, call, &summary)
	if errors.Is(err, db.ErrAgentNotFound) {
		return agentNotFound(summary)
	}
	if err != nil {
		return toolError("followup_agent failed: "+err.Error(), summary)
	}
	snapshot := publicSnapshot(claim.Agent, false)
	snapshot["presentation"] = presentation(summary, task)
	return toolResult(fmt.Sprintf(
		"Started follow-up run %s for agent %s (%s)%s\n\nFollow-up task sent:\n%s\n\nCall wait_agent before relying on its result.",
		live.runID, quoted(claim.Agent.Name), live.agentID, conversationDetail(claim.Agent, "."), task,
	), snapshot, "")
}

func (a *Application) followup(ctx context.Context, agentID, task string, call ToolCall, summary *string) (types.Claim, *LiveRun, error) {
	owner, err := ownerConversationID(call)
	if err != nil {
		return types.Claim{}, nil, err
	}
	store, err := a.runtime.storeForCall(ctx, call)
	if err != nil {
		return types.Claim{}, nil, err
	}
	agent, err := store.Get(ctx, owner, agentID, "")
	if err != nil {
		return types.Claim{}, nil, err
	}
	*summary = "Follow up " + agent.Name
	if agent.Status == types.AgentCanceling {
		return types.Claim{}, nil, errors.New("agent cancellation is still in progress; retry followup_agent shortly")
	}
	claim, err := a.runtime.reserveClaim(ctx, func(ctx context.Context) (types.Claim, error) {
		return store.Claim(ctx, owner, agentID, task)
	}, task, store, false)
	if err != nil {
		return types.Claim{}, nil, err
	}
	claim, live, err := a.runtime.prepareClaim(ctx, claim, task, store, call, false)
	if err != nil {
		return types.Claim{}, nil, err
	}
	a.runtime.safeSyncAgentWidget(ctx, call.UI(), store, owner)
	return claim, live, nil
}

// ListAgents lists the conversation's agents.
func (a *Application) ListAgents(ctx context.Context, _ ListAgentsInput, call ToolCall) extension.ToolResult {
	summary := "List agents"
	if isAgentChild(call) {
		return childToolError(summary)
	}
	agents, err := a.listAgents(ctx, call)
	if err != nil {
		return toolError("list_agents failed: "+err.Error(), summary)
	}
	snapshots := make([]map[string]any, 0, len(agents))
	lines := []string{"Background agents:", "Name\tAgent ID\tConversation ID\tAgent state\tRun state\tTask"}
	for _, agent := range agents {
		snapshots = append(snapshots, publicSnapshot(agent, false))
		conversation := "pending"
		if agent.ConversationID != nil {
			conversation = *agent.ConversationID
		}
		lines = append(lines, strings.Join([]string{
			agent.Name, agent.ID, conversation, string(agent.Status), string(agent.Run.Status),
			truncate(collapseWhitespace(agent.Run.Task), listTaskPreviewLength),
		}, "\t"))
	}
	data := map[string]any{"agents": snapshots, "presentation": agentListPresentation(agents)}
	if len(agents) == 0 {
		return toolResult("No background agents have been spawned by this conversation.", data, "")
	}
	return toolResult(strings.Join(lines, "\n"), data, "")
}

func (a *Application) listAgents(ctx context.Context, call ToolCall) ([]types.Agent, error) {
	owner, err := ownerConversationID(call)
	if err != nil {
		return nil, err
	}
	store, err := a.runtime.storeForCall(ctx, call)
	if err != nil {
		return nil, err
	}
	agents, err := store.List(ctx, owner)
	if err != nil {
		return nil, err
	}
	a.runtime.safeSyncAgentWidget(ctx, call.UI(), store, owner)
	return agents, nil
}

// SteerAgent queues guidance for a running agent.
func (a *Application) SteerAgent(ctx context.Context, input SteerAgentInput, call ToolCall) extension.ToolResult {
	summary := "Steer agent"
	if isAgentChild(call) {
		return childToolError(summary)
	}
	agentID := strings.TrimSpace(input.AgentID)
	message := strings.TrimSpace(input.Message)
	agent, result, err := a.steer(ctx, agentID, message, call, &summary)
	if errors.Is(err, db.ErrAgentNotFound) {
		return agentNotFound(summary)
	}
	if err != nil {
		return toolError("steer_agent failed: "+err.Error(), summary)
	}
	queued := ""
	if result.AlreadyPending {
		queued = " behind pending steering"
	}
	return toolResult(fmt.Sprintf(
		"Queued steering for agent %s (%s)%s.\n\nSteering message queued for delivery:\n%s",
		quoted(agent.Name), agent.ID, queued, message,
	), map[string]any{
		"agent_id":       agent.ID,
		"name":           agent.Name,
		"message":        message,
		"presentation":   presentation(summary, message),
		"accepted":       result.Accepted,
		"alreadyPending": result.AlreadyPending,
	}, "")
}

func (a *Application) steer(ctx context.Context, agentID, message string, call ToolCall, summary *string) (types.Agent, types.EnqueueSteeringResult, error) {
	owner, err := ownerConversationID(call)
	if err != nil {
		return types.Agent{}, types.EnqueueSteeringResult{}, err
	}
	store, err := a.runtime.storeForCall(ctx, call)
	if err != nil {
		return types.Agent{}, types.EnqueueSteeringResult{}, err
	}
	agent, err := store.Get(ctx, owner, agentID, "")
	if err != nil {
		return types.Agent{}, types.EnqueueSteeringResult{}, err
	}
	*summary = "Steer " + agent.Name
	result, err := store.EnqueueSteering(ctx, owner, agentID, message)
	return agent, result, err
}

// CancelAgent cancels an agent; it can be resumed later with followup_agent.
func (a *Application) CancelAgent(ctx context.Context, input CancelAgentInput, call ToolCall) extension.ToolResult {
	summary := "Cancel agent"
	if isAgentChild(call) {
		return childToolError(summary)
	}
	agent, cleanupComplete, err := a.cancel(ctx, strings.TrimSpace(input.AgentID), call, &summary)
	if errors.Is(err, db.ErrAgentNotFound) {
		return agentNotFound(summary)
	}
	if err != nil {
		return toolError("cancel_agent failed: "+err.Error(), summary)
	}
	return cancelResult(agent.current, agent.canceledRunID, cleanupComplete, summary)
}

type canceledAgent struct {
	current       types.Agent
	canceledRunID string
}

func (a *Application) cancel(ctx context.Context, agentID string, call ToolCall, summary *string) (canceledAgent, bool, error) {
	owner, err := ownerConversationID(call)
	if err != nil {
		return canceledAgent{}, false, err
	}
	store, err := a.runtime.storeForCall(ctx, call)
	if err != nil {
		return canceledAgent{}, false, err
	}
	agent, err := store.Cancel(ctx, owner, agentID)
	if err != nil {
		return canceledAgent{}, false, err
	}
	*summary = "Cancel " + agent.Name
	a.runtime.safeSyncAgentWidget(ctx, call.UI(), store, owner)
	canceledRunID := agent.Run.ID
	cleanupComplete := true
	if agent.Status == types.AgentCanceling {
		cleanupComplete = a.runtime.cancelLiveRun(ctx, store, agent.ID, canceledRunID, a.runtime.timing.CancelCleanupTimeout)
	}
	current, err := store.Get(ctx, owner, agent.ID, "")
	if err != nil {
		return canceledAgent{}, false, err
	}
	return canceledAgent{current: current, canceledRunID: canceledRunID}, cleanupComplete, nil
}

func cancelResult(agent types.Agent, canceledRunID string, cleanupComplete bool, summary string) extension.ToolResult {
	snapshot := publicSnapshot(agent, false)
	name := quoted(agent.Name)
	var content, body string
	switch {
	case agent.Run.ID != canceledRunID:
		content = fmt.Sprintf("Recorded cancellation for agent %s; the agent has since changed generation.", name)
		body = "Cancellation was recorded; the agent has since changed generation."
	case agent.Status == types.AgentCanceling:
		content = fmt.Sprintf("Cancellation persisted for agent %s; worker cleanup is still finishing. "+
			"Retry followup_agent after cleanup completes.", name)
		body = "Cancellation saved; worker cleanup is still finishing. Retry followup_agent after cleanup completes."
	case !cleanupComplete:
		content = fmt.Sprintf("Canceled agent %s; it can now be resumed with followup_agent while "+
			"remaining local resource cleanup finishes.", name)
		body = "Canceled and resumable. Remaining local resource cleanup is still finishing."
	default:
		content = fmt.Sprintf("Canceled agent %s. Use followup_agent to resume it later.", name)
		body = "Canceled. Use followup_agent to resume this agent later."
	}
	snapshot["presentation"] = presentation(summary, body)
	return toolResult(content, snapshot, "")
}
