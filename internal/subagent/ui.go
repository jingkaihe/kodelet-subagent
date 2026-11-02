package subagent

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/jingkaihe/kodelet/sdk/go/taskprogress"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// Widget settings.
const (
	WidgetID         = "background-agents"
	WidgetAgentLimit = 8
)

type discardPublisher struct{}

func (discardPublisher) Update(context.Context, string, map[string]any) error { return nil }

// newAgentTaskProgress keeps bounded activity history for a run independently
// of its waiters, which forward snapshots only while their own call is alive.
func newAgentTaskProgress(agent types.Agent) *taskprogress.Progress {
	title := "Wait for " + agent.Name
	return taskprogress.New(discardPublisher{}, taskprogress.Options{
		Kind:             "subagent",
		Task:             agent.Run.Task,
		CWD:              agent.CWD,
		RunningTitle:     title,
		CompletedTitle:   title,
		FailedTitle:      title,
		RespondingDetail: "agent is responding",
	})
}

// publishTaskProgress forwards a snapshot to a live wait call.
func publishTaskProgress(ctx context.Context, call ToolCall, snapshot taskprogress.Snapshot) {
	content := snapshot.Title
	if snapshot.Detail != "" {
		content += " - " + snapshot.Detail
	}
	if err := call.Update(ctx, content, map[string]any{"taskRun": snapshot}); err != nil && ctx.Err() == nil {
		call.Logger().Warn("failed to publish tool update", "error", err.Error())
	}
}

// timestamp renders epoch seconds as the public UTC representation, with
// microseconds only when they are non-zero.
func timestamp(value float64) string {
	seconds, fraction := math.Modf(value)
	micros := math.RoundToEven(fraction * 1e6)
	if micros >= 1e6 {
		seconds++
		micros -= 1e6
	}
	at := time.Unix(int64(seconds), int64(micros)*int64(time.Microsecond)).UTC()
	if micros == 0 {
		return at.Format("2006-01-02T15:04:05") + "Z"
	}
	return at.Format("2006-01-02T15:04:05.000000") + "Z"
}

func optionalTimestamp(value *float64) any {
	if value == nil {
		return nil
	}
	return timestamp(*value)
}

func optionalString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

// publicSnapshot is the stable assistant-facing representation of a run.
func publicSnapshot(agent types.Agent, includeResult bool) map[string]any {
	snapshot := map[string]any{
		"agent_id":        agent.ID,
		"name":            agent.Name,
		"run_id":          agent.Run.ID,
		"generation":      agent.Run.Generation,
		"conversation_id": optionalString(agent.ConversationID),
		"status":          string(agent.Run.Status),
		"agent_status":    string(agent.Status),
		"task":            agent.Run.Task,
		"cwd":             agent.CWD,
		"context_mode":    string(agent.ContextMode),
		"created_at":      timestamp(agent.Run.CreatedAt),
		"started_at":      optionalTimestamp(agent.Run.StartedAt),
		"completed_at":    optionalTimestamp(agent.Run.CompletedAt),
		"updated_at":      timestamp(agent.Run.UpdatedAt),
	}
	if agent.Run.Error != nil && *agent.Run.Error != "" {
		snapshot["error"] = *agent.Run.Error
	}
	if includeResult && agent.Run.Result != nil {
		key := "partial_result"
		if agent.Run.Status == types.RunCompleted {
			key = "result"
		}
		snapshot[key] = *agent.Run.Result
	}
	return snapshot
}

// displayStatus reports canceling agents by their agent state.
func displayStatus(agent types.Agent) string {
	if agent.Status == types.AgentCanceling {
		return string(types.AgentCanceling)
	}
	return string(agent.Run.Status)
}

var widgetIcons = map[string]string{
	"starting":    "◌",
	"running":     "●",
	"canceling":   "×",
	"completed":   "✓",
	"failed":      "!",
	"interrupted": "!",
	"canceled":    "×",
}

var (
	boldStyle = &extension.Style{Bold: true}
	dimStyle  = &extension.Style{Dim: true}
)

func agentWidgetLine(agent types.Agent) extension.FrameLine {
	name := agent.Name
	if runes := []rune(name); len(runes) > 88 {
		name = string(runes[:85]) + "..."
	}
	status := displayStatus(agent)
	iconStyle := dimStyle
	if types.RunStatus(status).Active() {
		iconStyle = boldStyle
	}
	statusStyle := dimStyle
	if status == string(types.RunFailed) || status == string(types.RunInterrupted) {
		statusStyle = boldStyle
	}
	id := agent.ID
	if len(id) > 8 {
		id = id[len(id)-8:]
	}
	return extension.Styled(
		extension.StyledSpan{Text: widgetIcons[status] + " ", Style: iconStyle},
		extension.StyledSpan{Text: name},
		extension.StyledSpan{Text: "  " + status, Style: statusStyle},
		extension.StyledSpan{Text: "  " + id, Style: dimStyle},
	)
}

// agentWidgetLines renders the complete background-agent widget.
func agentWidgetLines(agents []types.Agent) []extension.FrameLine {
	var active, canceling, completed, attention, canceled int
	for _, agent := range agents {
		switch {
		case agent.Run.Status.Active():
			active++
		case agent.Run.Status == types.RunCompleted:
			completed++
		case agent.Run.Status == types.RunFailed || agent.Run.Status == types.RunInterrupted:
			attention++
		case agent.Run.Status == types.RunCanceled && agent.Status != types.AgentCanceling:
			canceled++
		}
		if agent.Status == types.AgentCanceling {
			canceling++
		}
	}
	summary := []string{fmt.Sprintf("%d active", active), fmt.Sprintf("%d completed", completed)}
	if canceling > 0 {
		summary = append(summary, fmt.Sprintf("%d canceling", canceling))
	}
	if attention > 0 {
		summary = append(summary, fmt.Sprintf("%d need attention", attention))
	}
	if canceled > 0 {
		summary = append(summary, fmt.Sprintf("%d canceled", canceled))
	}
	lines := []extension.FrameLine{extension.Styled(
		extension.StyledSpan{Text: "Background agents", Style: boldStyle},
		extension.StyledSpan{Text: "  " + strings.Join(summary, " · "), Style: dimStyle},
	)}
	for i, agent := range agents {
		if i == WidgetAgentLimit {
			break
		}
		lines = append(lines, agentWidgetLine(agent))
	}
	if len(agents) > WidgetAgentLimit {
		lines = append(lines, extension.Styled(extension.StyledSpan{
			Text:  fmt.Sprintf("… and %d more", len(agents)-WidgetAgentLimit),
			Style: dimStyle,
		}))
	}
	return lines
}
