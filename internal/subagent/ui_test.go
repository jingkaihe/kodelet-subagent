package subagent

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func TestTimestampMatchesPythonISOFormat(t *testing.T) {
	// Expected values come from datetime.fromtimestamp(value, UTC).isoformat().
	cases := map[float64]string{
		0:                  "1970-01-01T00:00:00Z",
		1.5:                "1970-01-01T00:00:01.500000Z",
		1000:               "1970-01-01T00:16:40Z",
		1730000000.123456:  "2024-10-27T03:33:20.123456Z",
		1730000000.9999995: "2024-10-27T03:33:21Z",
		1000.0000004:       "1970-01-01T00:16:40Z",
		1762077600.25:      "2025-11-02T10:00:00.250000Z",
	}
	for value, expected := range cases {
		require.Equal(t, expected, timestamp(value), fmt.Sprint(value))
	}
}

func agentWith(name string, status types.AgentStatus, run types.RunStatus) types.Agent {
	return types.Agent{ID: "agt_0123456789abcdef", Name: name, Status: status, Run: types.Run{Status: run, Task: "task"}}
}

func renderLine(line extension.FrameLine) string {
	var text strings.Builder
	for _, span := range line.Spans {
		text.WriteString(span.Text)
	}
	return text.String()
}

func TestAgentWidgetSummarizesStatesAndTruncates(t *testing.T) {
	agents := []types.Agent{
		agentWith("starting-agent", types.AgentStarting, types.RunStarting),
		agentWith("running-agent", types.AgentRunning, types.RunRunning),
		agentWith("canceling-agent", types.AgentCanceling, types.RunCanceled),
		agentWith("done-agent", types.AgentIdle, types.RunCompleted),
		agentWith("broken-agent", types.AgentFailed, types.RunFailed),
		agentWith("stopped-agent", types.AgentInterrupted, types.RunInterrupted),
		agentWith("canceled-agent", types.AgentCanceled, types.RunCanceled),
	}
	for index := range 3 {
		agents = append(agents, agentWith(fmt.Sprintf("extra-%d", index), types.AgentIdle, types.RunCompleted))
	}
	lines := agentWidgetLines(agents)
	require.Len(t, lines, 1+WidgetAgentLimit+1)
	require.Equal(t, "Background agents  2 active · 4 completed · 1 canceling · 2 need attention · 1 canceled", renderLine(lines[0]))
	require.Equal(t, "◌ starting-agent  starting  89abcdef", renderLine(lines[1]))
	require.Equal(t, "× canceling-agent  canceling  89abcdef", renderLine(lines[3]))
	require.Equal(t, boldStyle, lines[1].Spans[0].Style)
	require.Equal(t, dimStyle, lines[3].Spans[0].Style)
	require.Equal(t, boldStyle, lines[5].Spans[2].Style)
	require.Nil(t, lines[1].Spans[1].Style)
	require.Equal(t, "… and 2 more", renderLine(lines[len(lines)-1]))

	long := agentWith(strings.Repeat("a", 90), types.AgentIdle, types.RunCompleted)
	require.Equal(t, strings.Repeat("a", 85)+"...", agentWidgetLine(long).Spans[1].Text)
}

func TestPublicSnapshotIncludesResultsOnlyWhenRequested(t *testing.T) {
	result, failure := "answer", "broken"
	agent := agentWith("worker", types.AgentIdle, types.RunCompleted)
	agent.Run.Result = &result
	require.NotContains(t, publicSnapshot(agent, false), "result")
	require.Equal(t, "answer", publicSnapshot(agent, true)["result"])

	agent.Run.Status = types.RunInterrupted
	agent.Run.Error = &failure
	snapshot := publicSnapshot(agent, true)
	require.Equal(t, "answer", snapshot["partial_result"])
	require.Equal(t, "broken", snapshot["error"])
	require.Contains(t, snapshot, "conversation_id")
	require.Nil(t, snapshot["conversation_id"])
	require.NotContains(t, withoutNulls(snapshot), "conversation_id")
}

func TestListPresentationCollapsesAndTruncatesTasks(t *testing.T) {
	agent := agentWith("worker", types.AgentIdle, types.RunCompleted)
	agent.Run.Task = "line one\n\n  line two " + strings.Repeat("x", 200)
	body := agentListPresentation([]types.Agent{agent}).Body
	lines := strings.Split(body, "\n")
	require.Equal(t, "- **worker** — completed", lines[0])
	require.True(t, strings.HasPrefix(lines[1], "  line one line two "))
	require.Len(t, []rune(strings.TrimPrefix(lines[1], "  ")), presentationTaskPreviewLength)
	require.True(t, strings.HasSuffix(lines[1], "..."))
}
