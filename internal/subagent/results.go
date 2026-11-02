package subagent

import (
	"encoding/json"
	"strings"

	"github.com/jingkaihe/kodelet/sdk/go/extension"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

const (
	presentationTaskPreviewLength = 160
	listTaskPreviewLength         = 100
	childToolErrorMessage         = "async agent tools are only available to the main agent"
)

// jsonNull is sent as structuredContent for failed calls.
var jsonNull = json.RawMessage("null")

// presentation returns tool presentation metadata; a body is Markdown.
func presentation(summary string, body ...string) extension.Presentation {
	result := extension.Presentation{Summary: summary}
	if len(body) > 0 {
		result.Body = body[0]
		result.Format = "markdown"
	}
	return result
}

func toolError(message, summary string) extension.ToolResult {
	return extension.ToolResult{
		Content:           message,
		Error:             message,
		Data:              map[string]any{"presentation": presentation(summary)},
		StructuredContent: jsonNull,
	}
}

func childToolError(summary string) extension.ToolResult {
	return toolError(childToolErrorMessage, summary)
}

func agentNotFound(summary string) extension.ToolResult {
	return toolError("agent not found", summary)
}

// toolResult exposes machine data as structured content, which keeps JSON
// nulls, while result metadata omits nulls and adds presentation and task
// progress.
func toolResult(content string, data map[string]any, errorMessage string) extension.ToolResult {
	structured := make(map[string]any, len(data))
	for key, value := range data {
		if key != "presentation" && key != "taskRun" {
			structured[key] = value
		}
	}
	metadata, _ := withoutNulls(data).(map[string]any)
	return extension.ToolResult{Content: content, Data: metadata, StructuredContent: structured, Error: errorMessage}
}

// withoutNulls drops nil values from nested maps.
func withoutNulls(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		cleaned := make(map[string]any, len(typed))
		for key, item := range typed {
			if item != nil {
				cleaned[key] = withoutNulls(item)
			}
		}
		return cleaned
	case []map[string]any:
		cleaned := make([]any, 0, len(typed))
		for _, item := range typed {
			cleaned = append(cleaned, withoutNulls(item))
		}
		return cleaned
	case []any:
		cleaned := make([]any, 0, len(typed))
		for _, item := range typed {
			cleaned = append(cleaned, withoutNulls(item))
		}
		return cleaned
	default:
		return value
	}
}

// quoted renders a name the way the original extension's messages did.
func quoted(name string) string {
	return "'" + name + "'"
}

func collapseWhitespace(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func truncate(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit-3]) + "..."
}

func agentListPresentation(agents []types.Agent) extension.Presentation {
	if len(agents) == 0 {
		return presentation("List agents", "No background agents.")
	}
	lines := make([]string, 0, 2*len(agents))
	for _, agent := range agents {
		task := truncate(collapseWhitespace(agent.Run.Task), presentationTaskPreviewLength)
		lines = append(lines, "- **"+agent.Name+"** — "+displayStatus(agent))
		if task != "" {
			lines = append(lines, "  "+task)
		}
	}
	return presentation("List agents", strings.Join(lines, "\n"))
}
