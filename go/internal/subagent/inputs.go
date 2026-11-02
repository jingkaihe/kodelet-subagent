package subagent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jingkaihe/kodelet-subagent/internal/paths"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// SpawnAgentInput is the spawn_agent input.
type SpawnAgentInput struct {
	Name        string            `json:"name"`
	Task        string            `json:"task"`
	CWD         *string           `json:"cwd"`
	ContextMode types.ContextMode `json:"context_mode"`
}

// WaitAgentInput is the wait_agent input.
type WaitAgentInput struct {
	AgentID   string `json:"agent_id"`
	TimeoutMS int    `json:"timeout_ms"`
}

// UnmarshalJSON accepts integral numbers written with a fraction, such as 1000.0.
func (in *WaitAgentInput) UnmarshalJSON(data []byte) error {
	var wire struct {
		AgentID   *string  `json:"agent_id"`
		TimeoutMS *float64 `json:"timeout_ms"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.AgentID != nil {
		in.AgentID = *wire.AgentID
	}
	if wire.TimeoutMS != nil {
		if *wire.TimeoutMS != float64(int(*wire.TimeoutMS)) {
			return fmt.Errorf("timeout_ms must be an integer, got %v", *wire.TimeoutMS)
		}
		in.TimeoutMS = int(*wire.TimeoutMS)
	}
	return nil
}

// ListAgentsInput is the list_agents input.
type ListAgentsInput struct{}

// FollowupAgentInput is the followup_agent input.
type FollowupAgentInput struct {
	AgentID string `json:"agent_id"`
	Task    string `json:"task"`
}

// SteerAgentInput is the steer_agent input.
type SteerAgentInput struct {
	AgentID string `json:"agent_id"`
	Message string `json:"message"`
}

// CancelAgentInput is the cancel_agent input.
type CancelAgentInput struct {
	AgentID string `json:"agent_id"`
}

// resolveAgentCWD resolves an agent directory against the workspace. Fresh
// agents may run elsewhere; forks must stay in the current workspace.
func resolveAgentCWD(raw *string, workspaceCWD string, mode types.ContextMode) (string, error) {
	workspace := paths.Resolve(workspaceCWD)
	if raw == nil {
		return workspace, nil
	}
	if strings.TrimSpace(*raw) == "" {
		return "", errors.New("cwd must be a non-empty string when provided")
	}
	candidate := paths.ExpandUser(*raw)
	if !filepath.IsAbs(candidate) {
		// Join without cleaning so ".." applies after symlinks resolve.
		candidate = workspaceCWD + string(filepath.Separator) + candidate
	}
	resolved := paths.Resolve(candidate)
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("cwd does not exist: %s", resolved)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("cwd is not a directory: %s", resolved)
	}
	if mode == types.ContextFork && resolved != workspace {
		return "", errors.New("context_mode='fork' requires the current workspace cwd")
	}
	return resolved, nil
}
