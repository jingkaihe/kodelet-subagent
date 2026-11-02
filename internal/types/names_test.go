package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAgentName(t *testing.T) {
	for _, name := range []string{"reviewer", "code-reviewer", "a1-b2-c3", strings.Repeat("a", AgentNameMaxLength)} {
		validated, err := ValidateAgentName(name)
		require.NoError(t, err, name)
		require.Equal(t, name, validated)
	}
	_, err := ValidateAgentName("")
	require.EqualError(t, err, "name is required")
	_, err = ValidateAgentName(strings.Repeat("a", AgentNameMaxLength+1))
	require.EqualError(t, err, "name must be at most 48 characters")
	for _, name := range []string{"Research-agent", "research agent", "research_agent", "one-two-three-four", "-reviewer", "reviewer-", "reviewer--one", "1-reviewer"} {
		_, err := ValidateAgentName(name)
		require.ErrorContains(t, err, "name must contain", name)
	}
}

func TestStatusPredicates(t *testing.T) {
	require.True(t, AgentStarting.Active())
	require.True(t, AgentRunning.Active())
	require.False(t, AgentCanceling.Active())
	require.True(t, AgentCanceled.Claimable())
	require.False(t, AgentCanceling.Claimable())
	require.True(t, RunRunning.Active())
	require.False(t, RunCanceled.Active())
}

func TestLeaseExpiryIsUpdatedAtomically(t *testing.T) {
	lease := NewLease("agent", "run", 1, "token", "runtime", 10.5)
	require.Equal(t, 10.5, lease.ExpiresAt())
	lease.SetExpiresAt(70.25)
	forged := lease.WithRuntimeID("other")
	require.Equal(t, "other", forged.RuntimeID)
	require.Equal(t, 70.25, forged.ExpiresAt())
	require.Equal(t, "runtime", lease.RuntimeID)
}
