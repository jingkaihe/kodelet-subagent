// Package types defines the durable background-agent domain model shared by
// the database and the runtime.
package types

import (
	"math"
	"sync/atomic"
)

// ContextMode selects how an agent's conversation is initialized.
type ContextMode string

// Context modes.
const (
	ContextFork  ContextMode = "fork"
	ContextFresh ContextMode = "fresh"
)

// AgentStatus is the persisted state of an agent.
type AgentStatus string

// Agent statuses.
const (
	AgentStarting    AgentStatus = "starting"
	AgentRunning     AgentStatus = "running"
	AgentCanceling   AgentStatus = "canceling"
	AgentIdle        AgentStatus = "idle"
	AgentFailed      AgentStatus = "failed"
	AgentInterrupted AgentStatus = "interrupted"
	AgentCanceled    AgentStatus = "canceled"
)

// Active reports whether the agent owns a live worker claim.
func (s AgentStatus) Active() bool {
	return s == AgentStarting || s == AgentRunning
}

// Claimable reports whether a follow-up run may claim the agent.
func (s AgentStatus) Claimable() bool {
	switch s {
	case AgentIdle, AgentFailed, AgentInterrupted, AgentCanceled:
		return true
	default:
		return false
	}
}

// RunStatus is the persisted state of one agent run.
type RunStatus string

// Run statuses.
const (
	RunStarting    RunStatus = "starting"
	RunRunning     RunStatus = "running"
	RunCompleted   RunStatus = "completed"
	RunFailed      RunStatus = "failed"
	RunInterrupted RunStatus = "interrupted"
	RunCanceled    RunStatus = "canceled"
)

// Active reports whether the run has not reached a terminal state.
func (s RunStatus) Active() bool {
	return s == RunStarting || s == RunRunning
}

// TerminalStatus is the agent status a worker may record when its run ends.
type TerminalStatus string

// Worker terminal statuses.
const (
	TerminalIdle        TerminalStatus = "idle"
	TerminalFailed      TerminalStatus = "failed"
	TerminalInterrupted TerminalStatus = "interrupted"
)

// Run is one generation of an agent.
type Run struct {
	ID          string
	Generation  int
	Task        string
	Status      RunStatus
	Result      *string
	Error       *string
	CreatedAt   float64
	StartedAt   *float64
	CompletedAt *float64
	UpdatedAt   float64
}

// Agent is an agent together with a selected run.
type Agent struct {
	ID                  string
	Name                string
	OwnerConversationID string
	ConversationID      *string
	ContextMode         ContextMode
	CWD                 string
	Status              AgentStatus
	Generation          int
	CreatedAt           float64
	UpdatedAt           float64
	Run                 Run
}

// Lease fences worker updates to one claimed run.
type Lease struct {
	AgentID    string
	RunID      string
	Generation int
	Token      string
	RuntimeID  string

	expiresAt atomic.Uint64
}

// NewLease returns a lease that expires at the given epoch time.
func NewLease(agentID, runID string, generation int, token, runtimeID string, expiresAt float64) *Lease {
	lease := &Lease{AgentID: agentID, RunID: runID, Generation: generation, Token: token, RuntimeID: runtimeID}
	lease.SetExpiresAt(expiresAt)
	return lease
}

// ExpiresAt returns the last persisted lease expiry as epoch seconds.
func (l *Lease) ExpiresAt() float64 {
	return math.Float64frombits(l.expiresAt.Load())
}

// SetExpiresAt records a renewed expiry.
func (l *Lease) SetExpiresAt(value float64) {
	l.expiresAt.Store(math.Float64bits(value))
}

// WithRuntimeID returns a copy of the lease claiming another runtime.
func (l *Lease) WithRuntimeID(runtimeID string) *Lease {
	return NewLease(l.AgentID, l.RunID, l.Generation, l.Token, runtimeID, l.ExpiresAt())
}

// Claim is a reserved run and the lease that owns it.
type Claim struct {
	Agent Agent
	Lease *Lease
}

// SteeringMessage is guidance queued for a running agent.
type SteeringMessage struct {
	ID      int64
	Message string
}

// EnqueueSteeringResult reports how a steering message was queued.
type EnqueueSteeringResult struct {
	Accepted       bool `json:"accepted"`
	AlreadyPending bool `json:"alreadyPending"`
}

// TerminalUpdate carries the optional values recorded with a terminal status.
type TerminalUpdate struct {
	ConversationID *string
	Result         *string
	Error          *string
}
