// Package subagent implements the Kodelet extension that runs durable,
// named background agents.
//
// Runtime ownership covers the database reservation, background-lease and
// named-conversation setup, the live ACP worker, and final client and
// background-lease cleanup.
package subagent

import (
	"context"
	"log/slog"

	"github.com/jingkaihe/kodelet/sdk/go/agent"
	"github.com/jingkaihe/kodelet/sdk/go/extension"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// Call is the host context of one tool or event invocation.
type Call interface {
	Info() extension.CallInfo
	DataDir() string
	// UI returns the persistent widget surface, or nil when unavailable.
	UI() WidgetUI
	Logger() *slog.Logger
}

// ToolCall is the host context of one tool invocation.
type ToolCall interface {
	Call
	Update(ctx context.Context, content string, data map[string]any) error
	AcquireBackgroundTask(ctx context.Context, description string) (BackgroundLease, error)
	// ForkConversation forks the calling conversation into a named child.
	ForkConversation(ctx context.Context, name string) (string, error)
}

// WidgetUI renders persistent widgets; *extension.UI implements it.
type WidgetUI interface {
	SetWidget(ctx context.Context, id string, lines []extension.FrameLine, placement extension.WidgetPlacement) error
	RemoveWidget(ctx context.Context, id string) error
}

// BackgroundLease keeps the extension process alive for background work.
type BackgroundLease interface {
	Close(ctx context.Context) error
}

// AgentSession is a child conversation; *agent.Session implements it.
type AgentSession interface {
	ID() string
	RunAndWait(ctx context.Context, opts agent.RunOptions) (*agent.Response, error)
	Steer(ctx context.Context, message string) (agent.SteerResult, error)
	Subscribe(handler func(agent.Event)) func()
}

// AgentClient creates child sessions through kodelet acp.
type AgentClient interface {
	CreateSession(ctx context.Context, opts agent.SessionOptions) (AgentSession, error)
	Close() error
}

// ClientFactory creates a client that runs command in cwd with extra env.
type ClientFactory func(command, cwd string, env map[string]string) AgentClient

// Store is the database API used by the runtime; *db.Store implements it.
type Store interface {
	Path() string
	RuntimeID() string
	Now() float64
	Initialize(ctx context.Context) error
	Create(ctx context.Context, ownerID, name, task, cwd string, mode types.ContextMode) (types.Claim, error)
	Get(ctx context.Context, ownerID, agentID, runID string) (types.Agent, error)
	List(ctx context.Context, ownerID string) ([]types.Agent, error)
	Claim(ctx context.Context, ownerID, agentID, task string) (types.Claim, error)
	AttachConversation(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error)
	AttachCancelingConversation(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error)
	MarkRunning(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error)
	Terminal(ctx context.Context, lease *types.Lease, status types.TerminalStatus, update types.TerminalUpdate) (types.Agent, error)
	Heartbeat(ctx context.Context, lease *types.Lease) (float64, error)
	HeartbeatCanceling(ctx context.Context, lease *types.Lease) (float64, error)
	Abort(ctx context.Context, lease *types.Lease) (bool, error)
	Cancel(ctx context.Context, ownerID, agentID string) (types.Agent, error)
	CompleteCancel(ctx context.Context, lease *types.Lease) (bool, error)
	EnqueueSteering(ctx context.Context, ownerID, agentID, message string) (types.EnqueueSteeringResult, error)
	NextSteering(ctx context.Context, lease *types.Lease) (*types.SteeringMessage, error)
	AcknowledgeSteering(ctx context.Context, lease *types.Lease, messageID int64) (bool, error)
	InterruptRuntime(ctx context.Context, message string) (int, error)
	ResolveMailbox(ctx context.Context, conversationID string, allowRoot bool) (types.MailboxIdentity, error)
	ListContacts(ctx context.Context, identity types.MailboxIdentity) ([]types.Contact, error)
	SendMail(ctx context.Context, identity types.MailboxIdentity, recipientID, subject, body string) (types.Mail, error)
	ListMails(ctx context.Context, identity types.MailboxIdentity, status string, limit, offset int) (types.MailPage, error)
	ReadMails(ctx context.Context, identity types.MailboxIdentity, ids []int64) ([]types.Mail, error)
	AckTerminalMail(ctx context.Context, ownerID, agentID, runID string) error
}

var _ Store = (*db.Store)(nil)
