package subagent

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/taskprogress"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

var errTaskStopped = errors.New("task stopped")

// task is a cancellable goroutine whose completion can be awaited.
type task struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

// goTask runs fn on a new goroutine with a context it can be canceled through.
func goTask(fn func(ctx context.Context)) *task {
	ctx, cancel := context.WithCancelCause(context.Background())
	t := &task{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(t.done)
		defer cancel(nil)
		fn(ctx)
	}()
	return t
}

func (t *task) finished() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

// stop cancels the task and waits for it to return.
func stopTask(t *task) {
	if t == nil {
		return
	}
	t.cancel(errTaskStopped)
	<-t.done
}

// sleep waits for d or until ctx ends, reporting the context's cause.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return context.Cause(ctx)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func seconds(value float64) time.Duration {
	return time.Duration(value * float64(time.Second))
}

// workerFailure is the terminal state recorded when a run does not complete.
type workerFailure struct {
	initial bool
	status  types.TerminalStatus
	message string
	result  *string
}

type runKey struct {
	path string
	id   string
}

// LiveRun holds the in-memory resources of one persisted run claim.
type LiveRun struct {
	store               Store
	agentID             string
	ownerConversationID string
	runID               string
	generation          int
	task                string
	cwd                 string
	contextMode         types.ContextMode
	lease               *types.Lease
	progress            *taskprogress.Progress

	// Owned tasks are guarded by Runtime.mu.
	setupTask   *task
	runnerTask  *task
	cleanupTask *task

	mu              sync.Mutex
	conversationID  string
	profile         string
	client          AgentClient
	session         AgentSession
	parentCanceled  bool
	heartbeatError  string
	terminalizing   bool
	ui              WidgetUI
	backgroundLease BackgroundLease
}

func newLiveRun(claim types.Claim, taskText string, store Store) *LiveRun {
	live := &LiveRun{
		store:               store,
		agentID:             claim.Agent.ID,
		ownerConversationID: claim.Agent.OwnerConversationID,
		runID:               claim.Lease.RunID,
		generation:          claim.Lease.Generation,
		task:                taskText,
		cwd:                 claim.Agent.CWD,
		contextMode:         claim.Agent.ContextMode,
		lease:               claim.Lease,
		progress:            newAgentTaskProgress(claim.Agent),
	}
	if claim.Agent.ConversationID != nil {
		live.conversationID = *claim.Agent.ConversationID
	}
	return live
}

func (l *LiveRun) liveKey() runKey  { return runKey{path: l.store.Path(), id: l.agentID} }
func (l *LiveRun) ownedKey() runKey { return runKey{path: l.store.Path(), id: l.runID} }

func (l *LiveRun) ConversationID() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conversationID
}

func (l *LiveRun) setConversationID(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conversationID = id
}

func (l *LiveRun) conversationPointer() *string {
	if id := l.ConversationID(); id != "" {
		return &id
	}
	return nil
}

func (l *LiveRun) Profile() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.profile
}

func (l *LiveRun) setProfile(profile string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.profile = profile
}

func (l *LiveRun) UI() WidgetUI {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ui
}

func (l *LiveRun) setUI(ui WidgetUI) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ui = ui
}

func (l *LiveRun) ParentCanceled() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.parentCanceled
}

func (l *LiveRun) setParentCanceled() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.parentCanceled = true
}

func (l *LiveRun) HeartbeatError() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.heartbeatError
}

func (l *LiveRun) Terminalizing() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.terminalizing
}

func (l *LiveRun) setTerminalizing() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.terminalizing = true
}

func (l *LiveRun) BackgroundLease() BackgroundLease {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.backgroundLease
}

func (l *LiveRun) setBackgroundLease(lease BackgroundLease) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.backgroundLease = lease
}

func (l *LiveRun) Client() AgentClient {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.client
}

func (l *LiveRun) setClient(client AgentClient) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.client = client
}

func (l *LiveRun) Session() AgentSession {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.session
}

func (l *LiveRun) setSession(session AgentSession) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.session = session
}

func (l *LiveRun) clearClient() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.client = nil
	l.session = nil
}
