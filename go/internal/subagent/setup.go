package subagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jingkaihe/kodelet/sdk/go/extension"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

const setupCanceledMessage = "agent setup was canceled before the worker started"

// reserveClaim runs a database reservation that cannot be abandoned midway.
// If the caller is canceled, or the session starts shutting down, while the
// reservation commits, the committed claim is compensated before returning.
func (r *Runtime) reserveClaim(ctx context.Context, reserve func(context.Context) (types.Claim, error), taskText string, store Store, initial bool) (types.Claim, error) {
	done := make(chan struct{})
	r.mu.Lock()
	if r.shuttingDown {
		r.mu.Unlock()
		return types.Claim{}, errShuttingDown
	}
	r.reservations[done] = struct{}{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.reservations, done)
		r.mu.Unlock()
		close(done)
	}()

	claim, err := reserve(context.WithoutCancel(ctx))
	if err != nil {
		return types.Claim{}, err
	}
	cause := context.Cause(ctx)
	if ctx.Err() == nil && r.ensureAccepting() != nil {
		cause = errShuttingDown
	}
	if cause == nil {
		return claim, nil
	}
	if err := r.abandonUnlaunchedClaim(claim, taskText, store, initial); err != nil {
		return types.Claim{}, err
	}
	return types.Claim{}, cause
}

// abandonUnlaunchedClaim deletes an initial reservation or interrupts a
// follow-up claim that never reached a worker.
func (r *Runtime) abandonUnlaunchedClaim(claim types.Claim, taskText string, store Store, initial bool) error {
	live := r.liveRunFromClaim(claim, taskText, store)
	live.setTerminalizing()
	heartbeat := r.startAgentHeartbeat(live)
	defer stopTask(heartbeat)
	ctx := context.Background()
	var committed bool
	if initial && live.ConversationID() == "" {
		committed, _ = r.safeWorkerAbort(ctx, live)
	} else {
		message := "agent setup stopped before the worker started"
		committed, _ = r.safeWorkerTerminal(ctx, live, types.TerminalInterrupted, nil, &message)
	}
	if !committed {
		return errors.New("failed to persist unlaunched agent cleanup")
	}
	return nil
}

func selectFreshProfile(parentProfile string) string {
	if configured := strings.TrimSpace(os.Getenv(ProfileEnv)); configured != "" {
		return configured
	}
	return strings.TrimSpace(parentProfile)
}

// prepareClaim acquires the background lease and, for unattached forks, the
// named child conversation, then launches the worker. The calling goroutine
// owns the setup until launch, so cancellation and shutdown can interrupt it.
func (r *Runtime) prepareClaim(ctx context.Context, claim types.Claim, taskText string, store Store, call ToolCall, initial bool) (types.Claim, *LiveRun, error) {
	setupCtx, cancel := context.WithCancelCause(ctx)
	setup := &task{cancel: cancel, done: make(chan struct{})}
	r.mu.Lock()
	if r.shuttingDown {
		r.mu.Unlock()
		cancel(nil)
		return claim, nil, errShuttingDown
	}
	r.setupTasks[setup] = struct{}{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.setupTasks, setup)
		r.mu.Unlock()
		cancel(nil)
		close(setup.done)
	}()

	live := r.liveRunFromClaim(claim, taskText, store)
	if live.ConversationID() == "" && live.contextMode == types.ContextFresh {
		// Startup-only selection; saved settings belong to the daemon.
		live.setProfile(selectFreshProfile(call.Info().Profile))
	}
	live.setUI(call.UI())
	r.ownLiveSetup(live, setup)
	heartbeat := r.startAgentHeartbeat(live)

	prepared, err := r.setupLiveRun(setupCtx, claim, live, store, call, heartbeat)
	if err == nil {
		return prepared, live, nil
	}
	live.setTerminalizing()
	failure := &workerFailure{initial: initial, status: types.TerminalFailed, message: agentFailureMessage("starting", err)}
	if setupCtx.Err() != nil {
		failure = &workerFailure{initial: initial, status: types.TerminalInterrupted, message: setupCanceledMessage}
		err = errors.New(setupCanceledMessage)
	}
	cleanup := r.startLiveRunCleanup(live, nil, nil, heartbeat, failure)
	<-cleanup.done
	return claim, nil, err
}

func (r *Runtime) setupLiveRun(ctx context.Context, claim types.Claim, live *LiveRun, store Store, call ToolCall, heartbeat *task) (types.Claim, error) {
	summary := []rune(collapseWhitespace(live.task))
	if len(summary) > presentationTaskPreviewLength {
		summary = summary[:presentationTaskPreviewLength]
	}
	description := fmt.Sprintf("subagent %s (%s): %s", claim.Agent.Name, live.agentID, string(summary))
	lease, err := call.AcquireBackgroundTask(ctx, description)
	if err != nil {
		return claim, err
	}
	live.setBackgroundLease(lease)
	if live.ConversationID() == "" && live.contextMode == types.ContextFork {
		conversationID, err := forkParentContext(ctx, call, claim.Agent.Name)
		if err != nil {
			return claim, err
		}
		live.setConversationID(conversationID)
		if message := live.HeartbeatError(); message != "" {
			return claim, errors.New(message)
		}
		if err := context.Cause(ctx); ctx.Err() != nil {
			return claim, err
		}
		attached, err := store.AttachConversation(ctx, live.lease, conversationID)
		if err != nil {
			return claim, err
		}
		claim = types.Claim{Agent: attached, Lease: claim.Lease}
	}
	if err := r.ensureAccepting(); err != nil {
		return claim, err
	}
	if _, err := store.Heartbeat(ctx, live.lease); err != nil {
		return claim, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx.Err() != nil {
		return claim, context.Cause(ctx)
	}
	r.launchLiveRunLocked(live, heartbeat)
	return claim, nil
}

func forkParentContext(ctx context.Context, call ToolCall, name string) (string, error) {
	conversationID, err := call.ForkConversation(ctx, name)
	if errors.Is(err, extension.ErrConversationForkUnavailable) {
		return "", errors.New("context_mode='fork' requires live conversation forking; use " +
			"context_mode='fresh' to start without parent conversation memory")
	}
	return conversationID, err
}

// ownLiveSetup records the setup goroutine as the owner of live.
func (r *Runtime) ownLiveSetup(live *LiveRun, setup *task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	live.setupTask = setup
	r.ownedRuns[live.ownedKey()] = live
	r.liveRuns[live.liveKey()] = live
}

// launchLiveRun starts the worker for live, adopting heartbeat if non-nil.
func (r *Runtime) launchLiveRun(live *LiveRun, heartbeat *task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.launchLiveRunLocked(live, heartbeat)
}

func (r *Runtime) launchLiveRunLocked(live *LiveRun, heartbeat *task) {
	live.runnerTask = goTask(func(ctx context.Context) { r.runAgentJob(ctx, live, heartbeat) })
	live.setupTask = nil
	r.ownedRuns[live.ownedKey()] = live
	r.liveRuns[live.liveKey()] = live
}
