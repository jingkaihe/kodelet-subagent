package subagent

import (
	"context"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// retryWorkerUpdate retries transient store failures with backoff while the
// claim's lease remains valid. It reports whether update eventually
// succeeded; the error is non-nil only when ctx was canceled.
func (r *Runtime) retryWorkerUpdate(ctx context.Context, live *LiveRun, update func() error) (bool, error) {
	delay := min(r.timing.WorkerUpdateRetryInitial, r.timing.WorkerUpdateRetryMax)
	for {
		err := update()
		if err == nil {
			return true, nil
		}
		if ctx.Err() != nil {
			return false, context.Cause(ctx)
		}
		if db.IsDefinitive(err) || !db.IsRetryable(err) {
			return false, nil
		}
		remaining := seconds(live.lease.ExpiresAt() - live.store.Now())
		if remaining <= 0 {
			return false, nil
		}
		if err := sleep(ctx, min(delay, remaining)); err != nil {
			return false, err
		}
		delay = min(r.timing.WorkerUpdateRetryMax, delay*2)
	}
}

// safeWorkerTerminal finishes the run's progress and records its terminal
// status, keeping the lease fence intact.
func (r *Runtime) safeWorkerTerminal(ctx context.Context, live *LiveRun, status types.TerminalStatus, result, message *string) (bool, error) {
	if live.progress.Snapshot().Status == "running" {
		errorText := ""
		if message != nil {
			errorText = *message
		}
		live.progress.Finish(ctx, status == types.TerminalIdle, errorText)
	}
	committed, err := r.retryWorkerUpdate(ctx, live, func() error {
		_, err := live.store.Terminal(ctx, live.lease, status, types.TerminalUpdate{
			ConversationID: live.conversationPointer(),
			Result:         result,
			Error:          message,
		})
		return err
	})
	if committed {
		r.safeSyncAgentWidget(ctx, live.UI(), live.store, live.ownerConversationID)
	}
	return committed, err
}

// safeWorkerAbort deletes an unstarted initial reservation.
func (r *Runtime) safeWorkerAbort(ctx context.Context, live *LiveRun) (bool, error) {
	committed, err := r.retryWorkerUpdate(ctx, live, func() error {
		_, err := live.store.Abort(ctx, live.lease)
		return err
	})
	if committed {
		r.safeSyncAgentWidget(ctx, live.UI(), live.store, live.ownerConversationID)
	}
	return committed, err
}

// safeCompleteCancel finishes a cancellation once local cleanup is done.
func (r *Runtime) safeCompleteCancel(ctx context.Context, live *LiveRun) (bool, error) {
	var completed bool
	_, err := r.retryWorkerUpdate(ctx, live, func() error {
		var err error
		completed, err = live.store.CompleteCancel(ctx, live.lease)
		return err
	})
	if completed {
		r.safeSyncAgentWidget(ctx, live.UI(), live.store, live.ownerConversationID)
	}
	return completed, err
}

// safeAttachCancelingConversation preserves a child conversation created
// before cancellation so the canceled agent can resume it.
func (r *Runtime) safeAttachCancelingConversation(ctx context.Context, live *LiveRun) (bool, error) {
	conversationID := live.ConversationID()
	if conversationID == "" {
		return true, nil
	}
	return r.retryWorkerUpdate(ctx, live, func() error {
		_, err := live.store.AttachCancelingConversation(ctx, live.lease, conversationID)
		return err
	})
}
