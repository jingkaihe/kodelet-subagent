package subagent

import (
	"context"
	"errors"
	"time"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
)

// stopLiveRunForError records why a run lost its claim and cancels the
// goroutine that owns it.
func (r *Runtime) stopLiveRunForError(live *LiveRun, message string) {
	live.mu.Lock()
	live.heartbeatError = message
	live.mu.Unlock()
	r.mu.Lock()
	owner := live.runnerTask
	if owner == nil {
		owner = live.setupTask
	}
	r.mu.Unlock()
	if owner != nil {
		owner.cancel(errors.New(message))
	}
}

func (r *Runtime) startAgentHeartbeat(live *LiveRun) *task {
	return goTask(func(ctx context.Context) { r.heartbeatAgent(ctx, live) })
}

func (r *Runtime) heartbeatRetryDelay(live *LiveRun) (time.Duration, bool) {
	remaining := seconds(live.lease.ExpiresAt() - live.store.Now())
	if remaining <= 0 {
		return 0, false
	}
	return min(r.timing.HeartbeatRetryMax, max(r.timing.MinHeartbeatInterval, remaining/2)), true
}

// heartbeatAgent renews an active claim, stopping the run if the claim is
// lost or cannot be renewed before it expires.
func (r *Runtime) heartbeatAgent(ctx context.Context, live *LiveRun) {
	for {
		if sleep(ctx, r.timing.heartbeatInterval()) != nil {
			return
		}
		for {
			_, err := live.store.Heartbeat(ctx, live.lease)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			if db.IsDefinitive(err) {
				if !live.Terminalizing() {
					r.stopLiveRunForError(live, "agent lease was lost: "+err.Error())
				}
				return
			}
			if !db.IsRetryable(err) {
				r.stopLiveRunForError(live, "agent heartbeat failed: "+err.Error())
				return
			}
			delay, ok := r.heartbeatRetryDelay(live)
			if !ok {
				r.stopLiveRunForError(live, "agent lease heartbeat expired after error: "+err.Error())
				return
			}
			if sleep(ctx, delay) != nil {
				return
			}
		}
	}
}

// heartbeatCleanup keeps the claim fenced while cleanup runs. Cancellation
// can arrive after cleanup starts, including from another extension process,
// so a lost active lease falls back to renewing the canceling lease.
func (r *Runtime) heartbeatCleanup(ctx context.Context, live *LiveRun) {
	for {
		_, err := live.store.Heartbeat(ctx, live.lease)
		if errors.Is(err, db.ErrLeaseLost) {
			_, err = live.store.HeartbeatCanceling(ctx, live.lease)
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if db.IsDefinitive(err) || !db.IsRetryable(err) {
				return
			}
			delay, ok := r.heartbeatRetryDelay(live)
			if !ok || sleep(ctx, delay) != nil {
				return
			}
			continue
		}
		if sleep(ctx, r.timing.heartbeatInterval()) != nil {
			return
		}
	}
}
