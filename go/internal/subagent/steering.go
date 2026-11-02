package subagent

import (
	"context"

	"github.com/jingkaihe/kodelet/sdk/go/agent"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// steeringPump delivers queued steering to the running session, deleting
// each message only after the session accepts it.
func (r *Runtime) steeringPump(ctx context.Context, live *LiveRun, session AgentSession) {
	for {
		queued, ok := r.waitForSteeringMessage(ctx, live)
		if !ok {
			return
		}
		if !r.deliverSteeringMessage(ctx, session, queued.Message) {
			return
		}
		if !r.acknowledgeSteeringMessage(ctx, live, queued.ID) {
			return
		}
	}
}

// handleSteeringStoreError stops the run for definitive or unexpected store
// errors and reports whether the operation may be retried.
func (r *Runtime) handleSteeringStoreError(ctx context.Context, live *LiveRun, err error) bool {
	switch {
	case ctx.Err() != nil:
		return false
	case db.IsDefinitive(err):
		r.stopLiveRunForError(live, "agent steering lease was lost: "+err.Error())
		return false
	case !db.IsRetryable(err):
		r.stopLiveRunForError(live, "agent steering failed: "+err.Error())
		return false
	default:
		return sleep(ctx, r.timing.SteeringRetry) == nil
	}
}

func (r *Runtime) waitForSteeringMessage(ctx context.Context, live *LiveRun) (*types.SteeringMessage, bool) {
	for {
		queued, err := live.store.NextSteering(ctx, live.lease)
		if err != nil {
			if !r.handleSteeringStoreError(ctx, live, err) {
				return nil, false
			}
			continue
		}
		if queued != nil {
			return queued, true
		}
		if sleep(ctx, r.timing.SteeringPoll) != nil {
			return nil, false
		}
	}
}

// deliverSteeringMessage reports false when delivery should stop. A message
// that requires a new prompt stays queued for the next follow-up.
func (r *Runtime) deliverSteeringMessage(ctx context.Context, session AgentSession, message string) bool {
	for {
		result, err := session.Steer(ctx, message)
		if ctx.Err() != nil {
			return false
		}
		if err == nil {
			switch result.Outcome {
			case agent.SteeringInjected, agent.SteeringStartedNewTurn:
				return true
			case agent.SteeringPromptRequired:
				return false
			}
		}
		if sleep(ctx, r.timing.SteeringRetry) != nil {
			return false
		}
	}
}

func (r *Runtime) acknowledgeSteeringMessage(ctx context.Context, live *LiveRun, messageID int64) bool {
	for {
		// The message was delivered, so record that even if the pump stops.
		_, err := live.store.AcknowledgeSteering(context.WithoutCancel(ctx), live.lease, messageID)
		if err == nil {
			return true
		}
		if !r.handleSteeringStoreError(ctx, live, err) {
			return false
		}
	}
}
