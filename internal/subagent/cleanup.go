package subagent

import (
	"context"
)

// startLiveRunCleanup starts the single cleanup for live, or returns the one
// already running. Cleanup is not canceled by the run's owner: failure and
// cancellation keep the active claim, with a fenced heartbeat, until the
// client has closed.
func (r *Runtime) startLiveRunCleanup(live *LiveRun, client AgentClient, steering, heartbeat *task, failure *workerFailure) *task {
	r.mu.Lock()
	defer r.mu.Unlock()
	if live.cleanupTask != nil {
		return live.cleanupTask
	}
	cleanup := &task{cancel: func(error) {}, done: make(chan struct{})}
	live.cleanupTask = cleanup
	r.cleanupTasks[cleanup] = struct{}{}
	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.cleanupTasks, cleanup)
			r.mu.Unlock()
			close(cleanup.done)
		}()
		r.cleanupLiveRun(context.Background(), live, client, steering, heartbeat, failure)
	}()
	return cleanup
}

func (r *Runtime) cleanupLiveRun(ctx context.Context, live *LiveRun, client AgentClient, steering, heartbeat *task, failure *workerFailure) {
	defer r.releaseLiveRun(live)
	if live.progress.Snapshot().Status == "running" {
		message := "agent canceled"
		if failure != nil {
			message = failure.message
		}
		live.progress.Finish(ctx, false, message)
	}
	stopTask(steering)
	stopTask(heartbeat)

	cleanupHeartbeat := goTask(func(heartbeatCtx context.Context) { r.heartbeatCleanup(heartbeatCtx, live) })
	if client != nil {
		r.closeAgentClient(ctx, client)
	}
	live.clearClient()
	if failure != nil && !live.ParentCanceled() {
		if failure.initial && live.ConversationID() == "" {
			_, _ = r.safeWorkerAbort(ctx, live)
		} else {
			message := failure.message
			_, _ = r.safeWorkerTerminal(ctx, live, failure.status, failure.result, &message)
		}
	}
	if attached, _ := r.safeAttachCancelingConversation(ctx, live); attached {
		_, _ = r.safeCompleteCancel(ctx, live)
	}
	stopTask(cleanupHeartbeat)
	r.closeBackgroundLease(ctx, live)
}

// releaseLiveRun forgets live once its cleanup has completed.
func (r *Runtime) releaseLiveRun(live *LiveRun) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.liveRuns[live.liveKey()] == live {
		delete(r.liveRuns, live.liveKey())
	}
	if r.ownedRuns[live.ownedKey()] == live {
		delete(r.ownedRuns, live.ownedKey())
	}
	live.setupTask = nil
	live.cleanupTask = nil
}

// closeAgentClient closes the client, retrying until the ACP process is gone.
func (r *Runtime) closeAgentClient(ctx context.Context, client AgentClient) {
	delay := r.timing.ClientCloseRetryInitial
	for client.Close() != nil {
		if sleep(ctx, delay) != nil {
			return
		}
		delay = min(r.timing.ClientCloseRetryMax, delay*2)
	}
}

// closeBackgroundLease releases the host lease, retrying transient failures.
func (r *Runtime) closeBackgroundLease(ctx context.Context, live *LiveRun) {
	lease := live.BackgroundLease()
	if lease == nil {
		return
	}
	delay := r.timing.BackgroundLeaseReleaseRetryInitial
	for {
		requestCtx, cancel := context.WithTimeout(ctx, r.timing.HostRequestTimeout)
		err := lease.Close(requestCtx)
		cancel()
		if err == nil {
			live.setBackgroundLease(nil)
			return
		}
		if sleep(ctx, delay) != nil {
			return
		}
		delay = min(r.timing.BackgroundLeaseReleaseRetryMax, delay*2)
	}
}
