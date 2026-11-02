package subagent

import (
	"context"
	"errors"
	"time"
)

var errParentCanceled = errors.New("agent canceled by parent")

// cancelLiveRun cancels this process's owner of a run whose cancellation has
// already been persisted. It reports false when cleanup is still in progress
// after timeout.
func (r *Runtime) cancelLiveRun(ctx context.Context, store Store, agentID, runID string, timeout time.Duration) bool {
	key := runKey{path: store.Path(), id: runID}
	r.mu.Lock()
	live := r.ownedRuns[key]
	if live == nil || live.agentID != agentID {
		r.mu.Unlock()
		return false
	}
	live.setParentCanceled()
	owner := live.runnerTask
	if owner == nil {
		owner = live.setupTask
	}
	if owner == nil {
		owner = live.cleanupTask
	}
	r.mu.Unlock()
	if owner == nil {
		completed, _ := r.safeCompleteCancel(ctx, live)
		return completed
	}
	owner.cancel(errParentCanceled)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-owner.done:
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, owned := r.ownedRuns[key]
	return !owned
}

// Shutdown stops new launches, drains reservations, cancels and awaits every
// owned run and its cleanup, and then interrupts rows still claimed by this
// runtime. Concurrent calls share one shutdown; ctx bounds only the wait.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.shutdownMu.Lock()
	op := r.shutdownOp
	if op == nil || op.finished() {
		op = goTask(func(context.Context) { r.shutdown(stoppedSessionMessage) })
		r.shutdownOp = op
	}
	r.shutdownMu.Unlock()
	select {
	case <-op.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) shutdown(message string) {
	r.mu.Lock()
	r.shuttingDown = true
	r.mu.Unlock()
	r.cancelOwnedTasks()
	// A reservation can finish concurrently with the first pass. It observes
	// shuttingDown and compensates, but drain again so shutdown also owns any
	// setup or worker that crossed a lifecycle boundary.
	r.waitForReservations()
	r.cancelOwnedTasks()
	r.waitForCleanupTasks()

	r.mu.Lock()
	stores := make([]Store, 0, len(r.stores))
	for _, store := range r.stores {
		if store.RuntimeID() == r.runtimeID {
			stores = append(stores, store)
		}
	}
	r.mu.Unlock()
	for _, store := range stores {
		_, _ = store.InterruptRuntime(context.Background(), message)
	}
}

func (r *Runtime) cancelOwnedTasks() {
	r.mu.Lock()
	var tasks []*task
	for setup := range r.setupTasks {
		if !setup.finished() {
			tasks = append(tasks, setup)
		}
	}
	for _, live := range r.ownedRuns {
		if live.store.RuntimeID() == r.runtimeID && live.runnerTask != nil && !live.runnerTask.finished() {
			tasks = append(tasks, live.runnerTask)
		}
	}
	r.mu.Unlock()
	for _, owned := range tasks {
		owned.cancel(errShuttingDown)
	}
	for _, owned := range tasks {
		<-owned.done
	}
}

func (r *Runtime) waitForReservations() {
	r.mu.Lock()
	pending := make([]chan struct{}, 0, len(r.reservations))
	for done := range r.reservations {
		pending = append(pending, done)
	}
	r.mu.Unlock()
	for _, done := range pending {
		<-done
	}
}

func (r *Runtime) waitForCleanupTasks() {
	for {
		r.mu.Lock()
		var pending []*task
		for cleanup := range r.cleanupTasks {
			pending = append(pending, cleanup)
		}
		r.mu.Unlock()
		if len(pending) == 0 {
			return
		}
		for _, cleanup := range pending {
			<-cleanup.done
		}
	}
}
