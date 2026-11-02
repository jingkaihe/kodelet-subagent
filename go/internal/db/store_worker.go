package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func (s *Store) reconcileExpiredTx(tx *sqlx.Tx, now float64) (int, error) {
	var rows []activeRow
	if err := tx.Select(&rows, `
		SELECT id, active_run_id, generation, status
		FROM agents
		WHERE status IN ('starting', 'running', 'canceling')
		  AND (lease_expires_at IS NULL OR lease_expires_at <= ?)`, now); err != nil {
		return 0, err
	}
	reconciled := 0
	for _, row := range rows {
		if row.Status == string(types.AgentCanceling) {
			completed, err := completeCancelRowTx(tx, row.ID, row.ActiveRunID, row.Generation, now)
			if err != nil {
				return 0, err
			}
			if completed {
				reconciled++
			}
			continue
		}
		if err := interruptRowTx(tx, row.ID, row.ActiveRunID, row.Generation, "agent lease expired", now); err != nil {
			return 0, err
		}
		reconciled++
	}
	return reconciled, nil
}

func interruptRowTx(tx *sqlx.Tx, agentID, runID string, generation int, message string, now float64) error {
	if _, err := tx.Exec(`
		UPDATE runs
		SET status = 'interrupted', error = ?, completed_at = ?, updated_at = ?
		WHERE id = ? AND status IN ('starting', 'running')`, message, now, now, runID); err != nil {
		return err
	}
	_, err := tx.Exec(`
		UPDATE agents
		SET status = 'interrupted', lease_runtime_id = NULL,
		    lease_token = NULL, lease_expires_at = NULL, updated_at = ?
		WHERE id = ? AND active_run_id = ? AND generation = ?
		  AND status IN ('starting', 'running')`, now, agentID, runID, generation)
	return err
}

func completeCancelRowTx(tx *sqlx.Tx, agentID, runID string, generation int, now float64) (bool, error) {
	result, err := tx.Exec(`
		UPDATE agents
		SET status = 'canceled', lease_runtime_id = NULL,
		    lease_token = NULL, lease_expires_at = NULL, updated_at = ?
		WHERE id = ? AND active_run_id = ? AND generation = ?
		  AND status = 'canceling'
		  AND EXISTS (
		      SELECT 1 FROM runs
		      WHERE runs.id = ? AND runs.agent_id = agents.id
		        AND runs.generation = ? AND runs.status = 'canceled'
		  )`, now, agentID, runID, generation, runID, generation)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (s *Store) attachChildConversationTx(tx *sqlx.Tx, agent agentRow, agentID, conversationID string, now float64) error {
	if agent.ChildConversationID != nil && *agent.ChildConversationID != conversationID {
		return newError(kindConflict, "agent child conversation cannot change")
	}
	if _, err := tx.Exec(`UPDATE agents SET child_conversation_id = ?, updated_at = ? WHERE id = ?`, conversationID, now, agentID); err != nil {
		if isConstraintViolation(err) {
			return wrapError(kindConflict, "child conversation is already attached to another agent", err)
		}
		return err
	}
	return nil
}

// AttachConversation records the child conversation for an active claim.
func (s *Store) AttachConversation(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error) {
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return types.Agent{}, errors.New("conversation id is required")
	}
	var agent types.Agent
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		agentRow, _, err := s.validateActiveLeaseTx(tx, lease, now)
		if err != nil {
			return err
		}
		if err := s.attachChildConversationTx(tx, agentRow, lease.AgentID, conversationID, now); err != nil {
			return err
		}
		agent, err = fetchAgentTx(tx, lease.AgentID, "", "")
		return err
	})
	return agent, err
}

// AttachCancelingConversation records the child conversation for a claim
// whose cancellation is still being cleaned up.
func (s *Store) AttachCancelingConversation(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error) {
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return types.Agent{}, errors.New("conversation id is required")
	}
	var agent types.Agent
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		agentRow, err := s.validateCancelingLeaseTx(tx, lease, now)
		if err != nil {
			return err
		}
		if err := s.attachChildConversationTx(tx, agentRow, lease.AgentID, conversationID, now); err != nil {
			return err
		}
		agent, err = fetchAgentTx(tx, lease.AgentID, "", "")
		return err
	})
	return agent, err
}

// MarkRunning records that the claimed run started in conversationID.
func (s *Store) MarkRunning(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error) {
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return types.Agent{}, errors.New("conversation id is required")
	}
	var agent types.Agent
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		agentRow, runRow, err := s.validateActiveLeaseTx(tx, lease, now)
		if err != nil {
			return err
		}
		if !types.AgentStatus(agentRow.Status).Active() {
			return newError(kindConflict, "agent is "+agentRow.Status)
		}
		if agentRow.ChildConversationID != nil && *agentRow.ChildConversationID != conversationID {
			return newError(kindConflict, "agent child conversation cannot change")
		}
		if _, err := tx.Exec(`
			UPDATE agents
			SET child_conversation_id = ?, status = 'running', updated_at = ?
			WHERE id = ?`, conversationID, now, lease.AgentID); err != nil {
			if isConstraintViolation(err) {
				return wrapError(kindConflict, "child conversation is already attached to another agent", err)
			}
			return err
		}
		if !types.RunStatus(runRow.Status).Active() {
			return newError(kindConflict, "run is "+runRow.Status)
		}
		if _, err := tx.Exec(`
			UPDATE runs
			SET status = 'running', started_at = COALESCE(started_at, ?),
			    updated_at = ?
			WHERE id = ?`, now, now, lease.RunID); err != nil {
			return err
		}
		agent, err = fetchAgentTx(tx, lease.AgentID, "", "")
		return err
	})
	return agent, err
}

// Terminal records the end of a claimed run and releases its lease. Repeating
// an identical terminal update is idempotent.
func (s *Store) Terminal(ctx context.Context, lease *types.Lease, status types.TerminalStatus, update types.TerminalUpdate) (types.Agent, error) {
	var desired types.RunStatus
	switch status {
	case types.TerminalIdle:
		desired = types.RunCompleted
	case types.TerminalFailed:
		desired = types.RunFailed
	case types.TerminalInterrupted:
		desired = types.RunInterrupted
	default:
		return types.Agent{}, fmt.Errorf("unsupported terminal status: %s", status)
	}
	var normalized *string
	if update.ConversationID != nil {
		trimmed := strings.TrimSpace(*update.ConversationID)
		normalized = &trimmed
	}
	var agent types.Agent
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		agentRow, err := agentRowTx(tx, lease.AgentID)
		if err != nil {
			return err
		}
		runRow, err := runRowTx(tx, lease.AgentID, lease.RunID)
		if err != nil {
			return err
		}
		if err := validateRunToken(runRow, lease); err != nil {
			return err
		}
		if !types.RunStatus(runRow.Status).Active() {
			if runRow.Status == string(desired) && equalStrings(runRow.Result, update.Result) && equalStrings(runRow.Error, update.Error) {
				agent, err = fetchAgentTx(tx, lease.AgentID, lease.RunID, "")
				return err
			}
			return newError(kindLeaseLost, "async-agent run is already terminal")
		}
		if err := s.validateActiveLeaseRows(agentRow, runRow, lease, now); err != nil {
			return err
		}
		current := types.AgentStatus(agentRow.Status)
		if status == types.TerminalIdle && current != types.AgentRunning {
			return newError(kindConflict, "agent must be running before becoming idle, currently "+string(current))
		}
		if !current.Active() {
			return newError(kindConflict, "agent is "+string(current))
		}
		existing := agentRow.ChildConversationID
		if existing != nil && normalized != nil && *existing != *normalized {
			return newError(kindConflict, "agent child conversation cannot change")
		}
		resolved := normalized
		if existing != nil {
			resolved = existing
		}
		if status == types.TerminalIdle && (resolved == nil || *resolved == "") {
			return newError(kindConflict, "completed agent must have a child conversation")
		}
		if _, err := tx.Exec(`
			UPDATE runs
			SET status = ?, result = ?, error = ?, completed_at = ?, updated_at = ?
			WHERE id = ?`, string(desired), update.Result, update.Error, now, now, lease.RunID); err != nil {
			return err
		}
		if _, err := tx.Exec(`
			UPDATE agents
			SET child_conversation_id = ?, status = ?, lease_runtime_id = NULL,
			    lease_token = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE id = ?`, resolved, string(status), now, lease.AgentID); err != nil {
			return err
		}
		agent, err = fetchAgentTx(tx, lease.AgentID, lease.RunID, "")
		return err
	})
	return agent, err
}

func (s *Store) renewLease(ctx context.Context, lease *types.Lease, canceling bool) (float64, error) {
	var expiresAt float64
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		expiresAt = now + LeaseDurationSeconds
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		var err error
		if canceling {
			_, err = s.validateCancelingLeaseTx(tx, lease, now)
		} else {
			_, _, err = s.validateActiveLeaseTx(tx, lease, now)
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE agents SET lease_expires_at = ?, updated_at = ? WHERE id = ?`, expiresAt, now, lease.AgentID)
		return err
	})
	if err != nil {
		return 0, err
	}
	lease.SetExpiresAt(expiresAt)
	return expiresAt, nil
}

// Heartbeat renews an active claim.
func (s *Store) Heartbeat(ctx context.Context, lease *types.Lease) (float64, error) {
	return s.renewLease(ctx, lease, false)
}

// HeartbeatCanceling renews a claim whose cancellation cleanup is in progress.
func (s *Store) HeartbeatCanceling(ctx context.Context, lease *types.Lease) (float64, error) {
	return s.renewLease(ctx, lease, true)
}

// CompleteCancel finishes a canceling claim once its owner has cleaned up.
func (s *Store) CompleteCancel(ctx context.Context, lease *types.Lease) (bool, error) {
	var completed bool
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		agentRow, err := agentRowTx(tx, lease.AgentID)
		if err != nil {
			return err
		}
		runRow, err := runRowTx(tx, lease.AgentID, lease.RunID)
		if err != nil {
			return err
		}
		if err := validateRunToken(runRow, lease); err != nil {
			return err
		}
		if agentRow.ActiveRunID != lease.RunID || agentRow.Generation != lease.Generation {
			return nil
		}
		switch types.AgentStatus(agentRow.Status) {
		case types.AgentCanceled:
			completed = true
			return nil
		case types.AgentCanceling:
		default:
			return nil
		}
		if err := s.validateCancelingLeaseRows(agentRow, runRow, lease, now); err != nil {
			return err
		}
		completed, err = completeCancelRowTx(tx, lease.AgentID, lease.RunID, lease.Generation, now)
		return err
	})
	return completed, err
}

// InterruptRuntime interrupts every active claim held by this store's runtime.
func (s *Store) InterruptRuntime(ctx context.Context, message string) (int, error) {
	var interrupted int
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		var rows []activeRow
		if err := tx.Select(&rows, `
			SELECT id, active_run_id, generation, status
			FROM agents
			WHERE lease_runtime_id = ? AND status IN ('starting', 'running')`, s.runtimeID); err != nil {
			return err
		}
		for _, row := range rows {
			if err := interruptRowTx(tx, row.ID, row.ActiveRunID, row.Generation, message, now); err != nil {
				return err
			}
		}
		interrupted = len(rows)
		return nil
	})
	return interrupted, err
}

// ReconcileExpired interrupts expired active claims and completes expired
// cancellations.
func (s *Store) ReconcileExpired(ctx context.Context) (int, error) {
	var reconciled int
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		var err error
		reconciled, err = s.reconcileExpiredTx(tx, now)
		return err
	})
	return reconciled, err
}
