package agentdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

func checkLimitsTx(tx *sqlx.Tx, ownerID string) error {
	var ownerCount int
	if err := tx.Get(&ownerCount, `
		SELECT COUNT(*) FROM agents
		WHERE owner_conversation_id = ?
		  AND status IN ('starting', 'running', 'canceling')`, ownerID); err != nil {
		return err
	}
	if ownerCount >= MaxActiveAgentsPerConversation {
		return newError(kindLimit, fmt.Sprintf(
			"this conversation already has the maximum of %d active agents", MaxActiveAgentsPerConversation))
	}
	var totalCount int
	if err := tx.Get(&totalCount, `
		SELECT COUNT(*) FROM agents
		WHERE status IN ('starting', 'running', 'canceling')`); err != nil {
		return err
	}
	if totalCount >= MaxActiveAgentsTotal {
		return newError(kindLimit, fmt.Sprintf(
			"this extension already has the maximum of %d active agents", MaxActiveAgentsTotal))
	}
	return nil
}

// Create reserves a new agent and its first run.
func (s *Store) Create(ctx context.Context, ownerID, name, task, cwd string, mode ContextMode) (Claim, error) {
	ownerID = strings.TrimSpace(ownerID)
	name, err := ValidateAgentName(name)
	if err != nil {
		return Claim{}, err
	}
	task = strings.TrimSpace(task)
	cwd = strings.TrimSpace(cwd)
	switch {
	case ownerID == "":
		return Claim{}, errors.New("async agents require an active conversation ID")
	case task == "":
		return Claim{}, errors.New("task is required")
	case cwd == "":
		return Claim{}, errors.New("cwd is required")
	case mode != ContextFork && mode != ContextFresh:
		return Claim{}, fmt.Errorf("unsupported context mode: %s", mode)
	}
	agentID := newID("agt_")
	runID := newID("run_")
	token := newToken()
	var agent Agent
	var expiresAt float64
	err = s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		expiresAt = now + LeaseDurationSeconds
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		if err := checkLimitsTx(tx, ownerID); err != nil {
			return err
		}
		var existing int
		if err := tx.Get(&existing, `SELECT COUNT(*) FROM agents WHERE owner_conversation_id = ? AND name = ?`, ownerID, name); err != nil {
			return err
		}
		if existing > 0 {
			return newError(kindConflict, "agent name already exists in this conversation: "+name)
		}
		if _, err := tx.Exec(`
			INSERT INTO agents (
			    id, name, owner_conversation_id, child_conversation_id,
			    context_mode, cwd, status, active_run_id, generation,
			    lease_runtime_id, lease_token, lease_expires_at, created_at,
			    updated_at
			) VALUES (?, ?, ?, NULL, ?, ?, 'starting', ?, 1, ?, ?, ?, ?, ?)`,
			agentID, name, ownerID, string(mode), cwd, runID, s.runtimeID, token, expiresAt, now, now); err != nil {
			return err
		}
		if _, err := tx.Exec(`
			INSERT INTO runs (
			    id, agent_id, generation, lease_token, task, status,
			    result, error, created_at, started_at, completed_at, updated_at
			) VALUES (?, ?, 1, ?, ?, 'starting', NULL, NULL, ?, NULL, NULL, ?)`,
			runID, agentID, token, task, now, now); err != nil {
			return err
		}
		var err error
		agent, err = fetchAgentTx(tx, agentID, "", "")
		return err
	})
	if err != nil {
		return Claim{}, err
	}
	return Claim{Agent: agent, Lease: NewLease(agentID, runID, 1, token, s.runtimeID, expiresAt)}, nil
}

// Get returns an owned agent with its active run, or with runID when given.
func (s *Store) Get(ctx context.Context, ownerID, agentID, runID string) (Agent, error) {
	var agent Agent
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		var err error
		agent, err = fetchAgentTx(tx, strings.TrimSpace(agentID), strings.TrimSpace(runID), strings.TrimSpace(ownerID))
		return err
	})
	return agent, err
}

// List returns an owner's agents, active agents first, most recent first.
func (s *Store) List(ctx context.Context, ownerID string) ([]Agent, error) {
	return s.ListLimit(ctx, ownerID, DefaultListLimit)
}

// ListLimit is List with an explicit limit between 1 and MaxListLimit.
func (s *Store) ListLimit(ctx context.Context, ownerID string, limit int) ([]Agent, error) {
	if limit < 1 || limit > MaxListLimit {
		return nil, fmt.Errorf("limit must be between 1 and %d", MaxListLimit)
	}
	var agents []Agent
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		var ids []string
		if err := tx.Select(&ids, `
			SELECT id FROM agents
			WHERE owner_conversation_id = ?
			ORDER BY
			    CASE WHEN status IN ('starting', 'running', 'canceling') THEN 0 ELSE 1 END,
			    updated_at DESC
			LIMIT ?`, strings.TrimSpace(ownerID), limit); err != nil {
			return err
		}
		agents = make([]Agent, 0, len(ids))
		for _, id := range ids {
			agent, err := fetchAgentTx(tx, id, "", "")
			if err != nil {
				return err
			}
			agents = append(agents, agent)
		}
		return nil
	})
	return agents, err
}

// Claim reserves a follow-up run for an idle, failed, interrupted, or canceled agent.
func (s *Store) Claim(ctx context.Context, ownerID, agentID, task string) (Claim, error) {
	ownerID = strings.TrimSpace(ownerID)
	agentID = strings.TrimSpace(agentID)
	task = strings.TrimSpace(task)
	if task == "" {
		return Claim{}, errors.New("task is required")
	}
	runID := newID("run_")
	token := newToken()
	var agent Agent
	var expiresAt float64
	var generation int
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		expiresAt = now + LeaseDurationSeconds
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		current, err := fetchAgentTx(tx, agentID, "", ownerID)
		if err != nil {
			return err
		}
		if !current.Status.Claimable() {
			return newError(kindConflict, "agent is "+string(current.Status))
		}
		if current.ConversationID == nil && current.Status == AgentIdle {
			return newError(kindConflict, "agent has no child conversation to resume")
		}
		if err := checkLimitsTx(tx, ownerID); err != nil {
			return err
		}
		generation = current.Generation + 1
		if _, err := tx.Exec(`
			INSERT INTO runs (
			    id, agent_id, generation, lease_token, task, status,
			    result, error, created_at, started_at, completed_at, updated_at
			) VALUES (?, ?, ?, ?, ?, 'starting', NULL, NULL, ?, NULL, NULL, ?)`,
			runID, agentID, generation, token, task, now, now); err != nil {
			return err
		}
		result, err := tx.Exec(`
			UPDATE agents
			SET status = 'starting', active_run_id = ?, generation = ?,
			    lease_runtime_id = ?, lease_token = ?, lease_expires_at = ?,
			    updated_at = ?
			WHERE id = ? AND owner_conversation_id = ? AND generation = ?
			  AND status IN ('idle', 'failed', 'interrupted', 'canceled')`,
			runID, generation, s.runtimeID, token, expiresAt, now, agentID, ownerID, current.Generation)
		if err != nil {
			return err
		}
		if affected, err := result.RowsAffected(); err != nil {
			return err
		} else if affected != 1 {
			return newError(kindConflict, "agent changed while claiming follow-up run")
		}
		if _, err := tx.Exec(`
			UPDATE steering_messages
			SET run_id = ?, generation = ?
			WHERE agent_id = ?`, runID, generation, agentID); err != nil {
			return err
		}
		agent, err = fetchAgentTx(tx, agentID, "", "")
		return err
	})
	if err != nil {
		return Claim{}, err
	}
	return Claim{Agent: agent, Lease: NewLease(agentID, runID, generation, token, s.runtimeID, expiresAt)}, nil
}

// Abort deletes an unstarted initial reservation.
func (s *Store) Abort(ctx context.Context, lease *Lease) (bool, error) {
	var deleted bool
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		agentRow, runRow, err := s.validateActiveLeaseTx(tx, lease, now)
		if errors.Is(err, ErrAgentNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if lease.Generation != 1 || agentRow.Status != string(AgentStarting) ||
			runRow.Status != string(RunStarting) || agentRow.ChildConversationID != nil {
			return newError(kindConflict, "only an unstarted initial reservation can be aborted")
		}
		result, err := tx.Exec(`DELETE FROM agents WHERE id = ?`, lease.AgentID)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		deleted = affected == 1
		return err
	})
	return deleted, err
}

// Cancel records cancellation. Active agents move to canceling until their
// owner finishes cleanup; inactive agents become canceled immediately.
func (s *Store) Cancel(ctx context.Context, ownerID, agentID string) (Agent, error) {
	var agent Agent
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		current, err := fetchAgentTx(tx, strings.TrimSpace(agentID), "", strings.TrimSpace(ownerID))
		if err != nil {
			return err
		}
		if current.Status != AgentCanceling && current.Status != AgentCanceled {
			if err := deleteRunMessagesTx(tx, current.ID, current.Run.ID, current.Run.Generation); err != nil {
				return err
			}
			if current.Status.Active() {
				if _, err := tx.Exec(`
					UPDATE runs
					SET status = 'canceled', error = 'agent canceled by parent',
					    completed_at = ?, updated_at = ?
					WHERE id = ? AND status IN ('starting', 'running')`, now, now, current.Run.ID); err != nil {
					return err
				}
				if _, err := tx.Exec(`
					UPDATE agents
					SET status = 'canceling', lease_expires_at = ?, updated_at = ?
					WHERE id = ? AND active_run_id = ? AND generation = ?
					  AND status IN ('starting', 'running')`,
					now+LeaseDurationSeconds, now, current.ID, current.Run.ID, current.Run.Generation); err != nil {
					return err
				}
			} else if _, err := tx.Exec(`
				UPDATE agents
				SET status = 'canceled', lease_runtime_id = NULL,
				    lease_token = NULL, lease_expires_at = NULL, updated_at = ?
				WHERE id = ? AND active_run_id = ? AND generation = ?
				  AND status = ?`,
				now, current.ID, current.Run.ID, current.Run.Generation, string(current.Status)); err != nil {
				return err
			}
		}
		agent, err = fetchAgentTx(tx, current.ID, "", "")
		return err
	})
	return agent, err
}
