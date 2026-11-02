package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// EnqueueSteering queues guidance for a running agent's current run.
func (s *Store) EnqueueSteering(ctx context.Context, ownerID, agentID, message string) (types.EnqueueSteeringResult, error) {
	message = strings.TrimSpace(message)
	if message == "" {
		return types.EnqueueSteeringResult{}, errors.New("steering message is required")
	}
	if utf8.RuneCountInString(message) > MaxSteeringMessageLength {
		return types.EnqueueSteeringResult{}, fmt.Errorf("steering message exceeds %d characters", MaxSteeringMessageLength)
	}
	var result types.EnqueueSteeringResult
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		current, err := fetchAgentTx(tx, strings.TrimSpace(agentID), "", strings.TrimSpace(ownerID))
		if err != nil {
			return err
		}
		agentRow, err := agentRowTx(tx, current.ID)
		if err != nil {
			return err
		}
		if current.Status != types.AgentRunning || current.Run.Status != types.RunRunning || current.ConversationID == nil ||
			agentRow.LeaseExpiresAt == nil || *agentRow.LeaseExpiresAt <= now {
			return newError(kindConflict, "agent is not currently running")
		}
		var pending int
		if err := tx.Get(&pending, `
			SELECT EXISTS(
			    SELECT 1 FROM steering_messages
			    WHERE agent_id = ? AND run_id = ? AND generation = ?
			)`, current.ID, current.Run.ID, current.Run.Generation); err != nil {
			return err
		}
		if _, err := tx.Exec(`
			INSERT INTO steering_messages (agent_id, run_id, generation, message, created_at)
			VALUES (?, ?, ?, ?, ?)`, current.ID, current.Run.ID, current.Run.Generation, message, now); err != nil {
			return err
		}
		result = types.EnqueueSteeringResult{Accepted: true, AlreadyPending: pending != 0}
		return nil
	})
	return result, err
}

// NextSteering returns the oldest queued message for a running claim, or nil.
func (s *Store) NextSteering(ctx context.Context, lease *types.Lease) (*types.SteeringMessage, error) {
	var message *types.SteeringMessage
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		agentRow, runRow, err := s.validateActiveLeaseTx(tx, lease, now)
		if err != nil {
			return err
		}
		if agentRow.Status != string(types.AgentRunning) || runRow.Status != string(types.RunRunning) {
			return newError(kindLeaseLost, "async-agent run is no longer running")
		}
		var row struct {
			ID      int64  `db:"id"`
			Message string `db:"message"`
		}
		err = tx.Get(&row, `
			SELECT id, message FROM steering_messages
			WHERE agent_id = ? AND run_id = ? AND generation = ?
			ORDER BY id ASC
			LIMIT 1`, lease.AgentID, lease.RunID, lease.Generation)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		message = &types.SteeringMessage{ID: row.ID, Message: row.Message}
		return nil
	})
	return message, err
}

// AcknowledgeSteering deletes a delivered steering message.
func (s *Store) AcknowledgeSteering(ctx context.Context, lease *types.Lease, messageID int64) (bool, error) {
	var deleted bool
	err := s.transaction(ctx, func(tx *sqlx.Tx, now float64) error {
		if _, err := s.reconcileExpiredTx(tx, now); err != nil {
			return err
		}
		if _, _, err := s.validateActiveLeaseTx(tx, lease, now); err != nil {
			return err
		}
		result, err := tx.Exec(`
			DELETE FROM steering_messages
			WHERE id = ? AND agent_id = ? AND run_id = ? AND generation = ?`,
			messageID, lease.AgentID, lease.RunID, lease.Generation)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		deleted = affected == 1
		return err
	})
	return deleted, err
}

func deleteRunMessagesTx(tx *sqlx.Tx, agentID, runID string, generation int) error {
	_, err := tx.Exec(`
		DELETE FROM steering_messages
		WHERE agent_id = ? AND run_id = ? AND generation = ?`, agentID, runID, generation)
	return err
}
