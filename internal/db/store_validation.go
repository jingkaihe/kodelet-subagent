package db

import (
	"crypto/subtle"

	"github.com/jmoiron/sqlx"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func tokensEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func validateRunToken(run runRow, lease *types.Lease) error {
	if run.Generation != lease.Generation {
		return newError(kindLeaseLost, "async-agent generation changed")
	}
	if !tokensEqual(run.LeaseToken, lease.Token) {
		return newError(kindLeaseLost, "async-agent lease token changed")
	}
	return nil
}

func (s *Store) validateLeaseOwnership(agent agentRow, run runRow, lease *types.Lease, now float64) error {
	if err := validateRunToken(run, lease); err != nil {
		return err
	}
	if lease.RuntimeID != s.runtimeID {
		return newError(kindLeaseLost, "async-agent worker runtime changed")
	}
	if agent.ActiveRunID != lease.RunID {
		return newError(kindLeaseLost, "async-agent active run changed")
	}
	if agent.Generation != lease.Generation {
		return newError(kindLeaseLost, "async-agent generation changed")
	}
	if agent.LeaseRuntimeID == nil || *agent.LeaseRuntimeID != s.runtimeID {
		return newError(kindLeaseLost, "async-agent worker runtime changed")
	}
	if agent.LeaseToken == nil || !tokensEqual(*agent.LeaseToken, lease.Token) {
		return newError(kindLeaseLost, "async-agent lease token changed")
	}
	if agent.LeaseExpiresAt == nil || *agent.LeaseExpiresAt <= now {
		return newError(kindLeaseLost, "async-agent lease expired")
	}
	return nil
}

func (s *Store) validateActiveLeaseRows(agent agentRow, run runRow, lease *types.Lease, now float64) error {
	if err := s.validateLeaseOwnership(agent, run, lease, now); err != nil {
		return err
	}
	if !types.AgentStatus(agent.Status).Active() {
		return newError(kindLeaseLost, "async-agent is "+agent.Status)
	}
	if !types.RunStatus(run.Status).Active() {
		return newError(kindLeaseLost, "async-agent run is "+run.Status)
	}
	return nil
}

func (s *Store) validateActiveLeaseTx(tx *sqlx.Tx, lease *types.Lease, now float64) (agentRow, runRow, error) {
	agent, err := agentRowTx(tx, lease.AgentID)
	if err != nil {
		return agent, runRow{}, err
	}
	run, err := runRowTx(tx, lease.AgentID, lease.RunID)
	if err != nil {
		return agent, run, err
	}
	return agent, run, s.validateActiveLeaseRows(agent, run, lease, now)
}

func (s *Store) validateCancelingLeaseRows(agent agentRow, run runRow, lease *types.Lease, now float64) error {
	if err := s.validateLeaseOwnership(agent, run, lease, now); err != nil {
		return err
	}
	if agent.Status != string(types.AgentCanceling) {
		return newError(kindLeaseLost, "async-agent is "+agent.Status)
	}
	if run.Status != string(types.RunCanceled) {
		return newError(kindLeaseLost, "async-agent run is "+run.Status)
	}
	return nil
}

func (s *Store) validateCancelingLeaseTx(tx *sqlx.Tx, lease *types.Lease, now float64) (agentRow, error) {
	agent, err := agentRowTx(tx, lease.AgentID)
	if err != nil {
		return agent, err
	}
	run, err := runRowTx(tx, lease.AgentID, lease.RunID)
	if err != nil {
		return agent, err
	}
	return agent, s.validateCancelingLeaseRows(agent, run, lease, now)
}
