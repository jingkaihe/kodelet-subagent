package db

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

// publishJoinedMailTx is called only when a run starts for the first time.
// A previously started agent must not be announced again, including when the
// previous start happened before mail was enabled. A retry of failed startup
// may, however, be the agent's first successful start.
func publishJoinedMailTx(tx *sqlx.Tx, agent types.Agent, now float64) error {
	var starts int
	if err := tx.Get(&starts, `SELECT COUNT(*) FROM runs WHERE agent_id = ? AND started_at IS NOT NULL`, agent.ID); err != nil {
		return err
	}
	if starts != 1 {
		return nil
	}
	body := fmt.Sprintf("Contact: %s\nName: %s\nTask: %s\nStatus: running\n\nUse message_contact to contact this agent.",
		agent.ID, agent.Name, boundedMailText(agent.Run.Task, 1_000))
	return publishEventMailTx(tx, "joined:"+agent.ID, agent, "agent_joined", agent.Name+" joined the team", body, body, now)
}

// publishTerminalMailTx runs inside the transaction that finalizes a run or
// completes cancellation cleanup. It never scans or replays old terminal runs.
func publishTerminalMailTx(tx *sqlx.Tx, agentID, runID string, now float64, enabled bool) error {
	if !enabled {
		return nil
	}
	agent, err := fetchAgentTx(tx, agentID, runID, "")
	if err != nil {
		return err
	}
	summary := fmt.Sprintf("Agent: %s (%s)\nRun: %s\nTask: %s\nOutcome: %s",
		agent.Name, agent.ID, runID, boundedMailText(agent.Run.Task, 1_000), agent.Run.Status)
	if agent.Run.Error != nil {
		summary += "\nError: " + boundedMailText(*agent.Run.Error, 1_000)
	}
	body := summary
	if agent.Run.Result != nil {
		label := "Result"
		if agent.Run.Status != types.RunCompleted {
			label = "Partial result (run did not complete)"
		}
		body += "\n\n" + label + ":\n" + *agent.Run.Result
	}
	return publishEventMailTx(tx, "terminal:"+runID, agent, "run_"+string(agent.Run.Status),
		agent.Name+" "+string(agent.Run.Status), body, summary, now)
}

// One event and its recipient snapshot commit together with the state change.
// The event key prevents a retry from broadcasting to later team members.
func publishEventMailTx(tx *sqlx.Tx, eventID string, agent types.Agent, kind, subject, mainBody, peerBody string, now float64) error {
	result, err := tx.Exec(`INSERT INTO mailbox_events (id, owner_conversation_id, created_at)
		SELECT ?, owner_conversation_id, ? FROM mailbox_rooms WHERE owner_conversation_id = ?
		ON CONFLICT (id) DO NOTHING`, eventID, now, agent.OwnerConversationID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 0 {
		return err
	}
	var peers []string
	if err := tx.Select(&peers, `SELECT id FROM agents WHERE owner_conversation_id = ? AND id != ? ORDER BY id`,
		agent.OwnerConversationID, agent.ID); err != nil {
		return err
	}
	for _, recipient := range append([]string{"main"}, peers...) {
		body := peerBody
		if recipient == "main" {
			body = mainBody
		}
		mail := types.Mail{
			Sender:      types.Contact{ID: "system", Name: "Subagent runtime", Kind: "system"},
			RecipientID: recipient, Subject: subject, Body: body, Kind: kind,
			AgentID: agent.ID, RunID: agent.Run.ID, CreatedAt: now,
		}
		mail.Body = boundedMailBody(mail)
		_, err := insertMailTx(tx, agent.OwnerConversationID, &eventID, mail)
		if err != nil {
			return err
		}
	}
	return nil
}

const mailTruncationSuffix = "\n[Truncated; full text remains in the stored run.]"

// Bound lifecycle mail to the same single-message read budget as SendMail.
// Only the notification is shortened; the original run result remains intact.
func boundedMailBody(mail types.Mail) string {
	body := boundedMailText(mail.Body, MaxMailBodyBytes)
	mail.Body = body
	if mailReadBytes(mail) <= MaxMailReadBytes {
		return body
	}
	// Search byte limits, with boundedMailText rounding down to UTF-8 boundaries
	// and reserving the truncation label. Lifecycle headers fit with room to spare.
	low, high := len(mailTruncationSuffix), len(body)-1
	for low < high {
		mid := low + (high-low+1)/2
		mail.Body = boundedMailText(body, mid)
		if mailReadBytes(mail) <= MaxMailReadBytes {
			low = mid
		} else {
			high = mid - 1
		}
	}
	return boundedMailText(body, low)
}

func boundedMailText(text string, limit int) string {
	text = strings.ToValidUTF8(text, "�")
	if len(text) <= limit {
		return text
	}
	end := limit - len(mailTruncationSuffix)
	for !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end] + mailTruncationSuffix
}
