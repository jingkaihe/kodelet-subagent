package db

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func eventMails(t *testing.T, store *Store, agentID, kind string) []mailRow {
	t.Helper()
	var mails []mailRow
	require.NoError(t, rawDB(t, store.Path()).Select(&mails, `SELECT `+mailColumns+`, body FROM mails
		WHERE agent_id = ? AND kind = ? ORDER BY id`, agentID, kind))
	return mails
}

func TestMailboxLifecycleRecipientsAndRunGenerations(t *testing.T) {
	store, _ := mailboxStore(t, true)
	peer := startMailAgent(t, store, mailAgent(t, store, "owner", "peer"))
	main := mailboxIdentity(t, store, "owner", true)
	worker := mailAgent(t, store, "owner", "worker")
	startMailAgent(t, store, worker)
	joined := eventMails(t, store, worker.Agent.ID, "agent_joined")
	require.Len(t, joined, 2)
	require.Equal(t, "main", joined[0].RecipientID)
	require.Equal(t, peer.ContactID, joined[1].RecipientID)
	for _, mail := range joined {
		require.Equal(t, "system", mail.SenderID)
		require.Equal(t, "system", mail.SenderKind)
		require.Contains(t, mail.Body, "worker task")
	}
	_, err := store.ReadMails(t.Context(), main, []int64{joined[0].ID})
	require.NoError(t, err)
	page, err := store.ListMails(t.Context(), peer, "unread", 20, 0)
	require.NoError(t, err)
	require.Equal(t, 1, page.UnreadCount, "broadcast recipients have independent read state")
	late := mailAgent(t, store, "owner", "late-peer")
	startMailAgent(t, store, mailAgent(t, store, "other-owner", "outsider"))
	_, err = store.MarkRunning(t.Context(), worker.Lease, "child-"+worker.Agent.ID)
	require.NoError(t, err)
	require.Equal(t, joined[1].ID, eventMails(t, store, worker.Agent.ID, "agent_joined")[1].ID)
	require.Len(t, eventMails(t, store, worker.Agent.ID, "agent_joined"), 2, "late peers do not receive old announcements")

	update := types.TerminalUpdate{Result: new("first run findings")}
	_, err = store.Terminal(t.Context(), worker.Lease, types.TerminalIdle, update)
	require.NoError(t, err)
	completed := eventMails(t, store, worker.Agent.ID, "run_completed")
	require.Len(t, completed, 3)
	recipients := []string{}
	for _, mail := range completed {
		recipients = append(recipients, mail.RecipientID)
		require.Equal(t, worker.Lease.RunID, mail.RunID)
		require.Contains(t, mail.Body, "Outcome: completed")
		require.NotContains(t, mail.Body, "first run findings")
	}
	require.ElementsMatch(t, []string{"main", peer.ContactID, late.Agent.ID}, recipients)
	_, err = store.Terminal(t.Context(), worker.Lease, types.TerminalIdle, update)
	require.NoError(t, err)
	require.Equal(t, completed, eventMails(t, store, worker.Agent.ID, "run_completed"))

	second, err := store.Claim(t.Context(), "owner", worker.Agent.ID, "second task")
	require.NoError(t, err)
	startMailAgent(t, store, second)
	require.Len(t, eventMails(t, store, worker.Agent.ID, "agent_joined"), 2)
	// Retrying the first terminal write cannot steal the newer run or republish mail.
	_, err = store.Terminal(t.Context(), worker.Lease, types.TerminalIdle, update)
	require.NoError(t, err)
	_, err = store.Terminal(t.Context(), second.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("second run findings")})
	require.NoError(t, err)
	all := eventMails(t, store, worker.Agent.ID, "run_completed")
	require.Len(t, all, 6)
	require.Equal(t, completed, all[:3])
	require.Equal(t, second.Lease.RunID, all[3].RunID)
	require.Contains(t, all[3].Body, "Task: second task")
	require.NotContains(t, all[3].Body, "second run findings")
	// Canceling an already completed agent changes no run outcome and sends no extra result.
	_, err = store.Cancel(t.Context(), "owner", worker.Agent.ID)
	require.NoError(t, err)
	require.Empty(t, eventMails(t, store, worker.Agent.ID, "run_canceled"))
}

func TestMailboxPublishesEveryTerminalPathOnce(t *testing.T) {
	for _, outcome := range []string{"completed", "failed", "interrupted", "startup-failed", "expired", "runtime-stop", "canceled", "cancel-expired"} {
		t.Run(outcome, func(t *testing.T) {
			store, clock := mailboxStore(t, true)
			mailboxIdentity(t, store, "owner", true)
			claim := mailAgent(t, store, "owner", "worker")
			if outcome != "startup-failed" {
				startMailAgent(t, store, claim)
			}
			kind := "run_" + outcome
			switch outcome {
			case "completed", "failed", "interrupted", "startup-failed":
				status := types.TerminalStatus(outcome)
				switch outcome {
				case "completed":
					status = types.TerminalIdle
				case "startup-failed":
					status, kind = types.TerminalFailed, "run_failed"
				}
				for range 2 {
					_, err := store.Terminal(t.Context(), claim.Lease, status, types.TerminalUpdate{Result: new("output"), Error: new("detail")})
					require.NoError(t, err)
				}
			case "expired":
				clock.advance(LeaseDurationSeconds + 1)
				count, err := store.ReconcileExpired(t.Context())
				require.NoError(t, err)
				require.Equal(t, 1, count)
				kind = "run_interrupted"
			case "runtime-stop":
				count, err := store.InterruptRuntime(t.Context(), "runtime stopped")
				require.NoError(t, err)
				require.Equal(t, 1, count)
				count, err = store.InterruptRuntime(t.Context(), "runtime stopped")
				require.NoError(t, err)
				require.Zero(t, count)
				kind = "run_interrupted"
			case "canceled", "cancel-expired":
				_, err := store.Cancel(t.Context(), "owner", claim.Agent.ID)
				require.NoError(t, err)
				require.Empty(t, eventMails(t, store, claim.Agent.ID, "run_canceled"), "cleanup is not complete")
				if outcome == "cancel-expired" {
					clock.advance(LeaseDurationSeconds + 1)
					count, err := store.ReconcileExpired(t.Context())
					require.NoError(t, err)
					require.Equal(t, 1, count)
				}
				for range 2 {
					completed, err := store.CompleteCancel(t.Context(), claim.Lease)
					require.NoError(t, err)
					require.True(t, completed)
				}
				kind = "run_canceled"
			}
			count, err := store.ReconcileExpired(t.Context())
			require.NoError(t, err)
			require.Zero(t, count)
			mails := eventMails(t, store, claim.Agent.ID, kind)
			require.Len(t, mails, 1)
			require.Equal(t, claim.Lease.RunID, mails[0].RunID)
			require.Equal(t, "main", mails[0].RecipientID)
			if outcome == "failed" || outcome == "interrupted" {
				require.Contains(t, mails[0].Body, "Error: detail")
			}
			require.NotContains(t, mails[0].Body, "output")
			if outcome == "startup-failed" {
				require.Empty(t, eventMails(t, store, claim.Agent.ID, "agent_joined"))
				retry, err := store.Claim(t.Context(), "owner", claim.Agent.ID, "retry startup")
				require.NoError(t, err)
				startMailAgent(t, store, retry)
				require.Len(t, eventMails(t, store, claim.Agent.ID, "agent_joined"), 1)
			}
		})
	}
}

func TestMailboxLifecyclePublicationIsAtomic(t *testing.T) {
	for _, transition := range []string{"start", "terminal", "cancel", "expiry", "runtime"} {
		t.Run(transition, func(t *testing.T) {
			store, clock := mailboxStore(t, true)
			peer := startMailAgent(t, store, mailAgent(t, store, "owner", "peer"))
			claim := mailAgent(t, store, "owner", "worker")
			before := "starting"
			if transition != "start" {
				startMailAgent(t, store, claim)
				before = "running"
			}
			if transition == "cancel" {
				_, err := store.Cancel(t.Context(), "owner", claim.Agent.ID)
				require.NoError(t, err)
				before = "canceling"
			}
			if transition == "expiry" {
				// Keep the peer's claim live so only the worker expires.
				clock.advance(LeaseDurationSeconds + 1)
				rawDB(t, store.Path()).MustExec(`UPDATE agents SET lease_expires_at = ? WHERE id = ?`, clock.now()+LeaseDurationSeconds, peer.ContactID)
			}
			apply := func() error {
				switch transition {
				case "start":
					_, err := store.MarkRunning(t.Context(), claim.Lease, "child-"+claim.Agent.ID)
					return err
				case "terminal":
					_, err := store.Terminal(t.Context(), claim.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("done")})
					return err
				case "cancel":
					_, err := store.CompleteCancel(t.Context(), claim.Lease)
					return err
				case "expiry":
					_, err := store.ReconcileExpired(t.Context())
					return err
				default:
					_, err := store.InterruptRuntime(t.Context(), "stopped")
					return err
				}
			}
			database := rawDB(t, store.Path())
			var eventsBefore int
			require.NoError(t, database.Get(&eventsBefore, `SELECT COUNT(*) FROM mailbox_events`))
			database.MustExec(`CREATE TRIGGER fail_mail BEFORE INSERT ON mails
				WHEN NEW.recipient_id != 'main' BEGIN SELECT RAISE(ABORT, 'mail write failed'); END`)
			require.ErrorContains(t, apply(), "mail write failed")
			var status string
			require.NoError(t, database.Get(&status, `SELECT status FROM agents WHERE id = ?`, claim.Agent.ID))
			require.Equal(t, before, status)
			var eventsAfter int
			require.NoError(t, database.Get(&eventsAfter, `SELECT COUNT(*) FROM mailbox_events`))
			require.Equal(t, eventsBefore, eventsAfter, "the event and earlier recipients roll back too")
			var mailCount int
			require.NoError(t, database.Get(&mailCount, `SELECT COUNT(*) FROM mails`))
			require.Equal(t, eventsBefore*2, mailCount)
			database.MustExec(`DROP TRIGGER fail_mail`)
			require.NoError(t, apply())
		})
	}
}

func TestMailboxTerminalSummariesNeverCopyResults(t *testing.T) {
	for _, tc := range []struct {
		name, result string
	}{
		{"unicode", strings.Repeat("界", MaxMailBodyBytes)},
		{"escaped", strings.Repeat("<>&\x00\u2028界", MaxMailBodyBytes)},
		{"escaped below raw limit", strings.Repeat("<", 4_000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := mailboxStore(t, true)
			main := mailboxIdentity(t, store, "owner", true)
			claim := mailAgent(t, store, "owner", "worker")
			startMailAgent(t, store, claim)
			_, err := store.Terminal(t.Context(), claim.Lease, types.TerminalIdle, types.TerminalUpdate{Result: &tc.result})
			require.NoError(t, err)
			mails := eventMails(t, store, claim.Agent.ID, "run_completed")
			require.Len(t, mails, 1)
			require.LessOrEqual(t, len(mails[0].Body), MaxMailBodyBytes)
			require.LessOrEqual(t, mailReadBytes(mails[0].mail()), MaxMailReadBytes)
			require.True(t, utf8.ValidString(mails[0].Body))
			require.Equal(t, "Agent: worker ("+claim.Agent.ID+")\nRun: "+claim.Lease.RunID+"\nTask: worker task\nOutcome: completed", mails[0].Body)
			read, err := store.ReadMails(t.Context(), main, []int64{mails[0].ID})
			require.NoError(t, err)
			require.Equal(t, mails[0].Body, read[0].Body)
			require.NotNil(t, read[0].ReadAt)
			agent, err := store.Get(t.Context(), "owner", claim.Agent.ID, claim.Lease.RunID)
			require.NoError(t, err)
			require.Equal(t, tc.result, *agent.Run.Result)
		})
	}
}

func TestAcknowledgeTerminalMailScopesRunOwnerAndRecipient(t *testing.T) {
	store, clock := mailboxStore(t, true)
	main := mailboxIdentity(t, store, "owner", true)
	peer := startMailAgent(t, store, mailAgent(t, store, "owner", "peer"))
	first := mailAgent(t, store, "owner", "worker")
	worker := startMailAgent(t, store, first)
	_, err := store.Terminal(t.Context(), first.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("first findings")})
	require.NoError(t, err)
	second, err := store.Claim(t.Context(), "owner", first.Agent.ID, "second task")
	require.NoError(t, err)
	startMailAgent(t, store, second)
	_, err = store.Terminal(t.Context(), second.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("second findings")})
	require.NoError(t, err)
	explicit, err := store.SendMail(t.Context(), worker, "main", "worker completed", "Explicit message, not a notification")
	require.NoError(t, err)
	before, err := store.ListMails(t.Context(), main, "unread", 20, 0)
	require.NoError(t, err)
	peerBefore, err := store.ListMails(t.Context(), peer, "unread", 20, 0)
	require.NoError(t, err)
	for _, scope := range [][3]string{
		{"other-owner", first.Agent.ID, first.Lease.RunID},
		{"owner", peer.ContactID, first.Lease.RunID},
		{"owner", first.Agent.ID, "missing-run"},
	} {
		require.NoError(t, store.AckTerminalMail(t.Context(), scope[0], scope[1], scope[2]))
	}
	unchanged, err := store.ListMails(t.Context(), main, "unread", 20, 0)
	require.NoError(t, err)
	require.Equal(t, before, unchanged)
	require.NoError(t, store.AckTerminalMail(t.Context(), "owner", first.Agent.ID, first.Lease.RunID))
	completed := eventMails(t, store, first.Agent.ID, "run_completed")
	for _, mail := range completed {
		if mail.RecipientID == "main" && mail.RunID == first.Lease.RunID {
			require.NotNil(t, mail.ReadAt)
		} else {
			require.Nil(t, mail.ReadAt, "later generations and peers stay unread")
		}
	}
	clock.advance(1)
	require.NoError(t, store.AckTerminalMail(t.Context(), "owner", first.Agent.ID, first.Lease.RunID))
	require.Equal(t, completed, eventMails(t, store, first.Agent.ID, "run_completed"), "the first read timestamp is retained")
	remaining, err := store.ListMails(t.Context(), main, "unread", 20, 0)
	require.NoError(t, err)
	require.Equal(t, before.UnreadCount-1, remaining.UnreadCount)
	require.Equal(t, explicit.ID, remaining.Mails[0].ID)
	for _, mail := range eventMails(t, store, first.Agent.ID, "agent_joined") {
		require.Nil(t, mail.ReadAt)
	}
	peerAfter, err := store.ListMails(t.Context(), peer, "unread", 20, 0)
	require.NoError(t, err)
	require.Equal(t, peerBefore, peerAfter)
}

func TestMailboxStoreFlagDoesNotReplayOldEvents(t *testing.T) {
	disabled, clock := mailboxStore(t, false)
	main := mailboxIdentity(t, disabled, "owner", true)
	claim := mailAgent(t, disabled, "owner", "worker")
	startMailAgent(t, disabled, claim)
	update := types.TerminalUpdate{Result: new("old result")}
	_, err := disabled.Terminal(t.Context(), claim.Lease, types.TerminalIdle, update)
	require.NoError(t, err)
	active := mailAgent(t, disabled, "owner", "active-worker")
	startMailAgent(t, disabled, active)
	require.NoError(t, disabled.Close())
	enabled := NewStore(disabled.Path(), disabled.RuntimeID(), clock.now, MailboxEnabled(true))
	t.Cleanup(func() { _ = enabled.Close() })
	mailboxIdentity(t, enabled, "owner", true)
	_, err = enabled.Terminal(t.Context(), claim.Lease, types.TerminalIdle, update)
	require.NoError(t, err)
	startMailAgent(t, enabled, active)
	page, err := enabled.ListMails(t.Context(), main, "all", 20, 0)
	require.NoError(t, err)
	require.Empty(t, page.Mails, "neither old results nor already-running agents are replayed")
	second, err := enabled.Claim(t.Context(), "owner", claim.Agent.ID, "new task")
	require.NoError(t, err)
	startMailAgent(t, enabled, second)
	require.Empty(t, eventMails(t, enabled, claim.Agent.ID, "agent_joined"))
	_, err = enabled.Terminal(t.Context(), second.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("new result")})
	require.NoError(t, err)
	page, err = enabled.ListMails(t.Context(), main, "all", 20, 0)
	require.NoError(t, err)
	require.Len(t, page.Mails, 1)
	require.Equal(t, second.Lease.RunID, page.Mails[0].RunID)
}

func TestMailboxRoomOptInDoesNotReplayExpiredClaims(t *testing.T) {
	store, clock := mailboxStore(t, true)
	claim := mailAgent(t, store, "owner", "old-worker")
	_, err := store.MarkRunning(t.Context(), claim.Lease, "old-child")
	require.NoError(t, err)
	clock.advance(LeaseDurationSeconds + 1)
	main := mailboxIdentity(t, store, "owner", true)
	page, err := store.ListMails(t.Context(), main, "all", 20, 0)
	require.NoError(t, err)
	require.Empty(t, page.Mails)
	agent, err := store.Get(t.Context(), "owner", claim.Agent.ID, "")
	require.NoError(t, err)
	require.Equal(t, types.RunInterrupted, agent.Run.Status)
	future := mailAgent(t, store, "owner", "new-worker")
	startMailAgent(t, store, future)
	page, err = store.ListMails(t.Context(), main, "all", 20, 0)
	require.NoError(t, err)
	require.Len(t, page.Mails, 1)
	require.Equal(t, "agent_joined", page.Mails[0].Kind)
	require.Equal(t, future.Agent.ID, page.Mails[0].AgentID)
}
