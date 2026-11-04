package subagent

import (
	"context"
	"testing"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/stretchr/testify/require"
)

func TestMailboxWidgetCannotHoldUpCommittedToolReplies(t *testing.T) {
	for _, blocker := range []string{"slow host", "held lock", "short deadline"} {
		for _, operation := range []string{"send", "read"} {
			t.Run(blocker+"/"+operation, func(t *testing.T) {
				t.Setenv(MailboxEnv, "enabled")
				f := newFixture(t)
				call := f.call("owner-1")
				store, identity, err := f.app.mailboxForCall(f.ctx, call)
				require.NoError(t, err)
				sender, senderInfo := mailboxSender(t, f, store, "owner-1")
				mail, err := store.SendMail(f.ctx, sender, "main", "Read this", "Delivered body")
				require.NoError(t, err)
				if blocker != "held lock" {
					call.ui.block, call.ui.blockStarted = newEvent(), newEvent()
					defer call.ui.block.set()
				} else {
					lock := f.runtime.widgetLock(store, "mailbox:"+identity.OwnerConversationID+":"+identity.ContactID)
					lock.Lock()
					// Release even if blocking Lock regresses, so a failure cannot hang the suite.
					release := time.AfterFunc(time.Second, lock.Unlock)
					defer func() {
						if release.Stop() {
							lock.Unlock()
						}
					}()
				}
				deadline := time.Second
				if blocker == "short deadline" {
					deadline = mailboxWidgetTimeout
				}
				ctx, cancel := context.WithTimeout(f.ctx, deadline)
				defer cancel()
				started := time.Now()
				var result extension.ToolResult
				if operation == "send" {
					sendCall := f.call(senderInfo.ConversationID, invokedBy("subagent"))
					sendCall.ui = call.ui
					result = f.app.MessageContact(ctx, MessageContactInput{ContactID: "main", Subject: "Sent", Body: "Stored body"}, sendCall)
				} else {
					result = f.app.ReadMails(ctx, ReadMailsInput{IDs: []int64{mail.ID}}, call)
				}
				require.Less(t, time.Since(started), 750*time.Millisecond, "UI refresh must not consume the tool deadline")
				require.NoError(t, ctx.Err())
				require.Empty(t, result.Error)
				if blocker == "short deadline" {
					select {
					case <-call.ui.blockStarted.ch:
						t.Fatal("widget refresh must be skipped when the tool deadline is near")
					default:
					}
				}
				page, err := store.ListMails(f.ctx, identity, "unread", 20, 0)
				require.NoError(t, err)
				if operation == "send" {
					require.Contains(t, result.Content, "Stored mail #")
					require.Equal(t, 2, page.UnreadCount)
				} else {
					require.Contains(t, result.Content, "Delivered body")
					require.Zero(t, page.UnreadCount)
				}
			})
		}
	}
}

func TestSendingMailDoesNotRefreshSendersInbox(t *testing.T) {
	t.Setenv(MailboxEnv, "enabled")
	f := newFixture(t)
	call := f.call("owner-1")
	store := f.store(call)
	worker := mailboxWorker(t, f, store, "receiver")
	_, _, err := f.app.mailboxForCall(f.ctx, call)
	require.NoError(t, err)
	call.ui.block, call.ui.blockStarted = newEvent(), newEvent()
	defer call.ui.block.set()
	result := f.app.MessageContact(f.ctx, MessageContactInput{ContactID: worker.ID, Subject: "Hello", Body: "Read later"}, call)
	require.Empty(t, result.Error)
	select {
	case <-call.ui.blockStarted.ch:
		t.Fatal("sending must not refresh the sender's unchanged inbox")
	default:
	}
	require.Empty(t, call.ui.text(mailboxWidgetID))
}
