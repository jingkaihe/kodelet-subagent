package subagent

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/jingkaihe/kodelet/sdk/go/extension/extensiontest"
	"github.com/stretchr/testify/require"

	"github.com/jingkaihe/kodelet-subagent/internal/db"
	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

func TestMailboxToolInputSchemas(t *testing.T) {
	defaults := ListMailsInput{Status: "unread", Limit: 20}
	listed, err := decodeInput("list_mails", json.RawMessage(`{}`), defaults)
	require.NoError(t, err)
	require.Equal(t, defaults, listed)
	listed, err = decodeInput("list_mails", json.RawMessage(`{"limit":2.0,"offset":1.0,"status":"all"}`), defaults)
	require.NoError(t, err)
	require.Equal(t, ListMailsInput{Status: "all", Limit: 2, Offset: 1}, listed)
	read, err := decodeInput("read_mails", json.RawMessage(`{"ids":[1.0,2e0]}`), ReadMailsInput{})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, read.IDs)

	for _, input := range []string{
		`{"status":"unknown"}`, `{"limit":0}`, `{"limit":51}`, `{"limit":1.5}`,
		`{"offset":-1}`, `{"offset":0.5}`, `{"offset":1e30}`, `{"status":null}`,
		`{"owner_conversation_id":"someone-else"}`,
	} {
		_, err := decodeInput("list_mails", json.RawMessage(input), defaults)
		require.Error(t, err, input)
	}
	for _, input := range []string{
		`{}`, `{"ids":[]}`, `{"ids":[0]}`, `{"ids":[-1]}`, `{"ids":[1,1.0]}`,
		`{"ids":[1.5]}`, `{"ids":[1,2,3,4,5,6,7,8,9,10,11]}`,
		`{"ids":[9007199254740992]}`, `{"ids":[1],"mark_read":false}`,
	} {
		_, err := decodeInput("read_mails", json.RawMessage(input), ReadMailsInput{})
		require.Error(t, err, input)
	}
	for _, input := range []map[string]any{
		{"contact_id": "main", "body": "missing subject"},
		{"contact_id": "main", "subject": "hello", "body": ""},
		{"contact_id": "main", "subject": strings.Repeat("x", db.MaxMailSubjectBytes+1), "body": "hello"},
		{"contact_id": "main", "subject": "hello", "body": strings.Repeat("x", db.MaxMailBodyBytes+1)},
		{"contact_id": "main", "subject": "hello", "body": "hello", "from": "system"},
	} {
		raw, err := json.Marshal(input)
		require.NoError(t, err)
		_, err = decodeInput("message_contact", raw, MessageContactInput{})
		require.Error(t, err)
	}
}

func TestMailboxToolRegistrationAndDisabledHandlers(t *testing.T) {
	t.Setenv("KODELET_AGENT_MAILBOX", "")
	f := newFixture(t)
	peer := newConnectionPeer(f.app)
	defer peer.conn.Close(nil)
	manifest := peer.initialize(t, 1, map[string]any{"runtime": map[string]any{"backgroundTasks": true}})
	require.Equal(t, AgentToolNames, toolNames(manifest))
	call := f.call("owner-1")
	for _, result := range []extension.ToolResult{
		f.app.ListContacts(f.ctx, ListContactsInput{}, call),
		f.app.MessageContact(f.ctx, MessageContactInput{}, call),
		f.app.ListMails(f.ctx, ListMailsInput{}, call),
		f.app.ReadMails(f.ctx, ReadMailsInput{}, call),
	} {
		require.Equal(t, mailboxDisabledMessage, result.Error)
	}

	t.Setenv("KODELET_AGENT_MAILBOX", "enabled")
	t.Setenv(RecursionGuardEnv, "1")
	child := newConnectionPeer(NewApplication(f.newRuntime("mailbox-child")))
	defer child.conn.Close(nil)
	// Messaging does not require a background lease or permission to manage agents.
	manifest = child.initialize(t, 1, map[string]any{})
	require.ElementsMatch(t, []string{"list_contacts", "message_contact", "list_mails", "read_mails"}, toolNames(manifest))
}

func executeMailbox(t *testing.T, f *fixture, h *extensiontest.Harness, caller extension.CallInfo, name string, input map[string]any) (extension.ToolResult, map[string]any) {
	t.Helper()
	result, err := h.ExecuteTool(f.ctx, name, input, extensiontest.WithCall(caller))
	require.NoError(t, err)
	require.Empty(t, result.Error, result.Content)
	payload, ok := result.StructuredContent.(map[string]any)
	require.True(t, ok)
	require.NoError(t, resolveSchema(t, toolSchemas[name].output).Validate(payload))
	presentation := result.Data["presentation"].(map[string]any)
	require.Equal(t, "text", presentation["format"])
	require.NotEmpty(t, presentation["summary"])
	require.Empty(t, presentation["body"])
	return result, payload
}

func mailboxWorker(t *testing.T, f *fixture, store Store, name string) types.Agent {
	t.Helper()
	claim, err := store.Create(f.ctx, "owner-1", name, "Investigate "+name, f.cwd, types.ContextFresh)
	require.NoError(t, err)
	_, err = store.MarkRunning(f.ctx, claim.Lease, "child-"+name)
	require.NoError(t, err)
	agent, err := store.Terminal(f.ctx, claim.Lease, types.TerminalIdle, types.TerminalUpdate{Result: new("done")})
	require.NoError(t, err)
	return agent
}

func TestMailboxToolsWorkerMessagingAndReadState(t *testing.T) {
	t.Setenv("KODELET_AGENT_MAILBOX", "enabled")
	f := newFixture(t)
	store := f.store(f.call("owner-1"))
	first := mailboxWorker(t, f, store, "researcher")
	second := mailboxWorker(t, f, store, "reviewer")
	h := f.harness(t)
	main := extension.CallInfo{ConversationID: "owner-1"}
	worker := extension.CallInfo{ConversationID: *first.ConversationID, InvokedBy: "subagent"}
	reviewer := extension.CallInfo{ConversationID: *second.ConversationID, InvokedBy: "subagent"}

	_, contacts := executeMailbox(t, f, h, worker, "list_contacts", map[string]any{})
	require.Equal(t, first.ID, contacts["self_contact_id"])
	require.Len(t, contacts["contacts"], 3)
	for _, recipient := range []struct {
		id   string
		call extension.CallInfo
	}{
		{"main", main}, {second.ID, reviewer},
	} {
		sent, data := executeMailbox(t, f, h, worker, "message_contact", map[string]any{
			"contact_id": recipient.id, "subject": "Review findings", "body": "**Do not render as Markdown**\nThe cancellation path needs a test.",
		})
		mail := data["mail"].(map[string]any)
		id := mail["id"]
		require.Equal(t, recipient.id, mail["recipient_id"])
		require.Equal(t, first.ID, mail["from"].(map[string]any)["contact_id"])
		require.Equal(t, "agent", mail["from"].(map[string]any)["kind"])
		require.NotContains(t, mail, "body")
		require.NotContains(t, sent.Content, "cancellation path")

		listed, inbox := executeMailbox(t, f, h, recipient.call, "list_mails", map[string]any{})
		require.Equal(t, float64(1), inbox["unread_count"])
		require.False(t, inbox["has_more"].(bool))
		listedMail := inbox["mails"].([]any)[0].(map[string]any)
		require.Equal(t, id, listedMail["id"])
		require.Nil(t, listedMail["read_at"])
		require.NotContains(t, listedMail, "body")
		require.NotContains(t, listed.Content, "cancellation path")

		read, batch := executeMailbox(t, f, h, recipient.call, "read_mails", map[string]any{"ids": []any{id}})
		readMail := batch["mails"].([]any)[0].(map[string]any)
		require.NotNil(t, readMail["read_at"])
		require.Contains(t, readMail["body"], "cancellation path")
		require.Contains(t, read.Content, "From: researcher (agent; "+first.ID+")")
		require.Contains(t, read.Content, "**Do not render as Markdown**")
		require.NotContains(t, read.Data, "mails", "mail bodies must not be duplicated in presentation metadata")
		_, again := executeMailbox(t, f, h, recipient.call, "read_mails", map[string]any{"ids": []any{id}})
		require.Equal(t, batch, again, "reads must be replayable with the original read timestamp")
		_, unread := executeMailbox(t, f, h, recipient.call, "list_mails", map[string]any{})
		require.Zero(t, unread["unread_count"])
		require.Empty(t, unread["mails"])
		_, all := executeMailbox(t, f, h, recipient.call, "list_mails", map[string]any{"status": "read"})
		require.Len(t, all["mails"], 1)
	}
	// Mail sent to an idle recipient must not start another run.
	require.Equal(t, types.AgentIdle, f.get(store, "owner-1", second.ID).Status)
	require.Equal(t, 1, f.get(store, "owner-1", second.ID).Generation)
	denied := f.app.SteerAgent(f.ctx, SteerAgentInput{AgentID: second.ID, Message: "control"}, f.call(*first.ConversationID, invokedBy("subagent")))
	require.Equal(t, childToolErrorMessage, denied.Error)
}

func TestMailboxToolsRejectCrossInboxReadsAndOversizedBatches(t *testing.T) {
	t.Setenv("KODELET_AGENT_MAILBOX", "enabled")
	f := newFixture(t)
	h := f.harness(t)
	main := extension.CallInfo{ConversationID: "owner-1"}
	other := extension.CallInfo{ConversationID: "owner-2"}
	_, foreign := executeMailbox(t, f, h, other, "message_contact", map[string]any{"contact_id": "main", "subject": "Private", "body": "other inbox"})
	foreignID := foreign["mail"].(map[string]any)["id"]
	ids := make([]any, 0, 4)
	for range 4 {
		_, sent := executeMailbox(t, f, h, main, "message_contact", map[string]any{
			"contact_id": "main", "subject": "Large message", "body": strings.Repeat("x", db.MaxMailBodyBytes),
		})
		ids = append(ids, sent["mail"].(map[string]any)["id"])
	}
	for _, batch := range [][]any{{ids[0], foreignID}, ids} {
		result, err := h.ExecuteTool(f.ctx, "read_mails", map[string]any{"ids": batch}, extensiontest.WithCall(main))
		require.NoError(t, err)
		require.NotEmpty(t, result.Error)
		require.Nil(t, result.StructuredContent)
		_, inbox := executeMailbox(t, f, h, main, "list_mails", map[string]any{})
		require.Equal(t, float64(4), inbox["unread_count"], "failed batch must not mark any mail read")
	}
	// Pagination accepts integral JSON floats and doesn't mark anything read.
	_, page := executeMailbox(t, f, h, main, "list_mails", map[string]any{"limit": 1.0, "offset": 1.0})
	require.Len(t, page["mails"], 1)
	require.True(t, page["has_more"].(bool))
	require.Equal(t, ids[2], page["mails"].([]any)[0].(map[string]any)["id"])
	result, err := h.ExecuteTool(f.ctx, "message_contact", map[string]any{
		"contact_id": "main", "subject": strings.Repeat("界", 100), "body": "UTF-8 byte limit",
	}, extensiontest.WithCall(main))
	require.NoError(t, err)
	require.NotEmpty(t, result.Error, "the store must enforce byte limits beyond JSON Schema character limits")
}
