package subagent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

const (
	mailboxWidgetID      = "agent-mailbox"
	mailboxWidgetLimit   = 3
	mailboxHookTimeout   = 2 * time.Second
	mailboxWidgetTimeout = 200 * time.Millisecond
)

// mailboxForCall derives membership from the host's conversation identity, never tool input.
func (a *Application) mailboxForCall(ctx context.Context, call Call) (Store, types.MailboxIdentity, error) {
	return a.resolveMailboxForCall(ctx, call, true)
}

func (a *Application) resolveMailboxForCall(ctx context.Context, call Call, create bool) (Store, types.MailboxIdentity, error) {
	if !a.runtime.mailboxEnabled {
		return nil, types.MailboxIdentity{}, errors.New(mailboxDisabledMessage)
	}
	conversationID, err := ownerConversationID(call)
	if err != nil {
		return nil, types.MailboxIdentity{}, err
	}
	store := a.runtime.store(call)
	key := runKey{path: store.Path(), id: conversationID}
	a.runtime.mu.Lock()
	identity, cached := a.runtime.mailboxIdentities[key]
	a.runtime.mu.Unlock()
	if cached {
		return store, identity, ctx.Err()
	}
	if create {
		identity, err = store.ResolveMailbox(ctx, conversationID, !isAgentChild(call))
	} else {
		identity, err = store.LookupMailbox(ctx, conversationID, !isAgentChild(call))
	}
	if err == nil {
		a.runtime.mu.Lock()
		a.runtime.mailboxIdentities[key] = identity
		a.runtime.mu.Unlock()
	}
	return store, identity, err
}

func (a *Application) restoreMailboxWidget(ctx context.Context, call Call) {
	if !a.runtime.mailboxEnabled || strings.TrimSpace(call.Info().ConversationID) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, mailboxHookTimeout)
	defer cancel()
	store, identity, err := a.resolveMailboxForCall(ctx, call, false)
	if err != nil {
		// A child's session.start may precede AttachConversation. turn.end will retry.
		call.Logger().Debug("mailbox not yet available", "error", err.Error())
		return
	}
	// Session/run starts may restore a widget that the host has discarded.
	a.runtime.mu.Lock()
	delete(a.runtime.mailboxWidgets, mailboxWidgetKey(store, identity))
	a.runtime.mu.Unlock()
	a.refreshMailboxWidget(ctx, call, store, identity)
}

func (a *Application) refreshMailboxWidget(ctx context.Context, call Call, store Store, identity types.MailboxIdentity) {
	if _, err := a.runtime.syncMailboxWidget(ctx, call.UI(), store, identity); err != nil && ctx.Err() == nil {
		call.Logger().Warn("failed to refresh agent mailbox widget", "error", err.Error())
	}
}

type mailboxWidgetState struct {
	unread int
	ids    [mailboxWidgetLimit]int64
}

func mailboxWidgetKey(store Store, identity types.MailboxIdentity) runKey {
	return runKey{path: store.Path(), id: "mailbox:" + identity.OwnerConversationID + ":" + identity.ContactID}
}

// syncMailboxWidget returns one consistent inbox snapshot for both the reminder
// and the widget. Widget contention/failure never prevents reminder delivery.
func (r *Runtime) syncMailboxWidget(ctx context.Context, ui WidgetUI, store Store, identity types.MailboxIdentity) (types.MailPage, error) {
	if !r.mailboxEnabled {
		return types.MailPage{}, nil
	}
	key := mailboxWidgetKey(store, identity)
	lock := r.widgetLock(store, key.id)
	locked := lock.TryLock()
	if locked {
		defer lock.Unlock()
	}
	page, err := store.ListMails(ctx, identity, "unread", mailboxWidgetLimit, 0)
	if err != nil || !locked || ui == nil {
		return page, err
	}
	state := mailboxWidgetState{unread: page.UnreadCount}
	for i, mail := range page.Mails {
		state.ids[i] = mail.ID
	}
	r.mu.Lock()
	previous, rendered := r.mailboxWidgets[key]
	r.mu.Unlock()
	if rendered && previous == state {
		return page, nil
	}
	timeout := min(mailboxWidgetTimeout, r.timing.HostRequestTimeout)
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < 2*timeout {
		// Keep response-delivery headroom when the database operation ran slowly.
		return page, nil
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if page.UnreadCount == 0 {
		err = ui.RemoveWidget(ctx, mailboxWidgetID)
	} else {
		err = ui.SetWidget(ctx, mailboxWidgetID, mailboxWidgetLines(page), extension.WidgetAboveComposer)
	}
	if err == nil {
		r.mu.Lock()
		r.mailboxWidgets[key] = state
		r.mu.Unlock()
	}
	return page, err
}

func mailboxWidgetLines(page types.MailPage) []extension.FrameLine {
	lines := []extension.FrameLine{extension.Styled(
		extension.StyledSpan{Text: "Inbox", Style: boldStyle},
		extension.StyledSpan{Text: fmt.Sprintf("  %d unread", page.UnreadCount), Style: dimStyle},
	)}
	for _, mail := range page.Mails {
		lines = append(lines, extension.Styled(
			extension.StyledSpan{Text: fmt.Sprintf("#%d  %s", mail.ID, collapseWhitespace(mail.Sender.Name))},
			extension.StyledSpan{Text: "  " + truncate(collapseWhitespace(mail.Subject), 100), Style: dimStyle},
		))
	}
	if page.UnreadCount > len(page.Mails) {
		lines = append(lines, extension.Styled(extension.StyledSpan{
			Text: fmt.Sprintf("… and %d more", page.UnreadCount-len(page.Mails)), Style: dimStyle,
		}))
	}
	return lines
}
