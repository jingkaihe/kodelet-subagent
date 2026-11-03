package types

// Contact identifies a participant in a main conversation's mailbox.
type Contact struct {
	ID     string
	Name   string
	Kind   string
	Status string
	Task   string
}

// MailboxIdentity is resolved from a host-provided conversation ID, never tool input.
type MailboxIdentity struct {
	OwnerConversationID string
	ContactID           string
}

// Mail is a recipient-owned message. ReadAt means fetched, not acted upon.
// Sender records authorship; its Status and Task are not populated.
type Mail struct {
	ID          int64
	Sender      Contact
	RecipientID string
	Subject     string
	Body        string
	Kind        string
	AgentID     string
	RunID       string
	CreatedAt   float64
	ReadAt      *float64
}

// MailPage lists metadata only: mail bodies are empty until explicitly read.
type MailPage struct {
	Mails       []Mail
	Total       int
	UnreadCount int
	HasMore     bool
}
