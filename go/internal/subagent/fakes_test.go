package subagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/agent"
	"github.com/jingkaihe/kodelet/sdk/go/extension"
	"github.com/jingkaihe/kodelet/sdk/go/taskprogress"

	"github.com/jingkaihe/kodelet-subagent/internal/types"
)

const eventTimeout = 5 * time.Second

// event is a one-shot signal, like asyncio.Event.
type event struct {
	once sync.Once
	ch   chan struct{}
}

func newEvent() *event { return &event{ch: make(chan struct{})} }

func (e *event) set() { e.once.Do(func() { close(e.ch) }) }

func (e *event) isSet() bool {
	select {
	case <-e.ch:
		return true
	default:
		return false
	}
}

func (e *event) wait(t *testing.T) {
	t.Helper()
	select {
	case <-e.ch:
	case <-time.After(eventTimeout):
		t.Fatal("timed out waiting for event")
	}
}

// waitCtx blocks until the event is set or ctx ends.
func (e *event) waitCtx(ctx context.Context) error {
	select {
	case <-e.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not satisfied")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fakeUI records the background-agent widget.
type fakeUI struct {
	mu      sync.Mutex
	widgets map[string][]extension.FrameLine
	// block, when set, holds SetWidget until released or canceled.
	block        *event
	blockStarted *event
}

func newFakeUI() *fakeUI { return &fakeUI{widgets: map[string][]extension.FrameLine{}} }

func (u *fakeUI) SetWidget(ctx context.Context, id string, lines []extension.FrameLine, _ extension.WidgetPlacement) error {
	u.mu.Lock()
	block, started := u.block, u.blockStarted
	u.mu.Unlock()
	if block != nil {
		started.set()
		if err := block.waitCtx(ctx); err != nil {
			return err
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.widgets[id] = lines
	return nil
}

func (u *fakeUI) RemoveWidget(_ context.Context, id string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.widgets, id)
	return nil
}

func (u *fakeUI) text(id string) string {
	u.mu.Lock()
	defer u.mu.Unlock()
	rendered := make([]string, 0, len(u.widgets[id]))
	for _, line := range u.widgets[id] {
		if line.Spans == nil {
			rendered = append(rendered, line.Text)
			continue
		}
		var text strings.Builder
		for _, span := range line.Spans {
			text.WriteString(span.Text)
		}
		rendered = append(rendered, text.String())
	}
	return strings.Join(rendered, "\n")
}

// fakeLease is a background task lease.
type fakeLease struct {
	description string

	mu           sync.Mutex
	failures     int
	closeCalls   int
	block        bool
	closeStarted *event
	closeRelease *event
}

func (l *fakeLease) Close(ctx context.Context) error {
	l.mu.Lock()
	l.closeCalls++
	block := l.block
	l.mu.Unlock()
	l.closeStarted.set()
	if block {
		if err := l.closeRelease.waitCtx(ctx); err != nil {
			return err
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failures > 0 {
		l.failures--
		return errors.New("temporary background lease release failure")
	}
	return nil
}

func (l *fakeLease) closes() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeCalls
}

type toolUpdate struct {
	content string
	data    map[string]any
}

func (u toolUpdate) taskRun() (taskprogress.Snapshot, bool) {
	snapshot, ok := u.data["taskRun"].(taskprogress.Snapshot)
	return snapshot, ok
}

// fakeCall is the host context of a tool or event call.
type fakeCall struct {
	info    extension.CallInfo
	dataDir string
	ui      *fakeUI

	mu              sync.Mutex
	blockFork       bool
	blockAcquire    bool
	blockRelease    bool
	releaseFailures int
	forkNames       []string
	updates         []toolUpdate
	leases          []*fakeLease
	forkStarted     *event
	forkRelease     *event
	acquireStarted  *event
	acquireRelease  *event
}

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func (c *fakeCall) Info() extension.CallInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info
}

func (c *fakeCall) setProfile(profile string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.info.Profile = profile
}

func (c *fakeCall) DataDir() string      { return c.dataDir }
func (c *fakeCall) UI() WidgetUI         { return c.ui }
func (c *fakeCall) Logger() *slog.Logger { return discardLogger }

func (c *fakeCall) Update(_ context.Context, content string, data map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updates = append(c.updates, toolUpdate{content: content, data: data})
	return nil
}

func (c *fakeCall) toolUpdates() []toolUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]toolUpdate(nil), c.updates...)
}

func (c *fakeCall) AcquireBackgroundTask(ctx context.Context, description string) (BackgroundLease, error) {
	c.acquireStarted.set()
	c.mu.Lock()
	block := c.blockAcquire
	c.mu.Unlock()
	if block {
		if err := c.acquireRelease.waitCtx(ctx); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lease := &fakeLease{
		description:  description,
		failures:     c.releaseFailures,
		block:        c.blockRelease,
		closeStarted: newEvent(),
		closeRelease: newEvent(),
	}
	c.leases = append(c.leases, lease)
	return lease, nil
}

func (c *fakeCall) backgroundLeases() []*fakeLease {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*fakeLease(nil), c.leases...)
}

func (c *fakeCall) ForkConversation(ctx context.Context, name string) (string, error) {
	c.mu.Lock()
	c.forkNames = append(c.forkNames, name)
	count := len(c.forkNames)
	block := c.blockFork
	c.mu.Unlock()
	c.forkStarted.set()
	if block {
		if err := c.forkRelease.waitCtx(ctx); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("child-%s-%d", c.info.ConversationID, count), nil
}

func (c *fakeCall) forks() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.forkNames...)
}

// fakeACP plays the kodelet acp side of every client created by its factory.
type fakeACP struct {
	mu             sync.Mutex
	clients        []*fakeClient
	blockRuns      bool
	blockStartup   bool
	blockClose     bool
	closeFailures  int
	acceptSteering bool
	steerOutcomes  []agent.SteeringOutcome
	runFailure     error
	runResponse    *agent.Response
	runRelease     *event
	runStarted     *event
	startupRelease *event
	startupStarted *event
	closeRelease   *event
	closeStarted   *event
	freshSessions  int
}

func newFakeACP() *fakeACP {
	return &fakeACP{
		acceptSteering: true,
		runRelease:     newEvent(),
		runStarted:     newEvent(),
		startupRelease: newEvent(),
		startupStarted: newEvent(),
		closeRelease:   newEvent(),
		closeStarted:   newEvent(),
	}
}

func (f *fakeACP) factory(command, cwd string, env map[string]string) AgentClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	client := &fakeClient{acp: f, command: command, cwd: cwd, env: env}
	f.clients = append(f.clients, client)
	return client
}

func (f *fakeACP) update(fn func(*fakeACP)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

// resetRun replaces the run gates, like reassigning the Python class events.
func (f *fakeACP) resetRun() {
	f.update(func(f *fakeACP) {
		f.runRelease = newEvent()
		f.runStarted = newEvent()
	})
}

func (f *fakeACP) started() *event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runStarted
}

func (f *fakeACP) release() *event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runRelease
}

func (f *fakeACP) instances() []*fakeClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*fakeClient(nil), f.clients...)
}

func (f *fakeACP) client(t *testing.T, index int) *fakeClient {
	t.Helper()
	clients := f.instances()
	if index < 0 {
		index += len(clients)
	}
	if index < 0 || index >= len(clients) {
		t.Fatalf("client %d does not exist; %d created", index, len(clients))
	}
	return clients[index]
}

func (f *fakeACP) releaseAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runRelease.set()
	f.startupRelease.set()
	f.closeRelease.set()
}

type fakeClient struct {
	acp     *fakeACP
	command string
	cwd     string
	env     map[string]string

	mu          sync.Mutex
	createCalls []agent.SessionOptions
	sessions    []*fakeSession
	closeCalls  int
	closed      bool
}

func (c *fakeClient) CreateSession(ctx context.Context, opts agent.SessionOptions) (AgentSession, error) {
	c.mu.Lock()
	c.createCalls = append(c.createCalls, opts)
	c.mu.Unlock()
	c.acp.mu.Lock()
	block, release := c.acp.blockStartup, c.acp.startupRelease
	c.acp.startupStarted.set()
	c.acp.mu.Unlock()
	if block {
		if err := release.waitCtx(ctx); err != nil {
			return nil, err
		}
	}
	id := opts.Resume
	if id == "" {
		c.acp.mu.Lock()
		c.acp.freshSessions++
		id = fmt.Sprintf("fresh-%d", c.acp.freshSessions)
		c.acp.mu.Unlock()
	}
	session := &fakeSession{client: c, id: id, subscribers: map[int]func(agent.Event){}, runStarted: newEvent(), steerReceived: newEvent()}
	c.mu.Lock()
	c.sessions = append(c.sessions, session)
	c.mu.Unlock()
	return session, nil
}

func (c *fakeClient) Close() error {
	c.mu.Lock()
	c.closeCalls++
	c.mu.Unlock()
	c.acp.mu.Lock()
	c.acp.closeStarted.set()
	if c.acp.closeFailures > 0 {
		c.acp.closeFailures--
		c.acp.mu.Unlock()
		return errors.New("temporary client close failure")
	}
	block, release := c.acp.blockClose, c.acp.closeRelease
	c.acp.mu.Unlock()
	if block {
		<-release.ch
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, session := range c.sessions {
		session.mu.Lock()
		session.closeCalls++
		session.mu.Unlock()
	}
	c.closed = true
	return nil
}

func (c *fakeClient) options(t *testing.T) agent.SessionOptions {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.createCalls) == 0 {
		t.Fatal("client created no sessions")
	}
	return c.createCalls[0]
}

func (c *fakeClient) session(t *testing.T) *fakeSession {
	t.Helper()
	var session *fakeSession
	waitUntil(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.sessions) > 0 {
			session = c.sessions[0]
		}
		return session != nil
	})
	return session
}

func (c *fakeClient) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeClient) closes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCalls
}

type fakeSession struct {
	client *fakeClient
	id     string

	mu            sync.Mutex
	runCalls      []string
	steerCalls    []string
	closeCalls    int
	subscribers   map[int]func(agent.Event)
	nextID        int
	runStarted    *event
	steerReceived *event
}

func (s *fakeSession) ID() string { return s.id }

func (s *fakeSession) Subscribe(handler func(agent.Event)) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	s.subscribers[id] = handler
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.subscribers, id)
	}
}

func (s *fakeSession) listeners() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subscribers)
}

func (s *fakeSession) emit(eventType string, data any) {
	s.mu.Lock()
	handlers := make([]func(agent.Event), 0, len(s.subscribers))
	for id := range s.nextID {
		if handler, ok := s.subscribers[id]; ok {
			handlers = append(handlers, handler)
		}
	}
	s.mu.Unlock()
	for _, handler := range handlers {
		handler(agent.Event{Type: eventType, Data: data, ConversationID: s.id})
	}
}

func (s *fakeSession) toolCall(id, name string, input map[string]any) {
	s.emit(agent.EventToolCall, agent.ToolCall{ToolCallID: id, ToolName: name, Input: input})
}

func (s *fakeSession) toolUpdate(id, result string) {
	s.emit(agent.EventToolUpdate, agent.ToolResult{ToolCallID: id, Result: result})
}

func (s *fakeSession) toolResult(id, status, result string) {
	s.emit(agent.EventToolResult, agent.ToolResult{ToolCallID: id, Status: status, Result: result})
}

func (s *fakeSession) RunAndWait(ctx context.Context, opts agent.RunOptions) (*agent.Response, error) {
	s.mu.Lock()
	s.runCalls = append(s.runCalls, opts.Message)
	s.mu.Unlock()
	s.runStarted.set()
	acp := s.client.acp
	acp.mu.Lock()
	acp.runStarted.set()
	block, release := acp.blockRuns, acp.runRelease
	acp.mu.Unlock()
	if block {
		if err := release.waitCtx(ctx); err != nil {
			return nil, err
		}
	}
	acp.mu.Lock()
	defer acp.mu.Unlock()
	if acp.runFailure != nil {
		return nil, acp.runFailure
	}
	if acp.runResponse != nil {
		response := *acp.runResponse
		return &response, nil
	}
	return &agent.Response{Content: "result for " + opts.Message, ConversationID: s.id}, nil
}

func (s *fakeSession) Steer(_ context.Context, message string) (agent.SteerResult, error) {
	s.mu.Lock()
	s.steerCalls = append(s.steerCalls, message)
	s.mu.Unlock()
	s.steerReceived.set()
	acp := s.client.acp
	acp.mu.Lock()
	defer acp.mu.Unlock()
	outcome := agent.SteeringFailed
	switch {
	case len(acp.steerOutcomes) > 0:
		outcome = acp.steerOutcomes[0]
		acp.steerOutcomes = acp.steerOutcomes[1:]
	case acp.acceptSteering:
		outcome = agent.SteeringInjected
	}
	result := agent.SteerResult{Outcome: outcome}
	if outcome == agent.SteeringPromptRequired {
		result.Reason = "noRunningTurn"
	}
	return result, nil
}

func (s *fakeSession) steers() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.steerCalls...)
}

// hookedStore lets a test intercept individual store operations.
type hookedStore struct {
	Store
	attachConversation          func(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error)
	attachCancelingConversation func(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error)
	terminal                    func(ctx context.Context, lease *types.Lease, status types.TerminalStatus, update types.TerminalUpdate) (types.Agent, error)
	cancel                      func(ctx context.Context, ownerID, agentID string) (types.Agent, error)
}

func (s *hookedStore) AttachConversation(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error) {
	if s.attachConversation != nil {
		return s.attachConversation(ctx, lease, conversationID)
	}
	return s.Store.AttachConversation(ctx, lease, conversationID)
}

func (s *hookedStore) AttachCancelingConversation(ctx context.Context, lease *types.Lease, conversationID string) (types.Agent, error) {
	if s.attachCancelingConversation != nil {
		return s.attachCancelingConversation(ctx, lease, conversationID)
	}
	return s.Store.AttachCancelingConversation(ctx, lease, conversationID)
}

func (s *hookedStore) Terminal(ctx context.Context, lease *types.Lease, status types.TerminalStatus, update types.TerminalUpdate) (types.Agent, error) {
	if s.terminal != nil {
		return s.terminal(ctx, lease, status, update)
	}
	return s.Store.Terminal(ctx, lease, status, update)
}

func (s *hookedStore) Cancel(ctx context.Context, ownerID, agentID string) (types.Agent, error) {
	if s.cancel != nil {
		return s.cancel(ctx, ownerID, agentID)
	}
	return s.Store.Cancel(ctx, ownerID, agentID)
}

// sqliteError is a store error carrying a SQLite result code.
type sqliteError struct {
	code    int
	message string
}

func (e sqliteError) Error() string { return e.message }
func (e sqliteError) Code() int     { return e.code }
