package subagent

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jingkaihe/kodelet/sdk/go/agent"
	"github.com/jingkaihe/kodelet/sdk/go/extension"
)

// DefaultClientFactory uses the SDK client, which owns bounded ACP transport
// and partial-startup and repeated-cancellation cleanup.
func DefaultClientFactory(command, cwd string, env map[string]string) AgentClient {
	return sdkClient{client: agent.NewClient(agent.ClientOptions{Command: command, CWD: cwd, Env: env})}
}

type sdkClient struct {
	client *agent.Client
}

func (c sdkClient) CreateSession(ctx context.Context, opts agent.SessionOptions) (AgentSession, error) {
	session, err := c.client.CreateSession(ctx, opts)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func (c sdkClient) Close() error {
	return c.client.Close()
}

type sdkCall struct {
	ctx *extension.CallContext
}

func newSDKEventCall(ec *extension.EventContext) Call {
	return sdkCall{ctx: &ec.CallContext}
}

func (c sdkCall) Info() extension.CallInfo { return c.ctx.CallInfo }

func (c sdkCall) DataDir() string { return c.ctx.Storage.DataDir }

func (c sdkCall) Logger() *slog.Logger { return c.ctx.Log }

func (c sdkCall) UI() WidgetUI {
	if c.ctx.UI == nil {
		return nil
	}
	return c.ctx.UI
}

type sdkToolCall struct {
	sdkCall
	tool *extension.ToolContext
}

func newSDKToolCall(tc *extension.ToolContext) ToolCall {
	return sdkToolCall{ctx: &tc.CallContext, tool: tc}
}

func (c sdkToolCall) Update(ctx context.Context, content string, data map[string]any) error {
	return c.tool.Update(ctx, content, data)
}

func (c sdkToolCall) AcquireBackgroundTask(ctx context.Context, description string) (BackgroundLease, error) {
	lease, err := c.tool.AcquireBackgroundTask(ctx, description)
	if err != nil {
		return nil, err
	}
	return hostLease{lease: lease}, nil
}

func (c sdkToolCall) ForkConversation(ctx context.Context, name string) (string, error) {
	return c.tool.ForkConversation(ctx, extension.ForkOptions{Name: name, AsChild: true})
}

type hostLease struct {
	lease *extension.BackgroundTaskLease
}

// Close releases the lease. Once the host connection has closed, the host has
// already dropped every lease it granted, so there is nothing left to retry.
func (l hostLease) Close(ctx context.Context) error {
	if err := l.lease.Close(ctx); err != nil && !errors.Is(err, extension.ErrRuntimeClosed) {
		return err
	}
	return nil
}
