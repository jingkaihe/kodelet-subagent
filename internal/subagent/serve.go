package subagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/jingkaihe/kodelet/sdk/go/extension"
)

var errHostDisconnected = errors.New("extension host disconnected")

// Connection serves one host connection. Agent tools are only advertised to
// hosts that support durable background tasks, and each initialization lets
// the runtime accept work again after a previous session ended.
type Connection struct {
	app     *Application
	runtime *extension.Runtime
	send    extension.Transport

	mu      sync.Mutex
	pending map[string]bool
}

// NewConnection returns a connection that sends messages with send.
func (a *Application) NewConnection(send extension.Transport, opts *extension.RuntimeOptions) *Connection {
	conn := &Connection{app: a, send: send, pending: map[string]bool{}}
	conn.runtime = extension.NewRuntime(a.extension, extension.TransportFunc(conn.deliver), opts)
	return conn
}

// Receive handles one incoming JSON-RPC message.
func (c *Connection) Receive(message []byte) error {
	var request struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			Capabilities map[string]any `json:"capabilities"`
		} `json:"params"`
	}
	if json.Unmarshal(message, &request) == nil && request.Method == "extension.initialize" && len(request.ID) > 0 {
		c.app.runtime.ResetForInitialize()
		c.mu.Lock()
		c.pending[idKey(request.ID)] = backgroundTasksSupported(request.Params.Capabilities)
		c.mu.Unlock()
	}
	return c.runtime.Receive(message)
}

// Close aborts in-flight handlers and runs session.end handlers once.
func (c *Connection) Close(cause error) {
	c.runtime.Close(cause)
}

func (c *Connection) deliver(ctx context.Context, message []byte) error {
	return c.send.Send(ctx, c.gateInitializeResult(message))
}

// gateInitializeResult removes agent tools from initialize results sent to
// hosts without background task support.
func (c *Connection) gateInitializeResult(message []byte) []byte {
	var response map[string]json.RawMessage
	if json.Unmarshal(message, &response) != nil || response["result"] == nil || response["id"] == nil {
		return message
	}
	key := idKey(response["id"])
	c.mu.Lock()
	supported, ok := c.pending[key]
	delete(c.pending, key)
	c.mu.Unlock()
	if !ok || supported {
		return message
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(response["result"], &result) != nil {
		return message
	}
	var tools []map[string]any
	if json.Unmarshal(result["tools"], &tools) != nil {
		return message
	}
	kept := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		if name, _ := tool["name"].(string); !slices.Contains(AgentToolNames, name) {
			kept = append(kept, tool)
		}
	}
	encodedTools, err1 := json.Marshal(kept)
	result["tools"] = encodedTools
	encodedResult, err2 := json.Marshal(result)
	response["result"] = encodedResult
	encoded, err3 := json.Marshal(response)
	if err := errors.Join(err1, err2, err3); err != nil {
		return message
	}
	return encoded
}

func idKey(id json.RawMessage) string {
	var compact bytes.Buffer
	if json.Compact(&compact, id) != nil {
		return string(id)
	}
	return compact.String()
}

func backgroundTasksSupported(capabilities map[string]any) bool {
	runtime, _ := capabilities["runtime"].(map[string]any)
	supported, _ := runtime["backgroundTasks"].(bool)
	return supported
}

// Serve speaks the extension protocol over in and out. It returns nil when in
// reaches EOF and the context error when ctx ends.
func (a *Application) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	writer := &frameWriter{writer: out}
	conn := a.NewConnection(extension.TransportFunc(func(_ context.Context, message []byte) error {
		return writer.write(message)
	}), nil)
	reader := newFrameReader(in)
	readErr := make(chan error, 1)
	go func() {
		for {
			payload, err := reader.read()
			if err == nil {
				err = conn.Receive(payload)
			}
			if err != nil {
				readErr <- err
				return
			}
		}
	}()
	select {
	case err := <-readErr:
		conn.Close(errHostDisconnected)
		if errors.Is(err, io.EOF) || errors.Is(err, extension.ErrRuntimeClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		conn.Close(context.Cause(ctx))
		return ctx.Err()
	}
}

// Run serves over stdin and stdout, then waits up to drainTimeout for owned
// agents to stop and their claims to be released. os.Stdout is redirected to
// stderr while serving so stray output cannot corrupt the protocol stream.
func (a *Application) Run(ctx context.Context, drainTimeout time.Duration) error {
	protocol := os.Stdout
	os.Stdout = os.Stderr
	defer func() { os.Stdout = protocol }()
	serveErr := a.Serve(ctx, os.Stdin, protocol)
	drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := a.runtime.Shutdown(drainCtx); err != nil {
		fmt.Fprintf(os.Stderr, "subagent: shutdown did not finish: %v\n", err)
	}
	return serveErr
}
