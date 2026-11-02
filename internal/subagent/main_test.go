package subagent

import (
	"bufio"
	"encoding/json"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Environment read by the fake kodelet acp process.
const (
	fakeACPModeEnv     = "SUBAGENT_TEST_ACP_MODE"
	fakeACPPIDEnv      = "SUBAGENT_TEST_ACP_PID"
	fakeACPArgsEnv     = "SUBAGENT_TEST_ACP_ARGS"
	fakeACPRequestsEnv = "SUBAGENT_TEST_ACP_REQUESTS"
	fakeACPBytesEnv    = "SUBAGENT_TEST_ACP_BYTES"
	fakeACPFinishEnv   = "SUBAGENT_TEST_ACP_FINISH"
)

// TestMain doubles as a fake kodelet acp process when the test binary is
// started by the SDK client.
func TestMain(m *testing.M) {
	if mode := os.Getenv(fakeACPModeEnv); mode != "" {
		os.Exit(runFakeACP(mode))
	}
	os.Exit(m.Run())
}

type fakeACPServer struct {
	mode      string
	sessionID string

	mu      sync.Mutex
	output  *json.Encoder
	pending json.RawMessage
}

func runFakeACP(mode string) int {
	if mode == "ignore-term" {
		signal.Ignore(syscall.SIGTERM)
	}
	_ = os.WriteFile(os.Getenv(fakeACPPIDEnv), []byte(strconv.Itoa(os.Getpid())), 0o644)
	if path := os.Getenv(fakeACPArgsEnv); path != "" {
		args, _ := json.Marshal(os.Args[1:])
		_ = os.WriteFile(path, args, 0o644)
	}
	server := &fakeACPServer{mode: mode, sessionID: "saved-child", output: json.NewEncoder(os.Stdout)}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for scanner.Scan() {
		server.handle(scanner.Bytes())
	}
	return 0
}

func (s *fakeACPServer) emit(message map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	message["jsonrpc"] = "2.0"
	_ = s.output.Encode(message)
}

func (s *fakeACPServer) update(value map[string]any) {
	s.emit(map[string]any{"method": "session/update", "params": map[string]any{"sessionId": s.sessionID, "update": value}})
}

func (s *fakeACPServer) text(value string) {
	s.update(map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": value}})
}

func (s *fakeACPServer) respond(id json.RawMessage, result any) {
	s.emit(map[string]any{"id": id, "result": result})
}

func toolContent(text string) []any {
	return []any{map[string]any{"type": "content", "content": map[string]any{"type": "text", "text": text}}}
}

func (s *fakeACPServer) handle(line []byte) {
	var request struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params map[string]any  `json:"params"`
	}
	if json.Unmarshal(line, &request) != nil {
		return
	}
	if path := os.Getenv(fakeACPRequestsEnv); path != "" {
		file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = file.Write(append(append([]byte(nil), line...), '\n'))
			_ = file.Close()
		}
	}
	switch request.Method {
	case "initialize":
		meta := map[string]any{"steering": map[string]any{"supported": true}}
		if s.mode != "no-hierarchy" {
			meta["conversationHierarchy"] = map[string]any{"version": 1}
		}
		switch s.mode {
		case "no-extensions":
		case "unsupported-extensions":
			meta["sessionExtensions"] = map[string]any{"version": 2}
		default:
			meta["sessionExtensions"] = map[string]any{"version": 1}
		}
		s.respond(request.ID, map[string]any{"protocolVersion": 1, "_meta": meta})
	case "session/new", "session/load":
		if s.mode == "ignore-term" && request.Method == "session/load" {
			return
		}
		if s.mode == "exit-on-load" && request.Method == "session/load" {
			os.Exit(7)
		}
		if id, ok := request.Params["sessionId"].(string); ok {
			s.sessionID = id
		}
		s.respond(request.ID, map[string]any{"sessionId": s.sessionID})
	case "session/prompt":
		if s.mode == "interactive" {
			s.mu.Lock()
			s.pending = request.ID
			s.mu.Unlock()
			return
		}
		size, _ := strconv.Atoi(os.Getenv(fakeACPBytesEnv))
		if size > 0 {
			s.text(strings.Repeat("p", size))
		}
		stopReason := "end_turn"
		if s.mode == "cancelled" {
			stopReason = "cancelled"
		}
		s.respond(request.ID, map[string]any{"stopReason": stopReason})
	case "session/cancel":
		s.mu.Lock()
		pending := s.pending
		s.pending = nil
		s.mu.Unlock()
		if pending != nil {
			s.respond(pending, map[string]any{"stopReason": "cancelled"})
		}
	case "_session/steering":
		s.steer(request.ID, request.Params)
	}
}

func (s *fakeACPServer) steer(id json.RawMessage, params map[string]any) {
	prompt, _ := params["prompt"].([]any)
	message := ""
	if len(prompt) > 0 {
		message, _ = prompt[0].(map[string]any)["text"].(string)
	}
	if message == "finish" {
		go s.finishWhenReleased()
	} else {
		s.update(map[string]any{
			"sessionUpdate": "tool_call", "toolCallId": "read-1", "toolName": "file_read",
			"rawInput": map[string]any{"path": "src/main.py"}, "status": "in_progress",
		})
		s.update(map[string]any{"sessionUpdate": "tool_call_update", "toolCallId": "read-1", "status": "in_progress", "content": toolContent("reading source")})
		s.update(map[string]any{"sessionUpdate": "tool_call_update", "toolCallId": "read-1", "status": "completed", "content": toolContent("source code")})
		s.text("Inspecting: ")
	}
	s.respond(id, map[string]any{"outcome": "injected"})
}

func (s *fakeACPServer) finishWhenReleased() {
	for {
		if _, err := os.Stat(os.Getenv(fakeACPFinishEnv)); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.text("complete")
	s.mu.Lock()
	pending := s.pending
	s.pending = nil
	s.mu.Unlock()
	s.respond(pending, map[string]any{"stopReason": "end_turn"})
}
