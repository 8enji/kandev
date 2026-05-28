package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	agentctl "github.com/kandev/kandev/internal/agent/runtime/agentctl"
	ws "github.com/kandev/kandev/pkg/websocket"
)

// TestConnectWorkspaceStream_IdempotentWhenAlreadyAttached is the regression
// test for the workspace-stream double-connect race. Two paths can call
// connectWorkspaceStream for the same execution (e.g. workspace-only ensure
// followed by full-launch promotion). The second call previously hit
// "workspace stream already connected" and burned 5 retries before logging
// a terminal ERROR. The fix short-circuits when a stream is already attached.
func TestConnectWorkspaceStream_IdempotentWhenAlreadyAttached(t *testing.T) {
	sm := NewStreamManager(newTestLogger(), StreamCallbacks{}, nil)

	execution := &AgentExecution{ID: "exec-1", SessionID: "sess-1"}
	// Pre-attach a non-nil workspace stream — simulates another goroutine
	// having already connected before this call.
	execution.SetWorkspaceStream(&agentctl.WorkspaceStream{})

	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		sm.connectWorkspaceStream(execution, ready)
		close(done)
	}()

	// Should return effectively immediately (well under the 1s first-retry
	// backoff). 500ms gives ample headroom on slow CI without masking a
	// regression that would burn through the full 5-retry loop.
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("connectWorkspaceStream did not exit early when stream was already attached")
	}

	// ready must be closed (deferred signalReady runs even on early exit).
	select {
	case <-ready:
	default:
		t.Error("ready channel was not closed on early-exit path")
	}
}

// fakeMCPHandler captures Dispatch calls from connectMCPStream tests.
type fakeMCPHandler struct {
	mu       sync.Mutex
	received []*ws.Message
	respond  func(req *ws.Message) (*ws.Message, error)
}

func (h *fakeMCPHandler) Dispatch(ctx context.Context, msg *ws.Message) (*ws.Message, error) {
	h.mu.Lock()
	h.received = append(h.received, msg)
	h.mu.Unlock()
	if h.respond != nil {
		return h.respond(msg)
	}
	resp, _ := ws.NewResponse(msg.ID, msg.Action, map[string]string{"ok": "yes"})
	return resp, nil
}

func (h *fakeMCPHandler) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.received)
}

// fakeAgentStreamServer returns an httptest server that accepts a WebSocket
// upgrade on /api/v1/agent/stream and gives the test full control of the
// connection lifecycle via the onConnect callback.
func fakeAgentStreamServer(t *testing.T, onConnect func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/agent/stream", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade error: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		onConnect(conn)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return httptest.NewServer(mux)
}

// newExecutionWithAgentctl builds an AgentExecution wired to a Client pointed
// at the given httptest server. SessionTraceContext returns a derived ctx;
// SetWorkspaceStream is left untouched.
func newExecutionWithAgentctl(t *testing.T, serverURL string) *AgentExecution {
	t.Helper()
	exec := &AgentExecution{
		ID:           "exec-mcp-1",
		SessionID:    "sess-mcp-1",
		promptDoneCh: make(chan PromptCompletionSignal, 1),
	}
	exec.agentctl = newTestAgentctlClient(t, serverURL)
	return exec
}

// newTestAgentctlClient is a thin construction helper for an agentctl.Client
// pointed at an httptest server. We use the public constructor + URL parsing
// to mirror production wiring.
func newTestAgentctlClient(t *testing.T, serverURL string) *agentctl.Client {
	t.Helper()
	u := strings.TrimPrefix(serverURL, "http://")
	parts := strings.Split(u, ":")
	if len(parts) != 2 {
		t.Fatalf("unexpected httptest URL form: %s", serverURL)
	}
	port := 0
	if _, err := fmt.Sscanf(parts[1], "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return agentctl.NewClient(parts[0], port, newTestLogger())
}

// TestConnectMCPStream_OpensAndDispatchesMCPRequest verifies the happy path:
// the WS connects, the fake agentctl sends an MCP request over the stream,
// the StreamManager dispatches via mcpHandler, and the response flows back.
func TestConnectMCPStream_OpensAndDispatchesMCPRequest(t *testing.T) {
	handler := &fakeMCPHandler{}

	var serverConn *websocket.Conn
	connReady := make(chan struct{})
	srv := fakeAgentStreamServer(t, func(conn *websocket.Conn) {
		serverConn = conn
		close(connReady)
		// Hold the connection open until the test closes it.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	exec := newExecutionWithAgentctl(t, srv.URL)
	sm := NewStreamManager(newTestLogger(), StreamCallbacks{}, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	go sm.connectMCPStreamWithCtx(ctx, exec, ready)

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("connectMCPStream did not signal ready within 2s")
	}
	<-connReady

	// Send an MCP request from the fake agentctl side.
	req, err := ws.NewRequest("req-1", "kandev.mcp.list_workspaces", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	data, _ := json.Marshal(req)
	if err := serverConn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("server WriteMessage: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if handler.calls() == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("mcpHandler.Dispatch never invoked; calls=%d", handler.calls())
}

// TestConnectMCPStream_NoOpForAgentEvents verifies that AgentEvent payloads
// sent over the stream do not invoke OnAgentEvent (which is wired for ACP
// sessions only; passthrough should drop these silently).
func TestConnectMCPStream_NoOpForAgentEvents(t *testing.T) {
	var eventCalls atomic.Int32

	connReady := make(chan struct{})
	srv := fakeAgentStreamServer(t, func(conn *websocket.Conn) {
		close(connReady)
		// Send an AgentEvent immediately.
		event := agentctl.AgentEvent{Type: "message_chunk", Text: "this is not for passthrough"}
		data, _ := json.Marshal(event)
		_ = conn.WriteMessage(websocket.TextMessage, data)
		// Hold the connection.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	exec := newExecutionWithAgentctl(t, srv.URL)
	sm := NewStreamManager(newTestLogger(), StreamCallbacks{
		OnAgentEvent: func(*AgentExecution, agentctl.AgentEvent) {
			eventCalls.Add(1)
		},
	}, &fakeMCPHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sm.connectMCPStreamWithCtx(ctx, exec, nil)

	<-connReady
	// Give the event time to traverse the WS read loop.
	time.Sleep(200 * time.Millisecond)

	if got := eventCalls.Load(); got != 0 {
		t.Errorf("OnAgentEvent invoked %d time(s) for passthrough stream; want 0", got)
	}
}

// TestConnectMCPStream_DisconnectDoesNotSignalPromptDoneCh verifies that a WS
// disconnect does NOT push an error onto execution.promptDoneCh — passthrough
// has no ACP prompt waiting, so this signal would be spurious.
func TestConnectMCPStream_DisconnectDoesNotSignalPromptDoneCh(t *testing.T) {
	srv := fakeAgentStreamServer(t, func(conn *websocket.Conn) {
		// Hold briefly, then close to trigger disconnect.
		time.Sleep(100 * time.Millisecond)
	})
	defer srv.Close()

	exec := newExecutionWithAgentctl(t, srv.URL)
	sm := NewStreamManager(newTestLogger(), StreamCallbacks{}, &fakeMCPHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sm.connectMCPStreamWithCtx(ctx, exec, nil)

	// Wait for the connection to be torn down.
	time.Sleep(400 * time.Millisecond)

	select {
	case sig := <-exec.promptDoneCh:
		t.Fatalf("promptDoneCh received unexpected signal: %+v", sig)
	default:
		// expected
	}
}

// TestConnectMCPStream_IdempotentWhenStreamAlreadyAttached verifies the
// idempotency guard: if HasAgentStream() is already true, connectMCPStream
// exits without dialing.
func TestConnectMCPStream_IdempotentWhenStreamAlreadyAttached(t *testing.T) {
	dialCount := atomic.Int32{}
	srv := fakeAgentStreamServer(t, func(conn *websocket.Conn) {
		dialCount.Add(1)
		// hold
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	exec := newExecutionWithAgentctl(t, srv.URL)
	sm := NewStreamManager(newTestLogger(), StreamCallbacks{}, &fakeMCPHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First connect succeeds.
	ready := make(chan struct{})
	go sm.connectMCPStreamWithCtx(ctx, exec, ready)
	<-ready

	// Wait for the WS dial to land.
	time.Sleep(100 * time.Millisecond)

	// Second connect should short-circuit (idempotency).
	done := make(chan struct{})
	go func() {
		sm.connectMCPStreamWithCtx(ctx, exec, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second connectMCPStream did not return; idempotency guard missing")
	}

	if got := dialCount.Load(); got != 1 {
		t.Errorf("agentctl was dialed %d time(s); want exactly 1", got)
	}
}

// TestConnectMCPStream_RetriesOnConnectFailure verifies that an initial dial
// failure does not abort the loop — connectMCPStream backs off and retries.
func TestConnectMCPStream_RetriesOnConnectFailure(t *testing.T) {
	var attempts atomic.Int32

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/agent/stream", func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 2 {
			// Reject the first attempt by returning an HTTP error (no WS upgrade).
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	exec := newExecutionWithAgentctl(t, srv.URL)
	sm := NewStreamManager(newTestLogger(), StreamCallbacks{}, &fakeMCPHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	go sm.connectMCPStreamWithCtx(ctx, exec, ready)

	// The first connect fails, then a 1s backoff sleep runs, then the second
	// connect succeeds. Allow up to 4s to land for slow-CI headroom.
	select {
	case <-ready:
	case <-time.After(4 * time.Second):
		t.Fatalf("connectMCPStream did not signal ready after retry; attempts=%d", attempts.Load())
	}

	if got := attempts.Load(); got < 2 {
		t.Errorf("expected at least 2 connect attempts, got %d", got)
	}
}

// TestConnectMCPStream_ReconnectsOnDisconnect verifies that after a successful
// connect followed by a server-side close, connectMCPStream dials again.
func TestConnectMCPStream_ReconnectsOnDisconnect(t *testing.T) {
	var dials atomic.Int32

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/agent/stream", func(w http.ResponseWriter, r *http.Request) {
		dials.Add(1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if dials.Load() == 1 {
			// First connection: close immediately so the loop reconnects.
			time.Sleep(50 * time.Millisecond)
			_ = conn.Close()
			return
		}
		// Second connection: hold open.
		defer func() { _ = conn.Close() }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	exec := newExecutionWithAgentctl(t, srv.URL)
	sm := NewStreamManager(newTestLogger(), StreamCallbacks{}, &fakeMCPHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sm.connectMCPStreamWithCtx(ctx, exec, nil)

	// Wait for two dials: the initial connect + the reconnect after disconnect.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if dials.Load() >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected at least 2 dials after disconnect, got %d", dials.Load())
}
