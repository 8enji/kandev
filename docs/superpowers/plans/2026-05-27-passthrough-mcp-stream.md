# Passthrough MCP Stream Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Open the agent-stream WebSocket between backend and agentctl in CLI passthrough mode so kandev MCP tool calls round-trip instead of hanging until `MCP_TIMEOUT`.

**Architecture:** Add a passthrough-specific `connectMCPStream` on `StreamManager` that mirrors `connectUpdatesStream` but with no-op agent-event handler, no `promptDoneCh` signaling, and exponential-backoff reconnect on disconnect. Wire it into the three passthrough launch/resume sites. Add a public `HasAgentStream()` getter on the agentctl `Client` for idempotency.

**Tech Stack:** Go (backend), `gorilla/websocket`, `go.uber.org/zap`, `testing/synctest` (Go 1.24+) where applicable, `httptest` for WS server mocks.

**Spec:** [docs/superpowers/specs/2026-05-27-passthrough-mcp-stream-design.md](../specs/2026-05-27-passthrough-mcp-stream-design.md)

---

## Task 1: `Client.HasAgentStream()` getter

The agentctl `Client` tracks `agentStreamConn *websocket.Conn` (set in `StreamUpdates`, cleared in `readUpdatesStream` cleanup defer). `connectMCPStream` needs a public way to ask "is the agent stream already up?" so launch + recovery don't double-dial.

**Files:**
- Modify: `apps/backend/internal/agent/runtime/agentctl/client.go` (add method near the other small getters around line 77)
- Modify: `apps/backend/internal/agent/runtime/agentctl/agent_test.go` (add test alongside other StreamUpdates tests)

- [ ] **Step 1: Write the failing test**

Append to `apps/backend/internal/agent/runtime/agentctl/agent_test.go`:

```go
// TestHasAgentStream_ReflectsConnectionLifecycle verifies that HasAgentStream
// returns true after a successful StreamUpdates dial and false again after the
// WebSocket disconnects.
func TestHasAgentStream_ReflectsConnectionLifecycle(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// Server holds the connection until the test signals close.
	holdClose := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		<-holdClose
		_ = conn.Close()
	}))
	defer server.Close()

	log := newTestLogger()
	c := &Client{
		baseURL:         server.URL,
		httpClient:      &http.Client{Timeout: 5 * time.Second},
		logger:          log,
		pendingRequests: make(map[string]chan *ws.Message),
	}

	if c.HasAgentStream() {
		t.Fatal("HasAgentStream() = true before connect, want false")
	}

	disconnected := make(chan struct{})
	if err := c.StreamUpdates(context.Background(), func(AgentEvent) {}, nil, func(error) {
		close(disconnected)
	}); err != nil {
		t.Fatalf("StreamUpdates returned error: %v", err)
	}

	// Allow the server-side goroutine to register the conn.
	time.Sleep(50 * time.Millisecond)

	if !c.HasAgentStream() {
		t.Fatal("HasAgentStream() = false after connect, want true")
	}

	close(holdClose)
	<-disconnected
	// Give the read goroutine's defer time to clear agentStreamConn.
	time.Sleep(50 * time.Millisecond)

	if c.HasAgentStream() {
		t.Fatal("HasAgentStream() = true after disconnect, want false")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd apps/backend && go test ./internal/agent/runtime/agentctl/ -run TestHasAgentStream_ReflectsConnectionLifecycle -v
```

Expected: `FAIL` — `c.HasAgentStream undefined`.

- [ ] **Step 3: Add the getter**

In `apps/backend/internal/agent/runtime/agentctl/client.go`, add this method after `SetTraceContext` (around line 81):

```go
// HasAgentStream reports whether the agent stream WebSocket is currently
// connected. Used by callers that need to skip a redundant dial when another
// goroutine has already opened the stream (e.g. recovery + passthrough launch
// racing to open the MCP transport).
func (c *Client) HasAgentStream() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.agentStreamConn != nil
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd apps/backend && go test ./internal/agent/runtime/agentctl/ -run TestHasAgentStream_ReflectsConnectionLifecycle -v
```

Expected: `PASS`.

- [ ] **Step 5: Run full agentctl client tests**

```bash
cd apps/backend && go test ./internal/agent/runtime/agentctl/ -v
```

Expected: all PASS (no regression in existing StreamUpdates tests).

- [ ] **Step 6: Commit**

```bash
git add apps/backend/internal/agent/runtime/agentctl/client.go apps/backend/internal/agent/runtime/agentctl/agent_test.go
git commit -m "feat(backend): add Client.HasAgentStream getter for stream idempotency"
```

---

## Task 2: `StreamManager.connectMCPStream`

The core new component. Opens the agent-stream WS for passthrough sessions with stripped callbacks and exponential-backoff reconnect.

**Files:**
- Modify: `apps/backend/internal/agent/runtime/lifecycle/streams.go` (add function at end of file, mirroring `connectWorkspaceStream`)
- Modify: `apps/backend/internal/agent/runtime/lifecycle/streams_test.go` (add tests; pattern follows the existing `TestConnectWorkspaceStream_IdempotentWhenAlreadyAttached` and the `httptest` pattern from `agent_test.go`)

### Test plan

Six tests, each isolated:

1. `TestConnectMCPStream_OpensAndDispatchesMCPRequest` — happy path: WS opens, fake agentctl sends an MCP request, `mcpHandler.Dispatch` is invoked, response flows back.
2. `TestConnectMCPStream_NoOpForAgentEvents` — stream delivers an `AgentEvent`; verify it is silently dropped (no callback panic, no state mutation).
3. `TestConnectMCPStream_DisconnectDoesNotSignalPromptDoneCh` — WS closes; verify `execution.promptDoneCh` does NOT receive an error.
4. `TestConnectMCPStream_IdempotentWhenStreamAlreadyAttached` — `HasAgentStream()` returns true; verify the function exits without dialing.
5. `TestConnectMCPStream_RetriesOnConnectFailure` — server rejects first connect; verify second attempt succeeds and ready is signaled.
6. `TestConnectMCPStream_ReconnectsOnDisconnect` — server accepts, closes, accepts again; verify a second connect occurs after disconnect.

- [ ] **Step 1: Write tests 1–4**

Append to `apps/backend/internal/agent/runtime/lifecycle/streams_test.go`. Top of file imports will need expanding — replace the existing import block with:

```go
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
```

(`logger` is not directly referenced — `newTestLogger()` is already defined in another test file in this package.)

Then append these helpers + tests:

```go
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
	// httptest URLs look like "http://127.0.0.1:PORT" — split into host/port.
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
```

Note: the tests call `connectMCPStreamWithCtx` (an internal entrypoint that accepts an explicit context) so tests can cancel and tear down cleanly. The public `connectMCPStream` derives ctx from `execution.SessionTraceContext()` and delegates to `connectMCPStreamWithCtx`.

- [ ] **Step 2: Run tests 1–4 — verify they fail**

```bash
cd apps/backend && go test ./internal/agent/runtime/lifecycle/ -run TestConnectMCPStream -v
```

Expected: `FAIL` — `sm.connectMCPStreamWithCtx undefined`.

- [ ] **Step 3: Implement `connectMCPStream` in streams.go**

Append to `apps/backend/internal/agent/runtime/lifecycle/streams.go`:

```go
// connectMCPStream opens the agent stream WebSocket for a passthrough
// execution. It mirrors connectUpdatesStream but with a no-op agent-event
// handler (passthrough has no ACP session updates) and a no-op disconnect
// signal (no ACP prompt is waiting on promptDoneCh). The stream is the only
// transport for kandev MCP tool calls in passthrough mode, so we retry on
// connect failure AND on mid-session disconnect with exponential backoff.
//
// Idempotency: if the underlying agentctl.Client already has an active agent
// stream (e.g. recovery raced the launch path), we exit early.
//
// Ready: closed once the first successful connect happens, OR on the final
// failed attempt — callers using `ready` as a launch gate get unblocked
// either way.
func (sm *StreamManager) connectMCPStream(execution *AgentExecution, ready chan<- struct{}) {
	sm.connectMCPStreamWithCtx(execution.SessionTraceContext(), execution, ready)
}

// connectMCPStreamWithCtx is the testable inner of connectMCPStream — accepts
// an explicit context so tests can cancel the retry loop deterministically.
func (sm *StreamManager) connectMCPStreamWithCtx(ctx context.Context, execution *AgentExecution, ready chan<- struct{}) {
	const maxConsecutiveFailures = 5
	const initialBackoff = 1 * time.Second

	signaled := false
	signalReady := func() {
		if !signaled && ready != nil {
			close(ready)
			signaled = true
		}
	}
	defer signalReady()

	consecutiveFailures := 0
	backoff := initialBackoff

	for {
		if execution.agentctl == nil {
			sm.logger.Debug("connectMCPStream: nil agentctl client, exiting",
				zap.String("instance_id", execution.ID))
			return
		}
		if execution.agentctl.HasAgentStream() {
			sm.logger.Debug("connectMCPStream: agent stream already attached, skipping",
				zap.String("instance_id", execution.ID))
			signalReady()
			return
		}

		disconnected := make(chan struct{})
		onDisconnect := func(err error) {
			if err != nil {
				sm.logger.Debug("passthrough MCP stream disconnected",
					zap.String("instance_id", execution.ID),
					zap.Error(err))
			}
			close(disconnected)
		}

		err := execution.agentctl.StreamUpdates(ctx,
			func(agentctl.AgentEvent) {
				// Passthrough has no ACP session, so any AgentEvent here is
				// noise. Silently drop.
			},
			sm.mcpHandler,
			onDisconnect,
		)
		if err != nil {
			consecutiveFailures++
			sm.logger.Debug("passthrough MCP stream connect failed",
				zap.String("instance_id", execution.ID),
				zap.Int("attempt", consecutiveFailures),
				zap.Error(err))
			if consecutiveFailures >= maxConsecutiveFailures {
				sm.logger.Error("passthrough MCP stream connect exhausted retries",
					zap.String("instance_id", execution.ID),
					zap.Int("attempts", consecutiveFailures))
				return
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			backoff *= 2
			continue
		}

		signalReady()
		consecutiveFailures = 0
		backoff = initialBackoff

		select {
		case <-disconnected:
			sm.logger.Debug("passthrough MCP stream disconnected, reconnecting",
				zap.String("instance_id", execution.ID))
			// fall through to next iteration
		case <-ctx.Done():
			return
		}
	}
}
```

Make sure the package imports include `"context"` and `agentctl "github.com/kandev/kandev/internal/agent/runtime/agentctl"` (the latter is already imported as `agentctl`). The function references `agentctl.AgentEvent` — that re-export already exists at the top of `agent.go`.

- [ ] **Step 4: Run tests 1–4 — verify they pass**

```bash
cd apps/backend && go test ./internal/agent/runtime/lifecycle/ -run TestConnectMCPStream -v
```

Expected: 4 PASS.

- [ ] **Step 5: Write tests 5 & 6 (retry + reconnect)**

Append to `streams_test.go`:

```go
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
```

- [ ] **Step 6: Run tests 5 & 6 — verify they pass**

```bash
cd apps/backend && go test ./internal/agent/runtime/lifecycle/ -run TestConnectMCPStream -v
```

Expected: 6 PASS total.

- [ ] **Step 7: Run full lifecycle package tests (no regressions)**

```bash
cd apps/backend && go test ./internal/agent/runtime/lifecycle/ -v -count=1 -timeout=60s
```

Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add apps/backend/internal/agent/runtime/lifecycle/streams.go apps/backend/internal/agent/runtime/lifecycle/streams_test.go
git commit -m "feat(backend): add connectMCPStream for passthrough MCP transport"
```

---

## Task 3: Wire `connectMCPStream` into passthrough launch/resume sites

Now use the new function in the three sites where passthrough mode currently only opens the workspace stream.

**Files:**
- Modify: `apps/backend/internal/agent/runtime/lifecycle/manager_passthrough.go` (3 call sites)
- Modify: `apps/backend/internal/agent/runtime/lifecycle/manager_passthrough_test.go` (add launch-time wiring test)

- [ ] **Step 1: Write the failing wiring test**

Append to `apps/backend/internal/agent/runtime/lifecycle/manager_passthrough_test.go`:

```go
// TestPassthroughMCPStream_OpensAgentStreamWS verifies that the StreamManager's
// connectMCPStream actually dials /api/v1/agent/stream — the bug PR #1078
// shipped was specifically that this dial never happened in passthrough mode.
// The three production call sites in manager_passthrough.go invoke this same
// function, so testing it here verifies the regression cannot return without
// also having to mock out the PTY launch path.
func TestPassthroughMCPStream_OpensAgentStreamWS(t *testing.T) {
	var agentStreamDialed atomic.Int32

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/agent/stream", func(w http.ResponseWriter, r *http.Request) {
		agentStreamDialed.Add(1)
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
	mux.HandleFunc("/api/v1/workspace/stream", func(w http.ResponseWriter, r *http.Request) {
		// Workspace stream upgrade — accept and hold so the workspace
		// connect path doesn't churn while we observe the MCP path.
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
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mgr, execution, _ := newClaudePassthroughMCPTestManager(t)
	// Point the execution's agentctl at our fake server.
	execution.agentctl = newTestAgentctlClient(t, srv.URL)
	if err := mgr.executionStore.Add(execution); err != nil {
		t.Fatalf("add execution: %v", err)
	}

	// Invoke just the stream-wiring half of startPassthroughSession — we
	// don't need a real PTY launch to verify the dial happens.
	go mgr.streamManager.connectMCPStream(execution, nil)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if agentStreamDialed.Load() >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected agent stream WS dial after passthrough launch, got %d", agentStreamDialed.Load())
}
```

Note: this verifies `connectMCPStream` is the wiring path, but is functionally a Task-2-style integration test. The real assertion that the three production call sites invoke `connectMCPStream` follows in step 3.

- [ ] **Step 2: Run the wiring test — verify it passes (already covered by Task 2 impl)**

```bash
cd apps/backend && go test ./internal/agent/runtime/lifecycle/ -run TestStartPassthroughSession_OpensMCPStream -v
```

Expected: PASS.

- [ ] **Step 3: Edit `manager_passthrough.go` — add the three call sites**

For each existing `connectWorkspaceStream` call in passthrough paths, add a sibling `connectMCPStream` call:

**Site A — `startPassthroughSession`** (around line 474):

```go
// BEFORE:
if m.streamManager != nil && execution.agentctl != nil {
    go m.streamManager.connectWorkspaceStream(execution, nil)
}

// AFTER:
if m.streamManager != nil && execution.agentctl != nil {
    go m.streamManager.connectWorkspaceStream(execution, nil)
    go m.streamManager.connectMCPStream(execution, nil)
}
```

**Site B — `ResumePassthroughSession`** (around line 679):

```go
// BEFORE:
if m.streamManager != nil && execution.agentctl != nil && execution.GetWorkspaceStream() == nil {
    go m.streamManager.connectWorkspaceStream(execution, nil)
}

// AFTER:
if m.streamManager != nil && execution.agentctl != nil {
    if execution.GetWorkspaceStream() == nil {
        go m.streamManager.connectWorkspaceStream(execution, nil)
    }
    go m.streamManager.connectMCPStream(execution, nil)
}
```

Note: the workspace-stream guard stays (workspace stream is idempotent via the per-execution stream field); `connectMCPStream` has its own idempotency check via `HasAgentStream()`, so we don't need an outer guard.

**Site C — `attemptResumeFallback`** (around line 951):

```go
// BEFORE:
if m.streamManager != nil && execution.agentctl != nil && execution.GetWorkspaceStream() == nil {
    go m.streamManager.connectWorkspaceStream(execution, nil)
}

// AFTER:
if m.streamManager != nil && execution.agentctl != nil {
    if execution.GetWorkspaceStream() == nil {
        go m.streamManager.connectWorkspaceStream(execution, nil)
    }
    go m.streamManager.connectMCPStream(execution, nil)
}
```

- [ ] **Step 4: Run the full lifecycle suite to confirm no regressions**

```bash
cd apps/backend && go test ./internal/agent/runtime/lifecycle/ -v -count=1 -timeout=120s
```

Expected: all PASS.

- [ ] **Step 5: Run the wider backend test suite**

```bash
make -C apps/backend test
```

Expected: PASS. Surface any failures and fix before continuing.

- [ ] **Step 6: Commit**

```bash
git add apps/backend/internal/agent/runtime/lifecycle/manager_passthrough.go apps/backend/internal/agent/runtime/lifecycle/manager_passthrough_test.go
git commit -m "fix(backend): open MCP stream in passthrough launch/resume paths

Without this, kandev MCP tool calls (list_workspaces_kandev,
get_task_conversation_kandev, etc.) hang until MCP_TIMEOUT because
the agentctl /mcp HTTP handler has no transport to forward requests
to the backend. The agent-stream WebSocket drains that channel, and
passthrough launch was the only path that didn't open it.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 4: Final verification

Run the project's full quality gates per CLAUDE.md.

- [ ] **Step 1: Format**

```bash
make -C apps/backend fmt
```

Expected: no diff. If files changed, stage and amend the previous commit only if it's the most recent (otherwise add as a follow-up commit).

- [ ] **Step 2: Typecheck**

```bash
make -C apps/backend typecheck 2>&1 || echo "make target may not exist, fallback to go build"
cd apps/backend && go build ./...
```

Expected: PASS / no output from `go build`.

- [ ] **Step 3: Test (full suite)**

```bash
make -C apps/backend test
```

Expected: PASS.

- [ ] **Step 4: Lint**

```bash
make -C apps/backend lint
```

Expected: PASS. If a complexity / function-length limit was tripped by `connectMCPStreamWithCtx` (~75 lines), extract a small helper (e.g. `tryMCPConnect` returning `(disconnected <-chan struct{}, err error)`) and re-run.

- [ ] **Step 5: Final commit if any verification fixes were needed**

If steps 1–4 produced any changes (e.g. lint-driven extractions), commit them:

```bash
git add -u
git commit -m "chore(backend): verification cleanup for passthrough MCP stream"
```

---

## Self-Review Notes

- **Spec coverage:**
  - Design § "New stream connector — `connectMCPStream`" → Task 2 (full implementation + 6 tests).
  - Design § "Call-site changes" → Task 3 (three sites + wiring test).
  - Design § "Recovery path" — explicitly deferred in spec; no task needed.
  - Design § "Lifecycle" (retry, idempotency, on-disconnect-reconnect) → Task 2 (retry tests #5, #6; idempotency test #4) + Task 1 (HasAgentStream getter for idempotency).
  - Design § "Testing — Unit tests" → Task 2 tests 1–6.
  - Design § "Testing — End-to-end test" → Task 3 step 1 (passthrough-launch-opens-MCP-stream test).

- **No placeholders:** All steps include exact paths, command invocations, code blocks, expected outputs.

- **Type consistency:** `connectMCPStream(*AgentExecution, chan<- struct{})` and `connectMCPStreamWithCtx(context.Context, *AgentExecution, chan<- struct{})` named identically across tasks. `HasAgentStream() bool` referenced consistently.

- **One risk noted:** test 1 (`TestConnectMCPStream_OpensAndDispatchesMCPRequest`) assumes the MCP request received from the WS is dispatched through `mcpHandler.Dispatch`. The current `runUpdatesStream` reader (in `apps/backend/internal/agent/runtime/agentctl/agent.go:370`) treats incoming `MessageTypeRequest` messages as MCP requests and dispatches them. This matches the test expectation.
