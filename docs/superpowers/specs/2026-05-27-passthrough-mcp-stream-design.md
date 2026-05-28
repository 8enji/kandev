---
status: draft
created: 2026-05-27
owner: 8enji
related: docs/specs/cli-mode-parity/spec.md
related-prs:
  - https://github.com/kdlbs/kandev/pull/1078  # wired MCP config into CLI passthrough
  - https://github.com/kdlbs/kandev/pull/3     # wired MCP_TIMEOUT into passthrough env
  - https://github.com/kdlbs/kandev/pull/2     # disabled ask_user_question in passthrough
---

# Passthrough MCP Stream

## Problem

In CLI passthrough mode, every kandev MCP tool call (`list_workspaces_kandev`, `get_task_conversation_kandev`, etc.) hangs until the agent's MCP client timeout fires. PR [#1078](https://github.com/kdlbs/kandev/pull/1078) wired the `--mcp-config` flag and PR [#3](https://github.com/kdlbs/kandev/pull/3) raised `MCP_TIMEOUT` to 2h — both partial mitigations. The underlying transport is still broken.

### Root cause

agentctl's MCP HTTP endpoint at `http://localhost:<standalone_port>/mcp` runs a `ChannelBackendClient` ([backend_client.go](../../../apps/backend/internal/mcp/server/backend_client.go)) that pushes every MCP request onto an internal channel and waits for a response. That channel is drained only by the agent-stream WebSocket handler in agentctl ([agent.go:197-237](../../../apps/backend/internal/agentctl/server/api/agent.go:197)), which runs only while the backend has an open WS to `/api/v1/agent/stream` on that agentctl instance.

The backend opens that WS via `StreamManager.connectUpdatesStream` ([streams.go:83](../../../apps/backend/internal/agent/runtime/lifecycle/streams.go:83)). It is called from `ConnectAll` (ACP launch & interactive prompt) and `ReconnectAll` (recovery). It is **never** called from the passthrough entrypoints:

- `startPassthroughSession` ([manager_passthrough.go:474](../../../apps/backend/internal/agent/runtime/lifecycle/manager_passthrough.go:474))
- `ResumePassthroughSession` ([manager_passthrough.go:679](../../../apps/backend/internal/agent/runtime/lifecycle/manager_passthrough.go:679))
- `attemptResumeFallback` ([manager_passthrough.go:951](../../../apps/backend/internal/agent/runtime/lifecycle/manager_passthrough.go:951))

The passthrough sites only open the workspace stream. With no reader on the MCP request channel, every tool call waits until `MCP_TIMEOUT` and dies.

### Why existing tests didn't catch it

The passthrough MCP tests in [manager_passthrough_test.go](../../../apps/backend/internal/agent/runtime/lifecycle/manager_passthrough_test.go) only verify the generated `--mcp-config` file points at the correct URL. No end-to-end round-trip is exercised, so a non-functional endpoint passes.

## Design

### New stream connector — `connectMCPStream`

A passthrough-specific sibling of `connectUpdatesStream` in [streams.go](../../../apps/backend/internal/agent/runtime/lifecycle/streams.go). Opens the same `/api/v1/agent/stream` WebSocket but with a stripped-down callback set:

| Concern | `connectUpdatesStream` (ACP) | `connectMCPStream` (passthrough) |
|---|---|---|
| Agent-event handler | `sm.callbacks.OnAgentEvent` (drives chat UI, mode, plan updates) | No-op. Passthrough has no ACP events; any stray event is logged-and-dropped. |
| `mcpHandler` | Set | Set (same handler — `gateway.Dispatcher`) |
| `onDisconnect` signal | Pushes `IsError` onto `execution.promptDoneCh` to unstick a waiting `SendPrompt` | Log only. No ACP prompt waits in passthrough mode. |
| Retry | Single attempt | Exponential backoff, 5 attempts (matches `connectWorkspaceStream`). MCP is the only feature gated on this stream — transient WS drops should self-heal. |
| Idempotency | Per-execution `agentStreamConn` field on the agentctl client | The agentctl `Client` already tracks `agentStreamConn` ([agent.go:304-306](../../../apps/backend/internal/agent/runtime/agentctl/agent.go:304)) and `StreamUpdates` overwrites it on a fresh connect. `connectMCPStream` checks the client's existing connection state before dialing — no new field needed. |

The function lives in `streams.go` alongside `connectUpdatesStream` and `connectWorkspaceStream`. It calls `execution.agentctl.StreamUpdates(ctx, func(AgentEvent) {}, sm.mcpHandler, onDisconnect)` — passing an inline no-op for the event handler and the existing `sm.mcpHandler` (already wired by `SetMCPHandler` at [manager.go:272](../../../apps/backend/internal/agent/runtime/lifecycle/manager.go:272)).

### Call-site changes

Three passthrough entrypoints get a single new line, gated by `execution.agentctl != nil`:

```go
go m.streamManager.connectMCPStream(execution, nil)
```

Sites:
- `startPassthroughSession` (alongside the existing `connectWorkspaceStream` call)
- `ResumePassthroughSession` (alongside the existing `connectWorkspaceStream` call)
- `attemptResumeFallback` (alongside the existing `connectWorkspaceStream` call)

### Recovery path

`ReconnectAll` ([manager_lifecycle.go:107](../../../apps/backend/internal/agent/runtime/lifecycle/manager_lifecycle.go:107)) currently calls the full `ConnectAll`, which opens the updates stream with ACP-style callbacks. For passthrough executions this works incidentally (MCP requests flow), but the ACP `OnAgentEvent` callbacks fire on dead executions.

**This spec leaves recovery alone.** Reasons:
1. The idempotency guard prevents a double-connect when `ResumePassthroughSession` later runs.
2. ACP callbacks on stray events are benign (no-ops if the event refs an unknown session).
3. Branching `ReconnectAll` on passthrough state widens the diff for no immediate user-facing win.

If recovery proves messy in practice, a follow-up can route passthrough executions through `connectMCPStream` instead.

### Lifecycle

- WS opens at launch / resume / fallback. Lives for the entire passthrough session (across PTY restarts).
- On disconnect, retry loop reconnects with exponential backoff: 1s, 2s, 4s, 8s delays between 5 total attempts (matches `connectWorkspaceStream`'s `backoff *= 2` doubling from a 1s base).
- Exhausted retries log error and exit. MCP stops working for the remainder of that session; user-visible failure mode is identical to today's bug (tool calls time out), so this is no regression and a strict improvement when the disconnect is transient.
- Teardown: the retry goroutine exits the same way `connectUpdatesStream` and `connectWorkspaceStream` peers do — via WS disconnect followed by retry-exhaustion (~15s window). The context comes from `execution.SessionTraceContext()` which is `context.Background()`-derived and does NOT cancel on `RemoveExecution`. If clean-shutdown latency matters here later, wire `Manager.stopCh` into the retry-loop selects (consistent with whatever the peer connectors adopt).

## Testing

### Unit tests (`streams_test.go` or `manager_passthrough_test.go`)

1. **`connectMCPStream` opens the WS and drains MCP requests.** Spin up an `httptest.Server` mocking agentctl's `/api/v1/agent/stream`. Connect. Push a fake MCP request from the agentctl side. Verify the backend's `mcpHandler.Dispatch` is invoked and the response travels back.

2. **`connectMCPStream` does NOT call `OnAgentEvent`.** Push an `AgentEvent` payload over the WS. Confirm `callbacks.OnAgentEvent` is not invoked (no-op handler).

3. **`connectMCPStream` does NOT signal `promptDoneCh`.** Send a WS close from the server side. Confirm `execution.promptDoneCh` does not receive an error signal.

4. **Idempotency.** Call `connectMCPStream` twice for the same execution; verify only one WS opens.

5. **Retry on disconnect.** Mock server accepts one connection, closes it, accepts a second. Verify backend reconnects.

### End-to-end test

Add a regression test that:
1. Builds a fake agentctl WS server that proxies MCP requests to a stub backend dispatcher.
2. Launches a passthrough session pointed at the fake server.
3. Sends an `MCP_LIST_WORKSPACES` request through the channel; verifies it round-trips.

This test would have caught the bug PR [#1078](https://github.com/kdlbs/kandev/pull/1078) shipped. It belongs alongside `TestPassthroughAgentCommandInjectsKandevMCPConfig` — those tests assert config is generated; this one asserts the config actually works.

## Out of scope

- **MCP_TIMEOUT env var** — keep as a safety net; PR [#3](https://github.com/kdlbs/kandev/pull/3) is correct independent of this fix.
- **ask_user_question tool gating** — already handled by PR [#2](https://github.com/kdlbs/kandev/pull/2) at tool-registration time.
- **Recovery refactor** — see "Recovery path" above; deferred.
- **In-process agentctl for standalone mode** — would eliminate the WS round-trip entirely but is a much larger architectural change. The channel/WS path is fine once it's connected.

## Open questions

None blocking. Spec ready for plan.
