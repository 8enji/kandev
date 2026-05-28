package lifecycle

import "github.com/kandev/kandev/internal/agent/agents"

// shouldDisableAskQuestion returns true when the interactive
// ask_user_question_kandev MCP tool should NOT be registered on the
// agentctl MCP server for this execution.
//
// The tool's matching UI (ClarificationInputOverlay) is mounted inside
// TaskChatPanel, which is hidden when the session runs in CLI passthrough
// mode (the PassthroughToolbar replaces it). Without that overlay the
// agent's MCP call has no surface to collect an answer from, so the
// agent's tool call will hang until the 2h MCP timeout fires.
//
// Two cases need the tool disabled:
//   - Pure TUI agents (*agents.TUIAgent), which only ever run passthrough.
//   - Any launch happening in passthrough mode for a dual-mode agent.
//
// Callers resolve isPassthroughLaunch with snapshot-first precedence via
// passthroughForLaunch — see its docstring for why the session snapshot
// must win over the live profile.
func shouldDisableAskQuestion(agent agents.Agent, isPassthroughLaunch bool) bool {
	if agents.IsPassthroughOnly(agent) {
		return true
	}
	return isPassthroughLaunch
}

// passthroughForLaunch resolves whether a session-bound launch is running
// in CLI passthrough mode. For session-bound launches the session's
// IsPassthrough snapshot (taken at session creation) is the source of
// truth so a profile that toggles CLIPassthrough after the session was
// created does not strand an in-flight session in the wrong UI mode.
// Sessionless launches (e.g. controller.LaunchAgent) fall back to the
// live profile.
func passthroughForLaunch(sessionID string, sessionIsPassthrough bool, profile *AgentProfileInfo) bool {
	if sessionID != "" {
		return sessionIsPassthrough
	}
	if profile != nil {
		return profile.CLIPassthrough
	}
	return false
}
