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
//   - Dual-mode agents (ClaudeACP, etc.) launched with the profile's
//     CLIPassthrough flag set.
func shouldDisableAskQuestion(agent agents.Agent, profile *AgentProfileInfo) bool {
	if agents.IsPassthroughOnly(agent) {
		return true
	}
	if profile != nil && profile.CLIPassthrough {
		return true
	}
	return false
}
