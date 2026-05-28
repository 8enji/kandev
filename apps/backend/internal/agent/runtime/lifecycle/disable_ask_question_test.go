package lifecycle

import (
	"testing"

	"github.com/kandev/kandev/internal/agent/agents"
)

func TestShouldDisableAskQuestion(t *testing.T) {
	tuiOnly := agents.NewTUIAgent(agents.TUIAgentConfig{
		AgentID:   "fake-tui",
		AgentName: "fake-tui",
		Command:   "fake-tui",
	})
	dualMode := agents.NewClaudeACP()

	tests := []struct {
		name    string
		agent   agents.Agent
		profile *AgentProfileInfo
		want    bool
	}{
		{
			name:    "TUI-only agent with no profile is disabled",
			agent:   tuiOnly,
			profile: nil,
			want:    true,
		},
		{
			name:    "TUI-only agent with non-passthrough profile is still disabled",
			agent:   tuiOnly,
			profile: &AgentProfileInfo{CLIPassthrough: false},
			want:    true,
		},
		{
			name:    "dual-mode agent in CLI passthrough is disabled",
			agent:   dualMode,
			profile: &AgentProfileInfo{CLIPassthrough: true},
			want:    true,
		},
		{
			name:    "dual-mode agent in ACP mode keeps ask_user_question enabled",
			agent:   dualMode,
			profile: &AgentProfileInfo{CLIPassthrough: false},
			want:    false,
		},
		{
			name:    "dual-mode agent with nil profile keeps ask_user_question enabled",
			agent:   dualMode,
			profile: nil,
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldDisableAskQuestion(tc.agent, tc.profile)
			if got != tc.want {
				t.Fatalf("shouldDisableAskQuestion = %v, want %v", got, tc.want)
			}
		})
	}
}
