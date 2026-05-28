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
		name             string
		agent            agents.Agent
		isPassthroughRun bool
		want             bool
	}{
		{
			name:             "TUI-only agent in any mode is disabled",
			agent:            tuiOnly,
			isPassthroughRun: false,
			want:             true,
		},
		{
			name:             "TUI-only agent in passthrough is disabled",
			agent:            tuiOnly,
			isPassthroughRun: true,
			want:             true,
		},
		{
			name:             "dual-mode agent in passthrough is disabled",
			agent:            dualMode,
			isPassthroughRun: true,
			want:             true,
		},
		{
			name:             "dual-mode agent in ACP mode keeps ask_user_question enabled",
			agent:            dualMode,
			isPassthroughRun: false,
			want:             false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldDisableAskQuestion(tc.agent, tc.isPassthroughRun)
			if got != tc.want {
				t.Fatalf("shouldDisableAskQuestion = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPassthroughForLaunch(t *testing.T) {
	profilePass := &AgentProfileInfo{CLIPassthrough: true}
	profileACP := &AgentProfileInfo{CLIPassthrough: false}

	tests := []struct {
		name                 string
		sessionID            string
		sessionIsPassthrough bool
		profile              *AgentProfileInfo
		want                 bool
	}{
		{
			name:                 "session-bound passthrough launch uses the snapshot",
			sessionID:            "sess-1",
			sessionIsPassthrough: true,
			profile:              profileACP, // live profile says ACP, snapshot wins
			want:                 true,
		},
		{
			name:                 "session-bound ACP launch ignores live profile flip",
			sessionID:            "sess-1",
			sessionIsPassthrough: false,
			profile:              profilePass, // profile was toggled mid-flight, snapshot wins
			want:                 false,
		},
		{
			name:                 "sessionless launch falls back to the live profile (passthrough)",
			sessionID:            "",
			sessionIsPassthrough: false,
			profile:              profilePass,
			want:                 true,
		},
		{
			name:                 "sessionless launch falls back to the live profile (ACP)",
			sessionID:            "",
			sessionIsPassthrough: false,
			profile:              profileACP,
			want:                 false,
		},
		{
			name:                 "sessionless launch with nil profile is non-passthrough",
			sessionID:            "",
			sessionIsPassthrough: false,
			profile:              nil,
			want:                 false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := passthroughForLaunch(tc.sessionID, tc.sessionIsPassthrough, tc.profile)
			if got != tc.want {
				t.Fatalf("passthroughForLaunch = %v, want %v", got, tc.want)
			}
		})
	}
}
