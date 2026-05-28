package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kandev/kandev/internal/common/logger"
	"github.com/kandev/kandev/internal/orchestrator"
	"github.com/kandev/kandev/internal/task/models"
	"github.com/kandev/kandev/internal/task/service"
)

type captureCreateTaskRepo struct {
	mockRepository
	captured *models.Task
}

func (m *captureCreateTaskRepo) GetWorkspaceTaskPrefix(_ context.Context, _ string) (string, string, error) {
	return "KAN", "wf-office", nil
}

func (m *captureCreateTaskRepo) CreateTask(_ context.Context, task *models.Task) error {
	m.captured = task
	return nil
}

// TestHTTPCreateTask_ProjectIDReachesOfficePath guards the wiring that broke
// the office "New Task" dialog: when the request body sets project_id (and
// omits workflow_id), the handler must forward it to the service so
// isOfficeRequest() returns true and the workflow is auto-resolved. Without
// this, requests fall through to the kanban validator with
// "workflow_id is required for non-ephemeral tasks".
func TestHTTPCreateTask_ProjectIDReachesOfficePath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	log := newTestLogger(t)

	repo := &captureCreateTaskRepo{}
	svc := service.NewService(service.Repos{
		Workspaces: repo, Tasks: repo, TaskRepos: repo,
		Workflows: repo, Messages: repo, Turns: repo,
		Sessions: repo, GitSnapshots: repo, RepoEntities: repo,
		Executors: repo, Environments: repo, TaskEnvironments: repo,
		Reviews: repo,
	}, nil, log, service.RepositoryDiscoveryConfig{})
	h := &TaskHandlers{service: svc, logger: log}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{
		"workspace_id": "ws-1",
		"title": "Analyse integrations",
		"project_id": "proj-1",
		"priority": "medium"
	}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.httpCreateTask(c)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.NotNil(t, repo.captured, "service.CreateTask was not called")
	assert.Equal(t, "proj-1", repo.captured.ProjectID)
	assert.Equal(t, "wf-office", repo.captured.WorkflowID, "office workflow should be auto-resolved")
}

func newTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.LoggingConfig{
		Level:      "debug",
		Format:     "console",
		OutputPath: "stdout",
	})
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	return log
}

// subtaskCountRepo lets the subtask-count handler test drive
// ListChildren to specific values / errors without standing up a real
// SQLite repo.
type subtaskCountRepo struct {
	mockRepository
	children []*models.Task
	err      error
}

func (r *subtaskCountRepo) ListChildren(_ context.Context, _ string) ([]*models.Task, error) {
	return r.children, r.err
}

func (r *subtaskCountRepo) CountToolCallMessagesBySession(
	_ context.Context, _ []string,
) (map[string]int, error) {
	return nil, nil
}

func TestHTTPTaskSubtaskCount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	log := newTestLogger(t)

	t.Run("returns count for task with subtasks", func(t *testing.T) {
		repo := &subtaskCountRepo{children: []*models.Task{{ID: "c1"}, {ID: "c2"}, {ID: "c3"}}}
		h := &TaskHandlers{repo: repo, logger: log}
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/tasks/root/subtask-count", nil)
		c.Params = gin.Params{{Key: "id", Value: "root"}}

		h.httpTaskSubtaskCount(c)

		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		assert.JSONEq(t, `{"count":3}`, rec.Body.String())
	})

	t.Run("returns zero for task with no subtasks", func(t *testing.T) {
		repo := &subtaskCountRepo{children: nil}
		h := &TaskHandlers{repo: repo, logger: log}
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/tasks/root/subtask-count", nil)
		c.Params = gin.Params{{Key: "id", Value: "root"}}

		h.httpTaskSubtaskCount(c)

		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		assert.JSONEq(t, `{"count":0}`, rec.Body.String())
	})

	t.Run("returns 500 with a generic error on repo failure", func(t *testing.T) {
		repo := &subtaskCountRepo{err: errors.New("sql: connection refused: postgres://user@host/db")}
		h := &TaskHandlers{repo: repo, logger: log}
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/tasks/root/subtask-count", nil)
		c.Params = gin.Params{{Key: "id", Value: "root"}}

		h.httpTaskSubtaskCount(c)

		require.Equal(t, http.StatusInternalServerError, rec.Code)
		// Must NOT leak the raw error (would expose DSN / driver details).
		assert.NotContains(t, rec.Body.String(), "postgres://")
		assert.Contains(t, rec.Body.String(), "failed to count subtasks")
	})
}

func TestHandleSelectedMoveError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	log := newTestLogger(t)

	tests := []struct {
		name             string
		err              error
		want             int
		wantBodyContains string
	}{
		{
			name: "not found",
			err:  errors.New("task not found: task-1"),
			want: http.StatusNotFound,
		},
		{
			name: "move conflict",
			err:  errors.New("task task-1 cannot be moved: task has an active session (running)"),
			want: http.StatusConflict,
		},
		{
			name: "bad request validation",
			err:  errors.New("invalid workflow id"),
			want: http.StatusBadRequest,
		},
		{
			name:             "internal",
			err:              errors.New("failed to count target workflow step tasks: database is locked"),
			want:             http.StatusInternalServerError,
			wantBodyContains: "task move failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)

			handleSelectedMoveError(c, log, tc.err)

			assert.Equal(t, tc.want, rec.Code)
			if tc.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tc.wantBodyContains)
			}
		})
	}
}

type moveTaskConflictRepo struct {
	mockRepository
	task      *models.Task
	sessions  []*models.TaskSession
	workflows map[string]*models.Workflow
}

func (m *moveTaskConflictRepo) GetTask(ctx context.Context, id string) (*models.Task, error) {
	return m.task, nil
}

func (m *moveTaskConflictRepo) UpdateTask(ctx context.Context, task *models.Task) error {
	m.task = task
	return nil
}

func (m *moveTaskConflictRepo) GetWorkflow(ctx context.Context, id string) (*models.Workflow, error) {
	if m.workflows != nil {
		if workflow, ok := m.workflows[id]; ok {
			return workflow, nil
		}
	}
	return &models.Workflow{ID: id, WorkspaceID: m.task.WorkspaceID}, nil
}

func (m *moveTaskConflictRepo) ListTaskSessions(ctx context.Context, taskID string) ([]*models.TaskSession, error) {
	return m.sessions, nil
}

func (m *moveTaskConflictRepo) GetPrimarySessionByTaskID(ctx context.Context, taskID string) (*models.TaskSession, error) {
	for _, session := range m.sessions {
		if session.TaskID == taskID && session.IsPrimary {
			return session, nil
		}
	}
	return nil, nil
}

func TestHTTPMoveTaskMapsMoveConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	log := newTestLogger(t)
	archivedAt := time.Now().UTC()

	tests := []struct {
		name     string
		task     *models.Task
		sessions []*models.TaskSession
	}{
		{
			name: "archived task",
			task: &models.Task{
				ID:             "task-archived",
				WorkspaceID:    "workspace-1",
				WorkflowID:     "wf-source",
				WorkflowStepID: "step-source",
				ArchivedAt:     &archivedAt,
			},
		},
		{
			name: "active non-primary session",
			task: &models.Task{
				ID:             "task-running",
				WorkspaceID:    "workspace-1",
				WorkflowID:     "wf-source",
				WorkflowStepID: "step-source",
			},
			sessions: []*models.TaskSession{{
				ID:     "session-running",
				TaskID: "task-running",
				State:  models.TaskSessionStateRunning,
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &moveTaskConflictRepo{task: tc.task, sessions: tc.sessions}
			svc := service.NewService(service.Repos{
				Workspaces: repo, Tasks: repo, TaskRepos: repo,
				Workflows: repo, Messages: repo, Turns: repo,
				Sessions: repo, GitSnapshots: repo, RepoEntities: repo,
				Executors: repo, Environments: repo, TaskEnvironments: repo,
				Reviews: repo,
			}, nil, log, service.RepositoryDiscoveryConfig{})
			h := &TaskHandlers{service: svc, logger: log}
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Params = gin.Params{{Key: "id", Value: tc.task.ID}}
			c.Request = httptest.NewRequest(http.MethodPost, "/tasks/"+tc.task.ID+"/move", strings.NewReader(`{
				"workflow_id": "wf-target",
				"workflow_step_id": "step-target",
				"position": 0
			}`))
			c.Request.Header.Set("Content-Type", "application/json")

			h.httpMoveTask(c)

			assert.Equal(t, http.StatusConflict, rec.Code)
		})
	}
}

func TestHTTPMoveTaskAllowsRunningPrimarySession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	log := newTestLogger(t)
	task := &models.Task{
		ID:             "task-running-primary",
		WorkspaceID:    "workspace-1",
		WorkflowID:     "wf-source",
		WorkflowStepID: "step-source",
	}
	repo := &moveTaskConflictRepo{
		task: task,
		sessions: []*models.TaskSession{{
			ID:        "session-running-primary",
			TaskID:    task.ID,
			State:     models.TaskSessionStateRunning,
			IsPrimary: true,
		}},
	}
	svc := service.NewService(service.Repos{
		Workspaces: repo, Tasks: repo, TaskRepos: repo,
		Workflows: repo, Messages: repo, Turns: repo,
		Sessions: repo, GitSnapshots: repo, RepoEntities: repo,
		Executors: repo, Environments: repo, TaskEnvironments: repo,
		Reviews: repo,
	}, nil, log, service.RepositoryDiscoveryConfig{})
	h := &TaskHandlers{service: svc, logger: log}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Params = gin.Params{{Key: "id", Value: task.ID}}
	c.Request = httptest.NewRequest(http.MethodPost, "/tasks/"+task.ID+"/move", strings.NewReader(`{
		"workflow_id": "wf-target",
		"workflow_step_id": "step-target",
		"position": 0
	}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.httpMoveTask(c)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	assert.Equal(t, "wf-target", repo.task.WorkflowID)
	assert.Equal(t, "step-target", repo.task.WorkflowStepID)
}

func TestResolveFreshBranchName(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		taskTitle string
		assert    func(t *testing.T, got string)
	}{
		{
			name:      "uses raw name when provided",
			raw:       "feature/x",
			taskTitle: "ignored",
			assert: func(t *testing.T, got string) {
				if got != "feature/x" {
					t.Fatalf("expected feature/x, got %q", got)
				}
			},
		},
		{
			name:      "trims whitespace from raw name",
			raw:       "  feature/y  ",
			taskTitle: "ignored",
			assert: func(t *testing.T, got string) {
				if got != "feature/y" {
					t.Fatalf("expected feature/y, got %q", got)
				}
			},
		},
		{
			name:      "derives from title when raw is empty",
			raw:       "",
			taskTitle: "Fix login bug",
			assert: func(t *testing.T, got string) {
				if !strings.HasPrefix(got, "fix-login-bug_") {
					t.Fatalf("expected fix-login-bug_ prefix, got %q", got)
				}
			},
		},
		{
			name:      "title with only special chars falls back to suffix only",
			raw:       "",
			taskTitle: "!!!",
			assert: func(t *testing.T, got string) {
				// SemanticWorktreeName returns just the suffix (3 chars from
				// the alphabet) when the sanitized title is empty.
				if len(got) != 3 {
					t.Fatalf("expected 3-char suffix, got %q (len %d)", got, len(got))
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.assert(t, resolveFreshBranchName(tc.raw, tc.taskTitle))
		})
	}
}

func TestAssociatePRFromRepoInputs(t *testing.T) {
	log := newTestLogger(t)

	t.Run("calls callback when repo input contains PR URL", func(t *testing.T) {
		var mu sync.Mutex
		var called bool
		var gotTaskID, gotSessionID, gotPRURL, gotBranch string

		h := &TaskHandlers{logger: log}
		h.SetOnTaskCreatedWithPR(func(ctx context.Context, taskID, sessionID, prURL, branch string) {
			mu.Lock()
			defer mu.Unlock()
			called = true
			gotTaskID = taskID
			gotSessionID = sessionID
			gotPRURL = prURL
			gotBranch = branch
		})

		// The callback runs in a goroutine, so we need a channel to sync
		done := make(chan struct{})
		h.onTaskCreatedWithPR = func(ctx context.Context, taskID, sessionID, prURL, branch string) {
			defer close(done)
			mu.Lock()
			defer mu.Unlock()
			called = true
			gotTaskID = taskID
			gotSessionID = sessionID
			gotPRURL = prURL
			gotBranch = branch
		}

		h.associatePRFromRepoInputs("task-1", "session-1", []httpTaskRepositoryInput{
			{
				GitHubURL:      "https://github.com/owner/repo/pull/123",
				CheckoutBranch: "feature-branch",
			},
		})

		<-done

		mu.Lock()
		defer mu.Unlock()
		require.True(t, called)
		assert.Equal(t, "task-1", gotTaskID)
		assert.Equal(t, "session-1", gotSessionID)
		assert.Equal(t, "https://github.com/owner/repo/pull/123", gotPRURL)
		assert.Equal(t, "feature-branch", gotBranch)
	})

	t.Run("does not call callback for plain repo URLs", func(t *testing.T) {
		called := false
		h := &TaskHandlers{logger: log}
		h.SetOnTaskCreatedWithPR(func(ctx context.Context, taskID, sessionID, prURL, branch string) {
			called = true
		})

		h.associatePRFromRepoInputs("task-1", "", []httpTaskRepositoryInput{
			{
				GitHubURL:      "https://github.com/owner/repo",
				CheckoutBranch: "main",
			},
		})

		assert.False(t, called)
	})

	t.Run("does not call callback when no onTaskCreatedWithPR set", func(t *testing.T) {
		h := &TaskHandlers{logger: log}
		// Should not panic
		h.associatePRFromRepoInputs("task-1", "", []httpTaskRepositoryInput{
			{
				GitHubURL:      "https://github.com/owner/repo/pull/123",
				CheckoutBranch: "feature-branch",
			},
		})
	})

	t.Run("passes empty session ID when no session created", func(t *testing.T) {
		done := make(chan struct{})
		var gotSessionID string

		h := &TaskHandlers{logger: log}
		h.onTaskCreatedWithPR = func(ctx context.Context, taskID, sessionID, prURL, branch string) {
			defer close(done)
			gotSessionID = sessionID
		}

		h.associatePRFromRepoInputs("task-1", "", []httpTaskRepositoryInput{
			{
				GitHubURL:      "https://github.com/owner/repo/pull/456",
				CheckoutBranch: "fix-branch",
			},
		})

		<-done
		assert.Equal(t, "", gotSessionID)
	})

	t.Run("only processes first PR URL when multiple repos have PRs", func(t *testing.T) {
		var count int
		var mu sync.Mutex
		done := make(chan struct{})

		h := &TaskHandlers{logger: log}
		h.onTaskCreatedWithPR = func(ctx context.Context, taskID, sessionID, prURL, branch string) {
			defer close(done)
			mu.Lock()
			defer mu.Unlock()
			count++
		}

		h.associatePRFromRepoInputs("task-1", "", []httpTaskRepositoryInput{
			{
				GitHubURL:      "https://github.com/owner/repo/pull/1",
				CheckoutBranch: "branch-1",
			},
			{
				GitHubURL:      "https://github.com/owner/repo/pull/2",
				CheckoutBranch: "branch-2",
			},
		})

		<-done
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 1, count)
	})
}

// recordingOrchestrator captures LaunchSession calls and returns a
// configurable passthrough verdict from IsPassthroughProfile. Used by
// the create-without-starting-agent tests to assert that the handler
// skips LaunchSession for passthrough profiles.
type recordingOrchestrator struct {
	launchCalls     []*orchestrator.LaunchSessionRequest
	passthroughByID map[string]bool
}

func (r *recordingOrchestrator) LaunchSession(_ context.Context, req *orchestrator.LaunchSessionRequest) (*orchestrator.LaunchSessionResponse, error) {
	r.launchCalls = append(r.launchCalls, req)
	return &orchestrator.LaunchSessionResponse{Success: true, TaskID: req.TaskID, SessionID: "sess-1"}, nil
}

func (r *recordingOrchestrator) EnsureSession(_ context.Context, _ string, _ ...orchestrator.EnsureSessionOptions) (*orchestrator.EnsureSessionResponse, error) {
	return nil, nil
}

func (r *recordingOrchestrator) IsPassthroughProfile(_ context.Context, profileID string) bool {
	return r.passthroughByID[profileID]
}

// TestHTTPCreateTask_PassthroughPrepareDefersAgentStart guards the contract
// that "Create without starting agent" creates a passthrough session row in
// CREATED state with no PTY running, mirroring ACP. The agent only starts
// when the user clicks Start in the terminal panel (which calls
// LaunchSession with IntentStartCreated).
func TestHTTPCreateTask_PassthroughPrepareDefersAgentStart(t *testing.T) {
	gin.SetMode(gin.TestMode)
	log := newTestLogger(t)

	repo := &captureCreateTaskRepo{}
	svc := service.NewService(service.Repos{
		Workspaces: repo, Tasks: repo, TaskRepos: repo,
		Workflows: repo, Messages: repo, Turns: repo,
		Sessions: repo, GitSnapshots: repo, RepoEntities: repo,
		Executors: repo, Environments: repo, TaskEnvironments: repo,
		Reviews: repo,
	}, nil, log, service.RepositoryDiscoveryConfig{})

	orch := &recordingOrchestrator{
		passthroughByID: map[string]bool{"profile-passthrough": true},
	}
	h := &TaskHandlers{service: svc, orchestrator: orch, logger: log}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{
		"workspace_id": "ws-1",
		"workflow_id": "wf-1",
		"title": "TUI task with no agent",
		"description": "look around",
		"agent_profile_id": "profile-passthrough",
		"prepare_session": true
	}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.httpCreateTask(c)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.Len(t, orch.launchCalls, 1, "passthrough prepare-only must still create a session row via LaunchSession; the agent start is deferred via SkipPassthroughUpgrade")
	call := orch.launchCalls[0]
	assert.Equal(t, orchestrator.IntentPrepare, call.Intent)
	assert.True(t, call.SkipPassthroughUpgrade,
		"handler must pass SkipPassthroughUpgrade=true for passthrough so launchPrepare creates the session in CREATED state without starting the PTY")
	assert.Contains(t, rec.Body.String(), `"session_id"`,
		"response must carry session_id so the frontend can call IntentStartCreated when the user clicks Start")
}

// TestHTTPCreateTask_NonPassthroughPrepareCallsLaunchSession is the control
// case for the passthrough skip: non-passthrough profiles must still go
// through LaunchSession(IntentPrepare) so the session row is created and
// the workspace is prepared. Without this guard a refactor could accidentally
// short-circuit prepare for everyone.
func TestHTTPCreateTask_NonPassthroughPrepareCallsLaunchSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	log := newTestLogger(t)

	repo := &captureCreateTaskRepo{}
	svc := service.NewService(service.Repos{
		Workspaces: repo, Tasks: repo, TaskRepos: repo,
		Workflows: repo, Messages: repo, Turns: repo,
		Sessions: repo, GitSnapshots: repo, RepoEntities: repo,
		Executors: repo, Environments: repo, TaskEnvironments: repo,
		Reviews: repo,
	}, nil, log, service.RepositoryDiscoveryConfig{})

	orch := &recordingOrchestrator{
		// "profile-acp" is intentionally absent from passthroughByID so
		// IsPassthroughProfile returns false (zero value of bool).
		passthroughByID: map[string]bool{},
	}
	h := &TaskHandlers{service: svc, orchestrator: orch, logger: log}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{
		"workspace_id": "ws-1",
		"workflow_id": "wf-1",
		"title": "ACP task with no agent",
		"description": "look around",
		"agent_profile_id": "profile-acp",
		"prepare_session": true
	}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.httpCreateTask(c)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.Len(t, orch.launchCalls, 1, "non-passthrough prepare must still call LaunchSession")
	assert.Equal(t, orchestrator.IntentPrepare, orch.launchCalls[0].Intent)
	assert.Equal(t, "profile-acp", orch.launchCalls[0].AgentProfileID)
	// SkipPassthroughUpgrade is set to !PlanMode by the handler unconditionally;
	// for non-passthrough profiles it is harmless because launchPrepare's upgrade
	// guard only fires when isPassthroughProfile returns true. This assertion
	// documents the current (correct) value so regressions show up as failures.
	assert.True(t, orch.launchCalls[0].SkipPassthroughUpgrade,
		"SkipPassthroughUpgrade is !PlanMode; for a non-plan-mode prepare it is true regardless of profile type (harmless for non-passthrough)")
}

// TestHTTPCreateTask_PassthroughPlanModePrepareStillCallsLaunchSession guards
// the exception to the passthrough skip: when the caller asks for a plan-mode
// prepare-only create (which happens when the user clicks "Start task" with
// no description for a passthrough profile), the handler MUST still call
// LaunchSession so the response carries a session_id. The frontend uses that
// session_id to activate the plan panel; without it, the click is silently
// dropped.
func TestHTTPCreateTask_PassthroughPlanModePrepareStillCallsLaunchSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	log := newTestLogger(t)

	repo := &captureCreateTaskRepo{}
	svc := service.NewService(service.Repos{
		Workspaces: repo, Tasks: repo, TaskRepos: repo,
		Workflows: repo, Messages: repo, Turns: repo,
		Sessions: repo, GitSnapshots: repo, RepoEntities: repo,
		Executors: repo, Environments: repo, TaskEnvironments: repo,
		Reviews: repo,
	}, nil, log, service.RepositoryDiscoveryConfig{})

	orch := &recordingOrchestrator{
		passthroughByID: map[string]bool{"profile-passthrough": true},
	}
	h := &TaskHandlers{service: svc, orchestrator: orch, logger: log}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{
		"workspace_id": "ws-1",
		"workflow_id": "wf-1",
		"title": "Plan-mode passthrough task",
		"agent_profile_id": "profile-passthrough",
		"prepare_session": true,
		"plan_mode": true
	}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.httpCreateTask(c)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.Len(t, orch.launchCalls, 1, "plan-mode passthrough prepare must still call LaunchSession so the frontend can activate the plan panel")
	assert.Equal(t, orchestrator.IntentPrepare, orch.launchCalls[0].Intent)
	assert.False(t, orch.launchCalls[0].SkipPassthroughUpgrade,
		"plan-mode passthrough prepare must keep the upgrade so the agent starts and the plan panel has session state to attach to")
}
