import { afterEach, describe, expect, it, vi, beforeEach } from "vitest";
import { cleanup, render, screen, fireEvent, waitFor } from "@testing-library/react";

vi.mock("@/lib/services/session-launch-service", () => ({
  launchSession: vi.fn().mockResolvedValue({ success: true }),
}));

vi.mock("@/lib/services/session-launch-helpers", () => ({
  buildStartCreatedRequest: vi.fn((taskId: string, sessionId: string) => ({
    request: { task_id: taskId, session_id: sessionId, intent: "start_created" },
    layout: "keep",
  })),
}));

import { launchSession } from "@/lib/services/session-launch-service";
import { buildStartCreatedRequest } from "@/lib/services/session-launch-helpers";
import { PassthroughTerminalStartOverlay } from "./passthrough-terminal-start-overlay";

describe("PassthroughTerminalStartOverlay", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });
  afterEach(cleanup);

  it("renders a Start button when the session is in CREATED state", () => {
    render(
      <PassthroughTerminalStartOverlay
        taskId="task-1"
        sessionId="sess-1"
        sessionState="CREATED"
      />,
    );
    expect(screen.getByRole("button", { name: /start agent/i })).toBeTruthy();
  });

  it("calls launchSession with a start_created request when clicked", async () => {
    render(
      <PassthroughTerminalStartOverlay
        taskId="task-1"
        sessionId="sess-1"
        sessionState="CREATED"
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /start agent/i }));

    await waitFor(() => {
      expect(buildStartCreatedRequest).toHaveBeenCalledWith("task-1", "sess-1");
      expect(launchSession).toHaveBeenCalledWith({
        task_id: "task-1",
        session_id: "sess-1",
        intent: "start_created",
      });
    });
  });

  it("shows a 'Starting...' label and disables the button after click", async () => {
    render(
      <PassthroughTerminalStartOverlay
        taskId="task-1"
        sessionId="sess-1"
        sessionState="CREATED"
      />,
    );
    const button = screen.getByRole("button", { name: /start agent/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText(/starting/i)).toBeTruthy();
    });
    expect((button as HTMLButtonElement).disabled).toBe(true);
  });
});
