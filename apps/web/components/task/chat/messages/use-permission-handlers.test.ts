import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { sessionId as toSessionId } from "@/lib/types/http";
import type { Message } from "@/lib/types/http";
import type { PermissionRequestMetadata } from "./use-permission-handlers";

const requestMock = vi.fn().mockResolvedValue({});

vi.mock("@/lib/ws/connection", () => ({
  getWebSocketClient: () => ({ request: requestMock }),
}));

describe("usePermissionResponseHandlers", () => {
  let usePermissionResponseHandlers: typeof import("./use-permission-handlers").usePermissionResponseHandlers;

  beforeEach(async () => {
    vi.resetModules();
    requestMock.mockReset().mockResolvedValue({});
    ({ usePermissionResponseHandlers } = await import("./use-permission-handlers"));
  });

  function makePermissionMessage(overrides: Partial<PermissionRequestMetadata> = {}): Message {
    const meta: PermissionRequestMetadata = {
      pending_id: "pend-1",
      tool_call_id: "tc-1",
      action_type: "mcp_tool",
      action_details: {},
      options: [
        { option_id: "allow", name: "Allow", kind: "allow_once" },
        { option_id: "deny", name: "Deny", kind: "reject_once" },
      ],
      ...overrides,
    };
    return {
      id: "msg-1",
      session_id: toSessionId("sess-1"),
      task_id: "task-1" as ReturnType<typeof import("@/lib/types/http").taskId>,
      author_type: "agent",
      content: "",
      type: "permission_request",
      created_at: "2026-05-25T00:00:00Z",
      metadata: meta as unknown as Record<string, unknown>,
    } as unknown as Message;
  }

  describe("handleApprove", () => {
    it("sends cancelled=false and rejected=false", async () => {
      const permissionMessage = makePermissionMessage();
      const { result } = renderHook(() =>
        usePermissionResponseHandlers({
          permissionMetadata: permissionMessage.metadata as unknown as PermissionRequestMetadata,
          permissionMessage,
        }),
      );

      await act(async () => {
        result.current.handleApprove();
      });

      expect(requestMock).toHaveBeenCalledOnce();
      const payload = requestMock.mock.calls[0][1] as Record<string, unknown>;
      expect(payload.option_id).toBe("allow");
      expect(payload.cancelled).toBeFalsy();
      expect(payload.rejected).toBeFalsy();
    });
  });

  describe("handleReject", () => {
    it("sends rejected=true and cancelled=false when a reject option exists", async () => {
      const permissionMessage = makePermissionMessage();
      const { result } = renderHook(() =>
        usePermissionResponseHandlers({
          permissionMetadata: permissionMessage.metadata as unknown as PermissionRequestMetadata,
          permissionMessage,
        }),
      );

      await act(async () => {
        result.current.handleReject();
      });

      expect(requestMock).toHaveBeenCalledOnce();
      const payload = requestMock.mock.calls[0][1] as Record<string, unknown>;
      expect(payload.option_id).toBe("deny");
      expect(payload.rejected).toBe(true);
      // Must NOT set cancelled=true: that triggers EventTypePermissionCancelled
      // which races against the orchestrator's UpdatePermissionMessage("rejected")
      // and would overwrite the status to "expired".
      expect(payload.cancelled).toBeFalsy();
    });

    it("falls back to cancelled=true when no reject option exists", async () => {
      const permissionMessage = makePermissionMessage({
        options: [{ option_id: "allow", name: "Allow", kind: "allow_once" }],
      });
      const { result } = renderHook(() =>
        usePermissionResponseHandlers({
          permissionMetadata: permissionMessage.metadata as unknown as PermissionRequestMetadata,
          permissionMessage,
        }),
      );

      await act(async () => {
        result.current.handleReject();
      });

      expect(requestMock).toHaveBeenCalledOnce();
      const payload = requestMock.mock.calls[0][1] as Record<string, unknown>;
      expect(payload.cancelled).toBe(true);
      expect(payload.option_id).toBeUndefined();
    });
  });
});
