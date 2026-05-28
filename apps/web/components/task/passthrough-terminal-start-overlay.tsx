"use client";

import { useCallback, useState } from "react";
import { IconPlayerPlay, IconLoader2 } from "@tabler/icons-react";
import { Button } from "@kandev/ui/button";
import { launchSession } from "@/lib/services/session-launch-service";
import { buildStartCreatedRequest } from "@/lib/services/session-launch-helpers";

type Props = {
  taskId: string;
  sessionId: string;
  sessionState: string | null;
};

/**
 * Overlay shown on a passthrough terminal when the session is in CREATED
 * state with no PTY running. Mirrors ACP's TaskDescriptionStartButton so the
 * user explicitly starts the agent rather than having the PTY launched
 * silently at task-open.
 */
export function PassthroughTerminalStartOverlay({ taskId, sessionId, sessionState }: Props) {
  const [isStarting, setIsStarting] = useState(false);

  const handleStart = useCallback(async () => {
    setIsStarting(true);
    try {
      const { request } = buildStartCreatedRequest(taskId, sessionId);
      await launchSession(request);
    } catch (error) {
      console.error("Failed to start passthrough agent:", error);
      setIsStarting(false);
    }
    // When start succeeds, the session transitions out of CREATED and the
    // parent unmounts this overlay; we deliberately leave isStarting=true
    // until then so the button stays disabled.
  }, [taskId, sessionId]);

  if (sessionState !== "CREATED") return null;

  return (
    <div
      data-testid="passthrough-start-overlay"
      className="absolute inset-0 flex items-start justify-center pt-12 bg-background"
    >
      <div className="flex flex-col items-center gap-3 text-muted-foreground">
        <Button
          size="sm"
          variant="default"
          className="cursor-pointer gap-1.5"
          onClick={handleStart}
          disabled={isStarting}
          data-testid="passthrough-start-button"
        >
          {isStarting ? (
            <IconLoader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <IconPlayerPlay className="h-3.5 w-3.5" />
          )}
          {isStarting ? "Starting…" : "Start agent"}
        </Button>
        <span className="text-sm">The TUI agent is ready to launch in this workspace.</span>
      </div>
    </div>
  );
}
