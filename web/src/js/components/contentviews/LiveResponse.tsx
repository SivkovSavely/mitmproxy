import * as React from "react";
import type { PreviewState } from "../../ducks/responsePreview";

export default function LiveResponse({ state }: { state: PreviewState }) {
    const preview = state.preview;
    const status = preview?.status ?? "waiting";
    const active = status === "waiting" || status === "streaming";
    return (
        <div className="contentview live-response">
            <div className="controls" role="status">
                <h5>
                    {active
                        ? "LIVE"
                        : status === "finished"
                          ? "Finished"
                          : "Aborted"}{" "}
                    response preview
                </h5>
                <span>
                    {preview?.received ?? 0} bytes received; {state.bytes} bytes
                    shown
                </span>
            </div>
            {status === "waiting" && <p>Waiting for response bytes...</p>}
            {preview?.reason && (
                <p role="note">
                    Preview {state.bytes ? "interrupted" : "unavailable"}:{" "}
                    {preview.reason}
                </p>
            )}
            {(preview?.missing ||
                (preview?.dropped ?? 0) > 0 ||
                state.capped) && (
                <p role="note">
                    Incomplete preview: earlier bytes or gaps are missing.{" "}
                    Dropped source bytes: {preview?.dropped ?? 0}. Preview
                    limited to {preview?.max_bytes ?? 0} UTF-8 bytes.
                </p>
            )}
            {!active && !preview?.stored && (
                <p>
                    Full body not retained. This is only the watched response
                    preview.
                </p>
            )}
            <pre
                aria-label="Live response text"
                tabIndex={0}
                style={{
                    overflow: "auto",
                    maxHeight: "60vh",
                    whiteSpace: "pre",
                    overflowAnchor: "none",
                }}
            >
                {state.chunks.map((chunk) => (
                    <span key={chunk.id}>{chunk.text}</span>
                ))}
            </pre>
        </div>
    );
}
