import * as React from "react";
import { useAppDispatch, useAppSelector } from "../../ducks";
import {
    applyDraft,
    refreshTimeRange,
    relativePresets,
    updateDraft,
} from "../../ducks/timeRange";
import type { TimeRangeMode } from "../../ducks/timeRange";

export function TimeRangeRefresh() {
    const dispatch = useAppDispatch();
    const instanceId = useAppSelector((state) => state.timeRange.instanceId);
    React.useEffect(() => {
        if (!instanceId) return;
        const refresh = () => {
            if (document.visibilityState !== "hidden")
                dispatch(refreshTimeRange());
        };
        refresh();
        const timer = window.setInterval(refresh, 30_000);
        window.addEventListener("focus", refresh);
        document.addEventListener("visibilitychange", refresh);
        return () => {
            window.clearInterval(timer);
            window.removeEventListener("focus", refresh);
            document.removeEventListener("visibilitychange", refresh);
        };
    }, [dispatch, instanceId]);
    return null;
}

export default function TimeRangeSelector() {
    const dispatch = useAppDispatch();
    const { ready, preference, draft, error } = useAppSelector(
        (state) => state.timeRange,
    );
    const label =
        preference.mode === "relative"
            ? `Last ${relativePresets.find(({ duration }) => duration === preference.duration)!.label}`
            : `${preference.from || "Any time"} to ${preference.until || "Any time"}`;
    return (
        <details className="time-range-selector">
            <summary className="btn btn-default btn-sm">
                Time range: {label}
            </summary>
            <div className="popover">
                <div className="popover-content">
                    <fieldset disabled={!ready}>
                        <label>
                            Range mode
                            <select
                                className="input"
                                value={draft.mode}
                                onChange={(event) =>
                                    dispatch(
                                        updateDraft({
                                            mode: event.target
                                                .value as TimeRangeMode,
                                        }),
                                    )
                                }
                            >
                                <option value="relative">Relative</option>
                                <option value="explicit">Explicit dates</option>
                            </select>
                        </label>
                        {draft.mode === "relative" ? (
                            <label>
                                Relative period
                                <select
                                    className="input"
                                    value={draft.duration}
                                    onChange={(event) =>
                                        dispatch(
                                            updateDraft({
                                                duration: Number(
                                                    event.target.value,
                                                ),
                                            }),
                                        )
                                    }
                                >
                                    {relativePresets.map(
                                        ({ duration, label }) => (
                                            <option
                                                key={duration}
                                                value={duration}
                                            >
                                                {label}
                                            </option>
                                        ),
                                    )}
                                </select>
                            </label>
                        ) : (
                            <>
                                <p>
                                    Local time: YYYY-MM-DD HH:mm:ss. Bounds are
                                    optional.
                                </p>
                                <label>
                                    From
                                    <input
                                        className="input"
                                        type="text"
                                        placeholder="YYYY-MM-DD HH:mm:ss"
                                        value={draft.from}
                                        onChange={(event) =>
                                            dispatch(
                                                updateDraft({
                                                    from: event.target.value,
                                                }),
                                            )
                                        }
                                    />
                                </label>
                                <label>
                                    Until
                                    <input
                                        className="input"
                                        type="text"
                                        placeholder="YYYY-MM-DD HH:mm:ss"
                                        value={draft.until}
                                        onChange={(event) =>
                                            dispatch(
                                                updateDraft({
                                                    until: event.target.value,
                                                }),
                                            )
                                        }
                                    />
                                </label>
                            </>
                        )}
                        {error && (
                            <p className="text-danger" role="alert">
                                {error}
                            </p>
                        )}
                        <button
                            className="btn btn-default btn-sm"
                            type="button"
                            onClick={() => dispatch(applyDraft())}
                        >
                            Apply
                        </button>
                    </fieldset>
                    {!ready && <p>Waiting for backend state...</p>}
                </div>
            </div>
        </details>
    );
}
