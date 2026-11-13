import { createSelector, createSlice } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";
import type { AppThunk, RootState } from "./store";
import { select } from "./flows";

export type TimeRangeMode = "relative" | "explicit";

export interface TimeRangePreference {
    mode: TimeRangeMode;
    duration: number;
    from: string;
    until: string;
}

export interface TimeRangeState {
    ready: boolean;
    instanceId: string | null;
    revision: number;
    historyRevision: number;
    preference: TimeRangePreference;
    draft: TimeRangePreference;
    error: string | null;
}

export const relativePresets = [
    { duration: 300, label: "5 minutes" },
    { duration: 900, label: "15 minutes" },
    { duration: 1800, label: "30 minutes" },
    { duration: 3600, label: "1 hour" },
    { duration: 21600, label: "6 hours" },
    { duration: 86400, label: "24 hours" },
    { duration: 604800, label: "7 days" },
];

const preference: TimeRangePreference = {
    mode: "relative",
    duration: 1800,
    from: "",
    until: "",
};

export const defaultState: TimeRangeState = {
    ready: false,
    instanceId: null,
    revision: 0,
    historyRevision: 0,
    preference,
    draft: preference,
    error: null,
};

const storageKey = "mitmweb.timeRange.v1";

export function parseLocalDateTime(value: string): number | null {
    const match = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/.exec(
        value,
    );
    if (!match) return null;
    const [year, month, day, hour, minute, second] = match.slice(1).map(Number);
    const date = new Date(0);
    date.setFullYear(year, month - 1, day);
    date.setHours(hour, minute, second, 0);
    // The round trip also rejects local times skipped by daylight-saving changes.
    if (
        date.getFullYear() !== year ||
        date.getMonth() !== month - 1 ||
        date.getDate() !== day ||
        date.getHours() !== hour ||
        date.getMinutes() !== minute ||
        date.getSeconds() !== second
    )
        return null;
    return date.getTime() / 1000;
}

function validationError(value: TimeRangePreference): string | null {
    if (value.mode === "relative") {
        return relativePresets.some(
            ({ duration }) => duration === value.duration,
        )
            ? null
            : "Choose a relative period.";
    }
    const from = value.from ? parseLocalDateTime(value.from) : null;
    const until = value.until ? parseLocalDateTime(value.until) : null;
    if ((value.from && from === null) || (value.until && until === null)) {
        return "Enter valid local dates as YYYY-MM-DD HH:mm:ss, or leave a bound empty.";
    }
    if (from !== null && until !== null && from > until) {
        return "From must not be later than Until.";
    }
    return null;
}

function isPreference(value: unknown): value is TimeRangePreference {
    if (!value || typeof value !== "object") return false;
    const fields = value as TimeRangePreference;
    return (
        (fields.mode === "relative" || fields.mode === "explicit") &&
        typeof fields.duration === "number" &&
        relativePresets.some(({ duration }) => duration === fields.duration) &&
        typeof fields.from === "string" &&
        typeof fields.until === "string"
    );
}

const slice = createSlice({
    name: "timeRange",
    initialState: defaultState,
    reducers: {
        backendConnecting(state) {
            state.ready = false;
        },
        established(
            state,
            action: PayloadAction<{
                instanceId: string;
                preference: TimeRangePreference;
                draft: TimeRangePreference;
            }>,
        ) {
            Object.assign(state, action.payload, { ready: true, error: null });
            state.revision++;
        },
        draftUpdated(
            state,
            action: PayloadAction<Partial<TimeRangePreference>>,
        ) {
            Object.assign(state.draft, action.payload);
            state.error = null;
        },
        applied(state) {
            state.preference = { ...state.draft };
            state.error = null;
            state.revision++;
            state.historyRevision++;
        },
        invalidDraft(state, action: PayloadAction<string>) {
            state.error = action.payload;
        },
        invalidateHistory(state) {
            state.historyRevision++;
        },
        refreshed(state) {
            state.revision++;
            state.historyRevision++;
        },
    },
});

export const { backendConnecting, invalidateHistory } = slice.actions;
export default slice.reducer;

function persist(state: TimeRangeState) {
    if (!state.ready || !state.instanceId) return;
    try {
        localStorage.setItem(
            storageKey,
            JSON.stringify({
                instanceId: state.instanceId,
                preference: state.preference,
                draft: state.draft,
            }),
        );
    } catch {
        // Restricted browser storage must not prevent in-memory preferences.
    }
}

export const establishBackend =
    (instanceId: string): AppThunk =>
    (dispatch, getState) => {
        if (typeof instanceId !== "string" || !instanceId.trim()) return;
        const current = getState().timeRange;
        let preference = defaultState.preference,
            draft = defaultState.draft;
        if (current.instanceId === instanceId) {
            preference = current.preference;
            draft = current.draft;
        } else {
            try {
                const stored = JSON.parse(
                    localStorage.getItem(storageKey) || "null",
                );
                if (
                    stored?.instanceId === instanceId &&
                    isPreference(stored.preference) &&
                    !validationError(stored.preference)
                ) {
                    preference = stored.preference;
                    draft = isPreference(stored.draft)
                        ? stored.draft
                        : preference;
                }
            } catch {
                // Malformed or unavailable storage falls back only after a real ID.
            }
        }
        dispatch(slice.actions.established({ instanceId, preference, draft }));
        persist(getState().timeRange);
    };

export const updateDraft =
    (value: Partial<TimeRangePreference>): AppThunk =>
    (dispatch, getState) => {
        if (!getState().timeRange.ready) return;
        dispatch(slice.actions.draftUpdated(value));
        persist(getState().timeRange);
    };

export const applyDraft = (): AppThunk<boolean> => (dispatch, getState) => {
    const state = getState().timeRange;
    if (!state.ready) return false;
    const error = validationError(state.draft);
    if (error) {
        dispatch(slice.actions.invalidDraft(error));
        return false;
    }
    dispatch(slice.actions.applied());
    persist(getState().timeRange);
    dispatch(pruneLegacySelection());
    return true;
};

export function historyQuery(state: TimeRangeState): URLSearchParams {
    if (!state.ready || !state.instanceId) {
        throw new Error("Backend instance is not established.");
    }
    const { mode, duration, from, until } = state.preference;
    const query = new URLSearchParams({
        instance_id: state.instanceId,
        mode: mode === "explicit" ? "absolute" : mode,
    });
    if (mode === "relative") {
        query.set("duration", String(duration));
    } else {
        if (from) query.set("from", String(parseLocalDateTime(from)));
        if (until) query.set("until", String(parseLocalDateTime(until)));
    }
    return query;
}

export function isWithinTimeRange(
    timestamp: number,
    state: TimeRangeState,
    now = Date.now() / 1000,
): boolean {
    if (!Number.isFinite(timestamp)) return false;
    const { mode, duration, from, until } = state.preference;
    if (mode === "relative")
        return timestamp >= now - duration && timestamp <= now;
    return (
        (!from || timestamp >= parseLocalDateTime(from)!) &&
        (!until || timestamp <= parseLocalDateTime(until)!)
    );
}

export const selectTimeRangeFlows = createSelector(
    [
        (state: RootState) => state.flows.view,
        (state: RootState) => state.timeRange.preference,
        (state: RootState) => state.timeRange.instanceId,
        (state: RootState) => state.timeRange.revision,
    ],
    (view, preference, instanceId, _revision) => {
        if (!instanceId) return view;
        const now = Date.now() / 1000;
        const range = { ...defaultState, preference };
        return view.filter((flow) =>
            isWithinTimeRange(flow.timestamp_created, range, now),
        );
    },
);

export const pruneLegacySelection = (): AppThunk => (dispatch, getState) => {
    const state = getState();
    if (
        state.backendState.sqlite_history ||
        !state.timeRange.instanceId ||
        !state.flows.selected.length
    )
        return;
    const now = Date.now() / 1000;
    const selected = state.flows.selected.filter((flow) =>
        isWithinTimeRange(flow.timestamp_created, state.timeRange, now),
    );
    if (selected.length !== state.flows.selected.length)
        dispatch(select(selected));
};

export const refreshTimeRange = (): AppThunk => (dispatch, getState) => {
    if (!getState().timeRange.instanceId) return;
    dispatch(slice.actions.refreshed());
    dispatch(pruneLegacySelection());
};
