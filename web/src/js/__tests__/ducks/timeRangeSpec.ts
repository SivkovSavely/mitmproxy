import { configureStore } from "@reduxjs/toolkit";
import timeRange, {
    applyDraft,
    backendConnecting,
    defaultState,
    establishBackend,
    historyQuery,
    invalidateHistory,
    isWithinTimeRange,
    parseLocalDateTime,
    updateDraft,
} from "../../ducks/timeRange";
import { reducer, middlewares } from "../../ducks/store";

function createStore() {
    return configureStore({
        reducer: { ...reducer, timeRange },
        middleware: (getDefaultMiddleware) => getDefaultMiddleware(middlewares),
    });
}

beforeEach(() => localStorage.clear());

test("preferences are unusable until a real authenticated backend ID is established", () => {
    const store = createStore();
    expect(store.getState().timeRange.ready).toBe(false);
    expect(() => historyQuery(store.getState().timeRange)).toThrow();
    store.dispatch(establishBackend(""));
    expect(store.getState().timeRange.ready).toBe(false);
    expect(localStorage.length).toBe(0);
    store.dispatch(establishBackend("backend-a"));
    expect(
        Object.fromEntries(historyQuery(store.getState().timeRange)),
    ).toEqual({
        instance_id: "backend-a",
        mode: "relative",
        duration: "1800",
    });
});

test("the initial range is the last 30 minutes, including its exact cutoff", () => {
    const store = createStore();
    store.dispatch(establishBackend("backend-a"));
    const state = store.getState().timeRange;
    expect(state.preference).toEqual({
        mode: "relative",
        duration: 1800,
        from: "",
        until: "",
    });
    expect(state.draft.duration).toBe(1800);
    expect(isWithinTimeRange(1800, state, 3600)).toBe(true);
    expect(isWithinTimeRange(1799, state, 3600)).toBe(false);
});

test("navigation, disconnected state, reconnect and refresh retain a same-instance preference and draft", () => {
    const store = createStore();
    store.dispatch(establishBackend("backend-a"));
    store.dispatch(updateDraft({ duration: 300 }));
    expect(store.dispatch(applyDraft())).toBe(true);
    store.dispatch(updateDraft({ mode: "explicit", from: "not finished" }));
    const preference = store.getState().timeRange.preference;
    store.dispatch(backendConnecting());
    expect(store.getState().timeRange).toMatchObject({
        ready: false,
        instanceId: "backend-a",
        preference,
    });
    store.dispatch(establishBackend("backend-a"));
    expect(store.getState().timeRange.preference.duration).toBe(300);
    const refreshed = createStore();
    expect(refreshed.getState().timeRange.ready).toBe(false);
    refreshed.dispatch(establishBackend("backend-a"));
    expect(refreshed.getState().timeRange.preference.duration).toBe(300);
    expect(refreshed.getState().timeRange.draft).toMatchObject({
        mode: "explicit",
        from: "not finished",
    });
    refreshed.dispatch(establishBackend("backend-b"));
    expect(refreshed.getState().timeRange.preference).toEqual({
        mode: "relative",
        duration: 1800,
        from: "",
        until: "",
    });
    expect(refreshed.getState().timeRange.draft).toEqual({
        mode: "relative",
        duration: 1800,
        from: "",
        until: "",
    });
});

test.each([
    "2026-02-30 10:00:00",
    "2026-02-29 10:00:00",
    "2026-04-31 10:00:00",
    "2026-01-01 24:00:00",
    "2026-01-01 12:60:00",
    "2026-01-01 12:00:60",
    "2026-1-01 12:00:00",
    "2026-01-01T12:00:00",
    "2026-01-01 12:00",
    " ",
])("rejects invalid local date %s", (value) => {
    expect(parseLocalDateTime(value)).toBeNull();
});

test("explicit bounds are local browser time, inclusive, and serialize as Unix seconds", () => {
    const store = createStore();
    store.dispatch(establishBackend("backend-a"));
    store.dispatch(
        updateDraft({
            mode: "explicit",
            from: "2026-01-02 03:04:05",
            until: "2026-01-02 04:04:05",
        }),
    );
    expect(store.dispatch(applyDraft())).toBe(true);
    const start = new Date(2026, 0, 2, 3, 4, 5).getTime() / 1000;
    const end = new Date(2026, 0, 2, 4, 4, 5).getTime() / 1000;
    const state = store.getState().timeRange;
    expect(Object.fromEntries(historyQuery(state))).toEqual({
        instance_id: "backend-a",
        mode: "absolute",
        from: String(start),
        until: String(end),
    });
    expect(isWithinTimeRange(start, state)).toBe(true);
    expect(isWithinTimeRange(end, state)).toBe(true);
    expect(isWithinTimeRange(start - 1, state)).toBe(false);
    expect(isWithinTimeRange(end + 1, state)).toBe(false);
});

test("invalid or inverted drafts do not replace the applied preference", () => {
    const store = createStore();
    store.dispatch(establishBackend("backend-a"));
    const applied = store.getState().timeRange.preference;
    for (const [from, until] of [
        ["2026-02-30 10:00:00", ""],
        ["2026-01-01 10:00", ""],
        ["2026-01-02 10:00:00", "2026-01-01 10:00:00"],
    ]) {
        store.dispatch(updateDraft({ mode: "explicit", from, until }));
        expect(store.dispatch(applyDraft())).toBe(false);
        expect(store.getState().timeRange.preference).toEqual(applied);
        expect(store.getState().timeRange.error).toBeTruthy();
    }
});

test("explicit bounds may be omitted without inventing midnight or now", () => {
    const store = createStore();
    store.dispatch(establishBackend("backend-a"));
    store.dispatch(
        updateDraft({
            mode: "explicit",
            from: "",
            until: "2026-01-02 03:04:05",
        }),
    );
    expect(store.dispatch(applyDraft())).toBe(true);
    expect(historyQuery(store.getState().timeRange).has("from")).toBe(false);
    expect(isWithinTimeRange(0, store.getState().timeRange)).toBe(true);
});

test("relative ranges expire against now without reinterpreting Search expressions", () => {
    const state = {
        ...defaultState,
        ready: true,
        instanceId: "backend-a",
        preference: { ...defaultState.preference, duration: 300 },
    };
    expect(isWithinTimeRange(700, state, 1000)).toBe(true);
    expect(isWithinTimeRange(699.9, state, 1000)).toBe(false);
    expect(isWithinTimeRange(1000, state, 1000)).toBe(true);
    expect(isWithinTimeRange(1001, state, 1000)).toBe(false);
    expect(isWithinTimeRange(NaN, state, 1000)).toBe(false);
});

test("history invalidation has its own revision without altering applied preferences", () => {
    const state = timeRange(defaultState, invalidateHistory());
    expect(state.historyRevision).toBe(defaultState.historyRevision + 1);
    expect(state.revision).toBe(defaultState.revision);
    expect(state.preference).toEqual(defaultState.preference);
});

const dstTest =
    Intl.DateTimeFormat().resolvedOptions().timeZone === "America/New_York"
        ? test
        : test.skip;
dstTest("rejects a nonexistent daylight-saving local time", () => {
    expect(parseLocalDateTime("2026-03-08 02:30:00")).toBeNull();
});
