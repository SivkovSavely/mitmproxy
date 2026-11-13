import * as React from "react";
import { configureStore } from "@reduxjs/toolkit";
import TimeRangeSelector, {
    TimeRangeRefresh,
} from "../../../components/Header/TimeRangeSelector";
import timeRange, {
    applyDraft,
    backendConnecting,
    establishBackend,
    updateDraft,
} from "../../../ducks/timeRange";
import { reducer, middlewares } from "../../../ducks/store";
import { FLOWS_RECEIVE, select } from "../../../ducks/flows";
import { TFlow } from "../../ducks/tutils";
import { act, fireEvent, render, screen } from "../../test-utils";

function createStore() {
    return configureStore({
        reducer: { ...reducer, timeRange },
        middleware: (getDefaultMiddleware) => getDefaultMiddleware(middlewares),
    });
}

beforeEach(() => localStorage.clear());
afterEach(() => jest.useRealTimers());

test("untrusted state disables controls and cannot persist a preference", () => {
    const store = createStore();
    render(<TimeRangeSelector />, { store });
    expect(screen.getByText("Time range: Last 30 minutes")).toBeVisible();
    fireEvent.click(screen.getByText(/Time range:/));
    expect(screen.getByLabelText("Range mode")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Apply" })).toBeDisabled();
    expect(localStorage.length).toBe(0);
});

test("a draft survives navigation without applying invalid, partial or inverted dates", () => {
    const store = createStore();
    store.dispatch(establishBackend("backend-a"));
    const first = render(<TimeRangeSelector />, { store });
    fireEvent.click(screen.getByText(/Time range:/));
    fireEvent.change(screen.getByLabelText("Range mode"), {
        target: { value: "explicit" },
    });
    fireEvent.change(screen.getByLabelText("From"), {
        target: { value: "2026-02-30 10:00:00" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    expect(screen.getByRole("alert")).toBeVisible();
    expect(store.getState().timeRange.preference.mode).toBe("relative");
    first.unmount();
    render(<TimeRangeSelector />, { store });
    fireEvent.click(screen.getByText(/Time range:/));
    expect(screen.getByLabelText("Range mode")).toHaveValue("explicit");
    expect(screen.getByLabelText("From")).toHaveValue("2026-02-30 10:00:00");
    fireEvent.change(screen.getByLabelText("From"), {
        target: { value: "2026-01-02 03:04:05" },
    });
    fireEvent.change(screen.getByLabelText("Until"), {
        target: { value: "2026-01-01 03:04:05" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    expect(screen.getByRole("alert")).toBeVisible();
    expect(store.getState().timeRange.preference.mode).toBe("relative");
    fireEvent.change(screen.getByLabelText("Until"), {
        target: { value: "2026-01-03 03:04:05" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    expect(screen.queryByRole("alert")).toBeNull();
    expect(store.getState().timeRange.preference).toMatchObject({
        mode: "explicit",
        from: "2026-01-02 03:04:05",
        until: "2026-01-03 03:04:05",
    });
});

test("switching mode retains the entered values for each mode", () => {
    const store = createStore();
    store.dispatch(establishBackend("backend-a"));
    render(<TimeRangeSelector />, { store });
    fireEvent.change(screen.getByLabelText("Relative period"), {
        target: { value: "300" },
    });
    fireEvent.change(screen.getByLabelText("Range mode"), {
        target: { value: "explicit" },
    });
    fireEvent.change(screen.getByLabelText("From"), {
        target: { value: "2026-01-02 03:04:05" },
    });
    fireEvent.change(screen.getByLabelText("Range mode"), {
        target: { value: "relative" },
    });
    expect(screen.getByLabelText("Relative period")).toHaveValue("300");
    fireEvent.change(screen.getByLabelText("Range mode"), {
        target: { value: "explicit" },
    });
    expect(screen.getByLabelText("From")).toHaveValue("2026-01-02 03:04:05");
});

test("expiry, focus and visible wakeups invalidate history and clear expired legacy selection, with cleanup", () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-01-02T12:00:00Z"));
    const store = createStore();
    store.dispatch(establishBackend("backend-a"));
    store.dispatch(updateDraft({ duration: 300 }));
    store.dispatch(applyDraft());
    const flow = { ...TFlow(), timestamp_created: Date.now() / 1000 - 299 };
    store.dispatch(FLOWS_RECEIVE([flow]));
    store.dispatch(select([flow]));
    const { unmount } = render(<TimeRangeRefresh />, { store });
    const before = store.getState().timeRange.historyRevision;
    act(() => jest.advanceTimersByTime(30_000));
    expect(store.getState().timeRange.historyRevision).toBeGreaterThan(before);
    expect(store.getState().flows.selected).toEqual([]);
    const afterExpiry = store.getState().timeRange.historyRevision;
    act(() => window.dispatchEvent(new Event("focus")));
    expect(store.getState().timeRange.historyRevision).toBeGreaterThan(
        afterExpiry,
    );
    const afterFocus = store.getState().timeRange.historyRevision;
    act(() => document.dispatchEvent(new Event("visibilitychange")));
    expect(store.getState().timeRange.historyRevision).toBeGreaterThan(
        afterFocus,
    );
    unmount();
    const afterUnmount = store.getState().timeRange.historyRevision;
    act(() => {
        jest.advanceTimersByTime(60_000);
        window.dispatchEvent(new Event("focus"));
        document.dispatchEvent(new Event("visibilitychange"));
    });
    expect(store.getState().timeRange.historyRevision).toBe(afterUnmount);
});

test("legacy ranges continue expiring during a network disconnect without resetting preference", () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-01-02T12:00:00Z"));
    const store = createStore();
    store.dispatch(establishBackend("backend-a"));
    store.dispatch(updateDraft({ duration: 300 }));
    store.dispatch(applyDraft());
    const flow = { ...TFlow(), timestamp_created: Date.now() / 1000 - 299 };
    store.dispatch(FLOWS_RECEIVE([flow]));
    store.dispatch(select([flow]));
    render(<TimeRangeRefresh />, { store });
    act(() => store.dispatch(backendConnecting()));
    act(() => jest.advanceTimersByTime(30_000));
    expect(store.getState().timeRange.preference.duration).toBe(300);
    expect(store.getState().flows.selected).toEqual([]);
});
