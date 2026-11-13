import { createAction } from "@reduxjs/toolkit";
import type { UnknownAction } from "@reduxjs/toolkit";
import type { HistoricalSummary } from "../history";
import type { Flow } from "../flow";
import type { AppThunk, RootState } from "./store";
import { FLOWS_RECEIVE, FLOWS_REMOVE, select } from "./flows";
import { fetchApi } from "../utils";
import { isWithinTimeRange } from "./timeRange";

export interface HistoryState {
    selected: HistoricalSummary | null;
    generation: number;
}

export const clearSelection = createAction("history/clearSelection");
const setSelection = createAction<HistoricalSummary>("history/setSelection");
const requests = new WeakMap<() => RootState, AbortController>();

export default function historyReducer(
    state: HistoryState = { selected: null, generation: 0 },
    action: UnknownAction,
): HistoryState {
    if (setSelection.match(action)) {
        return { selected: action.payload, generation: state.generation + 1 };
    }
    if (
        clearSelection.match(action) ||
        (FLOWS_REMOVE.match(action) && state.selected?.id === action.payload)
    ) {
        return { selected: null, generation: state.generation + 1 };
    }
    return state;
}

export function clearSelectedSummary(): AppThunk {
    return (dispatch, getState) => {
        requests.get(getState)?.abort();
        requests.delete(getState);
        dispatch(clearSelection());
        dispatch(FLOWS_RECEIVE([]));
    };
}

export function selectSummary(row: HistoricalSummary): AppThunk<Promise<void>> {
    return async (dispatch, getState) => {
        requests.get(getState)?.abort();
        requests.delete(getState);
        if (!getState().timeRange.ready) return;
        // WebSocket updates must know the requested ID before the live GET begins.
        dispatch(setSelection(row));
        dispatch(FLOWS_RECEIVE([]));
        if (!row.live) return;

        const controller = new AbortController();
        requests.set(getState, controller);
        const { generation } = getState().history;
        const { timeRange, ui } = getState();
        const { instanceId } = timeRange;
        const selectionQuery = JSON.stringify([
            timeRange.preference,
            ui.filter.search,
        ]);
        // Relative eligibility uses the server clock and is invalidated by history pages.
        const current = () => {
            const state = getState();
            return (
                !controller.signal.aborted &&
                state.history.generation === generation &&
                state.history.selected?.id === row.id &&
                state.history.selected.live &&
                state.timeRange.ready &&
                state.timeRange.instanceId === instanceId &&
                JSON.stringify([
                    state.timeRange.preference,
                    state.ui.filter.search,
                ]) === selectionQuery &&
                (state.timeRange.preference.mode === "relative" ||
                    isWithinTimeRange(row.timestamp_created, state.timeRange))
            );
        };
        try {
            const response = await fetchApi(
                `/flows/${encodeURIComponent(row.id)}`,
                { signal: controller.signal },
            );
            if (!current()) return;
            if (!response.ok) {
                if (response.status === 404 || response.status === 410) {
                    dispatch(setSelection({ ...row, live: false }));
                    dispatch(FLOWS_RECEIVE([]));
                }
                return;
            }
            const fetched: Flow = await response.json();
            if (!current() || fetched.id !== row.id) return;
            const flow = getState().flows.byId.get(row.id) ?? fetched;
            dispatch(FLOWS_RECEIVE([flow]));
            dispatch(select([flow]));
        } catch (error) {
            if (
                current() &&
                !(error instanceof DOMException && error.name === "AbortError")
            ) {
                // Keep the summary selected; the read-only pane offers retry.
                dispatch(select([]));
            }
        } finally {
            if (requests.get(getState) === controller)
                requests.delete(getState);
        }
    };
}
