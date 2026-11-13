import { enableFetchMocks } from "jest-fetch-mock";
import { TStore } from "../ducks/tutils";
import StaticBackend from "../../backends/static";
import { waitFor } from "../test-utils";
import { configureStore } from "@reduxjs/toolkit";
import { middlewares, reducer } from "../../ducks/store";
import timeRange, {
    applyDraft,
    establishBackend,
    selectTimeRangeFlows,
    updateDraft,
} from "../../ducks/timeRange";
import { select } from "../../ducks/flows";
import { TFlow } from "../ducks/tutils";

enableFetchMocks();

test("static backend", async () => {
    fetchMock.mockOnceIf("./flows", "[]");
    fetchMock.mockOnceIf("./options", "{}");
    const store = TStore();
    new StaticBackend(store);
    await waitFor(() => {
        expect(store.getState().flows.list).toEqual([]);
        expect(store.getState().options).toEqual({});
    });
});

test("static flow selection stays legacy even with saved history preferences and a SQLite option", async () => {
    const createStore = () =>
        configureStore({
            reducer: { ...reducer, timeRange },
            middleware: (getDefaultMiddleware) =>
                getDefaultMiddleware(middlewares),
        });
    localStorage.clear();
    const previous = createStore();
    previous.dispatch(establishBackend("previous-backend"));
    previous.dispatch(updateDraft({ duration: 300 }));
    previous.dispatch(applyDraft());
    fetchMock.resetMocks();
    const flow = { ...TFlow(), timestamp_created: 1 };
    fetchMock.mockOnceIf("./flows", JSON.stringify([flow]));
    fetchMock.mockOnceIf(
        "./options",
        JSON.stringify({
            save_sqlite_file: { value: "exported.sqlite" },
        }),
    );
    const store = createStore();
    new StaticBackend(store);
    await waitFor(() => expect(store.getState().flows.list).toEqual([flow]));
    store.dispatch(select(selectTimeRangeFlows(store.getState())));
    expect(store.getState().flows.selected).toEqual([flow]);
    expect(store.getState().timeRange.ready).toBe(false);
    expect(store.getState().timeRange.instanceId).toBeNull();
    expect(store.getState().backendState.sqlite_history).toBe(false);
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
        "./flows",
        "./options",
    ]);
    localStorage.clear();
});
