import { configureStore } from "@reduxjs/toolkit";
import fetchMock from "jest-fetch-mock";
import history, { clearSelection, selectSummary } from "../../ducks/history";
import flows, {
    FLOWS_RECEIVE,
    FLOWS_REMOVE,
    FLOWS_UPDATE,
    select,
} from "../../ducks/flows";
import { TFlow } from "./tutils";
import { summary } from "../historyFixtures";
import timeRange, {
    applyDraft,
    defaultState as defaultTimeRange,
    refreshTimeRange,
    updateDraft,
} from "../../ducks/timeRange";
import ui from "../../ducks/ui";
import { setFilter } from "../../ducks/ui/filter";

function makeStore() {
    return configureStore({
        reducer: {
            history,
            flows,
            timeRange,
            ui,
            backendState: () => ({ sqlite_history: true }),
        },
        preloadedState: {
            timeRange: {
                ...defaultTimeRange,
                ready: true,
                instanceId: "instance-a",
                preference: {
                    ...defaultTimeRange.preference,
                    mode: "explicit" as const,
                },
            },
        },
        middleware: (defaults) => defaults({ serializableCheck: false }),
    });
}

describe("history selection", () => {
    beforeEach(() => {
        fetchMock.enableMocks();
        fetchMock.resetMocks();
    });
    afterEach(() => jest.restoreAllMocks());

    it("selects a version-2 historical summary without requesting any full flow", async () => {
        const store = makeStore();
        const previous = TFlow();
        store.dispatch(FLOWS_RECEIVE([previous]));
        store.dispatch(select([previous]));
        await store.dispatch(selectSummary(summary(1)) as any);
        expect(store.getState().history.selected?.payload_version).toBe(2);
        expect(store.getState().flows.selected).toEqual([]);
        expect(store.getState().flows.list).toEqual([]);
        expect(fetch).not.toHaveBeenCalled();
        store.dispatch(clearSelection());
        expect(store.getState().history.selected).toBeNull();
    });

    it("announces live selection before fetching and prefers a newer websocket update", async () => {
        const store = makeStore();
        const row = summary(2, true);
        let finish!: (value: Response) => void;
        jest.mocked(fetch).mockImplementation(
            () =>
                new Promise((resolve) => {
                    expect(store.getState().history.selected?.id).toBe(row.id);
                    expect(store.getState().flows.list).toHaveLength(0);
                    finish = resolve;
                }),
        );
        const pending = store.dispatch(selectSummary(row) as any);
        const fresh = { ...TFlow(), id: row.id, comment: "fresh websocket" };
        store.dispatch(FLOWS_UPDATE({ flow: fresh, matching_filters: {} }));
        finish({
            ok: true,
            json: async () => ({ ...fresh, comment: "old HTTP" }),
        } as Response);
        await pending;
        expect(fetch).toHaveBeenCalledWith(
            `./flows/${row.id}`,
            expect.objectContaining({ signal: expect.any(AbortSignal) }),
        );
        expect(store.getState().flows.list).toHaveLength(1);
        expect(store.getState().flows.selected[0].comment).toBe(
            "fresh websocket",
        );
    });

    it("aborts a superseded live fetch and rejects its result even if abort is ignored", async () => {
        const store = makeStore();
        let finish!: (value: Response) => void;
        jest.mocked(fetch).mockImplementation(
            () =>
                new Promise((resolve) => {
                    finish = resolve;
                }),
        );
        const pending = store.dispatch(selectSummary(summary(3, true)) as any);
        const signal = jest.mocked(fetch).mock.calls[0][1]?.signal;
        await store.dispatch(selectSummary(summary(4)) as any);
        expect(signal?.aborted).toBe(true);
        finish({
            ok: true,
            json: async () => ({ ...TFlow(), id: "history-3" }),
        } as Response);
        await pending;
        expect(store.getState().history.selected?.id).toBe("history-4");
        expect(store.getState().flows.list).toHaveLength(0);
    });

    it("does not resurrect a live selection removed while HTTP was in flight", async () => {
        const store = makeStore();
        let finish!: (value: Response) => void;
        jest.mocked(fetch).mockImplementation(
            () =>
                new Promise((resolve) => {
                    finish = resolve;
                }),
        );
        const pending = store.dispatch(selectSummary(summary(5, true)) as any);
        store.dispatch(FLOWS_REMOVE("history-5"));
        finish({
            ok: true,
            json: async () => ({ ...TFlow(), id: "history-5" }),
        } as Response);
        await pending;
        expect(store.getState().history.selected).toBeNull();
        expect(store.getState().flows.selected).toHaveLength(0);
        expect(store.getState().flows.list).toHaveLength(0);
    });

    it("drops a raced full flow when live GET reports the row is no longer live", async () => {
        const store = makeStore();
        const row = summary(6, true);
        let finish!: (value: Response) => void;
        jest.mocked(fetch).mockImplementation(
            () =>
                new Promise((resolve) => {
                    finish = resolve;
                }),
        );
        const pending = store.dispatch(selectSummary(row) as any);
        store.dispatch(
            FLOWS_UPDATE({
                flow: { ...TFlow(), id: row.id },
                matching_filters: {},
            }),
        );
        finish({ ok: false, status: 404 } as Response);
        await pending;
        expect(store.getState().history.selected).toEqual({
            ...row,
            live: false,
        });
        expect(store.getState().flows.list).toHaveLength(0);
        expect(store.getState().flows.selected).toHaveLength(0);
    });

    it.each(["periodic refresh", "reapplying unchanged preferences"])(
        "finishes an eligible live GET after %s",
        async (change) => {
            const store = makeStore();
            store.dispatch(
                updateDraft({ mode: "relative", duration: 300 }) as any,
            );
            store.dispatch(applyDraft() as any);
            const row = {
                ...summary(7, true),
                timestamp_created: Date.now() / 1000 - 10,
            };
            const flow = {
                ...TFlow(),
                id: row.id,
                timestamp_created: row.timestamp_created,
            };
            let finish!: (value: Response) => void;
            jest.mocked(fetch).mockImplementation(
                () =>
                    new Promise((resolve) => {
                        finish = resolve;
                    }),
            );
            const pending = store.dispatch(selectSummary(row) as any);
            if (change === "periodic refresh")
                store.dispatch(refreshTimeRange() as any);
            else store.dispatch(applyDraft() as any);
            finish({ ok: true, json: async () => flow } as Response);
            await pending;
            expect(store.getState().flows.selected).toEqual([flow]);
            expect(store.getState().history.selected?.id).toBe(row.id);
        },
    );

    it.each([2000, 3000])(
        "accepts a server-relative live row when the browser clock is %s",
        async (browserNow) => {
            jest.spyOn(Date, "now").mockReturnValue(browserNow * 1000);
            const store = makeStore();
            store.dispatch(
                updateDraft({ mode: "relative", duration: 300 }) as any,
            );
            store.dispatch(applyDraft() as any);
            const row = { ...summary(7, true), timestamp_created: 2010 };
            const flow = {
                ...TFlow(),
                id: row.id,
                timestamp_created: row.timestamp_created,
            };
            fetchMock.mockResponseOnce(JSON.stringify(flow));

            await store.dispatch(selectSummary(row) as any);

            expect(store.getState().flows.selected).toEqual([flow]);
            expect(store.getState().history.selected).toEqual(row);
            expect(fetch).toHaveBeenCalledTimes(1);
            expect(fetch).toHaveBeenCalledWith(
                `./flows/${row.id}`,
                expect.objectContaining({ signal: expect.any(AbortSignal) }),
            );
        },
    );

    it.each([true, false])(
        "rejects a pending live GET after explicit bounds change (previous row still eligible=%s)",
        async (eligible) => {
            const store = makeStore();
            const row = {
                ...summary(8, true),
                timestamp_created:
                    new Date(2026, 0, 2, 3, 30, 0).getTime() / 1000,
            };
            const flow = {
                ...TFlow(),
                id: row.id,
                timestamp_created: row.timestamp_created,
            };
            let finish!: (value: Response) => void;
            jest.mocked(fetch).mockImplementation(
                () =>
                    new Promise((resolve) => {
                        finish = resolve;
                    }),
            );
            const pending = store.dispatch(selectSummary(row) as any);
            store.dispatch(
                updateDraft({
                    mode: "explicit",
                    from: eligible
                        ? "2026-01-02 03:00:00"
                        : "2026-01-02 04:00:00",
                    until: "2026-01-02 05:00:00",
                }) as any,
            );
            store.dispatch(applyDraft() as any);
            finish({ ok: true, json: async () => flow } as Response);
            await pending;
            expect(store.getState().flows.selected).toEqual([]);
            expect(store.getState().flows.list).toEqual([]);
        },
    );

    it.each(["Search", "relative range"])(
        "rejects a pending live GET after the applied %s changes",
        async (change) => {
            const store = makeStore();
            store.dispatch(
                updateDraft({ mode: "relative", duration: 1800 }) as any,
            );
            store.dispatch(applyDraft() as any);
            const row = {
                ...summary(8, true),
                timestamp_created: Date.now() / 1000 - 600,
            };
            let finish!: (value: Response) => void;
            jest.mocked(fetch).mockImplementation(
                () =>
                    new Promise((resolve) => {
                        finish = resolve;
                    }),
            );
            const pending = store.dispatch(selectSummary(row) as any);
            if (change === "Search")
                store.dispatch(setFilter("~u /not-this-flow"));
            else {
                store.dispatch(updateDraft({ duration: 300 }) as any);
                store.dispatch(applyDraft() as any);
            }
            finish({
                ok: true,
                json: async () => ({ ...TFlow(), id: row.id }),
            } as Response);
            await pending;

            expect(store.getState().flows.selected).toHaveLength(0);
            expect(store.getState().flows.list).toHaveLength(0);
            expect(fetch).toHaveBeenCalledTimes(1);
        },
    );
});
