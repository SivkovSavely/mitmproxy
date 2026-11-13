import WebSocketBackend, { Resource } from "../../backends/websocket";
import type { MockResponseInit } from "jest-fetch-mock";
import fetchMock from "jest-fetch-mock";
import { waitFor } from "../test-utils";
import * as connectionActions from "../../ducks/connection";
import { configureStore } from "@reduxjs/toolkit";
import type { EventLogItem } from "../../ducks/eventLog";
import { LogLevel } from "../../ducks/eventLog";
import { ScriptStatusKind } from "../../ducks/scripts";
import { FLOWS_RECEIVE, FLOWS_UPDATE, select } from "../../ducks/flows";
import { TFlow } from "../ducks/tutils";
import { selectTab } from "../../ducks/ui/flow";
import { setCurrent, Tab } from "../../ducks/ui/tabs";
import { STATE_RECEIVE, STATE_UPDATE } from "../../ducks/backendState";
import { FilterName, setFilter, setHighlight } from "../../ducks/ui/filter";
import { ConnectionState } from "../../ducks/connection";
import { TBackendState } from "../ducks/_tbackendstate";
import { reducer, middlewares } from "../../ducks/store";
import timeRange, {
    applyDraft,
    establishBackend,
    historyQuery,
    updateDraft,
} from "../../ducks/timeRange";
import history, { selectSummary } from "../../ducks/history";

function TStore(_state: null) {
    return configureStore({
        reducer: { ...reducer, timeRange, history },
        middleware: (getDefaultMiddleware) => getDefaultMiddleware(middlewares),
    });
}

const serverState = (sqlite_history = false) => ({
    ...TBackendState(),
    instance_id: "backend-a",
    sqlite_history,
});

function preparedBackend(store: ReturnType<typeof TStore>) {
    const backend = new WebSocketBackend(store);
    store.dispatch(STATE_RECEIVE(serverState()));
    store.dispatch(establishBackend("backend-a"));
    store.dispatch(updateDraft({ mode: "explicit" }));
    store.dispatch(applyDraft());
    backend.previewDisconnected = false;
    return backend;
}

function summary(id: string, live = true) {
    return {
        id,
        live,
        sequence: "1",
        timestamp_created: 1000,
        type: "http",
        method: "GET",
        host: "example.test",
        path: "/",
        status_code: 200,
        request_size: 0,
        response_size: 0,
        payload_version: 1,
        highlighted: false,
        cursor: [1000, "1", id] as [number, string, string],
    };
}

beforeEach(() => {
    fetchMock.enableMocks();
    fetchMock.resetMocks();
    localStorage.clear();
    const WebSocketOrig = WebSocket;
    // @ts-expect-error jest mock stuff
    jest.spyOn(global, "WebSocket").mockImplementation(() => ({
        addEventListener: () => 0,
        send: () => 0,
        readyState: WebSocketOrig.CONNECTING,
    }));
    // @ts-expect-error jest mock stuff
    global.WebSocket.OPEN = WebSocketOrig.OPEN;
    // @ts-expect-error jest mock stuff
    global.WebSocket.CONNECTING = WebSocketOrig.CONNECTING;
});

afterEach(() => jest.restoreAllMocks());

describe("websocket backend", () => {
    test("waits for authenticated state before making a flow request", async () => {
        let resolveState!: (response: string) => void;
        fetchMock.mockResponse((request) => {
            if (request.url === "./state") {
                return new Promise((resolve) => {
                    resolveState = resolve;
                });
            }
            if (request.url === "./scripts")
                return Promise.resolve('{"scripts":[]}');
            return Promise.resolve("[]");
        });
        const backend = new WebSocketBackend(TStore(null));
        const opened = backend.onOpen();
        await waitFor(() => expect(resolveState).toBeDefined());
        expect(fetchMock.mock.calls.map(([url]) => url)).not.toContain(
            "./flows",
        );
        resolveState(JSON.stringify(serverState()));
        await opened;
        expect(fetchMock.mock.calls.map(([url]) => url)).toContain("./flows");
        expect(fetchMock.mock.calls[0][1]).toMatchObject({
            credentials: "same-origin",
        });
    });

    test("SQLite history never fetches the unbounded flow resource, including reset and reconnect", async () => {
        fetchMock.mockResponse((request) => {
            if (request.url === "./state")
                return Promise.resolve(JSON.stringify(serverState(true)));
            if (request.url === "./scripts")
                return Promise.resolve('{"scripts":[]}');
            return Promise.resolve("[]");
        });
        const store = TStore(null);
        store.dispatch(FLOWS_RECEIVE([TFlow()]));
        const backend = new WebSocketBackend(store);
        await backend.onOpen();
        expect(store.getState().flows.list).toEqual([]);
        await backend.fetchData(Resource.Flows);
        await backend.onMessage({ type: "flows/reset" });
        await backend.onOpen();
        expect(fetchMock.mock.calls.map(([url]) => url)).not.toContain(
            "./flows",
        );
    });

    test("enabling SQLite history aborts a legacy response body already being consumed", async () => {
        const store = TStore(null);
        const backend = new WebSocketBackend(store);
        let enabled = false,
            legacySignal: AbortSignal | null | undefined;
        const legacyResponse = new Response();
        const readLegacy = jest.spyOn(legacyResponse, "json");
        fetchMock.mockImplementation((url, options) => {
            if (url === "./state")
                return Promise.resolve(
                    new Response(JSON.stringify(serverState(enabled))),
                );
            if (url === "./flows") {
                legacySignal = options?.signal;
                readLegacy.mockImplementation(
                    () =>
                        new Promise((_resolve, reject) => {
                            legacySignal?.addEventListener("abort", () =>
                                reject(
                                    new DOMException("Cancelled", "AbortError"),
                                ),
                            );
                        }),
                );
                return Promise.resolve(legacyResponse);
            }
            return Promise.resolve(
                new Response(url === "./scripts" ? '{"scripts":[]}' : "[]"),
            );
        });
        const initial = backend.onOpen();
        await waitFor(() => expect(readLegacy).toHaveBeenCalledTimes(1));
        enabled = true;
        const changed = backend.onMessage({
            type: "options/update",
            payload: { save_sqlite_file: { value: "history.sqlite" } },
        });
        expect(legacySignal?.aborted).toBe(true);
        expect(store.getState().timeRange.ready).toBe(false);
        await Promise.all([initial, changed]);
        expect(store.getState().timeRange.ready).toBe(true);
        expect(store.getState().backendState.sqlite_history).toBe(true);
        expect(store.getState().connection.state).not.toBe(
            ConnectionState.ERROR,
        );
        expect(store.getState().flows.list).toEqual([]);
        expect(
            fetchMock.mock.calls.filter(([url]) => url === "./flows"),
        ).toHaveLength(1);
    });

    test.each(Object.values(Resource))(
        "superseding %s aborts its request without parsing the obsolete response body",
        async (resource) => {
            const store = TStore(null);
            const backend = preparedBackend(store);
            const data =
                resource === Resource.State
                    ? serverState()
                    : resource === Resource.Scripts
                      ? { scripts: [] }
                      : resource === Resource.Options
                        ? {}
                        : [];
            const response = new Response(JSON.stringify(data));
            const readObsolete = jest.spyOn(response, "json");
            let resolveObsolete!: (response: Response) => void;
            fetchMock.mockImplementationOnce(
                () =>
                    new Promise((resolve) => {
                        resolveObsolete = resolve;
                    }),
            );
            const obsolete = backend.fetchData(resource);
            const signal = fetchMock.mock.calls[0][1]?.signal;
            fetchMock.mockResponseOnce(JSON.stringify(data));
            const current =
                resource === Resource.Flows
                    ? backend.onMessage({ type: "flows/reset" })
                    : resource === Resource.Events
                      ? backend.onMessage({ type: "events/reset" })
                      : backend.fetchData(resource);
            expect(signal?.aborted).toBe(true);
            resolveObsolete(response);
            await Promise.all([obsolete, current]);
            expect(readObsolete).not.toHaveBeenCalled();
            expect(store.getState().timeRange.ready).toBe(true);
            expect(store.getState().connection.state).not.toBe(
                ConnectionState.ERROR,
            );
        },
    );

    test("superseding the initial state fetch stays gated without reporting a connection error", async () => {
        const store = TStore(null);
        const backend = new WebSocketBackend(store);
        fetchMock.mockImplementationOnce(
            (_url, options) =>
                new Promise((_resolve, reject) => {
                    options?.signal?.addEventListener("abort", () =>
                        reject(new DOMException("Cancelled", "AbortError")),
                    );
                }),
        );
        let resolveCurrent!: (response: Response) => void;
        fetchMock.mockImplementationOnce(
            () =>
                new Promise((resolve) => {
                    resolveCurrent = resolve;
                }),
        );
        const initial = backend.onOpen();
        const current = backend.fetchData(Resource.State);
        await initial;
        expect(store.getState().connection.state).toBe(
            ConnectionState.FETCHING,
        );
        expect(store.getState().timeRange.ready).toBe(false);
        expect(localStorage.length).toBe(0);
        expect(() => historyQuery(store.getState().timeRange)).toThrow();
        resolveCurrent(
            new Response(
                JSON.stringify({
                    ...serverState(true),
                    instance_id: "backend-b",
                }),
            ),
        );
        await current;
        expect(store.getState().timeRange).toMatchObject({
            ready: true,
            instanceId: "backend-b",
            preference: { duration: 1800 },
        });
        expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
            "./state",
            "./state",
        ]);
        expect(store.getState().connection.state).not.toBe(
            ConnectionState.ERROR,
        );
    });

    test.each(["headers", "body"])(
        "an obsolete %s completion cannot publish or discard the replacement flow buffer",
        async (stage) => {
            const store = TStore(null);
            const backend = preparedBackend(store);
            const obsoleteFlow = { ...TFlow(), id: "obsolete" };
            const baseline = { ...TFlow(), id: "baseline" };
            const latest = { ...TFlow(), id: "latest" };
            const obsoleteResponse = new Response(
                JSON.stringify([obsoleteFlow]),
            );
            const readObsolete = jest.spyOn(obsoleteResponse, "json");
            let resolveObsolete!: () => void;
            if (stage === "headers") {
                fetchMock.mockImplementationOnce(
                    () =>
                        new Promise((resolve) => {
                            resolveObsolete = () => resolve(obsoleteResponse);
                        }),
                );
            } else {
                readObsolete.mockImplementationOnce(
                    () =>
                        new Promise((resolve) => {
                            resolveObsolete = () => resolve([obsoleteFlow]);
                        }),
                );
                fetchMock.mockImplementationOnce(() =>
                    Promise.resolve(obsoleteResponse),
                );
            }
            const obsolete = backend.fetchData(Resource.Flows);
            await waitFor(() => expect(resolveObsolete).toBeDefined());
            const obsoleteSignal = fetchMock.mock.calls[0][1]?.signal;
            let resolveCurrent!: (response: Response) => void;
            fetchMock.mockImplementationOnce(
                () =>
                    new Promise((resolve) => {
                        resolveCurrent = resolve;
                    }),
            );
            const current = backend.onMessage({ type: "flows/reset" });
            const currentSignal = fetchMock.mock.calls[1][1]?.signal;
            expect(obsoleteSignal?.aborted).toBe(true);
            expect(currentSignal?.aborted).toBe(false);
            resolveObsolete();
            await obsolete;
            expect(store.getState().flows.list).toEqual([]);
            if (stage === "headers")
                expect(readObsolete).not.toHaveBeenCalled();
            backend.onMessage({
                type: "flows/add",
                payload: { flow: latest, matching_filters: {} },
            });
            expect(store.getState().flows.list).toEqual([]);
            resolveCurrent(new Response(JSON.stringify([baseline])));
            await current;
            expect(store.getState().flows.list).toEqual([baseline, latest]);
            expect(store.getState().connection.state).not.toBe(
                ConnectionState.ERROR,
            );
        },
    );

    test("reset cancellation while initially fetching does not become a connection error", async () => {
        const store = TStore(null);
        const backend = new WebSocketBackend(store);
        let snapshot = 0,
            signal: AbortSignal | null | undefined,
            reading = false;
        fetchMock.mockImplementation((url, options) => {
            if (url === "./state")
                return Promise.resolve(
                    new Response(JSON.stringify(serverState())),
                );
            if (url === "./flows" && snapshot++ === 0) {
                signal = options?.signal;
                const response = new Response();
                jest.spyOn(response, "json").mockImplementationOnce(
                    () =>
                        new Promise((_resolve, reject) => {
                            reading = true;
                            signal?.addEventListener("abort", () =>
                                reject(
                                    new DOMException("Cancelled", "AbortError"),
                                ),
                            );
                        }),
                );
                return Promise.resolve(response);
            }
            return Promise.resolve(
                new Response(url === "./scripts" ? '{"scripts":[]}' : "[]"),
            );
        });
        const initial = backend.onOpen();
        await waitFor(() => expect(reading).toBe(true));
        const reset = backend.onMessage({ type: "flows/reset" });
        expect(signal?.aborted).toBe(true);
        await Promise.all([initial, reset]);
        expect(store.getState().timeRange.ready).toBe(true);
        expect(store.getState().connection.state).toBe(
            ConnectionState.ESTABLISHED,
        );
        expect(store.getState().flows.list).toEqual([]);
    });

    test.each(["reconnect", "close", "error"])(
        "%s aborts every pending resource without changing the trusted preference",
        async (event) => {
            const store = TStore(null);
            const backend = preparedBackend(store);
            store.dispatch(updateDraft({ mode: "relative", duration: 300 }));
            store.dispatch(applyDraft());
            const stored = localStorage.getItem("mitmweb.timeRange.v1");
            const rejectRequests: Array<(reason: DOMException) => void> = [];
            fetchMock.mockImplementation(
                () =>
                    new Promise((_resolve, reject) => {
                        rejectRequests.push(reject);
                    }),
            );
            const pending = Object.values(Resource).map((resource) =>
                backend.fetchData(resource),
            );
            const signals = fetchMock.mock.calls.map(
                ([, options]) => options?.signal,
            );
            jest.spyOn(console, "error").mockImplementation(() => {});
            if (event === "reconnect") backend.connect();
            else if (event === "close")
                backend.onClose(new CloseEvent("close"));
            else backend.onError(new Event("error"));
            expect(signals).toHaveLength(Object.values(Resource).length);
            expect(signals.every((signal) => signal?.aborted)).toBe(true);
            const connection = store.getState().connection;
            rejectRequests.forEach((reject) =>
                reject(new DOMException("Cancelled", "AbortError")),
            );
            await expect(Promise.all(pending)).resolves.toEqual(
                pending.map(() => undefined),
            );
            expect(store.getState().connection).toEqual(connection);
            expect(store.getState().timeRange).toMatchObject({
                ready: false,
                instanceId: "backend-a",
                preference: { duration: 300 },
            });
            expect(localStorage.getItem("mitmweb.timeRange.v1")).toBe(stored);
            expect(() => historyQuery(store.getState().timeRange)).toThrow();
        },
    );

    test("an unauthorized state response cannot trigger flows or restore preferences", async () => {
        fetchMock.mockResponse("{}", { status: 403 });
        const backend = new WebSocketBackend(TStore(null));
        await backend.onOpen();
        expect(fetchMock.mock.calls.map(([url]) => url)).toEqual(["./state"]);
        expect(backend.store.getState().connection.state).toBe(
            ConnectionState.ERROR,
        );
    });

    test("malformed state cannot accidentally enable legacy unbounded fetching", async () => {
        fetchMock.mockResponse(JSON.stringify({ instance_id: "backend-a" }));
        const store = TStore(null);
        const backend = new WebSocketBackend(store);
        await backend.onOpen();
        expect(fetchMock.mock.calls.map(([url]) => url)).toEqual(["./state"]);
        expect(store.getState().timeRange.ready).toBe(false);
        expect(localStorage.length).toBe(0);
    });

    test("a new connection cannot be established by an older pending state response", async () => {
        let resolveState!: (value: string) => void;
        fetchMock.mockResponse((request) => {
            if (request.url === "./state")
                return new Promise((resolve) => {
                    resolveState = resolve;
                });
            if (request.url === "./scripts")
                return Promise.resolve('{"scripts":[]}');
            return Promise.resolve("[]");
        });
        const store = TStore(null);
        const backend = new WebSocketBackend(store);
        const pending = backend.onOpen();
        await waitFor(() => expect(resolveState).toBeDefined());
        backend.connect();
        resolveState(JSON.stringify(serverState()));
        await pending;
        expect(store.getState().timeRange.ready).toBe(false);
        expect(fetchMock.mock.calls.map(([url]) => url)).toEqual(["./state"]);
        expect(localStorage.length).toBe(0);
    });

    test("an obsolete state response cannot replace a newer authenticated backend or restore its old range", async () => {
        const resolveStates: Array<(value: string) => void> = [];
        fetchMock.mockResponse((request) => {
            if (request.url === "./state")
                return new Promise((resolve) => resolveStates.push(resolve));
            if (request.url === "./scripts")
                return Promise.resolve('{"scripts":[]}');
            return Promise.resolve("[]");
        });
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(updateDraft({ mode: "relative", duration: 300 }));
        store.dispatch(applyDraft());
        const obsolete = backend.onOpen();
        await waitFor(() => expect(resolveStates).toHaveLength(1));
        backend.connect();
        const current = backend.onOpen();
        await waitFor(() => expect(resolveStates).toHaveLength(2));
        expect(() => historyQuery(store.getState().timeRange)).toThrow();
        resolveStates[1](
            JSON.stringify({ ...serverState(true), instance_id: "backend-b" }),
        );
        await current;
        resolveStates[0](JSON.stringify(serverState(false)));
        await obsolete;
        expect(store.getState().backendState).toMatchObject({
            instance_id: "backend-b",
            sqlite_history: true,
        });
        expect(
            Object.fromEntries(historyQuery(store.getState().timeRange)),
        ).toEqual({
            instance_id: "backend-b",
            mode: "relative",
            duration: "1800",
        });
        expect(fetchMock.mock.calls.map(([url]) => url)).not.toContain(
            "./flows",
        );
        const refreshed = TStore(null);
        refreshed.dispatch(establishBackend("backend-b"));
        expect(refreshed.getState().timeRange.preference.duration).toBe(1800);
    });

    test("SQLite filter updates never cache unknown Search or Highlight IDs", () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(STATE_UPDATE({ sqlite_history: true }));
        const flow = { ...TFlow(), id: "chosen" };
        store.dispatch(FLOWS_RECEIVE([flow]));
        const before = store.getState().timeRange.historyRevision;
        const unknown = Array.from(
            { length: 200 },
            (_, index) => `other-${index}`,
        );
        backend.onMessage({
            type: "flows/filterUpdate",
            payload: {
                name: FilterName.Highlight,
                matching_flow_ids: [flow.id, ...unknown],
            },
        });
        expect([...store.getState().flows.highlightedIds]).toEqual([flow.id]);
        backend.onMessage({
            type: "flows/filterUpdate",
            payload: {
                name: FilterName.Search,
                matching_flow_ids: [flow.id, ...unknown],
            },
        });
        expect(store.getState().flows.view).toEqual([flow]);
        expect([...store.getState().flows.byId.keys()]).toEqual([flow.id]);
        backend.onMessage({ type: "flows/reset" });
        backend.onMessage({
            type: "flows/filterUpdate",
            payload: { name: FilterName.Highlight, matching_flow_ids: unknown },
        });
        expect(store.getState().flows.highlightedIds.size).toBe(0);
        expect(store.getState().flows.byId.size).toBe(0);
        expect(store.getState().timeRange.historyRevision).toBe(before + 4);
    });

    test("SQLite option changes await authoritative state before history or legacy flow requests", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(updateDraft({ mode: "relative", duration: 300 }));
        store.dispatch(applyDraft());
        store.dispatch(FLOWS_RECEIVE([TFlow()]));
        for (const [value, enabled] of [
            ["first.sqlite", true],
            ["second.sqlite", true],
            [undefined, false],
        ] as const) {
            fetchMock.resetMocks();
            let resolveState!: (value: string) => void;
            fetchMock.mockResponse((request) => {
                if (request.url === "./state")
                    return new Promise((resolve) => {
                        resolveState = resolve;
                    });
                if (request.url === "./flows")
                    return Promise.resolve(JSON.stringify([TFlow()]));
                if (request.url === "./scripts")
                    return Promise.resolve('{"scripts":[]}');
                return Promise.resolve("[]");
            });
            const changed = backend.onMessage({
                type: "options/update",
                payload: { save_sqlite_file: { value } },
            });
            expect(store.getState().timeRange.ready).toBe(false);
            expect(store.getState().timeRange.preference.duration).toBe(300);
            expect(() => historyQuery(store.getState().timeRange)).toThrow();
            await waitFor(() => expect(resolveState).toBeDefined());
            await backend.fetchData(Resource.Flows);
            await backend.onMessage({ type: "flows/reset" });
            expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
                "./state",
            ]);
            resolveState(JSON.stringify(serverState(enabled)));
            await changed;
            expect(store.getState().timeRange.ready).toBe(true);
            expect(store.getState().timeRange.preference.duration).toBe(300);
            expect(store.getState().backendState.sqlite_history).toBe(enabled);
            expect(store.getState().flows.list).toHaveLength(enabled ? 0 : 1);
            expect(
                fetchMock.mock.calls.map(([url]) => url).includes("./flows"),
            ).toBe(!enabled);
        }
    });

    test.each([true, false])(
        "history capability comes from state, not the save option value (%s)",
        async (enabled) => {
            fetchMock.mockResponse((request) => {
                if (request.url === "./state")
                    return Promise.resolve(
                        JSON.stringify(serverState(enabled)),
                    );
                if (request.url === "./scripts")
                    return Promise.resolve('{"scripts":[]}');
                return Promise.resolve("[]");
            });
            const store = TStore(null);
            const backend = preparedBackend(store);
            await backend.onMessage({
                type: "options/update",
                payload: {
                    save_sqlite_file: {
                        value: enabled ? undefined : "stale.sqlite",
                    },
                },
            });
            expect(store.getState().backendState.sqlite_history).toBe(enabled);
            expect(fetchMock.mock.calls.map(([url]) => url)).toContain(
                "./state",
            );
            expect(fetchMock.mock.calls[0][0]).toBe("./state");
            expect(
                fetchMock.mock.calls.map(([url]) => url).includes("./flows"),
            ).toBe(!enabled);
        },
    );

    test("disabling history clears its full live detail before the legacy snapshot arrives", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(STATE_UPDATE({ sqlite_history: true }));
        const chosen = { ...TFlow(), id: "chosen" };
        fetchMock.mockResponseOnce(JSON.stringify(chosen));
        await store.dispatch(selectSummary(summary("chosen")));
        expect(store.getState().flows.selected).toEqual([chosen]);
        fetchMock.resetMocks();
        let resolveFlows!: (value: string) => void;
        fetchMock.mockResponse((request) => {
            if (request.url === "./state")
                return Promise.resolve(JSON.stringify(serverState(false)));
            if (request.url === "./flows")
                return new Promise((resolve) => {
                    resolveFlows = resolve;
                });
            if (request.url === "./scripts")
                return Promise.resolve('{"scripts":[]}');
            return Promise.resolve("[]");
        });
        const pending = backend.onMessage({
            type: "options/update",
            payload: { save_sqlite_file: { value: undefined } },
        });
        await waitFor(() => expect(resolveFlows).toBeDefined());
        expect(store.getState().flows.list).toEqual([]);
        expect(store.getState().flows.selected).toEqual([]);
        expect(store.getState().history.selected).toBeNull();
        const legacy = { ...TFlow(), id: "legacy" };
        resolveFlows(JSON.stringify([legacy]));
        await pending;
        expect(store.getState().flows.list).toEqual([legacy]);
    });

    test("an option-state network failure gates requests without resetting the same backend preference", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(updateDraft({ mode: "relative", duration: 300 }));
        store.dispatch(applyDraft());
        const stored = localStorage.getItem("mitmweb.timeRange.v1");
        fetchMock.mockRejectOnce(new TypeError("Network unavailable"));
        await backend.onMessage({
            type: "options/update",
            payload: { save_sqlite_file: { value: "new.sqlite" } },
        });
        expect(store.getState().timeRange).toMatchObject({
            ready: false,
            instanceId: "backend-a",
            preference: { duration: 300 },
        });
        expect(store.getState().connection.state).toBe(ConnectionState.ERROR);
        expect(localStorage.getItem("mitmweb.timeRange.v1")).toBe(stored);
        expect(() => historyQuery(store.getState().timeRange)).toThrow();
        await backend.fetchData(Resource.Flows);
        expect(fetchMock.mock.calls.map(([url]) => url)).toEqual(["./state"]);
        fetchMock.mockResponse((request) => {
            if (request.url === "./state")
                return Promise.resolve(JSON.stringify(serverState(true)));
            if (request.url === "./scripts")
                return Promise.resolve('{"scripts":[]}');
            return Promise.resolve("[]");
        });
        await backend.onOpen();
        expect(store.getState().timeRange).toMatchObject({
            ready: true,
            instanceId: "backend-a",
            preference: { duration: 300 },
        });
        expect(fetchMock.mock.calls.map(([url]) => url)).not.toContain(
            "./flows",
        );
    });

    test("state revalidation immediately ends old response subscriptions until authentication completes", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        // @ts-expect-error writable mock
        backend.socket.readyState = WebSocket.OPEN;
        const send = jest.spyOn(backend.socket, "send");
        const flow = TFlow();
        Object.assign(flow.response, { stream_preview: true });
        store.dispatch(selectTab("response"));
        store.dispatch(select([flow]));
        expect(store.getState().responsePreview.flow_id).toBe(flow.id);
        let resolveState!: (value: string) => void;
        fetchMock.mockResponse((request) => {
            if (request.url === "./state")
                return new Promise((resolve) => {
                    resolveState = resolve;
                });
            if (request.url === "./scripts")
                return Promise.resolve('{"scripts":[]}');
            return Promise.resolve("[]");
        });
        const pending = backend.onMessage({
            type: "options/update",
            payload: { save_sqlite_file: { value: "new.sqlite" } },
        });
        expect(store.getState().timeRange.ready).toBe(false);
        expect(store.getState().responsePreview.flow_id).toBeNull();
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: null, subscription: 2 },
            }),
        );
        const sent = send.mock.calls.length;
        store.dispatch(FLOWS_UPDATE({ flow, matching_filters: {} }));
        expect(send).toHaveBeenCalledTimes(sent);
        await waitFor(() => expect(resolveState).toBeDefined());
        resolveState(JSON.stringify(serverState(true)));
        await pending;
    });

    test("a WebSocket network error marks the connection unavailable without resetting preference", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(updateDraft({ mode: "relative", duration: 300 }));
        store.dispatch(applyDraft());
        store.dispatch(connectionActions.startFetching());
        store.dispatch(connectionActions.finishFetching());
        expect(store.getState().connection.state).toBe(
            ConnectionState.ESTABLISHED,
        );
        const stored = localStorage.getItem("mitmweb.timeRange.v1");
        jest.spyOn(console, "error").mockImplementation(() => {});
        backend.onError(new Event("error"));
        expect(store.getState().connection.state).toBe(ConnectionState.ERROR);
        expect(store.getState().timeRange).toMatchObject({
            ready: false,
            instanceId: "backend-a",
            preference: { duration: 300 },
        });
        expect(localStorage.getItem("mitmweb.timeRange.v1")).toBe(stored);
        await backend.fetchData(Resource.Flows);
        expect(fetchMock).not.toHaveBeenCalled();
    });

    test("SQLite events only retain requested live detail and prefer its newest WebSocket metadata", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(STATE_UPDATE({ sqlite_history: true }));
        const before = store.getState().timeRange.historyRevision;
        for (let index = 0; index < 200; index++) {
            backend.onMessage({
                type: "flows/add",
                payload: {
                    flow: { ...TFlow(), id: `other-${index}` },
                    matching_filters: {},
                },
            });
        }
        expect(store.getState().flows.byId.size).toBe(0);
        expect(store.getState().timeRange.historyRevision).toBe(before + 200);
        let resolveDetail!: (value: string) => void;
        fetchMock.mockResponseOnce(
            () =>
                new Promise((resolve) => {
                    resolveDetail = resolve;
                }),
        );
        const pending = store.dispatch(selectSummary(summary("chosen")));
        const newest = {
            ...TFlow(),
            id: "chosen",
            comment: "newest websocket metadata",
        };
        backend.onMessage({
            type: "flows/update",
            payload: { flow: newest, matching_filters: {} },
        });
        backend.onMessage({
            type: "flows/update",
            payload: {
                flow: { ...newest, id: "unselected" },
                matching_filters: {},
            },
        });
        expect(store.getState().flows.list).toEqual([newest]);
        await waitFor(() => expect(resolveDetail).toBeDefined());
        resolveDetail(
            JSON.stringify({ ...newest, comment: "older REST metadata" }),
        );
        await pending;
        expect(store.getState().flows.selected).toEqual([newest]);
        expect(store.getState().flows.byId.size).toBe(1);
        backend.onMessage({ type: "flows/remove", payload: "chosen" });
        expect(store.getState().history.selected).toBeNull();
        expect(store.getState().flows.list).toEqual([]);
        expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
            "./flows/chosen",
        ]);
    });

    test("removing a requested live detail prevents its in-flight GET from reviving selection", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(STATE_UPDATE({ sqlite_history: true }));
        let resolveDetail!: (value: string) => void;
        fetchMock.mockResponseOnce(
            () =>
                new Promise((resolve) => {
                    resolveDetail = resolve;
                }),
        );
        const pending = store.dispatch(selectSummary(summary("chosen")));
        backend.onMessage({ type: "flows/remove", payload: "chosen" });
        await waitFor(() => expect(resolveDetail).toBeDefined());
        resolveDetail(JSON.stringify({ ...TFlow(), id: "chosen" }));
        await pending;
        expect(store.getState().history.selected).toBeNull();
        expect(store.getState().flows.list).toEqual([]);
        expect(store.getState().flows.selected).toEqual([]);
    });

    test("history reset preserves read-only summary selection and never requests live snapshots", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(STATE_UPDATE({ sqlite_history: true }));
        await store.dispatch(selectSummary(summary("archived", false)));
        const before = store.getState().timeRange.historyRevision;
        await backend.onMessage({ type: "flows/reset" });
        expect(store.getState().history.selected?.id).toBe("archived");
        expect(store.getState().timeRange.historyRevision).toBe(before + 1);
        expect(store.getState().flows.list).toEqual([]);
        expect(fetchMock).not.toHaveBeenCalled();
    });

    test("switching SQLite mode clears full flow Redux and ignores an older legacy fetch", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        let resolveFlows!: (value: string) => void;
        fetchMock.mockResponseOnce(
            () =>
                new Promise((resolve) => {
                    resolveFlows = resolve;
                }),
        );
        const pending = backend.fetchData(Resource.Flows);
        await waitFor(() => expect(resolveFlows).toBeDefined());
        store.dispatch(STATE_UPDATE({ sqlite_history: true }));
        resolveFlows(JSON.stringify([TFlow()]));
        await pending;
        expect(store.getState().flows.list).toEqual([]);
        fetchMock.mockResponseOnce(JSON.stringify([TFlow()]));
        store.dispatch(STATE_UPDATE({ sqlite_history: false }));
        await waitFor(() =>
            expect(store.getState().flows.list).toHaveLength(1),
        );
        expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
            "./flows",
            "./flows",
        ]);
    });

    test("reconnect sends current search and highlight even when unchanged", async () => {
        fetchMock.mockResponse((request) => {
            if (request.url === "./state")
                return Promise.resolve(JSON.stringify(serverState()));
            if (request.url === "./scripts")
                return Promise.resolve('{"scripts":[]}');
            return Promise.resolve("[]");
        });
        const store = TStore(null);
        const backend = new WebSocketBackend(store);
        const send = jest.spyOn(backend.socket, "send");
        // @ts-expect-error writable mock
        backend.socket.readyState = WebSocket.OPEN;
        await backend.onOpen();
        store.dispatch(setFilter("~dt 5m"));
        store.dispatch(setHighlight("~u example"));
        send.mockClear();
        await backend.onOpen();
        expect(
            send.mock.calls.map(([message]) => JSON.parse(String(message))),
        ).toEqual(
            expect.arrayContaining([
                {
                    type: "flows/updateFilter",
                    payload: { name: "search", expr: "~dt 5m" },
                },
                {
                    type: "flows/updateFilter",
                    payload: { name: "highlight", expr: "~u example" },
                },
            ]),
        );
    });

    test("authenticated reconnect preserves preference and only a different real instance resets it", async () => {
        let instanceId = "backend-a";
        fetchMock.mockResponse((request) => {
            if (request.url === "./state")
                return Promise.resolve(
                    JSON.stringify({
                        ...serverState(true),
                        instance_id: instanceId,
                    }),
                );
            if (request.url === "./scripts")
                return Promise.resolve('{"scripts":[]}');
            return Promise.resolve("[]");
        });
        const store = TStore(null);
        const backend = new WebSocketBackend(store);
        await backend.onOpen();
        store.dispatch(updateDraft({ duration: 300 }));
        store.dispatch(applyDraft());
        jest.spyOn(console, "error").mockImplementation(() => {});
        backend.onClose(new CloseEvent("close"));
        expect(store.getState().timeRange).toMatchObject({
            ready: false,
            instanceId: "backend-a",
            preference: { duration: 300 },
        });
        await backend.onOpen();
        expect(store.getState().timeRange.preference.duration).toBe(300);
        instanceId = "backend-b";
        await backend.onOpen();
        expect(store.getState().timeRange).toMatchObject({
            ready: true,
            instanceId: "backend-b",
            preference: { duration: 1800 },
            draft: { duration: 1800 },
        });
        expect(fetchMock.mock.calls.map(([url]) => url)).not.toContain(
            "./flows",
        );
    });

    test("the unavailable error tab falls back to a response only without a flow error", () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        const send = jest.spyOn(backend.socket, "send");
        // @ts-expect-error writable mock
        backend.socket.readyState = WebSocket.OPEN;
        const flow = TFlow();
        Object.assign(flow.response, { stream_preview: true });
        store.dispatch(selectTab("error"));
        store.dispatch(select([flow]));
        expect(send).not.toHaveBeenCalled();
        const noError = { ...flow, error: undefined };
        store.dispatch(FLOWS_UPDATE({ flow: noError, matching_filters: {} }));
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: flow.id, subscription: 1 },
            }),
        );
        store.dispatch(FLOWS_UPDATE({ flow, matching_filters: {} }));
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: null, subscription: 2 },
            }),
        );
    });

    test("same-id response generations rotate subscriptions without restarting terminal sessions", () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        const send = jest.spyOn(backend.socket, "send");
        // @ts-expect-error writable mock
        backend.socket.readyState = WebSocket.OPEN;
        const flow = TFlow();
        Object.assign(flow.response, { stream_preview: true });
        store.dispatch(selectTab("response"));
        store.dispatch(select([flow]));
        const preview = {
            flow_id: flow.id,
            subscription: 1,
            seq: 1,
            start: 0,
            end: 3,
            received: 3,
            dropped: 0,
            missing: false,
            status: "finished",
            reason: null,
            text: "old",
            reset: true,
            stored: false,
            max_bytes: 32,
        };
        backend.onMessage({ type: "response/preview", payload: preview });
        const completed = {
            ...flow,
            response: { ...flow.response, stream_preview: false },
        };
        store.dispatch(FLOWS_UPDATE({ flow: completed, matching_filters: {} }));
        expect(send).toHaveBeenCalledTimes(1);
        const next = {
            ...flow,
            response: {
                ...flow.response,
                timestamp_start: flow.response.timestamp_start + 1,
            },
        };
        store.dispatch(FLOWS_UPDATE({ flow: next, matching_filters: {} }));
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: flow.id, subscription: 2 },
            }),
        );
        expect(store.getState().responsePreview.chunks).toEqual([]);
        backend.onMessage({
            type: "response/preview",
            payload: { ...preview, seq: 99 },
        });
        expect(store.getState().responsePreview.preview).toBeNull();
    });

    test("selection changes invalidate stale previews and tab changes unsubscribe", () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        const send = jest.spyOn(backend.socket, "send");
        // @ts-expect-error writable mock
        backend.socket.readyState = WebSocket.OPEN;
        const flow = TFlow();
        Object.assign(flow.response, {
            stream_preview: true,
            timestamp_end: null,
        });
        const other = { ...flow, id: "other" };
        store.dispatch(selectTab("response"));
        store.dispatch(select([flow]));
        const preview = {
            flow_id: flow.id,
            subscription: 1,
            seq: 1,
            start: 0,
            end: 9,
            received: 9,
            dropped: 0,
            missing: false,
            status: "streaming",
            reason: null,
            text: "data: a\n\n",
            reset: true,
            stored: false,
            max_bytes: 32,
        };
        backend.onMessage({ type: "response/preview", payload: preview });
        expect(store.getState().responsePreview.chunks[0].text).toBe(
            "data: a\n\n",
        );
        expect(flow.response.contentLength).not.toBe(9);
        store.dispatch(select([other]));
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: "other", subscription: 2 },
            }),
        );
        backend.onMessage({
            type: "response/preview",
            payload: { ...preview, seq: 99 },
        });
        expect(store.getState().responsePreview.chunks).toEqual([]);
        backend.onMessage({
            type: "response/preview",
            payload: {
                ...preview,
                flow_id: "other",
                subscription: 2,
                text: "new",
            },
        });
        backend.onMessage({
            type: "response/preview",
            payload: {
                ...preview,
                flow_id: "other",
                subscription: 2,
                text: "duplicate",
            },
        });
        expect(store.getState().responsePreview.chunks[0].text).toBe("new");
        store.dispatch(selectTab("request"));
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: null, subscription: 3 },
            }),
        );
        expect(store.getState().responsePreview.preview).toBeNull();
    });

    test("eligibility uses stream_preview, visible workspace and exactly one selected response", () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        const send = jest.spyOn(backend.socket, "send");
        // @ts-expect-error writable mock
        backend.socket.readyState = WebSocket.OPEN;
        const flow = TFlow();
        Object.assign(flow.response, { stream: true, timestamp_end: null });
        store.dispatch(selectTab("response"));
        store.dispatch(select([flow]));
        expect(send).not.toHaveBeenCalled();
        const watched = {
            ...flow,
            response: { ...flow.response, stream_preview: true },
        };
        store.dispatch(setCurrent(Tab.Capture));
        store.dispatch(select([watched]));
        store.dispatch(setCurrent(Tab.Scripts));
        expect(send).not.toHaveBeenCalled();
        store.dispatch(select([watched, { ...watched, id: "other" }]));
        store.dispatch(setCurrent(Tab.FlowList));
        expect(send).not.toHaveBeenCalled();
        store.dispatch(select([watched]));
        expect(send).toHaveBeenCalledTimes(1);
        store.dispatch(select([]));
        expect(send).toHaveBeenCalledTimes(2);
        expect(store.getState().responsePreview.flow_id).toBeNull();
    });

    test("reset invalidates the session and resubscribes with a fresh token after fetching", async () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        const send = jest.spyOn(backend.socket, "send");
        // @ts-expect-error writable mock
        backend.socket.readyState = WebSocket.OPEN;
        const flow = TFlow();
        Object.assign(flow.response, { stream_preview: true });
        store.dispatch(selectTab("response"));
        store.dispatch(select([flow]));
        fetchMock.mockResponseOnce(JSON.stringify([flow]));
        const fetched = backend.onMessage({ type: "flows/reset" });
        expect(store.getState().responsePreview.flow_id).toBeNull();
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: null, subscription: 2 },
            }),
        );
        await fetched;
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: flow.id, subscription: 3 },
            }),
        );
    });

    test("connecting and disconnected sockets never queue response subscriptions", () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        const send = jest.spyOn(backend.socket, "send");
        const flow = TFlow();
        Object.assign(flow.response, { stream_preview: true });
        store.dispatch(selectTab("response"));
        store.dispatch(select([flow]));
        expect(backend.messageQueue).toEqual([]);
        // @ts-expect-error writable mock
        backend.socket.readyState = WebSocket.OPEN;
        backend.onStoreUpdate();
        expect(send).toHaveBeenCalledTimes(1);
        jest.spyOn(console, "error").mockImplementation(() => {});
        backend.onClose(new CloseEvent("close"));
        expect(store.getState().responsePreview.flow_id).toBeNull();
        store.dispatch(selectTab("request"));
        store.dispatch(selectTab("response"));
        expect(send).toHaveBeenCalledTimes(1);
        expect(backend.messageQueue).toEqual([]);
    });

    test("only the visible selected response subscribes, and completion keeps the session", () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        const send = jest.spyOn(backend.socket, "send");
        // @ts-expect-error writable mock
        backend.socket.readyState = WebSocket.OPEN;
        store.dispatch(connectionActions.finishFetching());
        const flow = TFlow();
        Object.assign(flow.response, {
            stream_preview: true,
            timestamp_end: null,
        });
        store.dispatch(FLOWS_RECEIVE([flow]));
        store.dispatch(select([flow]));
        expect(send).not.toHaveBeenCalled();
        store.dispatch(selectTab("response"));
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: flow.id, subscription: 1 },
            }),
        );
        const completed = {
            ...flow,
            response: {
                ...flow.response,
                stream_preview: false,
                timestamp_end: 100,
            },
        };
        store.dispatch(FLOWS_UPDATE({ flow: completed, matching_filters: {} }));
        expect(send).toHaveBeenCalledTimes(1);
        store.dispatch(setCurrent(Tab.Scripts));
        expect(send).toHaveBeenLastCalledWith(
            JSON.stringify({
                type: "flows/subscribeResponse",
                payload: { flow_id: null, subscription: 2 },
            }),
        );
        store.dispatch(setCurrent(Tab.FlowList));
        expect(send).toHaveBeenCalledTimes(2);
    });

    test("message queueing", async () => {
        let resolve;
        const events: Promise<MockResponseInit> = new Promise((r) => {
            resolve = r;
        });
        const never = async () => new Promise<MockResponseInit>(() => {});
        fetchMock.mockOnceIf("./state", JSON.stringify(serverState()));
        fetchMock.mockOnceIf("./flows", never);
        fetchMock.mockOnceIf("./events", () => events);
        fetchMock.mockOnceIf("./options", never);
        fetchMock.mockOnceIf("./scripts", never);

        const store = TStore(null);
        const backend = new WebSocketBackend(store);

        backend.sendMessage({
            type: "unknown",
            payload: {},
        });
        expect(backend.messageQueue.length).toBe(1);
        // @ts-expect-error jest mock stuff
        backend.socket.readyState = WebSocket.OPEN;
        backend.onOpen();
        expect(store.getState().connection.state).toEqual(
            ConnectionState.FETCHING,
        );
        await waitFor(() => expect(backend.messageQueue.length).toBe(0));

        backend.sendMessage({
            type: "unknown",
            payload: {},
        });
        expect(backend.messageQueue.length).toBe(0);

        let payload: EventLogItem = {
            message: "test",
            level: LogLevel.debug,
            id: "123",
        };
        backend.onMessage({
            type: "events/add",
            payload,
        });

        expect(store.getState().eventLog.list.length).toBe(0);

        resolve("[]");
        await waitFor(() =>
            expect(store.getState().eventLog.list.length).toBe(1),
        );
    });

    test("basic", async () => {
        fetchMock.mockOnceIf("./state", JSON.stringify(serverState()));
        fetchMock.mockOnceIf("./flows", "[]");
        fetchMock.mockOnceIf("./events", "[]");
        fetchMock.mockOnceIf("./options", "{}");
        fetchMock.mockOnceIf("./scripts", '{"scripts": []}');

        const store = TStore(null);
        const backend = new WebSocketBackend(store);

        await backend.onOpen();

        expect(store.getState().connection.state).toBe(
            ConnectionState.ESTABLISHED,
        );
        expect(store.getState().timeRange).toMatchObject({
            ready: true,
            instanceId: "backend-a",
        });
        backend.onMessage({
            type: "events/add",
            payload: {
                id: "42",
                message: "test",
                level: LogLevel.info,
            } as EventLogItem,
        });
        expect(store.getState().eventLog.list).toEqual([
            { id: "42", level: LogLevel.info, message: "test" },
        ]);

        fetchMock.mockOnceIf("./events", "[]");
        await backend.onMessage({
            type: "events/reset",
        });
        expect(store.getState().eventLog.list).toEqual([]);
        expect(fetchMock.mock.calls).toHaveLength(6);

        jest.spyOn(console, "error").mockImplementation(() => {});
        backend.onClose(new CloseEvent("Connection closed"));
        expect(console.error).toHaveBeenCalledTimes(1);
        expect(store.getState().connection.state).toBe(ConnectionState.ERROR);
        expect(store.getState().timeRange).toMatchObject({
            ready: false,
            instanceId: "backend-a",
        });

        backend.onError(null);
        expect(console.error).toHaveBeenCalledTimes(2);
    });

    test("onMessage handling", async () => {
        fetchMock.mockOnceIf("./flows", "[]");
        fetchMock.mockOnceIf("./events", "[]");
        const store = TStore(null);
        const prepared = preparedBackend(store);
        const backend = new WebSocketBackend({
            getState: store.getState,
            dispatch: () => {},
            subscribe: () => {},
        });
        expect(prepared.store.getState().timeRange.ready).toBe(true);
        backend.onMessage({ type: "flows/add" });
        backend.onMessage({ type: "flows/update" });
        backend.onMessage({ type: "flows/remove" });
        backend.onMessage({ type: "flows/reset" });
        backend.onMessage({ type: "flows/filterUpdate" });
        backend.onMessage({ type: "events/add" });
        backend.onMessage({ type: "events/reset" });
        backend.onMessage({ type: "options/update" });
        backend.onMessage({ type: "state/update" });
        backend.onMessage({
            type: "scripts/update",
            scripts: [
                {
                    path: "foo.py",
                    fullpath: "/tmp/foo.py",
                    status: ScriptStatusKind.loaded,
                    error: null,
                },
            ],
        });
        expect(fetchMock.mock.calls.length).toBe(2);
    });

    test("filter updates", () => {
        const store = TStore(null);
        const backend = preparedBackend(store);
        store.dispatch(setFilter("foo"));
        expect(backend.messageQueue).toEqual([
            {
                type: "flows/updateFilter",
                payload: { expr: "foo", name: "search" },
            },
        ]);
    });
});
