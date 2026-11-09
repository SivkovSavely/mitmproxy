import WebSocketBackend from "../../backends/websocket";
import type { MockResponseInit } from "jest-fetch-mock";
import fetchMock from "jest-fetch-mock";
import { waitFor } from "../test-utils";
import * as connectionActions from "../../ducks/connection";
import type { UnknownAction } from "@reduxjs/toolkit";
import type { EventLogItem } from "../../ducks/eventLog";
import { EVENTS_ADD, EVENTS_RECEIVE, LogLevel } from "../../ducks/eventLog";
import { OPTIONS_RECEIVE } from "../../ducks/options";
import { SCRIPTS_RECEIVE, ScriptStatusKind } from "../../ducks/scripts";
import { FLOWS_RECEIVE, FLOWS_UPDATE, select } from "../../ducks/flows";
import { TFlow } from "../ducks/tutils";
import { selectTab } from "../../ducks/ui/flow";
import { setCurrent, Tab } from "../../ducks/ui/tabs";
import { STATE_RECEIVE } from "../../ducks/backendState";
import { setFilter } from "../../ducks/ui/filter";
import { TStore } from "../ducks/tutils";
import { ConnectionState } from "../../ducks/connection";

beforeEach(() => {
    fetchMock.enableMocks();
    fetchMock.mockClear();
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

describe("websocket backend", () => {
    test("the unavailable error tab falls back to a response only without a flow error", () => {
        const store = TStore(null);
        const backend = new WebSocketBackend(store);
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
        const backend = new WebSocketBackend(store);
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
        const backend = new WebSocketBackend(store);
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
        const backend = new WebSocketBackend(store);
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
        const backend = new WebSocketBackend(store);
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
        const backend = new WebSocketBackend(store);
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
        const backend = new WebSocketBackend(store);
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
        fetchMock.mockOnceIf("./state", never);
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
        expect(backend.messageQueue.length).toBe(0);

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
        fetchMock.mockOnceIf("./state", "{}");
        fetchMock.mockOnceIf("./flows", "[]");
        fetchMock.mockOnceIf("./events", "[]");
        fetchMock.mockOnceIf("./options", "{}");
        fetchMock.mockOnceIf("./scripts", '{"scripts": []}');

        const actions: Array<UnknownAction> = [];
        const backend = new WebSocketBackend({
            getState: TStore(null).getState,
            dispatch: (e) => actions.push(e),
            subscribe: () => {},
        });

        await backend.onOpen();

        expect(actions).toEqual([
            connectionActions.startFetching(),
            // @ts-expect-error mocked
            STATE_RECEIVE({}),
            FLOWS_RECEIVE([]),
            EVENTS_RECEIVE([]),
            // @ts-expect-error mocked
            OPTIONS_RECEIVE({}),
            SCRIPTS_RECEIVE([]),
            connectionActions.finishFetching(),
        ]);

        actions.length = 0;
        backend.onMessage({
            type: "events/add",
            payload: {
                id: "42",
                message: "test",
                level: LogLevel.info,
            } as EventLogItem,
        });
        expect(actions).toEqual([
            EVENTS_ADD({ id: "42", level: LogLevel.info, message: "test" }),
        ]);
        actions.length = 0;

        fetchMock.mockOnceIf("./events", "[]");
        backend.onMessage({
            type: "events/reset",
        });
        await waitFor(() => expect(actions).toEqual([EVENTS_RECEIVE([])]));
        actions.length = 0;
        expect(fetchMock.mock.calls).toHaveLength(6);

        console.error = jest.fn();
        backend.onClose(new CloseEvent("Connection closed"));
        expect(console.error).toHaveBeenCalledTimes(1);
        expect(actions[0].type).toBe(connectionActions.connectionError.type);
        actions.length = 0;

        backend.onError(null);
        expect(console.error).toHaveBeenCalledTimes(2);

        jest.restoreAllMocks();
    });

    test("onMessage handling", async () => {
        fetchMock.mockOnceIf("./flows", "[]");
        fetchMock.mockOnceIf("./events", "[]");
        // Not useful, only for coverage
        const backend = new WebSocketBackend({
            getState: TStore(null).getState,
            dispatch: () => {},
            subscribe: () => {},
        });
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
        const backend = new WebSocketBackend(store);
        store.dispatch(setFilter("foo"));
        expect(backend.messageQueue).toEqual([
            {
                type: "flows/updateFilter",
                payload: { expr: "foo", name: "search" },
            },
        ]);
    });
});
