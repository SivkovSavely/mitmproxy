/**
 *  The WebSocket backend is responsible for updating our knowledge of flows and events
 *  from the REST API and live updates delivered via a WebSocket connection.
 *  An alternative backend may use the REST API only to host static instances.
 */
import { assertNever, fetchApi } from "../utils";
import * as connectionActions from "../ducks/connection";
import type { RootStore } from "../ducks";
import { STATE_RECEIVE, STATE_UPDATE } from "../ducks/backendState";
import { EVENTS_ADD, EVENTS_RECEIVE } from "../ducks/eventLog";
import { OPTIONS_RECEIVE, OPTIONS_UPDATE } from "../ducks/options";
import { SCRIPTS_RECEIVE } from "../ducks/scripts";
import type { ScriptStatus } from "../ducks/scripts";
import * as responsePreview from "../ducks/responsePreview";
import { Tab } from "../ducks/ui/tabs";
import {
    FLOWS_ADD,
    FLOWS_FILTER_UPDATE,
    FLOWS_RECEIVE,
    FLOWS_REMOVE,
    FLOWS_UPDATE,
} from "../ducks/flows";
import type { Action, PayloadAction } from "@reduxjs/toolkit";
import {
    FilterName,
    initialState as initialFilterState,
} from "../ducks/ui/filter";
import {
    backendConnecting,
    establishBackend,
    invalidateHistory,
    pruneLegacySelection,
} from "../ducks/timeRange";
import { clearSelection } from "../ducks/history";

export enum Resource {
    State = "state",
    Flows = "flows",
    Events = "events",
    Options = "options",
    Scripts = "scripts",
}

/// All possible events emitted by the WebSocket backend.
type WebsocketMessageType =
    | "flows/add"
    | "flows/update"
    | "flows/filterUpdate"
    | "flows/remove"
    | "flows/reset"
    | "events/add"
    | "events/reset"
    | "options/update"
    | "state/update"
    | "scripts/update"
    | "response/preview";

export default class WebsocketBackend {
    activeFetches: Partial<
        Record<Resource, { queue: Array<Action>; controller: AbortController }>
    >;
    store: RootStore;
    filterState: typeof initialFilterState;
    socket: WebSocket;
    messageQueue: Action[];
    previewFlow: string | null = null;
    previewResponseStart: number | undefined;
    previewSubscription = 0;
    previewDisconnected = false;
    previewResetting = false;
    connectionGeneration = 0;
    establishing = false;
    historyMode = false;

    constructor(store) {
        this.activeFetches = {};
        this.store = store;
        this.filterState = initialFilterState;
        this.messageQueue = [];
        this.connect();
        this.store.subscribe(this.onStoreUpdate.bind(this));
    }

    connect() {
        this.connectionGeneration++;
        this.abortFetches();
        this.previewDisconnected = true;
        this.store.dispatch(backendConnecting());
        this.clearResponseSubscription();
        const socket = new WebSocket(
            location.origin.replace("http", "ws") +
                location.pathname.replace(/\/$/, "") +
                "/updates",
        );
        this.socket = socket;
        socket.addEventListener("open", () => {
            if (this.socket === socket) this.onOpen();
        });
        socket.addEventListener("close", (event) => {
            if (this.socket === socket) this.onClose(event);
        });
        socket.addEventListener("message", (msg) => {
            if (this.socket === socket) this.onMessage(JSON.parse(msg.data));
        });
        socket.addEventListener("error", (error) => {
            if (this.socket === socket) this.onError(error);
        });
    }

    async onOpen() {
        const generation = ++this.connectionGeneration;
        this.establishing = true;
        this.store.dispatch(backendConnecting());
        this.previewDisconnected = false;
        this.clearResponseSubscription();
        this.abortFetches();
        this.store.dispatch(connectionActions.startFetching());
        try {
            await this.fetchData(Resource.State);
            if (
                generation !== this.connectionGeneration ||
                !this.store.getState().timeRange.ready
            )
                return;
            const historyMode =
                this.store.getState().backendState.sqlite_history;
            if (historyMode || this.historyMode !== historyMode)
                this.clearLiveFlows();
            this.historyMode = historyMode;
            this.store.dispatch(invalidateHistory());
            this.establishing = false;
            for (const message of this.messageQueue)
                this.socket.send(JSON.stringify(message));
            this.messageQueue = [];
            // Filters are per WebSocket connection, not backend instance.
            this.filterState = initialFilterState;
            this.sendFilters(true);
            await Promise.all([
                this.fetchData(Resource.Flows),
                this.fetchData(Resource.Events),
                this.fetchData(Resource.Options),
                this.fetchData(Resource.Scripts),
            ]);
            if (generation === this.connectionGeneration)
                this.store.dispatch(connectionActions.finishFetching());
        } catch (error) {
            if (generation !== this.connectionGeneration) return;
            this.store.dispatch(backendConnecting());
            this.store.dispatch(
                connectionActions.connectionError(String(error)),
            );
        } finally {
            if (generation === this.connectionGeneration)
                this.establishing = false;
        }
    }

    onStoreUpdate() {
        const state = this.store.getState();
        if (!state.timeRange.ready || this.establishing) return;
        if (this.historyMode !== state.backendState.sqlite_history) {
            this.historyMode = state.backendState.sqlite_history;
            this.clearLiveFlows();
            this.store.dispatch(invalidateHistory());
            if (!this.historyMode)
                this.fetchData(Resource.Flows).catch((error) => {
                    this.store.dispatch(
                        connectionActions.connectionError(String(error)),
                    );
                });
        }
        this.store.dispatch(pruneLegacySelection());
        this.updateResponseSubscription();
        this.sendFilters();
    }

    sendFilters(force = false) {
        const storeFilterState = this.store.getState().ui.filter;
        if (!force && storeFilterState === this.filterState) {
            return;
        }
        for (const name of Object.values(FilterName)) {
            if (force || this.filterState[name] !== storeFilterState[name]) {
                this.sendMessage({
                    type: "flows/updateFilter",
                    payload: {
                        name,
                        expr: storeFilterState[name],
                    },
                });
            }
        }
        this.filterState = storeFilterState;
    }

    updateResponseSubscription() {
        const state = this.store.getState();
        const selected = state.flows.selected;
        const flow = selected.length === 1 ? selected[0] : undefined;
        const visible =
            (state.ui.flow.tab === "response" ||
                (state.ui.flow.tab === "error" &&
                    flow?.type === "http" &&
                    flow.response &&
                    !flow.error)) &&
            state.ui.tabs.current !== Tab.Capture &&
            state.ui.tabs.current !== Tab.Scripts;
        const connected =
            state.timeRange.ready &&
            this.socket.readyState === WebSocket.OPEN &&
            !this.previewDisconnected &&
            state.connection.state !== connectionActions.ConnectionState.ERROR;
        // Keep a watched session through flow completion: its terminal preview can
        // arrive after the canonical flow update, particularly for nonstored bodies.
        const responseStart =
            flow?.type === "http" ? flow.response?.timestamp_start : undefined;
        const sameResponse =
            this.previewFlow === flow?.id &&
            this.previewResponseStart === responseStart;
        const desired =
            connected &&
            visible &&
            !this.previewResetting &&
            !this.activeFetches[Resource.Flows] &&
            flow?.type === "http" &&
            flow.response &&
            (sameResponse || flow.response.stream_preview === true)
                ? flow.id
                : null;
        if (
            desired !== this.previewFlow ||
            (desired !== null && !sameResponse)
        ) {
            this.previewFlow = desired;
            this.previewResponseStart =
                desired === null ? undefined : responseStart;
            const payload = {
                flow_id: desired,
                subscription: ++this.previewSubscription,
            };
            this.store.dispatch(responsePreview.subscribe(payload));
            if (connected)
                this.sendMessage({ type: "flows/subscribeResponse", payload });
        }
    }

    fetchData(resource: Resource) {
        this.abortFetches(resource);
        const state = this.store.getState();
        if (
            resource === Resource.Flows &&
            (!state.timeRange.ready || state.backendState.sqlite_history)
        )
            return Promise.resolve();
        const generation = this.connectionGeneration;
        const queue: Array<Action> = [];
        const controller = new AbortController();
        this.activeFetches[resource] = { queue, controller };
        const current = () =>
            !controller.signal.aborted &&
            generation === this.connectionGeneration &&
            this.activeFetches[resource]?.queue === queue;
        return fetchApi(`./${resource}`, { signal: controller.signal })
            .then((res) => {
                if (!current()) return;
                if (!res.ok)
                    throw new Error(
                        `Fetching ${resource} failed (${res.status}).`,
                    );
                return res.json();
            })
            .then((json) => {
                if (current()) this.receive(resource, json);
            })
            .catch((error) => {
                if (!controller.signal.aborted) throw error;
            })
            .finally(() => {
                if (this.activeFetches[resource]?.queue === queue)
                    delete this.activeFetches[resource];
            });
    }

    abortFetches(resource?: Resource) {
        for (const name of resource ? [resource] : Object.values(Resource)) {
            const active = this.activeFetches[name];
            delete this.activeFetches[name];
            active?.controller.abort();
        }
    }

    onMessage(msg: {
        type: WebsocketMessageType;
        payload?: any;
        scripts?: ScriptStatus[];
    }) {
        if (msg.type.startsWith("flows/")) {
            const state = this.store.getState();
            if (!state.timeRange.ready) return;
            if (state.backendState.sqlite_history) {
                this.store.dispatch(invalidateHistory());
                const selected = state.history.selected;
                switch (msg.type) {
                    case "flows/add":
                    case "flows/update": {
                        const id = msg.payload.flow.id;
                        if (state.flows.byId.has(id))
                            this.store.dispatch(FLOWS_UPDATE(msg.payload));
                        else if (selected?.live && selected.id === id)
                            this.store.dispatch(FLOWS_ADD(msg.payload));
                        return;
                    }
                    case "flows/remove":
                        if (selected?.id === msg.payload)
                            this.store.dispatch(clearSelection());
                        if (
                            state.flows.byId.has(msg.payload) ||
                            selected?.id === msg.payload
                        )
                            this.store.dispatch(FLOWS_REMOVE(msg.payload));
                        return;
                    case "flows/filterUpdate":
                        if (state.flows.byId.size)
                            this.store.dispatch(
                                FLOWS_FILTER_UPDATE({
                                    ...msg.payload,
                                    matching_flow_ids:
                                        msg.payload.matching_flow_ids?.filter(
                                            (id: string) =>
                                                state.flows.byId.has(id),
                                        ) ?? null,
                                }),
                            );
                        return;
                    case "flows/reset":
                        this.clearLiveFlows();
                        return;
                }
            }
        }
        switch (msg.type) {
            case "response/preview":
                return this.store.dispatch(
                    responsePreview.receive(msg.payload),
                );
            case "flows/add":
                return this.queueOrDispatch(
                    Resource.Flows,
                    FLOWS_ADD(msg.payload),
                );
            case "flows/update":
                return this.queueOrDispatch(
                    Resource.Flows,
                    FLOWS_UPDATE(msg.payload),
                );
            case "flows/filterUpdate":
                return this.queueOrDispatch(
                    Resource.Flows,
                    FLOWS_FILTER_UPDATE(msg.payload),
                );
            case "flows/remove":
                return this.queueOrDispatch(
                    Resource.Flows,
                    FLOWS_REMOVE(msg.payload),
                );
            case "events/add":
                return this.queueOrDispatch(
                    Resource.Events,
                    EVENTS_ADD(msg.payload),
                );
            case "options/update": {
                // The option alone does not establish the current backend capability.
                const refreshed =
                    msg.payload && "save_sqlite_file" in msg.payload
                        ? this.onOpen()
                        : undefined;
                this.queueOrDispatch(
                    Resource.Options,
                    OPTIONS_UPDATE(msg.payload),
                );
                return refreshed;
            }
            case "state/update":
                return this.queueOrDispatch(
                    Resource.State,
                    STATE_UPDATE(msg.payload),
                );
            case "scripts/update":
                return this.queueOrDispatch(
                    Resource.Scripts,
                    SCRIPTS_RECEIVE(msg.scripts ?? []),
                );
            case "flows/reset":
                this.previewResetting = true;
                this.clearResponseSubscription();
                return this.fetchData(Resource.Flows);
            case "events/reset":
                return this.fetchData(Resource.Events);
            /* istanbul ignore next @preserve */
            default:
                assertNever(msg.type);
        }
    }

    sendMessage(action: PayloadAction<any>) {
        if (this.socket.readyState === WebSocket.OPEN) {
            this.socket.send(JSON.stringify(action));
        } else if (this.socket.readyState === WebSocket.CONNECTING) {
            this.messageQueue.push(action);
        } else {
            console.error("WebSocket is not open. Cannot send:", action);
        }
    }

    queueOrDispatch(resource: Resource, action: Action) {
        const queue = this.activeFetches[resource]?.queue;
        if (queue !== undefined) {
            queue.push(action);
        } else {
            this.store.dispatch(action);
        }
    }

    receive(resource: Resource, data) {
        switch (resource) {
            case Resource.State:
                if (
                    typeof data.instance_id !== "string" ||
                    !data.instance_id.trim()
                )
                    throw new Error("Backend state has no instance ID.");
                if (typeof data.sqlite_history !== "boolean")
                    throw new Error("Backend state has no history capability.");
                this.store.dispatch(STATE_RECEIVE(data));
                this.store.dispatch(establishBackend(data.instance_id));
                break;
            case Resource.Options:
                this.store.dispatch(OPTIONS_RECEIVE(data));
                break;
            case Resource.Events:
                this.store.dispatch(EVENTS_RECEIVE(data));
                break;
            case Resource.Flows:
                if (!this.store.getState().backendState.sqlite_history)
                    this.store.dispatch(FLOWS_RECEIVE(data));
                break;
            case Resource.Scripts:
                this.store.dispatch(SCRIPTS_RECEIVE(data.scripts));
                break;
            /* istanbul ignore next @preserve */
            default:
                assertNever(resource);
        }
        const queue = this.activeFetches[resource]!.queue;
        delete this.activeFetches[resource];
        queue.forEach((msg) => this.store.dispatch(msg));
        if (resource === Resource.Flows) {
            this.previewResetting = false;
            this.onStoreUpdate();
        }
    }

    clearLiveFlows() {
        this.abortFetches(Resource.Flows);
        this.previewResetting = true;
        this.clearResponseSubscription();
        if (this.store.getState().history.selected?.live)
            this.store.dispatch(clearSelection());
        this.store.dispatch(FLOWS_RECEIVE([]));
        this.previewResetting = false;
    }

    clearResponseSubscription() {
        if (this.previewFlow === null) return;
        this.previewFlow = null;
        this.previewResponseStart = undefined;
        const payload = {
            flow_id: null,
            subscription: ++this.previewSubscription,
        };
        // Invalidate before dispatching so nested store listeners cannot resubscribe.
        this.store.dispatch(responsePreview.subscribe(payload));
        if (
            !this.previewDisconnected &&
            this.socket.readyState === WebSocket.OPEN
        ) {
            this.sendMessage({ type: "flows/subscribeResponse", payload });
        }
    }

    onClose(closeEvent: CloseEvent) {
        this.connectionGeneration++;
        this.abortFetches();
        this.previewDisconnected = true;
        this.store.dispatch(backendConnecting());
        this.clearResponseSubscription();
        this.store.dispatch(
            connectionActions.connectionError(
                `Connection closed at ${new Date().toUTCString()} with error code ${
                    closeEvent.code
                }.`,
            ),
        );
        console.error("websocket connection closed", closeEvent);
    }

    onError(...args) {
        this.connectionGeneration++;
        this.abortFetches();
        this.previewDisconnected = true;
        this.store.dispatch(backendConnecting());
        this.clearResponseSubscription();
        this.store.dispatch(
            connectionActions.connectionError("WebSocket connection failed."),
        );
        console.error("websocket connection errored", args);
    }
}
