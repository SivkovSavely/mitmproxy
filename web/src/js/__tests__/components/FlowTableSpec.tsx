import * as React from "react";
import FlowTable, { PureFlowTable } from "../../components/FlowTable";

import { act, fireEvent, render as renderWithStore } from "../test-utils";
import {
    FLOWS_ADD,
    FLOWS_FILTER_UPDATE,
    FLOWS_RECEIVE,
    FLOWS_REMOVE,
    select,
} from "../../ducks/flows";
import { configureStore } from "@reduxjs/toolkit";
import { reducer, middlewares } from "../../ducks/store";
import timeRange, {
    applyDraft,
    defaultState,
    establishBackend,
    refreshTimeRange,
    updateDraft,
} from "../../ducks/timeRange";
import { testState, TFlow } from "../ducks/tutils";
import { FilterName } from "../../ducks/ui/filter";
import history from "../../ducks/history";

function defaultStore() {
    return configureStore({
        reducer: { ...reducer, timeRange, history },
        preloadedState: {
            ...testState,
            timeRange: defaultState,
            history: { selected: null, generation: 0 },
        },
        middleware: (getDefaultMiddleware) => getDefaultMiddleware(middlewares),
    });
}

function render(ui: React.ReactElement, { store = defaultStore() } = {}) {
    return renderWithStore(ui, { store });
}

function renderScrollableTable(established = true) {
    const store = defaultStore();
    const now = Date.now() / 1000;
    const flows = Array.from({ length: 100 }, (_, index) => ({
        ...TFlow(),
        id: `flow-${index}`,
        timestamp_created: now - (index === 0 ? 1790 : 60),
    }));
    store.dispatch(FLOWS_RECEIVE(flows));
    if (established) store.dispatch(establishBackend("backend-a"));
    const rendered = render(<FlowTable />, { store });
    const viewport =
        rendered.container.querySelector<HTMLElement>(".flow-table")!;
    Object.defineProperties(viewport, {
        clientHeight: { value: 160 },
        offsetHeight: { value: 160 },
        scrollHeight: {
            get: () =>
                Array.from(
                    viewport.querySelectorAll<HTMLElement>("tbody tr"),
                ).reduce(
                    (height, row) =>
                        height + Number.parseFloat(row.style.height),
                    0,
                ),
        },
    });
    fireEvent.scroll(viewport, { target: { scrollTop: 320 } });
    return { ...rendered, viewport, flows };
}

window.addEventListener = jest.fn();

beforeEach(() => localStorage.clear());
afterEach(() => jest.restoreAllMocks());

describe("FlowTable Component", () => {
    it("intersects the global range with server Search results without changing Search semantics", () => {
        const store = configureStore({
            reducer: { ...reducer, timeRange },
            middleware: (getDefaultMiddleware) =>
                getDefaultMiddleware(middlewares),
        });
        const now = Date.now() / 1000;
        const recent = {
            ...TFlow(),
            id: "recent",
            timestamp_created: now - 60,
        };
        const old = { ...TFlow(), id: "old", timestamp_created: now - 3600 };
        const unmatched = {
            ...TFlow(),
            id: "unmatched",
            timestamp_created: now - 30,
        };
        store.dispatch(FLOWS_RECEIVE([old, recent, unmatched]));
        store.dispatch(
            FLOWS_FILTER_UPDATE({
                name: FilterName.Search,
                matching_flow_ids: ["old", "recent"],
            }),
        );
        store.dispatch(establishBackend("backend-a"));
        store.dispatch(updateDraft({ duration: 300 }));
        store.dispatch(applyDraft());
        const { container } = render(<FlowTable />, { store });
        expect(container.querySelectorAll("tbody tr")).toHaveLength(3);
        expect(store.getState().flows.view.map((flow) => flow.id)).toEqual([
            "old",
            "recent",
        ]);
        expect(store.getState().flows.list).toHaveLength(3);
    });

    it("scrolls to newest when an applied relative range changes while browsing older flows", () => {
        const { store, viewport } = renderScrollableTable();
        expect(viewport.scrollTop + viewport.clientHeight).toBeLessThan(
            viewport.scrollHeight,
        );
        act(() => {
            store.dispatch(updateDraft({ duration: 300 }));
            store.dispatch(applyDraft());
        });
        expect(
            viewport.scrollTop + viewport.clientHeight,
        ).toBeGreaterThanOrEqual(viewport.scrollHeight);
    });

    it("scrolls to newest when an applied explicit bound changes", () => {
        const { store, viewport } = renderScrollableTable();
        act(() => {
            store.dispatch(
                updateDraft({
                    mode: "explicit",
                    from: "2000-01-01 00:00:00",
                    until: "2099-12-31 23:59:59",
                }),
            );
            store.dispatch(applyDraft());
        });
        fireEvent.scroll(viewport, { target: { scrollTop: 320 } });
        act(() => {
            store.dispatch(updateDraft({ until: "2090-12-31 23:59:59" }));
            store.dispatch(applyDraft());
        });
        expect(
            viewport.scrollTop + viewport.clientHeight,
        ).toBeGreaterThanOrEqual(viewport.scrollHeight);
    });

    it.each([true, false])(
        "an applied-range jump takes priority over a new selection (visible: %s)",
        (visible) => {
            const { store, viewport, flows } = renderScrollableTable();
            act(() => {
                store.dispatch(updateDraft({ duration: 300 }));
                store.dispatch(applyDraft());
                store.dispatch(select([flows[visible ? 10 : 0]]));
            });
            expect(
                viewport.scrollTop + viewport.clientHeight,
            ).toBeGreaterThanOrEqual(viewport.scrollHeight);
        },
    );

    it("does not scroll to an invisible selected flow before its selection is pruned", () => {
        const { store, viewport, flows } = renderScrollableTable();
        act(() => {
            store.dispatch(updateDraft({ duration: 300 }));
            store.dispatch(applyDraft());
        });
        fireEvent.scroll(viewport, { target: { scrollTop: 320 } });
        act(() => store.dispatch(select([flows[0]])));
        expect(store.getState().flows.selectedIds.has(flows[0].id)).toBe(true);
        expect(viewport.scrollTop).toBe(320);
    });

    it("preserves browsing position for draft edits, unchanged applies, expiry, and live updates", () => {
        const now = Date.now();
        const clock = jest.spyOn(Date, "now").mockReturnValue(now);
        const { store, viewport } = renderScrollableTable();
        act(() => store.dispatch(updateDraft({ duration: 300 })));
        expect(viewport.scrollTop).toBe(320);
        act(() => {
            store.dispatch(updateDraft({ duration: 1800 }));
            store.dispatch(applyDraft());
        });
        expect(viewport.scrollTop).toBe(320);
        clock.mockReturnValue(now + 30_000);
        act(() => store.dispatch(refreshTimeRange()));
        expect(viewport.scrollHeight).toBe(99 * 32);
        expect(viewport.scrollTop).toBe(320);
        act(() =>
            store.dispatch(
                FLOWS_ADD({
                    flow: {
                        ...TFlow(),
                        id: "live",
                        timestamp_created: Date.now() / 1000,
                    },
                    matching_filters: {},
                }),
            ),
        );
        expect(viewport.scrollHeight).toBe(100 * 32);
        expect(viewport.scrollTop).toBe(320);
    });

    it("still follows ordinary live additions when already at the bottom", () => {
        const { store, viewport } = renderScrollableTable();
        fireEvent.scroll(viewport, {
            target: {
                scrollTop: viewport.scrollHeight - viewport.clientHeight,
            },
        });
        act(() =>
            store.dispatch(
                FLOWS_ADD({
                    flow: {
                        ...TFlow(),
                        id: "live",
                        timestamp_created: Date.now() / 1000,
                    },
                    matching_filters: {},
                }),
            ),
        );
        expect(viewport.scrollHeight).toBe(101 * 32);
        expect(
            viewport.scrollTop + viewport.clientHeight,
        ).toBeGreaterThanOrEqual(viewport.scrollHeight);
    });

    it("does not force a range jump for unestablished static flow updates", () => {
        const { store, viewport } = renderScrollableTable(false);
        act(() => {
            store.dispatch(refreshTimeRange());
            store.dispatch(
                FLOWS_ADD({
                    flow: { ...TFlow(), id: "static", timestamp_created: 1 },
                    matching_filters: {},
                }),
            );
        });
        expect(store.getState().timeRange.instanceId).toBeNull();
        expect(viewport.scrollHeight).toBe(101 * 32);
        expect(viewport.scrollTop).toBe(320);
    });

    it("should render correctly", () => {
        const { asFragment } = render(<FlowTable />);
        expect(asFragment()).toMatchSnapshot();
    });

    it("should scroll current selection into view", () => {
        const height = PureFlowTable.defaultProps.rowHeight;
        const { asFragment, store } = render(
            <div style={{ height, overflow: "hidden" }}>
                <FlowTable />
            </div>,
        );
        expect(asFragment()).toMatchSnapshot();

        act(() => store.dispatch(select([store.getState().flows.view[3]])));
        expect(asFragment()).toMatchSnapshot();
    });

    it("does not call onViewportUpdate when flowView and rowHeight are unchanged", () => {
        // Regression guard for an infinite componentDidUpdate -> setState
        // cycle. Before the FlowTableProps-comparison gate in
        // componentDidUpdate, onViewportUpdate was called on EVERY update —
        // including the setState the previous call itself produced — so
        // setState could feed itself indefinitely when state.viewportTop
        // and viewport.scrollTop never converged.
        const spy = jest.spyOn(PureFlowTable.prototype, "onViewportUpdate");
        const { store } = render(<FlowTable />);
        spy.mockClear(); // ignore the componentDidMount call

        // A `select` dispatch changes connect-mapped props that reach
        // FlowTable (onlySelectedId, firstSelectedIndex) and triggers
        // componentDidUpdate, but does NOT change flowView or rowHeight —
        // so onViewportUpdate must not run.
        act(() => store.dispatch(select([store.getState().flows.view[0]])));
        expect(spy).not.toHaveBeenCalled();

        spy.mockRestore();
    });

    it("calls onViewportUpdate when flowView changes", () => {
        // Complement of the previous test: removing a flow changes
        // `state.flows.view`, so the connect-mapped `flowView` prop differs
        // from `prevProps.flowView` on the resulting componentDidUpdate.
        // The gate must let onViewportUpdate run in this branch — otherwise
        // adding/removing flows would not refresh the virtual-scroll window.
        const spy = jest.spyOn(PureFlowTable.prototype, "onViewportUpdate");
        const { store } = render(<FlowTable />);
        spy.mockClear();

        const firstFlowId = store.getState().flows.view[0].id;
        act(() => store.dispatch(FLOWS_REMOVE(firstFlowId)));
        expect(spy).toHaveBeenCalled();

        spy.mockRestore();
    });
});
