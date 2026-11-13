import * as React from "react";
import { fireEvent, render, screen } from "../test-utils";
import { TStore, testState } from "../ducks/tutils";
import MainView from "../../components/MainView";
import { Tab, setCurrent } from "../../ducks/ui/tabs";
import { configureStore } from "@reduxjs/toolkit";
import { middlewares, reducer } from "../../ducks/store";
import history from "../../ducks/history";
import { summary } from "../historyFixtures";

jest.mock("../../components/FlowTable", () => ({
    __esModule: true,
    default: () => <div>flow-table</div>,
}));
jest.mock("../../components/FlowView", () => ({
    __esModule: true,
    default: () => <div>flow-view</div>,
}));
jest.mock("../../components/History/HistoryTable", () => ({
    __esModule: true,
    default: () => <div>history-table</div>,
    HistorySummary: ({ onClose }: { onClose: () => void }) => (
        <div>
            history-summary
            <button onClick={onClose}>Close metadata</button>
        </div>
    ),
}));
jest.mock("../../components/Modes", () => ({
    __esModule: true,
    default: () => <div>modes</div>,
}));
jest.mock("../../components/Scripts/ScriptsView", () => ({
    __esModule: true,
    default: () => <div>scripts-view</div>,
}));

describe("MainView tab switching", () => {
    it("shows the flow workspace on Flow List", () => {
        const store = TStore(testState);
        store.dispatch(setCurrent(Tab.FlowList));
        const { container } = render(<MainView />, { store });
        expect(container.textContent).toContain("flow-table");
        expect(container.textContent).not.toContain("modes");
        expect(container.textContent).not.toContain("scripts-view");
        // one flow is selected in the default state
        expect(container.textContent).toContain("flow-view");
    });

    it("shows Capture setup on Capture and keeps flows hidden", () => {
        const store = TStore(testState);
        store.dispatch(setCurrent(Tab.Capture));
        const { container } = render(<MainView />, { store });
        expect(container.textContent).toContain("modes");
        expect(container.textContent).not.toContain("flow-table");
        expect(container.textContent).not.toContain("flow-view");
        expect(container.textContent).not.toContain("scripts-view");
        expect(container.textContent).not.toContain("capture-setup");
    });

    it("replaces the whole workspace with Scripts, even with a selected flow", () => {
        const store = TStore(testState);
        store.dispatch(setCurrent(Tab.Scripts));
        const { container } = render(<MainView />, { store });
        expect(container.textContent).toContain("scripts-view");
        expect(container.textContent).not.toContain("flow-table");
        expect(container.textContent).not.toContain("flow-view");
        expect(container.textContent).not.toContain("capture-setup");
        expect(container.textContent).not.toContain("modes");
        expect(container.querySelectorAll(".splitter")).toHaveLength(0);
    });

    it("returns to the flow workspace when switching back", () => {
        const store = TStore(testState);
        store.dispatch(setCurrent(Tab.Scripts));
        store.dispatch(setCurrent(Tab.FlowList));
        const { container } = render(<MainView />, { store });
        expect(container.textContent).toContain("flow-table");
        expect(container.textContent).not.toContain("scripts-view");
    });

    it("renders CaptureSetup instead of FlowTable when there are no flows", () => {
        const store = TStore({
            ...testState,
            flows: { ...testState.flows, list: [], view: [], selected: [] },
        });
        store.dispatch(setCurrent(Tab.FlowList));
        const { container } = render(<MainView />, { store });
        expect(container.textContent).not.toContain("flow-table");
        expect(container.textContent).not.toContain("flow-view");
    });

    it("shows SQLite history even when no live flows are loaded", () => {
        const store = TStore({
            ...testState,
            backendState: { ...testState.backendState, sqlite_history: true },
            flows: { ...testState.flows, list: [], view: [], selected: [] },
        });
        store.dispatch(setCurrent(Tab.FlowList));
        const { container } = render(<MainView />, { store });
        expect(container.textContent).toContain("history-table");
        expect(container.textContent).not.toContain("flow-table");
        expect(container.textContent).not.toContain("capture-setup");
    });

    it("closes historical metadata without reopening either details pane", () => {
        const store = configureStore({
            reducer: { ...reducer, history },
            preloadedState: {
                ...testState,
                backendState: {
                    ...testState.backendState,
                    sqlite_history: true,
                },
                history: { selected: summary(1), generation: 1 },
            },
            middleware: (defaults) => defaults(middlewares),
        });
        store.dispatch(setCurrent(Tab.FlowList));
        const { container } = render(<MainView />, { store });
        expect(container.textContent).toContain("history-summary");
        fireEvent.click(screen.getByRole("button", { name: "Close metadata" }));
        expect(store.getState().history.selected).toBeNull();
        expect(store.getState().flows.selected).toHaveLength(0);
        expect(container.textContent).not.toContain("history-summary");
        expect(container.textContent).not.toContain("flow-view");
        expect(container.querySelector(".splitter")).not.toBeInTheDocument();
    });
});
