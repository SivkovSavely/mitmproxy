import * as React from "react";
import { render } from "../test-utils";
import { TStore, testState } from "../ducks/tutils";
import MainView from "../../components/MainView";
import { Tab, setCurrent } from "../../ducks/ui/tabs";

jest.mock("../../components/FlowTable", () => ({
    __esModule: true,
    default: () => <div>flow-table</div>,
}));
jest.mock("../../components/FlowView", () => ({
    __esModule: true,
    default: () => <div>flow-view</div>,
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
});
