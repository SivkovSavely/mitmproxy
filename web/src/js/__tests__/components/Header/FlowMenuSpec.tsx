import * as React from "react";
import FlowMenu from "../../../components/Header/FlowMenu";
import { fireEvent, render, screen, waitFor } from "../../test-utils";
import * as flowsActions from "../../../ducks/flows";
import { TStore } from "../../ducks/tutils";
import { THTTPFlow, TTCPFlow } from "../../ducks/_tflow";
import type { Flow, HTTPFlow } from "../../../flow";

test("FlowMenu", async () => {
    const { asFragment } = render(<FlowMenu />);
    expect(asFragment()).toMatchSnapshot();
});

const makeHttpFlow = (id: string, overrides: Partial<HTTPFlow> = {}): Flow => ({
    ...THTTPFlow(),
    id,
    websocket: undefined,
    ...overrides,
});

const findButton = (text: string) => {
    const btn = screen
        .getAllByRole("button")
        .find((b) => b.textContent?.includes(text));
    expect(btn).toBeDefined();
    return btn!;
};

/** Open a Dropdown menu by clicking its wrapper anchor (works for disabled buttons, too). */
const openDropdown = async (buttonText: string) => {
    fireEvent.click(findButton(buttonText).closest("a")!);
    await waitFor(() =>
        expect(document.querySelector(".dropdown-menu.is-open")).toBeTruthy(),
    );
};

/** Close the open menu again: Dropdown keeps a one-shot document listener armed otherwise. */
const closeDropdown = async () => {
    fireEvent.click(document.body);
    await waitFor(() =>
        expect(document.querySelector(".dropdown-menu.is-open")).toBeNull(),
    );
};

const menuItem = (label: string) =>
    screen.getByText(label).closest("a") as HTMLAnchorElement;

function renderWithSelection(flows: Flow[]) {
    const store = TStore();
    for (const flow of flows) {
        store.dispatch(
            flowsActions.FLOWS_ADD({ flow, matching_filters: {} }),
        );
    }
    store.dispatch(flowsActions.select(flows));
    render(<FlowMenu />, { store });
    return store;
}

describe("Export", () => {
    it("is enabled for a single HTTP flow", () => {
        render(<FlowMenu />);
        expect(findButton("Export")).not.toBeDisabled();
    });

    it("is enabled for 2+ selected HTTP flows", () => {
        renderWithSelection([
            makeHttpFlow("multi-a"),
            makeHttpFlow("multi-b"),
        ]);
        expect(findButton("Export")).not.toBeDisabled();
    });

    it("is disabled for a mixed HTTP/non-HTTP selection", () => {
        renderWithSelection([makeHttpFlow("mixed-a"), TTCPFlow()]);
        expect(findButton("Export")).toBeDisabled();
    });

    it("contains body and redacted copy actions", async () => {
        render(<FlowMenu />);
        await openDropdown("Export");
        for (const label of [
            "Copy raw request body",
            "Copy raw response body",
            "Copy raw request and response bodies",
            "Copy redacted request",
            "Copy redacted response",
            "Copy redacted request and response",
        ]) {
            expect(menuItem(label)).toBeInTheDocument();
        }
        await closeDropdown();
    });
});

describe("Download", () => {
    it("keeps the current UI for a single flow", async () => {
        render(<FlowMenu />);
        await openDropdown("Download▾");
        expect(screen.getByText("Download request")).toBeInTheDocument();
        expect(screen.getByText("Download response")).toBeInTheDocument();
        expect(screen.queryByText("Download requests")).toBeNull();
        expect(
            screen.queryByText("Download requests and responses"),
        ).toBeNull();
        await closeDropdown();
    });

    it("renders a plain button for a single request-only flow", () => {
        renderWithSelection([
            makeHttpFlow("request-only", { response: undefined }),
        ]);
        const btn = findButton("Download");
        expect(btn.textContent!.endsWith("▾")).toBe(false);
        expect(btn).not.toBeDisabled();
    });

    it("is enabled with the three menu actions for 2+ selected HTTP flows", async () => {
        renderWithSelection([
            makeHttpFlow("multi-a"),
            makeHttpFlow("multi-b"),
        ]);
        expect(findButton("Download")).not.toBeDisabled();

        await openDropdown("Download▾");
        for (const label of [
            "Download requests",
            "Download responses",
            "Download requests and responses",
        ]) {
            expect(menuItem(label)).not.toHaveAttribute("disabled");
        }
        await closeDropdown();
    });

    it("is disabled for a mixed HTTP/non-HTTP selection", async () => {
        renderWithSelection([makeHttpFlow("mixed-a"), TTCPFlow()]);
        expect(findButton("Download")).toBeDisabled();

        await openDropdown("Download▾");
        for (const label of [
            "Download requests",
            "Download responses",
            "Download requests and responses",
        ]) {
            expect(menuItem(label)).toHaveAttribute("disabled");
        }
        await closeDropdown();
    });

    it("disables request/response actions only when no selected flow has that body", async () => {
        // Neither flow has a request body, so only responses can be downloaded.
        renderWithSelection([
            makeHttpFlow("resp-a", {
                request: { ...THTTPFlow().request, contentLength: 0 },
            }),
            makeHttpFlow("resp-b", {
                request: { ...THTTPFlow().request, contentLength: 0 },
                response: undefined,
            }),
        ]);

        await openDropdown("Download▾");
        expect(menuItem("Download requests")).toHaveAttribute("disabled");
        expect(menuItem("Download responses")).not.toHaveAttribute("disabled");
        expect(
            menuItem("Download requests and responses"),
        ).not.toHaveAttribute("disabled");
        await closeDropdown();
    });

    it("disables all actions if no selected flow has any body", async () => {
        renderWithSelection([
            makeHttpFlow("empty-a", {
                request: { ...THTTPFlow().request, contentLength: 0 },
                response: undefined,
            }),
            makeHttpFlow("empty-b", {
                request: { ...THTTPFlow().request, contentLength: 0 },
                response: undefined,
            }),
        ]);

        await openDropdown("Download▾");
        for (const label of [
            "Download requests",
            "Download responses",
            "Download requests and responses",
        ]) {
            expect(menuItem(label)).toHaveAttribute("disabled");
        }
        await closeDropdown();
    });
});
