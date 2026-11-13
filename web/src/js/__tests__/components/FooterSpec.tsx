import * as React from "react";
import Footer from "../../components/Footer";
import { STATE_UPDATE } from "../../ducks/backendState";
import { FLOWS_FILTER_UPDATE, FLOWS_RECEIVE, select } from "../../ducks/flows";
import { establishBackend } from "../../ducks/timeRange";
import { FilterName } from "../../ducks/ui/filter";
import { TFlow, TStore } from "../ducks/tutils";
import { render, screen } from "../test-utils";

beforeEach(() => localStorage.clear());
afterEach(() => jest.restoreAllMocks());

test("counts only flows in both the exact search result and global time range", () => {
    jest.spyOn(Date, "now").mockReturnValue(1700002000000);
    const store = TStore(null);
    const flows = [
        1700000000, 1700001900, 1700001901, 1700001950, 1700002001,
    ].map((timestamp_created, index) => ({
        ...TFlow(),
        id: String(index),
        timestamp_created,
    }));
    store.dispatch(FLOWS_RECEIVE(flows));
    store.dispatch(
        FLOWS_FILTER_UPDATE({
            name: FilterName.Search,
            matching_flow_ids: ["0", "1", "3", "4"],
        }),
    );
    store.dispatch(establishBackend("footer-test"));
    store.dispatch(select([flows[0], flows[1], flows[2]]));
    render(<Footer />, { store });

    expect(screen.getByText("1 of 2 flows selected")).toBeInTheDocument();
    expect(store.getState().flows.list).toHaveLength(5);
});

test("retains the original offline search view without a backend instance ID", () => {
    render(<Footer />);
    expect(screen.getByText("1 of 4 flows selected")).toBeInTheDocument();
});

test("does not show the selected live-flow cache as the SQLite history total", () => {
    const store = TStore();
    store.dispatch(STATE_UPDATE({ sqlite_history: true }));
    const flow = store.getState().flows.selected[0];
    store.dispatch(FLOWS_RECEIVE([flow]));
    render(<Footer />, { store });

    expect(screen.queryByText(/flows selected/)).not.toBeInTheDocument();
    expect(screen.getByTitle("Mitmproxy Version")).toBeInTheDocument();
});

test("does not display a flow count when the range has no matching flows", () => {
    const store = TStore();
    store.dispatch(establishBackend("footer-test"));
    render(<Footer />, { store });

    expect(screen.queryByText(/flows selected/)).not.toBeInTheDocument();
});
