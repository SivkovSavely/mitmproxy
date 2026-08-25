import * as React from "react";
import { fireEvent, render, screen } from "../../test-utils";
import { TStore } from "../../ducks/tutils";
import { setFilter } from "../../../ducks/ui/filter";
import { Request, Response } from "../../../components/FlowView/HttpMessages";

// The message body rendering is out of scope here.
jest.mock("../../../components/contentviews/HttpMessage", () => ({
    __esModule: true,
    default: () => <div>http-message</div>,
}));

describe("HTTP header filter action", () => {
    it("emits an exact request header filter", () => {
        const store = TStore();
        store.dispatch(setFilter(""));
        render(<Request />, { store });
        fireEvent.click(
            screen.getAllByTitle("Filter by exact request header")[0]!,
        );
        expect(store.getState().ui.filter.search).toBe(
            '~hqc "^header: qvalue\\\\r?$"',
        );
        // only the search filter is modified
        expect(store.getState().ui.filter.highlight).toBe("~u /path");
    });

    it("emits an exact response header filter", () => {
        const store = TStore();
        store.dispatch(setFilter(""));
        render(<Response />, { store });
        fireEvent.click(
            screen.getAllByTitle("Filter by exact response header")[1]!,
        );
        expect(store.getState().ui.filter.search).toBe(
            '~hsc "^content-length: 7\\\\r?$"',
        );
    });

    it("parenthesizes an existing or-filter before ANDing", () => {
        const store = TStore();
        store.dispatch(setFilter("~d openrouter | ~d example"));
        render(<Request />, { store });
        fireEvent.click(
            screen.getAllByTitle("Filter by exact request header")[0]!,
        );
        expect(store.getState().ui.filter.search).toBe(
            '(~d openrouter | ~d example) & ~hqc "^header: qvalue\\\\r?$"',
        );
    });

    it("keeps the composed filter parseable when a filter exists", () => {
        const store = TStore(); // default state ships a non-empty search
        render(<Request />, { store });
        fireEvent.click(
            screen.getAllByTitle("Filter by exact request header")[1]!,
        );
        expect(store.getState().ui.filter.search).toBe(
            '(~u /second | ~tcp | ~dns | ~udp) & ~hqc "^content-length: 7\\\\r?$"',
        );
    });
});
