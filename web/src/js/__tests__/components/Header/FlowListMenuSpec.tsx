import * as React from "react";
import FlowListMenu from "../../../components/Header/FlowListMenu";
import { act, fireEvent, render, screen } from "../../test-utils";
import { testState } from "../../ducks/tutils";
import { configureStore } from "@reduxjs/toolkit";
import { middlewares, reducer } from "../../../ducks/store";
import { establishBackend } from "../../../ducks/timeRange";

test("grouping is available for ready SQLite history backends", () => {
    localStorage.clear();
    const legacyStore = configureStore({
        reducer,
        preloadedState: testState,
        middleware: (getDefaultMiddleware) => getDefaultMiddleware(middlewares),
    });
    const legacy = render(<FlowListMenu />, { store: legacyStore });
    expect(screen.queryByLabelText("Group flows by")).not.toBeInTheDocument();
    legacy.unmount();

    const store = configureStore({
        reducer,
        preloadedState: {
            ...testState,
            backendState: { ...testState.backendState, sqlite_history: true },
        },
        middleware: (getDefaultMiddleware) => getDefaultMiddleware(middlewares),
    });
    const { unmount } = render(<FlowListMenu />, { store });
    const selector = screen.getByLabelText(
        "Group flows by",
    ) as HTMLSelectElement;
    expect(selector).toBeDisabled();
    expect(screen.getByText("Group by")).toBeInTheDocument();
    expect(selector.parentElement?.previousElementSibling).toHaveClass(
        "time-range-selector",
    );
    expect(selector.parentElement?.nextElementSibling).toHaveClass(
        "filter-input",
    );

    act(() => store.dispatch(establishBackend("group-menu-test")));
    expect(selector).toBeEnabled();
    expect(selector).toHaveValue("none");
    expect(
        Array.from(selector.options, (option) => option.textContent),
    ).toEqual([
        "No grouping",
        "Request host",
        "Request method",
        "Response status",
        "Flow protocol/type",
    ]);

    for (const value of ["host", "method", "status", "type", "none"]) {
        fireEvent.change(selector, { target: { value } });
        expect(store.getState().timeRange.grouping).toBe(value);
    }
    unmount();
});
