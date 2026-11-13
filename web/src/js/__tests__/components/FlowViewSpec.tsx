import * as React from "react";
import { act, fireEvent, render, screen } from "../test-utils";
import FlowView from "../../components/FlowView";
import * as flowActions from "../../ducks/flows";
import fetchMock, { enableFetchMocks } from "jest-fetch-mock";
import { configureStore } from "@reduxjs/toolkit";
import { middlewares, reducer } from "../../ducks/store";
import history from "../../ducks/history";
import { testState } from "../ducks/tutils";
import { summary } from "../historyFixtures";

enableFetchMocks();

test("FlowView", async () => {
    fetchMock.mockReject(new Error("backend missing"));

    const { asFragment, getByTestId, store } = render(<FlowView />);
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Response"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("WebSocket"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Connection"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Timing"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Comment"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Error"));
    expect(asFragment()).toMatchSnapshot();

    act(() =>
        store.dispatch(flowActions.select([store.getState().flows.list[2]])),
    );

    fireEvent.click(screen.getByText("Stream Data"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Error"));
    expect(asFragment()).toMatchSnapshot();

    act(() =>
        store.dispatch(flowActions.select([store.getState().flows.list[3]])),
    );

    fireEvent.click(screen.getByText("Request"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Response"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Error"));
    expect(asFragment()).toMatchSnapshot();

    act(() =>
        store.dispatch(flowActions.select([store.getState().flows.list[4]])),
    );

    fireEvent.click(screen.getByText("Datagrams"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Error"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(getByTestId("close-button-id"));
    expect(store.getState().flows.selected).toEqual([]);
});

test("closing SQLite live details clears metadata selection and retained full flows", () => {
    const store = configureStore({
        reducer: { ...reducer, history },
        preloadedState: {
            ...testState,
            backendState: { ...testState.backendState, sqlite_history: true },
            history: {
                selected: {
                    ...summary(1, true),
                    id: testState.flows.selected[0].id,
                },
                generation: 1,
            },
        },
        middleware: (defaults) => defaults(middlewares),
    });
    render(<FlowView />, { store });
    fireEvent.click(screen.getByTestId("close-button-id"));
    expect(store.getState().history.selected).toBeNull();
    expect(store.getState().flows.selected).toHaveLength(0);
    expect(store.getState().flows.byId.size).toBe(0);
});
