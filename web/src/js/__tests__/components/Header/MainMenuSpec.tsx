import * as React from "react";
import FlowListMenu from "../../../components/Header/FlowListMenu";
import { render } from "../../test-utils";
import { configureStore } from "@reduxjs/toolkit";
import { reducer, middlewares } from "../../../ducks/store";
import timeRange, { defaultState } from "../../../ducks/timeRange";
import history from "../../../ducks/history";
import { testState } from "../../ducks/tutils";

test("MainMenu", () => {
    const store = configureStore({
        reducer: { ...reducer, timeRange, history },
        preloadedState: {
            ...testState,
            timeRange: defaultState,
            history: { selected: null, generation: 0 },
        },
        middleware: (getDefaultMiddleware) => getDefaultMiddleware(middlewares),
    });
    const { asFragment } = render(<FlowListMenu />, { store });
    expect(asFragment()).toMatchSnapshot();
});
