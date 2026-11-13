import { selectTab } from "./flow";
import * as flowsActions from "../flows";
import * as modalActions from "./modal";
import { tabsForFlow } from "../../components/FlowView";
import { runCommand } from "../../utils";
import type { AppDispatch, RootState } from "../store";
import { selectTimeRangeFlows } from "../timeRange";
import { buildIndex } from "../flows/_utils";
import { clearSelectedSummary } from "../history";

export function onKeyDown(e: KeyboardEvent) {
    const target = e.target instanceof HTMLElement ? e.target : null;
    if (
        e.ctrlKey ||
        e.metaKey ||
        e.defaultPrevented ||
        target?.closest(
            'input, select, textarea, [contenteditable]:not([contenteditable="false"])',
        ) ||
        (["Enter", " ", "Tab"].includes(e.key) &&
            target?.closest("button, a[href], summary"))
    ) {
        return () => {};
    }
    const key = e.key;
    e.preventDefault();
    return (dispatch: AppDispatch, getState: () => RootState) => {
        const state = getState();
        const { flows } = state;
        const selectedFlows = flows.selected;
        const flow = selectedFlows[0];
        const navigate = (shift: number) => {
            if (state.backendState.sqlite_history) return;
            const view = selectTimeRangeFlows(state);
            dispatch(
                flowsActions.selectRelative(
                    { ...flows, view, _viewIndex: buildIndex(view) },
                    shift,
                ),
            );
        };

        switch (key) {
            case "k":
            case "ArrowUp":
                navigate(-1);
                break;

            case "j":
            case "ArrowDown":
                navigate(+1);
                break;

            case " ":
            case "PageDown":
                navigate(+10);
                break;

            case "PageUp":
                navigate(-10);
                break;

            case "End":
                navigate(+1e10);
                break;

            case "Home":
                navigate(-1e10);
                break;

            case "Escape":
                if (getState().ui.modal.activeModal) {
                    dispatch(modalActions.hideModal());
                } else if (state.backendState.sqlite_history) {
                    dispatch(clearSelectedSummary());
                } else {
                    dispatch(flowsActions.select([]));
                }
                break;

            case "ArrowLeft": {
                if (!flow) break;
                const tabs = tabsForFlow(flow);
                const currentTab = getState().ui.flow.tab;
                const currentTabIndex = tabs.findIndex(
                    (tab) => tab === currentTab,
                );
                const nextTab =
                    tabs[
                        (Math.max(0, currentTabIndex) - 1 + tabs.length) %
                            tabs.length
                    ];
                dispatch(selectTab(nextTab));
                break;
            }

            case "Tab":
            case "ArrowRight": {
                if (!flow) break;
                const tabs = tabsForFlow(flow);
                const currentTab = getState().ui.flow.tab;
                const currentTabIndex = tabs.findIndex(
                    (tab) => tab === currentTab,
                );
                const nextTab =
                    tabs[(Math.max(0, currentTabIndex) + 1) % tabs.length];
                dispatch(selectTab(nextTab));
                break;
            }

            case "Delete":
            case "d": {
                dispatch(flowsActions.remove(selectedFlows));
                break;
            }

            case "n": {
                runCommand("view.flows.create", "get", "https://example.com/");
                break;
            }

            case "D": {
                dispatch(flowsActions.duplicate(selectedFlows));
                break;
            }
            case "a": {
                dispatch(flowsActions.resume(selectedFlows));
                break;
            }
            case "A": {
                dispatch(flowsActions.resumeAll());
                break;
            }

            case "r": {
                dispatch(flowsActions.replay(selectedFlows));
                break;
            }

            case "v": {
                dispatch(flowsActions.revert(selectedFlows));
                break;
            }

            case "x": {
                dispatch(flowsActions.kill(selectedFlows));
                break;
            }

            case "X": {
                dispatch(flowsActions.killAll());
                break;
            }

            case "z": {
                dispatch(flowsActions.clear());
                break;
            }

            default:
                return;
        }
    };
}
