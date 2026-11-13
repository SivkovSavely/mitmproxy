import * as React from "react";
import Splitter from "./common/Splitter";
import FlowTable from "./FlowTable";
import FlowView from "./FlowView";
import HistoryTable, { HistorySummary } from "./History/HistoryTable";
import ScriptsView from "./Scripts/ScriptsView";
import { useAppDispatch, useAppSelector } from "../ducks";
import CaptureSetup from "./Modes/CaptureSetup";
import Modes from "./Modes";
import { Tab } from "../ducks/ui/tabs";
import { clearSelectedSummary } from "../ducks/history";

export default function MainView() {
    const dispatch = useAppDispatch();
    const hasOneFlowSelected = useAppSelector(
        (state) => state.flows.selected.length === 1,
    );
    const hasFlows = useAppSelector((state) => state.flows.list.length > 0);
    const currentTab = useAppSelector((state) => state.ui.tabs.current);
    const sqliteHistory = useAppSelector(
        (state) => state.backendState.sqlite_history,
    );
    const historicalSelection = useAppSelector(
        (state) => state.history.selected,
    );
    const selectedLiveId = useAppSelector(
        (state) => state.flows.selected[0]?.id,
    );
    const showLiveDetails =
        hasOneFlowSelected &&
        (!sqliteHistory ||
            (historicalSelection?.live &&
                historicalSelection.id === selectedLiveId));

    let content;
    if (currentTab === Tab.Capture) {
        content = <Modes />;
    } else if (currentTab === Tab.Scripts) {
        content = <ScriptsView />;
    } else {
        content = (
            <>
                {sqliteHistory ? (
                    <HistoryTable />
                ) : hasFlows ? (
                    <FlowTable />
                ) : (
                    <CaptureSetup />
                )}
                {showLiveDetails && (
                    <>
                        <Splitter key="splitter" />
                        <FlowView key="flowDetails" />
                    </>
                )}
                {sqliteHistory && historicalSelection && !showLiveDetails && (
                    <>
                        <Splitter key="historySplitter" />
                        <HistorySummary
                            row={historicalSelection}
                            onClose={() => dispatch(clearSelectedSummary())}
                        />
                    </>
                )}
            </>
        );
    }

    return <div className="main-view">{content}</div>;
}
