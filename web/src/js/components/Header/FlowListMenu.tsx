import * as React from "react";
import FilterInput, { FilterIcon } from "./FilterInput";
import * as flowsActions from "../../ducks/flows";
import Button from "../common/Button";
import { update as updateOptions } from "../../ducks/options";
import { useAppDispatch, useAppSelector } from "../../ducks";
import { FilterName, setFilter, setHighlight } from "../../ducks/ui/filter";
import TimeRangeSelector from "./TimeRangeSelector";
import { setGrouping } from "../../ducks/timeRange";
import type { Grouping } from "../../ducks/timeRange";

FlowListMenu.title = "Flow List";

export default function FlowListMenu() {
    return (
        <div className="main-menu">
            <div className="menu-group">
                <div className="menu-content">
                    <TimeRangeSelector />
                    <GroupingSelector />
                    <FlowFilterInput />
                    <HighlightInput />
                </div>
                <div className="menu-legend">Find</div>
            </div>

            <div className="menu-group">
                <div className="menu-content">
                    <InterceptInput />
                    <ResumeAll />
                </div>
                <div className="menu-legend">Intercept</div>
            </div>
        </div>
    );
}

function GroupingSelector() {
    const dispatch = useAppDispatch();
    const { ready, grouping } = useAppSelector((state) => state.timeRange);
    const enabled = useAppSelector(
        (state) => state.backendState.sqlite_history,
    );
    if (!enabled) return null;

    const options: [Grouping, string][] = [
        ["none", "No grouping"],
        ["host", "Request host"],
        ["method", "Request method"],
        ["status", "Response status"],
        ["type", "Flow protocol/type"],
    ];
    return (
        <label>
            Group by
            <select
                className="input"
                aria-label="Group flows by"
                value={grouping}
                disabled={!ready}
                onChange={(event) =>
                    dispatch(setGrouping(event.target.value as Grouping))
                }
            >
                {options.map(([value, label]) => (
                    <option key={value} value={value}>
                        {label}
                    </option>
                ))}
            </select>
        </label>
    );
}

function InterceptInput() {
    const dispatch = useAppDispatch();
    const value = useAppSelector((state) => state.options.intercept);
    return (
        <FilterInput
            value={value || ""}
            placeholder="Intercept"
            icon={FilterIcon.INTERCEPT}
            color="hsl(208, 56%, 53%)"
            onChange={(val) => dispatch(updateOptions("intercept", val))}
        />
    );
}

function FlowFilterInput() {
    const dispatch = useAppDispatch();
    const value = useAppSelector((state) => state.ui.filter[FilterName.Search]);
    return (
        <FilterInput
            value={value}
            placeholder="Search"
            icon={FilterIcon.SEARCH}
            color="var(--mitmweb-fg)"
            onChange={(expr) => dispatch(setFilter(expr))}
        />
    );
}

function HighlightInput() {
    const dispatch = useAppDispatch();
    const value = useAppSelector(
        (state) => state.ui.filter[FilterName.Highlight],
    );
    return (
        <FilterInput
            value={value}
            placeholder="Highlight"
            icon={FilterIcon.HIGHLIGHT}
            color="hsl(48, 100%, 50%)"
            onChange={(expr) => dispatch(setHighlight(expr))}
        />
    );
}

export function ResumeAll() {
    const dispatch = useAppDispatch();
    return (
        <Button
            className="btn-sm"
            title="[a]ccept all"
            icon="resumeAll"
            iconClassName="text-success"
            onClick={() => dispatch(flowsActions.resumeAll())}
        >
            Resume All
        </Button>
    );
}
