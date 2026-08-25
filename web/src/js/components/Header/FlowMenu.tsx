import * as React from "react";
import Button from "../common/Button";
import {
    canReplay,
    canResumeOrKill,
    canRevert,
    MessageUtils,
} from "../../flow/utils";
import HideInStatic from "../common/HideInStatic";
import { useAppDispatch, useAppSelector } from "../../ducks";
import {
    duplicate as duplicateFlows,
    kill as killFlows,
    remove as removeFlows,
    replay as replayFlows,
    resume as resumeFlows,
    revert as revertFlows,
    mark as markFlows,
} from "../../ducks/flows";
import Dropdown, { MenuItem } from "../common/Dropdown";
import { copy } from "../../flow/export";
import { fetchApi } from "../../utils";
import type { Flow } from "../../flow";

import type { JSX } from "react";

FlowMenu.title = "Flow";

export default function FlowMenu(): JSX.Element {
    const dispatch = useAppDispatch();

    const selectedFlows = useAppSelector((state) => state.flows.selected);
    // ctrl-/shift-click selection order is not guaranteed to match the visible
    // order, so derive the ordered selection from the current view.
    const view = useAppSelector((state) => state.flows.view);
    const selectedIds = useAppSelector((state) => state.flows.selectedIds);
    const orderedFlows = React.useMemo(
        () => view.filter((f) => selectedIds.has(f.id)),
        [view, selectedIds],
    );

    const canResumeOrKillAny = selectedFlows.some(canResumeOrKill);

    if (selectedFlows.length === 0) return <div />;
    return (
        <div className="flow-menu">
            <HideInStatic>
                <div className="menu-group">
                    <div className="menu-content">
                        <Button
                            title="[r]eplay flow"
                            icon="replay"
                            iconClassName="text-primary"
                            onClick={() => dispatch(replayFlows(selectedFlows))}
                            disabled={!selectedFlows.some(canReplay)}
                        >
                            Replay
                        </Button>
                        <Button
                            title="[D]uplicate flow"
                            icon="duplicate"
                            iconClassName="text-info"
                            onClick={() =>
                                dispatch(duplicateFlows(selectedFlows))
                            }
                        >
                            Duplicate
                        </Button>
                        <Button
                            disabled={!selectedFlows.some(canRevert)}
                            title="revert changes to flow [V]"
                            icon="revert"
                            iconClassName="text-warning"
                            onClick={() => dispatch(revertFlows(selectedFlows))}
                        >
                            Revert
                        </Button>
                        <Button
                            title="[d]elete flow"
                            icon="delete"
                            iconClassName="text-danger"
                            onClick={() => {
                                dispatch(removeFlows(selectedFlows));
                            }}
                        >
                            Delete
                        </Button>

                        <MarkButton flows={selectedFlows} />
                    </div>
                    <div className="menu-legend">Flow Modification</div>
                </div>
            </HideInStatic>

            <div className="menu-group">
                <div className="menu-content">
                    <DownloadButton flows={orderedFlows} />
                    <ExportButton flows={orderedFlows} />
                </div>
                <div className="menu-legend">Export</div>
            </div>

            <HideInStatic>
                <div className="menu-group">
                    <div className="menu-content">
                        <Button
                            disabled={!canResumeOrKillAny}
                            title="[a]ccept intercepted flow"
                            icon="resume"
                            iconClassName="text-success"
                            onClick={() => dispatch(resumeFlows(selectedFlows))}
                        >
                            Resume
                        </Button>
                        <Button
                            disabled={!canResumeOrKillAny}
                            title="kill intercepted flow [x]"
                            icon="abort"
                            iconClassName="text-danger"
                            onClick={() => dispatch(killFlows(selectedFlows))}
                        >
                            Abort
                        </Button>
                    </div>
                    <div className="menu-legend">Interception</div>
                </div>
            </HideInStatic>
        </div>
    );
}

// Reference: https://stackoverflow.com/a/63627688/9921431
const openInNewTab = (url: string) => {
    const newWindow = window.open(url, "_blank", "noopener,noreferrer");
    if (newWindow) newWindow.opener = null;
};

async function downloadBodies(
    flows: Flow[],
    parts: ("request" | "response")[],
): Promise<void> {
    try {
        const response = await fetchApi.post("/flows/download", {
            flow_ids: flows.map((f) => f.id),
            parts,
        });
        if (!response.ok) {
            throw new Error(await response.text());
        }
        const url = URL.createObjectURL(await response.blob());
        const link = document.createElement("a");
        link.href = url;
        link.download = "mitmweb-bodies.zip";
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
    } catch (err) {
        alert(err);
    }
}

function MultiDownloadDropdown({ flows }: { flows: Flow[] }) {
    const allHttp = flows.every((f) => f.type === "http");
    const hasRequests = flows.some(
        (f) => f.type === "http" && !!f.request.contentLength,
    );
    const hasResponses = flows.some(
        (f) => f.type === "http" && !!f.response?.contentLength,
    );

    return (
        <Dropdown
            text={
                <Button icon="download" onClick={() => 1} disabled={!allHttp}>
                    Download▾
                </Button>
            }
            options={{ placement: "bottom-start" }}
        >
            <MenuItem
                disabled={!allHttp || !hasRequests}
                onClick={() => downloadBodies(flows, ["request"])}
            >
                Download requests
            </MenuItem>
            <MenuItem
                disabled={!allHttp || !hasResponses}
                onClick={() => downloadBodies(flows, ["response"])}
            >
                Download responses
            </MenuItem>
            <MenuItem
                disabled={!allHttp || (!hasRequests && !hasResponses)}
                onClick={() => downloadBodies(flows, ["request", "response"])}
            >
                Download requests and responses
            </MenuItem>
        </Dropdown>
    );
}

function DownloadButton({ flows }: { flows: Flow[] }) {
    if (flows.length > 1) return <MultiDownloadDropdown flows={flows} />;

    const flow = flows[0];
    if (!flow || flow.type !== "http")
        return (
            <Button icon="download" onClick={() => 0} disabled>
                Download
            </Button>
        );

    if (flow.request.contentLength && !flow.response?.contentLength) {
        return (
            <Button
                icon="download"
                onClick={() =>
                    openInNewTab(MessageUtils.getContentURL(flow, flow.request))
                }
            >
                Download
            </Button>
        );
    }
    if (flow.response) {
        const response = flow.response;
        if (!flow.request.contentLength && flow.response.contentLength) {
            return (
                <Button
                    icon="download"
                    onClick={() =>
                        openInNewTab(MessageUtils.getContentURL(flow, response))
                    }
                >
                    Download
                </Button>
            );
        }
        if (flow.request.contentLength && flow.response.contentLength) {
            return (
                <Dropdown
                    text={
                        <Button icon="download" onClick={() => 1}>
                            Download▾
                        </Button>
                    }
                    options={{ placement: "bottom-start" }}
                >
                    <MenuItem
                        onClick={() =>
                            openInNewTab(
                                MessageUtils.getContentURL(flow, flow.request),
                            )
                        }
                    >
                        Download request
                    </MenuItem>
                    <MenuItem
                        onClick={() =>
                            openInNewTab(
                                MessageUtils.getContentURL(flow, response),
                            )
                        }
                    >
                        Download response
                    </MenuItem>
                </Dropdown>
            );
        }
    }

    return null;
}

function ExportButton({ flows }: { flows: Flow[] }) {
    const enabled = flows.length > 0 && flows.every((f) => f.type === "http");
    return (
        <Dropdown
            className=""
            text={
                <Button
                    title="Export flow."
                    icon="export"
                    onClick={() => 1}
                    disabled={!enabled}
                >
                    Export▾
                </Button>
            }
            options={{ placement: "bottom-start" }}
        >
            <MenuItem onClick={() => copy(flows, "raw_request")}>
                Copy raw request
            </MenuItem>
            <MenuItem onClick={() => copy(flows, "raw_response")}>
                Copy raw response
            </MenuItem>
            <MenuItem onClick={() => copy(flows, "raw")}>
                Copy raw request and response
            </MenuItem>
            <MenuItem onClick={() => copy(flows, "curl")}>
                Copy as cURL
            </MenuItem>
            <MenuItem onClick={() => copy(flows, "httpie")}>
                Copy as HTTPie
            </MenuItem>
        </Dropdown>
    );
}

const markers = {
    ":red_circle:": "🔴",
    ":orange_circle:": "🟠",
    ":yellow_circle:": "🟡",
    ":green_circle:": "🟢",
    ":large_blue_circle:": "🔵",
    ":purple_circle:": "🟣",
    ":brown_circle:": "🟤",
};

function MarkButton({ flows }: { flows: Flow[] }) {
    const dispatch = useAppDispatch();
    return (
        <Dropdown
            className=""
            text={
                <Button
                    title="mark flow"
                    icon="mark"
                    iconClassName="text-success"
                    onClick={() => 1}
                >
                    Mark▾
                </Button>
            }
            options={{ placement: "bottom-start" }}
        >
            <MenuItem onClick={() => dispatch(markFlows(flows, ""))}>
                ⚪ (no marker)
            </MenuItem>
            {Object.entries(markers).map(([name, sym]) => (
                <MenuItem
                    key={name}
                    onClick={() => dispatch(markFlows(flows, name))}
                >
                    {sym} {name.replace(/[:_]/g, " ")}
                </MenuItem>
            ))}
        </Dropdown>
    );
}
