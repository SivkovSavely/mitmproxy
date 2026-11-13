import * as React from "react";
import { connect } from "react-redux";
import type { TimeRangeState } from "../../ducks/timeRange";
import { historyQuery } from "../../ducks/timeRange";
import type { HistoricalSummary, HistoryPage } from "../../history";
import {
    HistoryCache,
    historyWindow,
    logicalToPhysical,
    PAGE_SIZE,
    physicalToLogical,
    ROW_HEIGHT,
    scrollHeight,
} from "../../history";
import { fetchApi, formatSize, formatTimeStamp } from "../../utils";
import type { RootState } from "../../ducks";
import { clearSelectedSummary, selectSummary } from "../../ducks/history";
import Icon from "../common/Icon";

type HistoryTableProps = {
    timeRange: TimeRangeState;
    search: string;
    highlight: string;
    columns: string[];
    selected: HistoricalSummary | null;
    onSelect: (row: HistoricalSummary) => void;
    onClear: () => void;
};

type HistoryTableState = {
    count: number | null;
    top: number;
    height: number;
    loading: boolean;
    error: string | null;
};

type Anchor = { id: string; index: number; delta: number };
type LoadOptions = {
    replace?: boolean;
    bottom?: boolean;
    anchor?: Anchor;
    prefetch?: boolean;
};

const labels: Record<string, string> = {
    index: "#",
    icon: "Type",
    type: "Type",
    method: "Method",
    path: "Host / Path",
    host: "Host",
    status: "Status",
    size: "Size",
    timestamp: "Start time",
    version: "Version",
    time: "Time",
    tls: "TLS",
    comment: "Comment",
};

const widths: Record<string, string> = {
    index: "180px",
    icon: "60px",
    type: "60px",
    method: "70px",
    path: "minmax(180px, 1fr)",
    host: "180px",
    status: "70px",
    size: "80px",
    timestamp: "180px",
    tls: "40px",
};

function cell(row: HistoricalSummary, column: string): string {
    switch (column) {
        case "index":
            return row.sequence;
        case "icon":
        case "type":
            return row.type;
        case "method":
            return row.method ?? "-";
        case "path":
            return `${row.host ?? ""}${row.path ?? ""}` || "-";
        case "host":
            return row.host ?? "-";
        case "status":
            return row.status_code === null ? "-" : String(row.status_code);
        case "size":
            return row.request_size === null && row.response_size === null
                ? "-"
                : formatSize(
                      (row.request_size ?? 0) + (row.response_size ?? 0),
                  );
        case "timestamp":
            return formatTimeStamp(row.timestamp_created);
        default:
            return "-";
    }
}

function queryKey(props: HistoryTableProps) {
    return JSON.stringify([
        props.timeRange.instanceId,
        props.timeRange.preference,
        props.search,
        props.highlight,
    ]);
}

export class PureHistoryTable extends React.Component<
    HistoryTableProps,
    HistoryTableState
> {
    state: HistoryTableState = {
        count: null,
        top: 0,
        height: 320,
        loading: false,
        error: null,
    };
    private viewport = React.createRef<HTMLDivElement>();
    private resizeObserver?: ResizeObserver;
    private cache = new HistoryCache();
    private epoch = 0;
    private controller?: AbortController;
    private debounce?: ReturnType<typeof setTimeout>;
    private prefetch?: ReturnType<typeof setTimeout>;
    private refreshTimer?: ReturnType<typeof setTimeout>;
    private scrolling = false;
    private refreshPending = false;

    componentDidMount() {
        window.addEventListener("resize", this.onResize);
        this.viewport.current!.addEventListener("wheel", this.onWheel, {
            passive: false,
        });
        if (typeof ResizeObserver !== "undefined") {
            this.resizeObserver = new ResizeObserver(this.onResize);
            this.resizeObserver.observe(this.viewport.current!);
        }
        this.onResize();
        if (this.props.timeRange.ready)
            void this.load({}, { replace: true, bottom: true });
    }

    componentDidUpdate(previous: HistoryTableProps) {
        if (queryKey(previous) !== queryKey(this.props)) {
            this.cancel();
            this.scrolling = false;
            this.refreshPending = false;
            this.cache.clear();
            this.props.onClear();
            this.setState(
                { count: null, top: 0, error: null, loading: false },
                () => {
                    this.syncPhysical();
                    if (this.props.timeRange.ready)
                        void this.load({}, { replace: true, bottom: true });
                },
            );
        } else if (previous.timeRange.ready && !this.props.timeRange.ready) {
            this.cancel();
            this.scrolling = false;
            this.refreshPending = false;
            this.setState({ loading: false });
        } else if (
            (!previous.timeRange.ready && this.props.timeRange.ready) ||
            previous.timeRange.historyRevision !==
                this.props.timeRange.historyRevision ||
            previous.timeRange.revision !== this.props.timeRange.revision
        ) {
            this.requestRefresh();
        }
    }

    componentWillUnmount() {
        this.cancel();
        window.removeEventListener("resize", this.onResize);
        this.viewport.current?.removeEventListener("wheel", this.onWheel);
        this.resizeObserver?.disconnect();
    }

    private cancel() {
        this.epoch++;
        this.controller?.abort();
        this.controller = undefined;
        clearTimeout(this.debounce);
        clearTimeout(this.prefetch);
        clearTimeout(this.refreshTimer);
        this.refreshTimer = undefined;
    }

    private atBottom() {
        return (
            this.state.count === null ||
            this.state.top >=
                this.state.count * ROW_HEIGHT - this.state.height - 1
        );
    }

    private anchor(): Anchor | undefined {
        const index = Math.floor(this.state.top / ROW_HEIGHT);
        const row = this.cache.rows.get(index);
        return row
            ? { id: row.id, index, delta: this.state.top - index * ROW_HEIGHT }
            : undefined;
    }

    private syncPhysical() {
        if (this.viewport.current)
            this.viewport.current.scrollTop = logicalToPhysical(
                this.state.top,
                this.state.count ?? 0,
                this.state.height,
            );
    }

    private onResize = () => {
        const height = this.viewport.current?.clientHeight || 320;
        if (height === this.state.height) return;
        const bottom = this.atBottom();
        this.setState({ height }, () =>
            this.moveTo(bottom ? Infinity : this.state.top),
        );
    };

    private onScroll = () => {
        const physical = this.viewport.current!.scrollTop;
        const expected = logicalToPhysical(
            this.state.top,
            this.state.count ?? 0,
            this.state.height,
        );
        if (Math.abs(physical - expected) < 0.6) return;
        this.moveTo(
            physicalToLogical(
                physical,
                this.state.count ?? 0,
                this.state.height,
            ),
        );
    };

    private onWheel = (event: WheelEvent) => {
        if (!event.deltaY || event.ctrlKey || event.shiftKey) return;
        event.preventDefault();
        const scale =
            event.deltaMode === 1
                ? ROW_HEIGHT
                : event.deltaMode === 2
                  ? this.state.height
                  : 1;
        this.moveTo(this.state.top + event.deltaY * scale);
    };

    private onKeyDown = (event: React.KeyboardEvent) => {
        let top: number;
        switch (event.key) {
            case "ArrowDown":
                top = this.state.top + ROW_HEIGHT;
                break;
            case "ArrowUp":
                top = this.state.top - ROW_HEIGHT;
                break;
            case "PageDown":
                top = this.state.top + this.state.height;
                break;
            case "PageUp":
                top = this.state.top - this.state.height;
                break;
            case "Home":
                top = 0;
                break;
            case "End":
                top = Infinity;
                break;
            default:
                return;
        }
        event.preventDefault();
        this.moveTo(top);
    };

    private moveTo(top: number) {
        if (this.state.count === null) return;
        const maximum = Math.max(
            0,
            (this.state.count ?? 0) * ROW_HEIGHT - this.state.height,
        );
        top = Math.max(0, Math.min(top, maximum));
        this.cancel();
        this.scrolling = true;
        this.setState({ top, loading: false }, () => {
            this.syncPhysical();
            this.debounce = setTimeout(() => {
                this.scrolling = false;
                this.loadVisible();
                if (this.refreshPending) this.requestRefresh();
            }, 200);
        });
    }

    private loadVisible() {
        const { start, end } = historyWindow(
            this.state.top,
            this.state.height,
            this.state.count ?? 0,
        );
        for (let index = start; index < end; index++) {
            if (!this.cache.rows.has(index)) {
                void this.load({ offset: String(start) });
                return;
            }
        }
        this.schedulePrefetch();
    }

    private refreshVisible = () => {
        if (!this.props.timeRange.ready) return;
        if (this.atBottom()) {
            void this.load({}, { replace: true, bottom: true });
        } else {
            const anchor = this.anchor();
            const offset = Math.max(
                0,
                Math.floor(this.state.top / ROW_HEIGHT) - 5,
            );
            void this.load(
                { offset: String(offset) },
                { replace: true, anchor },
            );
        }
    };

    private requestRefresh() {
        if (!this.props.timeRange.ready) return;
        this.refreshPending = true;
        clearTimeout(this.prefetch);
        if (this.scrolling || this.controller || this.refreshTimer) return;
        // Do not postpone this fixed window when more live updates arrive.
        this.refreshTimer = setTimeout(() => {
            this.refreshTimer = undefined;
            if (
                !this.props.timeRange.ready ||
                this.scrolling ||
                this.controller
            )
                return;
            this.refreshPending = false;
            this.refreshVisible();
        }, 1000);
    }

    private async load(
        extra: Record<string, string>,
        options: LoadOptions = {},
    ) {
        if (!this.props.timeRange.ready) return;
        this.cancel();
        const epoch = this.epoch;
        const instance = this.props.timeRange.instanceId;
        const controller = new AbortController();
        this.controller = controller;
        const query = historyQuery(this.props.timeRange);
        query.set("limit", String(PAGE_SIZE));
        query.set("search", this.props.search);
        query.set("highlight", this.props.highlight);
        for (const [key, value] of Object.entries(extra)) query.set(key, value);
        if (options.anchor) query.set("anchor", options.anchor.id);
        const selectedId = this.props.selected?.id;
        if (selectedId) query.set("selected", selectedId);
        this.setState({ loading: !options.prefetch, error: null });
        const current = () =>
            this.epoch === epoch &&
            !controller.signal.aborted &&
            this.props.timeRange.ready &&
            this.props.timeRange.instanceId === instance;
        try {
            const response = await fetchApi(`/flow-history?${query}`, {
                signal: controller.signal,
            });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const result: HistoryPage = await response.json();
            if (!current()) return;
            if (result.instance_id !== instance)
                throw new Error("Backend instance changed");
            if (
                !Number.isSafeInteger(result.count) ||
                result.count < 0 ||
                !Number.isSafeInteger(result.offset) ||
                result.offset < 0 ||
                result.rows.length > PAGE_SIZE
            )
                throw new Error("Invalid history page");
            if (
                result.selected_eligible === false &&
                selectedId &&
                this.props.selected?.id === selectedId
            )
                this.props.onClear();

            let top = this.state.top;
            const anchorMoved =
                options.anchor &&
                result.anchor_position !== null &&
                result.anchor_position !== options.anchor.index;
            if (
                options.replace ||
                anchorMoved ||
                result.count !== this.state.count
            )
                this.cache.clear();
            if (options.bottom)
                top = Math.max(
                    0,
                    result.count * ROW_HEIGHT - this.state.height,
                );
            else if (options.anchor && result.anchor_position !== null)
                top =
                    result.anchor_position * ROW_HEIGHT + options.anchor.delta;
            top = Math.max(
                0,
                Math.min(
                    top,
                    Math.max(0, result.count * ROW_HEIGHT - this.state.height),
                ),
            );
            this.cache.add(result, Math.floor(top / ROW_HEIGHT));
            this.controller = undefined;
            this.setState({ count: result.count, top, loading: false }, () => {
                this.syncPhysical();
                this.loadVisible();
                if (this.refreshPending) this.requestRefresh();
            });
        } catch (error) {
            if (current()) {
                this.controller = undefined;
                this.setState(
                    {
                        loading: false,
                        error:
                            error instanceof Error
                                ? error.message
                                : "Request failed",
                    },
                    () => {
                        if (this.refreshPending) this.requestRefresh();
                    },
                );
            }
        }
    }

    private schedulePrefetch() {
        clearTimeout(this.prefetch);
        if (this.refreshPending) return;
        const index = Math.floor(this.state.top / ROW_HEIGHT);
        const cached = this.cache.pages.find(
            (page) =>
                page.offset <= index && page.offset + page.rows.length > index,
        );
        if (!cached?.rows.length) return;
        let extra: Record<string, string> | undefined;
        if (
            index - cached.offset < 100 &&
            cached.offset > 0 &&
            !this.cache.rows.has(cached.offset - 1)
        ) {
            extra = { before: JSON.stringify(cached.rows[0].cursor) };
        } else if (
            cached.offset + cached.rows.length - index < 100 &&
            cached.offset + cached.rows.length < (this.state.count ?? 0) &&
            !this.cache.rows.has(cached.offset + cached.rows.length)
        ) {
            extra = {
                after: JSON.stringify(
                    cached.rows[cached.rows.length - 1].cursor,
                ),
            };
        }
        if (extra)
            this.prefetch = setTimeout(
                () =>
                    void this.load(extra!, {
                        prefetch: true,
                        anchor: this.anchor(),
                    }),
                800,
            );
    }

    render() {
        const { count, top, height, loading, error } = this.state;
        const { timeRange, search, highlight, selected, onSelect } = this.props;
        const columns = this.props.columns.filter(
            (column) => column !== "quickactions",
        );
        if (!columns.length)
            columns.push("method", "path", "status", "size", "timestamp");
        const gridTemplateColumns = columns
            .map((column) => widths[column] ?? "90px")
            .join(" ");
        const minWidth = columns.reduce(
            (width, column) =>
                width +
                (column === "path"
                    ? 180
                    : parseInt(widths[column] ?? "90", 10)),
            16,
        );
        const { start, end } = historyWindow(top, height, count ?? 0);
        const rows: React.ReactNode[] = [];
        for (let index = start; index < end; index++) {
            const row = this.cache.rows.get(index);
            rows.push(
                <div
                    role="row"
                    aria-rowindex={index + 2}
                    aria-selected={row ? selected?.id === row.id : undefined}
                    aria-label={row ? undefined : `Loading row ${index + 1}`}
                    data-history-index={index}
                    className={`history-row${row?.highlighted && row.live ? " highlighted" : ""}${row && selected?.id === row.id ? " selected" : ""}${!row ? " placeholder" : ""}`}
                    key={index}
                    style={{ gridTemplateColumns, height: ROW_HEIGHT }}
                    tabIndex={row && timeRange.ready ? 0 : undefined}
                    onClick={
                        row && timeRange.ready ? () => onSelect(row) : undefined
                    }
                    onKeyDown={
                        row && timeRange.ready
                            ? (event) => {
                                  if (
                                      event.key === "Enter" ||
                                      event.key === " "
                                  ) {
                                      event.preventDefault();
                                      onSelect(row);
                                  }
                              }
                            : undefined
                    }
                    title={
                        row
                            ? row.live
                                ? "Live flow"
                                : "Historical metadata (read-only)"
                            : undefined
                    }
                >
                    {columns.map((column, i) => (
                        <div
                            role="cell"
                            className={`col-${column}`}
                            key={`${column}-${i}`}
                            title={row ? cell(row, column) : undefined}
                        >
                            {row
                                ? cell(row, column)
                                : column === "path"
                                  ? "Loading..."
                                  : "-"}
                        </div>
                    ))}
                </div>,
            );
        }
        return (
            <section className="history-list" aria-label="Flow history">
                <div className="history-notice">
                    <span>
                        {!timeRange.ready
                            ? "Waiting for backend..."
                            : count === null
                              ? "Loading history..."
                              : `${count.toLocaleString()} flows`}
                    </span>
                    <span>
                        Chronological order. Column sorting is unavailable.
                    </span>
                    {search && (
                        <span>
                            Search shows LIVE matching flows only. Historical
                            searching is unsupported.
                        </span>
                    )}
                    {highlight && (
                        <span>
                            Historical highlighting is unavailable; only live
                            matches are highlighted.
                        </span>
                    )}
                    {loading && count !== null && <span>Loading...</span>}
                    {error && (
                        <span role="alert">
                            History could not be loaded: {error}.{" "}
                            <button onClick={this.refreshVisible}>Retry</button>
                        </span>
                    )}
                </div>
                <div
                    role="table"
                    aria-label="Flow history rows"
                    aria-rowcount={count === null ? undefined : count + 1}
                    className="history-table"
                >
                    <div
                        role="row"
                        className="history-head"
                        style={{ gridTemplateColumns, minWidth }}
                    >
                        {columns.map((column, index) => (
                            <div role="columnheader" key={`${column}-${index}`}>
                                {labels[column] ?? column}
                            </div>
                        ))}
                    </div>
                    <div
                        ref={this.viewport}
                        className="history-viewport"
                        style={{ minWidth }}
                        aria-label="History scroll area"
                        tabIndex={0}
                        onScroll={this.onScroll}
                        onKeyDown={this.onKeyDown}
                    >
                        <div
                            className="history-scroll-space"
                            style={{ height: scrollHeight(count ?? 0, height) }}
                        >
                            <div
                                className="history-rows"
                                style={{
                                    top:
                                        logicalToPhysical(
                                            top,
                                            count ?? 0,
                                            height,
                                        ) +
                                        start * ROW_HEIGHT -
                                        top,
                                }}
                            >
                                {rows}
                            </div>
                        </div>
                    </div>
                </div>
            </section>
        );
    }
}

export function HistorySummary({
    row,
    onClose,
}: {
    row: HistoricalSummary;
    onClose: () => void;
}) {
    return (
        <aside className="history-summary" aria-label="Selected flow metadata">
            <button
                className="btn btn-default btn-xs history-close"
                aria-label="Close flow details"
                onClick={onClose}
            >
                <Icon name="closeCircle" />
            </button>
            <h3>
                {row.live ? "Live Flow Metadata" : "Historical Flow Metadata"}
            </h3>
            <p>
                {row.live
                    ? "Live details are not loaded. Select the row again to retry."
                    : "Read-only summary. Historical payloads and flow actions are not loaded."}
            </p>
            <dl>
                <dt>ID</dt>
                <dd>{row.id}</dd>
                <dt>Sequence</dt>
                <dd>{row.sequence}</dd>
                <dt>Start time</dt>
                <dd>{formatTimeStamp(row.timestamp_created)}</dd>
                <dt>Type</dt>
                <dd>{row.type}</dd>
                <dt>Method</dt>
                <dd>{cell(row, "method")}</dd>
                <dt>Host / Path</dt>
                <dd>{cell(row, "path")}</dd>
                <dt>Status</dt>
                <dd>{cell(row, "status")}</dd>
                <dt>Request size</dt>
                <dd>
                    {row.request_size === null
                        ? "-"
                        : formatSize(row.request_size)}
                </dd>
                <dt>Response size</dt>
                <dd>
                    {row.response_size === null
                        ? "-"
                        : formatSize(row.response_size)}
                </dd>
            </dl>
        </aside>
    );
}

export default connect(
    (state: RootState) => ({
        timeRange: state.timeRange,
        search: state.ui.filter.search,
        highlight: state.ui.filter.highlight,
        columns: state.options.web_columns,
        selected: state.history.selected,
    }),
    { onSelect: selectSummary, onClear: clearSelectedSummary },
)(PureHistoryTable);
