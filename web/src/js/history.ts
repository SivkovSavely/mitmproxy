export type HistoryCursor = [number, string, string];

export interface HistoricalSummary {
    id: string;
    sequence: string;
    timestamp_created: number;
    type: string;
    method: string | null;
    host: string | null;
    path: string | null;
    status_code: number | null;
    request_size: number | null;
    response_size: number | null;
    payload_version: number | null;
    live: boolean;
    highlighted: boolean;
    cursor: HistoryCursor;
}

export interface HistoryPage {
    instance_id: string;
    count: number;
    offset: number;
    rows: HistoricalSummary[];
    anchor_position: number | null;
    selected_eligible: boolean | null;
    lower: number | null;
    upper: number | null;
    historical_search_unsupported: boolean;
    historical_highlight_unsupported: boolean;
}

export const PAGE_SIZE = 500;
export const ROW_HEIGHT = 32;

// The thumb spans the full count; the rendered window is rebased around logical pixels.
export function scrollHeight(count: number, viewportHeight: number) {
    return Math.max(viewportHeight, Math.min(1000000, count * ROW_HEIGHT));
}

export function logicalToPhysical(top: number, count: number, height: number) {
    const logicalMax = Math.max(0, count * ROW_HEIGHT - height);
    const physicalMax = scrollHeight(count, height) - height;
    return logicalMax
        ? (Math.max(0, Math.min(top, logicalMax)) / logicalMax) * physicalMax
        : 0;
}

export function physicalToLogical(top: number, count: number, height: number) {
    const physicalMax = scrollHeight(count, height) - height;
    const logicalMax = Math.max(0, count * ROW_HEIGHT - height);
    return physicalMax
        ? (Math.max(0, Math.min(top, physicalMax)) / physicalMax) * logicalMax
        : 0;
}

export function historyWindow(top: number, height: number, count: number) {
    top = Math.max(0, Math.min(top, Math.max(0, count * ROW_HEIGHT - height)));
    return {
        start: Math.max(0, Math.floor(top / ROW_HEIGHT) - 5),
        end: Math.min(count, Math.ceil((top + height) / ROW_HEIGHT) + 5),
    };
}

export class HistoryCache {
    pages: HistoryPage[] = [];
    rows = new Map<number, HistoricalSummary>();

    add(page: HistoryPage, anchor: number) {
        this.pages = this.pages.filter(
            (existing) => existing.offset !== page.offset,
        );
        this.pages.push(page);
        if (this.pages.length > 4) {
            this.pages.sort(
                (a, b) =>
                    Math.abs(a.offset + a.rows.length / 2 - anchor) -
                    Math.abs(b.offset + b.rows.length / 2 - anchor),
            );
            this.pages.length = 4;
        }
        this.rows.clear();
        for (const cached of this.pages) {
            cached.rows.forEach((row, index) =>
                this.rows.set(cached.offset + index, row),
            );
        }
    }

    clear() {
        this.pages = [];
        this.rows.clear();
    }
}
