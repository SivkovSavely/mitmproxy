import * as React from "react";
import fetchMock from "jest-fetch-mock";
import { act, fireEvent, render, screen } from "../test-utils";
import HistoryTable, {
    HistorySummary,
    PureHistoryTable,
} from "../../components/History/HistoryTable";
import timeRange, {
    applyDraft,
    defaultState,
    refreshTimeRange,
    updateDraft,
} from "../../ducks/timeRange";
import { page, summary } from "../historyFixtures";
import { configureStore } from "@reduxjs/toolkit";
import { middlewares, reducer } from "../../ducks/store";
import history, { selectSummary } from "../../ducks/history";
import { setFilter } from "../../ducks/ui/filter";
import { testState, TFlow, TStore } from "../ducks/tutils";

let requests: {
    url: URL;
    signal?: AbortSignal | null;
    resolve: (response: Response) => void;
}[];

function props() {
    return {
        timeRange: { ...defaultState, ready: true, instanceId: "instance-a" },
        search: "",
        highlight: "",
        columns: [
            "index",
            "method",
            "path",
            "status",
            "size",
            "timestamp",
            "version",
        ],
        selected: null as ReturnType<typeof summary> | null,
        onSelect: jest.fn(),
        onClear: jest.fn(),
    };
}

async function respond(index: number, data: unknown) {
    const result = data as { groups?: { key: string; label?: string }[] };
    await act(async () => {
        requests[index].resolve({
            ok: true,
            json: async () => ({
                ...(requests[index].url.pathname.endsWith("flow-history")
                    ? { snapshot: "s1" }
                    : {}),
                ...result,
                ...(result.groups
                    ? {
                          groups: result.groups.map((group) => ({
                              label: group.key,
                              ...group,
                          })),
                      }
                    : {}),
            }),
        } as Response);
    });
}

function scroll(top: number) {
    fireEvent.scroll(screen.getByLabelText("History scroll area"), {
        target: { scrollTop: top },
    });
}

function settle(ms = 200) {
    act(() => jest.advanceTimersByTime(ms));
}

beforeEach(() => {
    jest.useFakeTimers();
    requests = [];
    fetchMock.enableMocks();
    fetchMock.resetMocks();
    fetchMock.mockImplementation(
        (input, init) =>
            new Promise((resolve) => {
                requests.push({
                    url: new URL(String(input), "http://localhost/"),
                    signal: init?.signal,
                    resolve,
                });
            }),
    );
});

afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
});

describe("virtual SQLite history", () => {
    it("shows grouped filter resource errors without clearing the selected flow", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        input.selected = summary(0);
        render(<PureHistoryTable {...input} />);
        await act(async () =>
            requests[0].resolve({
                ok: false,
                status: 503,
                text: async () =>
                    "Live filter input unavailable: use No grouping for larger inputs.",
            } as Response),
        );
        expect(screen.getByRole("alert")).toHaveTextContent(
            "Live filter input unavailable: use No grouping for larger inputs.",
        );
        expect(input.onClear).not.toHaveBeenCalled();
    });

    it("refreshes from the displayed snapshot using the current relative window", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, {
            instance_id: "instance-a",
            snapshot: "s1",
            lower: 100,
            upper: 200,
            count: 0,
            group_count: 0,
            offset: 0,
            groups: [],
        });
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        settle(1000);
        expect(requests[1].url.searchParams.get("refresh")).toBe("s1");
        expect(requests[1].url.searchParams.has("snapshot")).toBe(false);
        expect(requests[1].url.searchParams.get("mode")).toBe("relative");
        await respond(1, {
            instance_id: "instance-a",
            snapshot: "s2",
            lower: 110,
            upper: 205,
            count: 0,
            group_count: 0,
            offset: 0,
            groups: [],
        });
    });

    it("discards stale snapshot rows and retries once with the current window and stable expansion", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        render(<PureHistoryTable {...input} />);
        const groups = (snapshot: string) => ({
            instance_id: "instance-a",
            snapshot,
            lower: 100,
            upper: 200,
            count: 20,
            group_count: 1,
            offset: 0,
            groups: [
                {
                    key: "g1:opaque",
                    label: "Visible label",
                    count: 20,
                    ordinal: 0,
                    members_before: 0,
                },
            ],
        });
        await respond(0, groups("s1"));
        expect(
            screen.queryByRole("button", { name: /g1:opaque/ }),
        ).not.toBeInTheDocument();
        fireEvent.click(screen.getByRole("button", { name: /Visible label/ }));
        await act(async () => {});
        await act(async () =>
            requests[1].resolve({ ok: false, status: 409 } as Response),
        );
        expect(
            screen.queryByRole("button", { name: /Visible label/ }),
        ).not.toBeInTheDocument();
        expect(requests[2].url.searchParams.has("snapshot")).toBe(false);
        expect(requests[2].url.searchParams.get("mode")).toBe("relative");
        expect(requests[2].url.searchParams.has("refresh")).toBe(false);
        await respond(2, groups("s2"));
        expect(requests[3].url.searchParams.get("snapshot")).toBe("s2");
        await respond(3, { ...page(0, 20), snapshot: "s2" });
        expect(
            screen.getByRole("button", { name: /Visible label/ }),
        ).toHaveAttribute("aria-expanded", "true");
        expect(screen.getByText("example.com/history/0")).toBeInTheDocument();
    });

    it("bounds repeated snapshot expiry without clearing selection", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        input.selected = summary(0);
        render(<PureHistoryTable {...input} />);
        await act(async () =>
            requests[0].resolve({ ok: false, status: 409 } as Response),
        );
        await act(async () =>
            requests[1].resolve({ ok: false, status: 409 } as Response),
        );
        settle(5000);
        expect(requests).toHaveLength(2);
        expect(
            screen.getByText(/History snapshot expired/),
        ).toBeInTheDocument();
        expect(input.onClear).not.toHaveBeenCalled();
    });

    it("starts grouped history at the bottom and keeps keyboard scrolling focused", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        render(<PureHistoryTable {...input} />);
        await respond(0, {
            instance_id: "instance-a",
            count: 40,
            group_count: 40,
            offset: 0,
            groups: Array.from({ length: 40 }, (_, ordinal) => ({
                key: `g${String(ordinal).padStart(2, "0")}`,
                count: 1,
                ordinal,
                members_before: ordinal,
            })),
        });
        const viewport = screen.getByLabelText("History scroll area");
        expect(viewport.scrollTop).toBe(960);
        const button = screen.getByRole("button", { name: /g30/ });
        button.focus();
        fireEvent.keyDown(button, { key: "ArrowUp" });
        expect(viewport.scrollTop).toBe(928);
        expect(viewport).toHaveFocus();
        button.focus();
        fireEvent.keyDown(button, { key: "Home" });
        expect(viewport.scrollTop).toBe(0);
        expect(viewport).toHaveFocus();
        fireEvent.keyDown(document.activeElement!, { key: "ArrowDown" });
        expect(viewport.scrollTop).toBe(32);
    });

    it.each([
        ...[true, false].map((survives) => ({
            grouping: "host" as const,
            oldKey: "old.example",
            newKey: "\u00c4new.example",
            before: { host: "old.example" },
            after: { host: "\u00c4NEW.EXAMPLE" },
            survives,
        })),
        {
            grouping: "status" as const,
            oldKey: 200,
            newKey: 404,
            before: { status_code: 200 },
            after: { status_code: 404 },
            survives: true,
        },
        {
            grouping: "method" as const,
            oldKey: "GET",
            newKey: "post",
            before: { method: "GET" },
            after: { method: "post" },
            survives: true,
        },
        {
            grouping: "type" as const,
            oldKey: "http",
            newKey: "tcp",
            before: { type: "http" },
            after: { type: "tcp" },
            survives: true,
        },
        {
            grouping: "host" as const,
            oldKey: "old.example",
            newKey: null,
            before: { host: "old.example" },
            after: { host: null },
            survives: true,
        },
    ])(
        "follows an anchored flow to $grouping key $newKey (old group survives=$survives)",
        async ({ grouping, oldKey, newKey, before, after, survives }) => {
            const input = props();
            input.timeRange.grouping = grouping;
            let moved = false;
            const anchored = { ...summary(4, true), ...before };
            const calls: URLSearchParams[] = [];
            fetchMock.mockResponse(async (request) => {
                const query = new URL(request.url, "http://localhost")
                    .searchParams;
                calls.push(query);
                const oldRows = Array.from({ length: 20 }, (_, i) => ({
                    ...summary(i, true),
                    ...before,
                })).filter(
                    (row) => !moved || (survives && row.id !== anchored.id),
                );
                const newRows = [
                    ...(moved ? [{ ...anchored, ...after }] : []),
                    ...Array.from({ length: 20 }, (_, i) => ({
                        ...summary(20 + i, true),
                        ...after,
                    })),
                ];
                const groups = [
                    ...(oldRows.length
                        ? [{ key: "g1:old", rows: oldRows }]
                        : []),
                    { key: "g1:new", rows: newRows },
                ];
                const descriptors = groups.map((group, ordinal) => ({
                    key: group.key,
                    label: String(ordinal ? newKey : oldKey),
                    ordinal,
                    count: group.rows.length,
                    members_before: ordinal ? oldRows.length : 0,
                }));
                if (query.has("lookup_flow")) {
                    const ordinal = groups.findIndex((group) =>
                        group.rows.some(
                            (row) => row.id === query.get("lookup_flow"),
                        ),
                    );
                    return JSON.stringify({
                        instance_id: "instance-a",
                        snapshot: moved ? "s2" : "s1",
                        group: descriptors[ordinal] ?? null,
                        member_ordinal:
                            ordinal < 0
                                ? null
                                : groups[ordinal].rows.findIndex(
                                      (row) =>
                                          row.id === query.get("lookup_flow"),
                                  ),
                    });
                }
                const all = [...oldRows, ...newRows].sort(
                    (a, b) => a.timestamp_created - b.timestamp_created,
                );
                if (query.has("group_by") && !query.has("group_key")) {
                    let before = 0;
                    return JSON.stringify({
                        instance_id: "instance-a",
                        snapshot: moved ? "s2" : "s1",
                        count: all.length,
                        group_count: groups.length,
                        offset: 0,
                        groups: groups.map((group, ordinal) => {
                            const result = {
                                key: group.key,
                                label: String(
                                    group.key === "g1:old" ? oldKey : newKey,
                                ),
                                ordinal,
                                count: group.rows.length,
                                members_before: before,
                            };
                            before += group.rows.length;
                            return result;
                        }),
                    });
                }
                const rows = query.has("group_key")
                    ? (groups.find(
                          (group) => group.key === query.get("group_key"),
                      )?.rows ?? [])
                    : all;
                const offset = Number(
                    query.get("offset") ??
                        Math.max(0, rows.length - Number(query.get("limit"))),
                );
                const position = rows.findIndex(
                    (row) => row.id === query.get("anchor"),
                );
                return JSON.stringify({
                    ...page(0, rows.length),
                    snapshot: moved ? "s2" : "s1",
                    offset,
                    rows: rows.slice(
                        offset,
                        offset + Number(query.get("limit")),
                    ),
                    anchor_position: position < 0 ? null : position,
                });
            });
            const view = render(<PureHistoryTable {...input} />);
            await act(async () => {});
            fireEvent.click(
                screen.getByRole("button", { name: "Expand all groups" }),
            );
            await act(async () => {});
            scroll(167);
            settle();
            await act(async () => {});
            moved = true;
            view.rerender(
                <PureHistoryTable
                    {...input}
                    timeRange={{ ...input.timeRange, historyRevision: 1 }}
                />,
            );
            settle(1000);
            await act(async () => {});
            expect(screen.getByLabelText("History scroll area").scrollTop).toBe(
                survives ? 679 : 39,
            );
            fireEvent.click(
                screen.getByText(
                    `${("host" in after ? after.host : anchored.host) ?? ""}/history/4`,
                ),
            );
            expect(input.onSelect).toHaveBeenCalledWith({
                ...anchored,
                ...after,
            });
            expect(
                calls.some(
                    (query) =>
                        query.get("lookup_flow") === anchored.id &&
                        query.get("snapshot") === "s2",
                ),
            ).toBe(true);
        },
    );

    it.each([true, false])(
        "validates selected live flows independently of their old group key (eligible=%s)",
        async (eligible) => {
            const input = props();
            input.timeRange.grouping = "host";
            input.selected = summary(0, true);
            render(<PureHistoryTable {...input} />);
            await respond(0, {
                instance_id: "instance-a",
                count: 1,
                group_count: 1,
                offset: 0,
                groups: [
                    {
                        key: "changed.example",
                        count: 1,
                        ordinal: 0,
                        members_before: 0,
                    },
                ],
            });
            expect(input.onClear).not.toHaveBeenCalled();
            expect(requests[1].url.searchParams.get("lookup_flow")).toBe(
                input.selected.id,
            );
            expect(requests[1].url.searchParams.get("snapshot")).toBe("s1");
            await respond(1, {
                instance_id: "instance-a",
                group: eligible
                    ? {
                          key: "changed.example",
                          label: "changed.example",
                          count: 1,
                          ordinal: 0,
                          members_before: 0,
                      }
                    : null,
                member_ordinal: eligible ? 0 : null,
            });
            expect(input.onClear).toHaveBeenCalledTimes(eligible ? 0 : 1);
            expect(
                screen.getByRole("button", { name: /changed.example/ }),
            ).toBeInTheDocument();
        },
    );

    it("preserves expansion when scrolling during a grouped refresh", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        const view = render(<PureHistoryTable {...input} />);
        const groups = {
            instance_id: "instance-a",
            count: 200,
            group_count: 20,
            offset: 0,
            groups: Array.from({ length: 20 }, (_, ordinal) => ({
                key: `g${String(ordinal).padStart(2, "0")}`,
                count: 10,
                ordinal,
                members_before: ordinal * 10,
            })),
        };
        await respond(0, groups);
        fireEvent.click(screen.getByRole("button", { name: /g10/ }));
        await act(async () => {});
        await respond(1, page(0, 10));
        scroll(160);
        settle();
        await act(async () => {});
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        settle(1000);
        scroll(192);
        settle();
        await act(async () => {});
        expect(requests[2].signal?.aborted).toBe(false);
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 2 }}
            />,
        );
        await respond(2, { ...groups, snapshot: "s2" });
        expect(requests[3].url.searchParams.get("group_key")).toBe("g10");
        await respond(3, { ...page(0, 10), snapshot: "s2" });
        expect(screen.getByRole("button", { name: /g10/ })).toHaveAttribute(
            "aria-expanded",
            "true",
        );
        expect(screen.getByRole("table")).toHaveAttribute(
            "aria-rowcount",
            "31",
        );
        settle(1000);
        expect(requests[4].url.searchParams.get("refresh")).toBe("s2");
    });

    it("anchors a successor to the latest scroll position instead of the old bottom", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        const view = render(<PureHistoryTable {...input} />);
        const groups = (insert: boolean, snapshot: string) => {
            const keys = Array.from({ length: 40 }, (_, i) => `g${i}`);
            if (insert) keys.unshift("new");
            return {
                instance_id: "instance-a",
                snapshot,
                lower: 100,
                upper: 200,
                count: keys.length,
                group_count: keys.length,
                offset: 0,
                groups: keys.map((key, ordinal) => ({
                    key,
                    ordinal,
                    count: 1,
                    members_before: ordinal,
                })),
            };
        };
        await respond(0, groups(false, "s1"));
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        settle(1000);
        scroll(160);
        settle();
        await act(async () => {});
        expect(requests[1].signal?.aborted).toBe(false);
        expect(screen.getByRole("button", { name: /g5 / })).toBeInTheDocument();
        await respond(1, groups(true, "s2"));
        expect(screen.getByLabelText("History scroll area").scrollTop).toBe(
            192,
        );
        expect(screen.getByRole("button", { name: /g5 / })).toBeInTheDocument();
    });

    it("starts a scheduled grouped refresh while scrolling continues", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, {
            instance_id: "instance-a",
            snapshot: "s1",
            count: 40,
            group_count: 40,
            offset: 0,
            groups: Array.from({ length: 40 }, (_, ordinal) => ({
                key: `g${ordinal}`,
                count: 1,
                ordinal,
                members_before: ordinal,
            })),
        });
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        for (let i = 0; i < 5; i++) {
            scroll(32 * i);
            settle(200);
        }
        expect(requests[1].url.searchParams.get("refresh")).toBe("s1");
        expect(requests[1].signal?.aborted).toBe(false);
    });

    it("loads old snapshot members during successor preparation without mixing versions", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        const view = render(<PureHistoryTable {...input} />);
        const group = {
            key: "g1:host",
            label: "host",
            count: 1000,
            ordinal: 0,
            members_before: 0,
        };
        const groupPage = (snapshot: string) => ({
            instance_id: "instance-a",
            snapshot,
            lower: 100,
            upper: 200,
            count: 1000,
            group_count: 1,
            offset: 0,
            groups: [group],
        });
        await respond(0, groupPage("s1"));
        fireEvent.click(screen.getByRole("button", { name: /host \(/ }));
        await act(async () => {});
        await respond(1, { ...page(0, 1000), snapshot: "s1" });
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        settle(1000);
        scroll(16032);
        settle();
        await act(async () => {});
        expect(requests[2].signal?.aborted).toBe(false);
        expect(requests[3].url.searchParams.get("snapshot")).toBe("s1");
        expect(requests[3].url.searchParams.get("offset")).toBe("500");
        await respond(3, { ...page(500, 1000), snapshot: "s1" });
        expect(screen.getByText("example.com/history/500")).toBeInTheDocument();
        await respond(2, groupPage("s2"));
        expect(requests[4].url.searchParams.get("lookup_flow")).toBe(
            summary(500).id,
        );
        scroll(0);
        settle();
        await act(async () => {});
        await respond(4, {
            instance_id: "instance-a",
            snapshot: "s2",
            group,
            member_ordinal: 500,
        });
        expect(requests[5].url.searchParams.get("snapshot")).toBe("s2");
        expect(requests[5].url.searchParams.get("offset")).toBe("0");
        await respond(5, {
            ...page(0, 1000),
            snapshot: "s2",
            rows: page(0, 1000).rows.map((row) => ({
                ...row,
                host: "successor",
            })),
        });
        expect(requests[6].url.searchParams.get("snapshot")).toBe("s2");
        expect(requests[6].url.searchParams.get("offset")).toBe("500");
        await respond(6, {
            ...page(500, 1000),
            snapshot: "s2",
            rows: page(500, 1000).rows.map((row) => ({
                ...row,
                host: "successor",
            })),
        });
        expect(screen.getByLabelText("History scroll area").scrollTop).toBe(0);
        expect(screen.getByText("successor/history/0")).toBeInTheDocument();
        expect(
            screen.queryByText("example.com/history/500"),
        ).not.toBeInTheDocument();
    });

    it.each(["query", "unmount"])(
        "cancels both grouped request lanes on %s",
        async (reason) => {
            const input = props();
            input.timeRange.grouping = "host";
            const view = render(<PureHistoryTable {...input} />);
            await respond(0, {
                instance_id: "instance-a",
                snapshot: "s1",
                count: 1000,
                group_count: 1,
                offset: 0,
                groups: [
                    { key: "host", count: 1000, ordinal: 0, members_before: 0 },
                ],
            });
            fireEvent.click(screen.getByRole("button", { name: /host \(/ }));
            await act(async () => {});
            await respond(1, { ...page(0, 1000), snapshot: "s1" });
            view.rerender(
                <PureHistoryTable
                    {...input}
                    timeRange={{ ...input.timeRange, historyRevision: 1 }}
                />,
            );
            settle(1000);
            scroll(16032);
            settle();
            await act(async () => {});
            expect(requests[3].url.searchParams.get("snapshot")).toBe("s1");
            if (reason === "query")
                view.rerender(<PureHistoryTable {...input} search="changed" />);
            else view.unmount();
            expect(requests[2].signal?.aborted).toBe(true);
            expect(requests[3].signal?.aborted).toBe(true);
            await respond(2, {
                instance_id: "instance-a",
                snapshot: "s2",
                count: 0,
                group_count: 0,
                offset: 0,
                groups: [],
            });
            await respond(3, { ...page(500, 1000), snapshot: "s1" });
            expect(
                screen.queryByText("example.com/history/500"),
            ).not.toBeInTheDocument();
        },
    );

    it("anchors grouped refreshes by key and coalesces live revisions", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        const view = render(<PureHistoryTable {...input} />);
        const groups = (insert: boolean) => {
            const keys = Array.from(
                { length: 20 },
                (_, i) => `g${String(i).padStart(2, "0")}`,
            );
            if (insert) keys.unshift("a");
            return {
                instance_id: "instance-a",
                count: keys.length * 10,
                group_count: keys.length,
                offset: 0,
                lower: 100,
                upper: 200,
                groups: keys.map((key, ordinal) => ({
                    key,
                    ordinal,
                    count: 10,
                    members_before: ordinal * 10,
                })),
            };
        };
        await respond(0, groups(false));
        scroll(160);
        settle();
        await act(async () => {});
        fireEvent.click(screen.getByRole("button", { name: /g05/ }));
        await act(async () => {});
        expect(requests[1].url.searchParams.get("mode")).toBe("absolute");
        expect(requests[1].url.searchParams.get("from")).toBe("100");
        await respond(1, page(0, 10));
        for (let revision = 1; revision <= 5; revision++) {
            view.rerender(
                <PureHistoryTable
                    {...input}
                    timeRange={{
                        ...input.timeRange,
                        historyRevision: revision,
                    }}
                />,
            );
        }
        expect(requests).toHaveLength(2);
        settle(1000);
        expect(requests).toHaveLength(3);
        await respond(2, groups(true));
        expect(requests[3].url.searchParams.get("group_key")).toBe("g05");
        await respond(3, page(0, 10));
        expect(screen.getByLabelText("History scroll area").scrollTop).toBe(
            192,
        );
        expect(screen.getByRole("button", { name: /g05/ })).toHaveAttribute(
            "aria-expanded",
            "true",
        );
        expect(requests).toHaveLength(4);
    });

    it("rejects stale group responses after grouping changes", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        const view = render(<PureHistoryTable {...input} />);
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, grouping: "method" }}
            />,
        );
        expect(requests[0].signal?.aborted).toBe(true);
        await respond(0, {
            instance_id: "instance-a",
            count: 1,
            group_count: 1,
            offset: 0,
            groups: [{ key: "stale", count: 1, ordinal: 0, members_before: 0 }],
        });
        expect(
            screen.queryByRole("button", { name: /stale/ }),
        ).not.toBeInTheDocument();
        await respond(1, {
            instance_id: "instance-a",
            count: 1,
            group_count: 1,
            offset: 0,
            groups: [{ key: "GET", count: 1, ordinal: 0, members_before: 0 }],
        });
        expect(screen.getByRole("button", { name: /GET/ })).toBeInTheDocument();
    });

    it("renders virtual group headers and expands only visible members", async () => {
        const input = props();
        input.timeRange.grouping = "host";
        render(<PureHistoryTable {...input} />);
        expect(requests[0].url.searchParams.get("group_by")).toBe("host");
        await respond(0, {
            instance_id: "instance-a",
            count: 1000000,
            group_count: 1,
            offset: 0,
            groups: [
                {
                    key: "example.com",
                    count: 1000000,
                    ordinal: 0,
                    members_before: 0,
                },
            ],
        });
        expect(
            screen.getByRole("button", { name: /example.com/ }),
        ).toHaveAttribute("aria-expanded", "false");
        expect(
            screen.getByText("1,000,000 flows in 1 group"),
        ).toBeInTheDocument();
        expect(requests).toHaveLength(1);
        fireEvent.click(
            screen.getByRole("button", { name: "Expand all groups" }),
        );
        await act(async () => {});
        expect(requests).toHaveLength(2);
        expect(requests[1].url.searchParams.get("group_key")).toBe(
            "example.com",
        );
        await respond(1, page(0));
        expect(screen.getByRole("table")).toHaveAttribute(
            "aria-rowcount",
            "1000002",
        );
        expect(screen.getByText("example.com/history/0")).toBeInTheDocument();
        fireEvent.click(screen.getByText("example.com/history/0"));
        expect(input.onSelect).toHaveBeenCalledWith(summary(0));
        fireEvent.click(
            screen.getByRole("button", { name: "Collapse all groups" }),
        );
        await act(async () => {});
        expect(screen.getByRole("table")).toHaveAttribute("aria-rowcount", "2");
        expect(input.onClear).not.toHaveBeenCalled();
    });

    it("keeps selection when switching grouping, without sending selected to group pages", async () => {
        const input = props();
        input.selected = summary(0);
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(0));
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, grouping: "host" }}
            />,
        );
        expect(input.onClear).not.toHaveBeenCalled();
        expect(
            requests[requests.length - 1].url.searchParams.has("selected"),
        ).toBe(false);
    });

    it("waits for trusted backend state before requesting metadata", () => {
        const input = props();
        render(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, ready: false }}
            />,
        );
        expect(requests).toHaveLength(0);
        expect(screen.getByText(/Waiting for backend/)).toBeInTheDocument();
    });

    it("does not cancel the initial page when a wheel event arrives before metadata", async () => {
        render(<PureHistoryTable {...props()} />);
        fireEvent.wheel(screen.getByLabelText("History scroll area"), {
            deltaY: -32,
        });
        expect(requests[0].signal?.aborted).toBe(false);
        await respond(0, page(999500));
        expect(
            screen.getByText("example.com/history/999999"),
        ).toBeInTheDocument();
    });

    it("opens the newest bounded page at the bottom while representing one million rows", async () => {
        const { container } = render(<PureHistoryTable {...props()} />);
        expect(requests[0].url.pathname).toBe("/flow-history");
        expect(requests[0].url.searchParams.get("limit")).toBe("500");
        expect(requests[0].url.searchParams.has("offset")).toBe(false);
        await respond(0, page(999500));
        expect(screen.getByRole("table")).toHaveAttribute(
            "aria-rowcount",
            "1000001",
        );
        expect(screen.getByLabelText("History scroll area").scrollTop).toBe(
            999680,
        );
        expect(
            container.querySelector<HTMLElement>(".history-scroll-space")?.style
                .height,
        ).toBe("1000000px");
        expect(
            container.querySelectorAll("[data-history-index]").length,
        ).toBeLessThan(30);
        expect(screen.getByText("900719925474999999")).toBeInTheDocument();
        expect(
            screen.getByText("example.com/history/999999"),
        ).toBeInTheDocument();
        expect(
            container.querySelector(
                "[data-history-index='999999'] .col-version",
            ),
        ).toHaveTextContent("-");
    });

    it("moves immediately to random-access placeholders and only requests the settled drag position", async () => {
        render(<PureHistoryTable {...props()} />);
        await respond(0, page(999500));
        scroll(499840);
        expect(
            screen.getByRole("row", { name: /Loading row 499991/ }),
        ).toBeInTheDocument();
        settle(199);
        expect(requests).toHaveLength(1);
        scroll(0);
        settle();
        expect(requests).toHaveLength(2);
        expect(requests[1].url.searchParams.get("offset")).toBe("0");
        await respond(1, page(0));
        expect(screen.getByText("example.com/history/0")).toBeInTheDocument();
    });

    it("cancels superseded requests and rejects stale responses even when abort is ignored", async () => {
        render(<PureHistoryTable {...props()} />);
        await respond(0, page(999500));
        scroll(499840);
        settle();
        scroll(0);
        expect(requests[1].signal?.aborted).toBe(true);
        settle();
        await respond(2, page(0));
        await respond(1, page(499990));
        expect(screen.getByLabelText("History scroll area").scrollTop).toBe(0);
        expect(screen.getByText("example.com/history/0")).toBeInTheDocument();
        expect(
            screen.queryByText("example.com/history/499999"),
        ).not.toBeInTheDocument();
    });

    it("rejects a response from another backend instance", async () => {
        render(<PureHistoryTable {...props()} />);
        await respond(0, { ...page(0), instance_id: "obsolete-instance" });
        expect(
            screen.queryByText("example.com/history/0"),
        ).not.toBeInTheDocument();
        expect(screen.getByRole("table")).not.toHaveAttribute(
            "aria-rowcount",
            "1000001",
        );
    });

    it("keeps wheel movement row-granular even in a million-row list", async () => {
        render(<PureHistoryTable {...props()} />);
        await respond(0, page(999500));
        fireEvent.wheel(screen.getByLabelText("History scroll area"), {
            deltaY: -32,
        });
        expect(
            document.querySelector("[data-history-index='999998']"),
        ).toBeInTheDocument();
        expect(
            screen.getByLabelText("History scroll area").scrollTop,
        ).toBeGreaterThan(999678);
        expect(
            screen.getByLabelText("History scroll area").scrollTop,
        ).toBeLessThan(999680);
        fireEvent.keyDown(screen.getByLabelText("History scroll area"), {
            key: "Home",
        });
        expect(screen.getByLabelText("History scroll area").scrollTop).toBe(0);
    });

    it("keeps the newest row visible when the viewport resizes without a window event", async () => {
        let resize: (() => void) | undefined;
        const original = globalThis.ResizeObserver;
        globalThis.ResizeObserver = jest.fn((callback) => {
            resize = callback;
            return {
                observe: jest.fn(),
                unobserve: jest.fn(),
                disconnect: jest.fn(),
            };
        }) as any;
        try {
            render(<PureHistoryTable {...props()} />);
            await respond(0, page(999500));
            jest.spyOn(
                HTMLElement.prototype,
                "clientHeight",
                "get",
            ).mockReturnValue(640);
            expect(resize).toEqual(expect.any(Function));
            act(() => resize!());
            expect(screen.getByLabelText("History scroll area").scrollTop).toBe(
                999360,
            );
            expect(
                screen.getByText("example.com/history/999999"),
            ).toBeInTheDocument();
        } finally {
            globalThis.ResizeObserver = original;
        }
    });

    it("prefetches adjacent rows slowly with a lossless keyset cursor and preserves position", async () => {
        render(<PureHistoryTable {...props()} />);
        await respond(0, page(999500));
        scroll(0);
        settle();
        await respond(1, page(0));
        // Near the end of the first cached page, but the entire viewport is cached.
        fireEvent.keyDown(screen.getByLabelText("History scroll area"), {
            key: "PageDown",
        });
        for (let i = 0; i < 43; i++) {
            fireEvent.keyDown(screen.getByLabelText("History scroll area"), {
                key: "PageDown",
            });
        }
        settle();
        const before = screen.getByLabelText("History scroll area").scrollTop;
        settle(799);
        expect(requests).toHaveLength(2);
        settle(1);
        expect(requests).toHaveLength(3);
        expect(JSON.parse(requests[2].url.searchParams.get("after")!)).toEqual(
            summary(499).cursor,
        );
        expect(requests[2].url.searchParams.has("offset")).toBe(false);
        await respond(2, page(500));
        expect(screen.getByLabelText("History scroll area").scrollTop).toBe(
            before,
        );
    });

    it("prefetches backward with before and cancels that request on a new drag", async () => {
        render(<PureHistoryTable {...props()} />);
        await respond(0, page(999500));
        for (let i = 0; i < 44; i++) {
            fireEvent.keyDown(screen.getByLabelText("History scroll area"), {
                key: "PageUp",
            });
        }
        settle();
        settle(800);
        expect(requests).toHaveLength(2);
        expect(JSON.parse(requests[1].url.searchParams.get("before")!)).toEqual(
            summary(999500).cursor,
        );
        scroll(0);
        expect(requests[1].signal?.aborted).toBe(true);
        settle();
        await respond(2, page(0));
        await respond(1, page(999000));
        expect(screen.getByLabelText("History scroll area").scrollTop).toBe(0);
        expect(screen.getByText("example.com/history/0")).toBeInTheDocument();
    });

    it("refreshes a non-bottom anchor by rank instead of jumping to newest", async () => {
        const input = props();
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        scroll(0);
        settle();
        await respond(1, page(0));
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        settle(1000);
        expect(requests[2].url.searchParams.get("anchor")).toBe("history-0");
        await respond(2, { ...page(0, 1000100), anchor_position: 100 });
        expect(
            screen.getByLabelText("History scroll area").scrollTop,
        ).toBeGreaterThan(0);
        expect(
            screen.getByLabelText("History scroll area").scrollTop,
        ).toBeLessThan(101);
        expect(screen.getByRole("table")).toHaveAttribute(
            "aria-rowcount",
            "1000101",
        );
    });

    it("fetches the page at the resolved anchor rank when inserts move it beyond the old page", async () => {
        const input = props();
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        scroll(0);
        settle();
        await respond(1, page(0));
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        settle(1000);
        await respond(2, { ...page(0, 1010000), anchor_position: 10000 });
        expect(requests).toHaveLength(4);
        expect(requests[3].url.searchParams.get("offset")).toBe("9995");
        await respond(3, page(9995, 1010000));
        expect(
            screen.getByText("example.com/history/10000"),
        ).toBeInTheDocument();
    });

    it("loads the clamped viewport after an empty page reports expired rows", async () => {
        render(<PureHistoryTable {...props()} />);
        await respond(0, page(999500));
        scroll(499840);
        settle();
        await respond(1, page(499990, 100, 0));
        expect(requests).toHaveLength(3);
        expect(requests[2].url.searchParams.get("offset")).toBe("85");
        await respond(2, page(85, 100));
        expect(screen.getByText("example.com/history/99")).toBeInTheDocument();
        expect(screen.getByRole("table")).toHaveAttribute(
            "aria-rowcount",
            "101",
        );
        expect(
            screen.queryByRole("row", { name: /Loading row/ }),
        ).not.toBeInTheDocument();
    });

    it("does not retry an empty zero-count page", async () => {
        render(<PureHistoryTable {...props()} />);
        await respond(0, page(0, 0));
        settle(5000);
        expect(requests).toHaveLength(1);
        expect(screen.getByRole("table")).toHaveAttribute("aria-rowcount", "1");
        expect(
            screen.queryByRole("row", { name: /Loading row/ }),
        ).not.toBeInTheDocument();
    });

    it("autoscrolls on history refresh only when previously at the bottom", async () => {
        const input = props();
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        settle(1000);
        expect(requests[1].url.searchParams.has("offset")).toBe(false);
        await respond(1, page(999600, 1000100));
        expect(
            screen.getByText("example.com/history/1000099"),
        ).toBeInTheDocument();
        expect(screen.getByLabelText("History scroll area").scrollTop).toBe(
            999680,
        );
    });

    it("retains a non-bottom anchor across a same-instance reconnect without querying untrusted state", async () => {
        const input = props();
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        scroll(0);
        settle();
        await respond(1, page(0));
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, ready: false }}
            />,
        );
        expect(requests).toHaveLength(2);
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, revision: 1 }}
            />,
        );
        settle(1000);
        expect(requests[2].url.searchParams.get("anchor")).toBe("history-0");
        expect(requests[2].url.searchParams.get("offset")).toBe("0");
        await respond(2, { ...page(0, 1000100), anchor_position: 100 });
        expect(
            screen.getByLabelText("History scroll area").scrollTop,
        ).toBeLessThan(101);
    });

    it("defers invalidations during a drag until its position settles", async () => {
        const input = props();
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        scroll(499840);
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        expect(requests).toHaveLength(1);
        settle();
        expect(requests).toHaveLength(2);
        expect(requests[1].url.searchParams.get("offset")).toBe("499990");
    });

    it("coalesces invalidations without starving an in-flight refresh", async () => {
        const input = props();
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 1 }}
            />,
        );
        expect(requests).toHaveLength(1);
        settle(1000);
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{ ...input.timeRange, historyRevision: 2 }}
            />,
        );
        expect(requests).toHaveLength(2);
        expect(requests[1].signal?.aborted).toBe(false);
        await respond(1, page(999500));
        expect(requests).toHaveLength(2);
        settle(999);
        expect(requests).toHaveLength(2);
        settle(1);
        expect(requests).toHaveLength(3);
    });

    it("coalesces a burst even when every metadata request completes immediately", async () => {
        const input = props();
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        let completed = 1;
        for (let revision = 1; revision <= 50; revision++) {
            view.rerender(
                <PureHistoryTable
                    {...input}
                    timeRange={{
                        ...input.timeRange,
                        historyRevision: revision,
                    }}
                />,
            );
            if (requests.length > completed)
                await respond(completed++, page(999500));
        }
        expect(requests).toHaveLength(1);
        settle(999);
        expect(requests).toHaveLength(1);
        settle(1);
        expect(requests).toHaveLength(2);
        await respond(1, page(999550, 1000050));
        expect(
            screen.getByText("example.com/history/1000049"),
        ).toBeInTheDocument();
    });

    it("refreshes during sustained updates at most once per second without waiting for silence", async () => {
        const input = props();
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        let completed = 1;
        for (let revision = 1; revision <= 50; revision++) {
            view.rerender(
                <PureHistoryTable
                    {...input}
                    timeRange={{
                        ...input.timeRange,
                        historyRevision: revision,
                    }}
                />,
            );
            settle(100);
            if (requests.length > completed)
                await respond(
                    completed++,
                    page(999500 + revision, 1000000 + revision),
                );
            expect(requests.length).toBeLessThanOrEqual(
                1 + Math.floor((revision * 100) / 1000),
            );
        }
        expect(requests.length).toBeGreaterThanOrEqual(5);
        expect(
            screen.getByText("example.com/history/1000049"),
        ).toBeInTheDocument();
    });

    it.each(["unmount", "disconnect", "query"])(
        "cancels a scheduled refresh on %s",
        async (change) => {
            const input = props();
            const view = render(<PureHistoryTable {...input} />);
            await respond(0, page(999500));
            view.rerender(
                <PureHistoryTable
                    {...input}
                    timeRange={{ ...input.timeRange, historyRevision: 1 }}
                />,
            );
            settle(500);
            expect(requests).toHaveLength(1);
            if (change === "unmount") view.unmount();
            else if (change === "disconnect")
                view.rerender(
                    <PureHistoryTable
                        {...input}
                        timeRange={{ ...input.timeRange, ready: false }}
                    />,
                );
            else {
                view.rerender(
                    <PureHistoryTable {...input} search="~u fresh" />,
                );
                expect(requests).toHaveLength(2);
                expect(requests[1].url.searchParams.get("search")).toBe(
                    "~u fresh",
                );
                await respond(1, page(0, 0));
            }
            settle(5000);
            expect(requests).toHaveLength(change === "query" ? 2 : 1);
        },
    );

    it("resets for applied ranges and Search and communicates historical filter limitations", async () => {
        const input = props();
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        scroll(0);
        settle();
        view.rerender(
            <PureHistoryTable
                {...input}
                search="~u example"
                highlight="~s"
                timeRange={{ ...input.timeRange, revision: 1 }}
            />,
        );
        expect(requests[1].signal?.aborted).toBe(true);
        expect(requests[2].url.searchParams.get("search")).toBe("~u example");
        expect(requests[2].url.searchParams.get("highlight")).toBe("~s");
        expect(requests[2].url.searchParams.get("mode")).toBe("relative");
        expect(requests[2].url.searchParams.has("offset")).toBe(false);
        expect(
            screen.getByText(/LIVE matching flows only/),
        ).toBeInTheDocument();
        expect(
            screen.getByText(/Historical highlighting is unavailable/),
        ).toBeInTheDocument();
        expect(
            screen.queryByText("example.com/history/999999"),
        ).not.toBeInTheDocument();
    });

    it("composes applied explicit bounds with Search and resets pages when the range changes", async () => {
        const input = props();
        input.search = "~u example";
        const view = render(<PureHistoryTable {...input} />);
        await respond(0, page(999500));
        view.rerender(
            <PureHistoryTable
                {...input}
                timeRange={{
                    ...input.timeRange,
                    revision: 1,
                    preference: {
                        ...input.timeRange.preference,
                        mode: "explicit",
                        from: "2026-01-02 03:04:05",
                        until: "2026-01-02 04:04:05",
                    },
                }}
            />,
        );
        expect(requests).toHaveLength(2);
        expect(requests[1].url.searchParams.get("from")).toBe(
            String(new Date(2026, 0, 2, 3, 4, 5).getTime() / 1000),
        );
        expect(requests[1].url.searchParams.get("until")).toBe(
            String(new Date(2026, 0, 2, 4, 4, 5).getTime() / 1000),
        );
        expect(requests[1].url.searchParams.get("search")).toBe("~u example");
        expect(requests[1].url.searchParams.has("offset")).toBe(false);
        expect(screen.getByRole("table")).not.toHaveAttribute("aria-rowcount");
        expect(
            screen.queryByText("example.com/history/999999"),
        ).not.toBeInTheDocument();
    });

    it("passes historical metadata to selection and clears an ineligible selected row", async () => {
        const input = { ...props(), selected: summary(999999) };
        render(<PureHistoryTable {...input} />);
        expect(requests[0].url.searchParams.get("selected")).toBe(
            "history-999999",
        );
        await respond(0, { ...page(999500), selected_eligible: false });
        expect(input.onClear).toHaveBeenCalledTimes(1);
        fireEvent.click(screen.getByText("example.com/history/999999"));
        expect(input.onSelect).toHaveBeenCalledWith(summary(999999));
        expect(
            requests.every(
                (request) => request.url.pathname === "/flow-history",
            ),
        ).toBe(true);
    });

    it.each(["range", "Search"])(
        "clears selected live details when an applied %s excludes the row",
        async (change) => {
            const store = configureStore({
                reducer: { ...reducer, history, timeRange },
                preloadedState: {
                    ...testState,
                    backendState: {
                        ...testState.backendState,
                        sqlite_history: true,
                    },
                    history: { selected: null, generation: 0 },
                    timeRange: {
                        ...defaultState,
                        ready: true,
                        instanceId: "instance-a",
                        preference: {
                            ...defaultState.preference,
                            mode: "explicit" as const,
                        },
                    },
                    ui: {
                        ...testState.ui,
                        filter: { search: "", highlight: "" },
                    },
                },
                middleware: (defaults) => defaults(middlewares),
            });
            const row = summary(9, true);
            const flow = {
                ...TFlow(),
                id: row.id,
                timestamp_created: row.timestamp_created,
            };
            const pending = store.dispatch(selectSummary(row));
            await respond(0, flow);
            await pending;
            render(<HistoryTable />, { store });
            await respond(1, { ...page(0, 20), selected_eligible: true });
            expect(store.getState().flows.selected[0].id).toBe(row.id);

            act(() => {
                if (change === "Search")
                    store.dispatch(setFilter("~u /not-this-flow"));
                else {
                    store.dispatch(
                        updateDraft({
                            mode: "explicit",
                            from: "2026-01-02 03:00:00",
                        }),
                    );
                    store.dispatch(applyDraft());
                }
            });
            expect(store.getState().history.selected).toBeNull();
            expect(store.getState().flows.selected).toHaveLength(0);
            if (change === "Search")
                expect(requests[2].url.searchParams.get("search")).toBe(
                    "~u /not-this-flow",
                );
            else expect(requests[2].url.searchParams.has("from")).toBe(true);
            await respond(2, { ...page(0, 0), selected_eligible: false });
            expect(store.getState().history.selected).toBeNull();
            expect(store.getState().flows.selected).toHaveLength(0);
            expect(store.getState().flows.byId.size).toBe(0);
            expect(requests).toHaveLength(3);
        },
    );

    it.each([
        ["Search", false],
        ["relative range", false],
        ["Search", true],
        ["relative range", true],
    ] as const)(
        "clears live selection on changed %s even when history fails (loaded=%s)",
        async (change, loaded) => {
            const row = {
                ...summary(9, true),
                timestamp_created: Date.now() / 1000 - 600,
            };
            const flow = {
                ...TFlow(),
                id: row.id,
                timestamp_created: row.timestamp_created,
            };
            const store = TStore({
                ...testState,
                backendState: {
                    ...testState.backendState,
                    sqlite_history: true,
                },
                history: { selected: null, generation: 0 },
                timeRange: {
                    ...defaultState,
                    ready: true,
                    instanceId: "instance-a",
                },
                ui: { ...testState.ui, filter: { search: "", highlight: "" } },
            });
            render(<HistoryTable />, { store });
            await respond(0, { ...page(0, 1), rows: [row] });
            fireEvent.click(screen.getByText("example.com/history/9"));
            if (loaded) {
                await respond(1, flow);
                expect(store.getState().flows.selected).toHaveLength(1);
            }

            act(() => {
                if (change === "Search")
                    store.dispatch(setFilter("~u /not-this-flow"));
                else {
                    store.dispatch(updateDraft({ duration: 300 }));
                    expect(store.dispatch(applyDraft())).toBe(true);
                }
            });
            expect(store.getState().history.selected).toBeNull();
            expect(store.getState().flows.selected).toHaveLength(0);
            if (!loaded) expect(requests[1].signal?.aborted).toBe(true);
            expect(requests[2].url.pathname).toBe("/flow-history");
            await act(async () =>
                requests[2].resolve({ ok: false, status: 503 } as Response),
            );
            expect(screen.getByRole("alert")).toHaveTextContent("HTTP 503");
            if (!loaded) {
                expect(requests[1].signal?.aborted).toBe(true);
                await respond(1, flow);
            }

            expect(store.getState().history.selected).toBeNull();
            expect(store.getState().flows.selected).toHaveLength(0);
            expect(store.getState().flows.byId.size).toBe(0);
            settle(5000);
            expect(requests).toHaveLength(3);
        },
    );

    it.each(["eligible", "expired", "error"])(
        "uses server eligibility for a clock-skewed relative selection during a same-query refresh (%s)",
        async (outcome) => {
            const eligible = outcome !== "expired";
            jest.setSystemTime(2000 * 1000);
            const row = { ...summary(9, true), timestamp_created: 2010 };
            const flow = {
                ...TFlow(),
                id: row.id,
                timestamp_created: row.timestamp_created,
            };
            const store = TStore({
                ...testState,
                backendState: {
                    ...testState.backendState,
                    sqlite_history: true,
                },
                history: { selected: null, generation: 0 },
                timeRange: {
                    ...defaultState,
                    ready: true,
                    instanceId: "instance-a",
                    preference: { ...defaultState.preference, duration: 300 },
                },
                ui: { ...testState.ui, filter: { search: "", highlight: "" } },
            });
            render(<HistoryTable />, { store });
            await respond(0, {
                ...page(0, 1),
                rows: [row],
                lower: 1711,
                upper: 2011,
            });
            fireEvent.click(screen.getByText("example.com/history/9"));
            expect(requests).toHaveLength(2);
            expect(requests[1].url.pathname).toBe(`/flows/${row.id}`);

            act(() => store.dispatch(refreshTimeRange()));
            expect(requests).toHaveLength(2);
            settle(1000);
            expect(requests[2].url.pathname).toBe("/flow-history");
            expect(requests[2].url.searchParams.get("selected")).toBe(row.id);
            if (outcome === "error")
                await act(async () =>
                    requests[2].resolve({ ok: false, status: 503 } as Response),
                );
            else
                await respond(2, {
                    ...page(0, eligible ? 1 : 0),
                    rows: eligible ? [row] : [],
                    lower: eligible ? 1712 : 2011,
                    upper: eligible ? 2012 : 2311,
                    selected_eligible: eligible,
                });
            expect(requests[1].signal?.aborted).toBe(!eligible);
            await respond(1, flow);

            expect(store.getState().history.selected).toEqual(
                eligible ? row : null,
            );
            expect(store.getState().flows.selected).toEqual(
                eligible ? [flow] : [],
            );
            expect(store.getState().flows.list).toHaveLength(eligible ? 1 : 0);
            expect(requests).toHaveLength(3);
        },
    );

    it("renders a historical metadata pane without payload controls or requests", () => {
        const onClose = jest.fn();
        render(<HistorySummary row={summary(1)} onClose={onClose} />);
        expect(screen.getByText("900719925474000001")).toBeInTheDocument();
        expect(
            screen.getByText(
                /Historical payloads and flow actions are not loaded/,
            ),
        ).toBeInTheDocument();
        fireEvent.click(
            screen.getByRole("button", { name: "Close flow details" }),
        );
        expect(onClose).toHaveBeenCalledTimes(1);
        expect(screen.getAllByRole("button")).toHaveLength(1);
        expect(requests).toHaveLength(0);
    });
});
