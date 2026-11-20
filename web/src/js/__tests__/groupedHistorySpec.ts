import { GroupedHistory } from "../groupedHistory";
import { summary } from "./historyFixtures";

function fixture(size = 1000000) {
    const requests: Record<string, string>[] = [];
    const fetch = async (query: Record<string, string>) => {
        requests.push(query);
        const offset = Number(query.offset ?? 0);
        if (query.lookup_group) {
            const ordinal = Number(query.lookup_group);
            return {
                snapshot: "s1",
                group:
                    Number.isInteger(ordinal) && ordinal < size
                        ? {
                              key: query.lookup_group,
                              label: query.lookup_group,
                              count: 10,
                              ordinal,
                              members_before: ordinal * 10,
                          }
                        : null,
            };
        }
        if (query.group_key) {
            return {
                snapshot: "s1",
                count: 10,
                offset,
                rows: Array.from(
                    { length: Math.min(500, 10 - offset) },
                    (_, i) => summary(i + offset),
                ),
                anchor_position: null,
                selected_eligible: true,
            };
        }
        return {
            snapshot: "s1",
            count: size * 10,
            group_count: size,
            offset,
            groups: Array.from(
                { length: Math.min(500, size - offset) },
                (_, i) => ({
                    key: String(offset + i).padStart(8, "0"),
                    label: String(offset + i),
                    count: 10,
                    ordinal: offset + i,
                    members_before: (offset + i) * 10,
                }),
            ),
        };
    };
    return { model: new GroupedHistory(fetch), requests };
}

test("collapsed million-group jumps fetch only bounded group pages", async () => {
    const { model, requests } = fixture();
    await model.initialize();
    expect(model.count).toBe(1000000);
    const rows = await model.window(900000, 900020);
    expect(rows[0].group.ordinal).toBe(900000);
    expect(rows.every((row) => row.member === null)).toBe(true);
    expect(requests.every((request) => !request.group_key)).toBe(true);
    expect(requests.length).toBeLessThan(25);
    expect(model.groupPages.size).toBeLessThanOrEqual(4);
});

test("expand all uses cumulative counts for arbitrary jumps without eager members", async () => {
    const { model, requests } = fixture();
    await model.initialize();
    model.expandAll(true);
    expect(requests).toHaveLength(1);
    expect(model.count).toBe(11000000);
    const rows = await model.window(9900000, 9900012);
    expect(rows[0].group.ordinal).toBe(900000);
    expect(rows[0].member).toBeNull();
    expect(rows[1].member).toBe(0);
    expect(rows[1].row?.id).toBe(summary(0).id);
    expect(requests.filter((request) => request.group_key)).toHaveLength(1);
    expect(model.groupPages.size).toBeLessThanOrEqual(4);
});

test("sparse exceptions change positions without materializing the group list", async () => {
    const { model } = fixture(100);
    await model.initialize();
    const group = (await model.window(0, 1))[0].group;
    model.toggle(group);
    expect(model.count).toBe(110);
    expect((await model.window(11, 12))[0].group.ordinal).toBe(1);
    model.expandAll(true);
    model.toggle(group);
    expect(model.count).toBe(1090);
    expect((await model.window(1, 2))[0].group.ordinal).toBe(1);
    model.expandAll(false);
    expect(model.exceptions.size).toBe(0);
    expect(model.count).toBe(100);
});

test("member pages remain bounded across unrelated group visits", async () => {
    const { model } = fixture(100);
    await model.initialize();
    model.expandAll(true);
    for (let i = 0; i < 10; i++) await model.window(i * 11 + 1, i * 11 + 2);
    expect(model.memberPages.size).toBeLessThanOrEqual(4);
    expect(await model.find("00000009")).toMatchObject({ ordinal: 9 });
    expect(await model.find("absent")).toBeUndefined();
});

test("nearby scrolling reuses the resolved group page instead of repeating the search", async () => {
    const { model, requests } = fixture();
    await model.initialize();
    await model.window(900000, 900020);
    const count = requests.length;
    await model.window(900020, 900040);
    expect(requests).toHaveLength(count);
});

test("refresh remaps sparse exceptions by key when earlier groups are inserted", async () => {
    let keys = ["b", "c", "missing"];
    const model = new GroupedHistory(async () => ({
        snapshot: "s1",
        count: keys.length * 10,
        group_count: keys.length,
        offset: 0,
        groups: keys.map((key, ordinal) => ({
            key,
            label: key,
            ordinal,
            count: 10,
            members_before: ordinal * 10,
        })),
    }));
    await model.initialize();
    model.toggle((await model.find("c"))!);
    keys = ["a", ...keys];
    await model.initialize();
    expect(model.count).toBe(14);
    expect(model.exceptions.get("c")?.ordinal).toBe(2);
    expect(model.position((await model.find("missing"))!)).toBe(13);
});

test("opaque keys use snapshot-scoped lookup, never key ordering", async () => {
    const key = (ordinal: number) => `g1:${1000 - ordinal}`;
    const model = new GroupedHistory(async (query) => {
        if (query.lookup_group) {
            expect(query.snapshot).toBe("s1");
            return {
                snapshot: "s1",
                group: {
                    key: key(750),
                    label: "750",
                    count: 1,
                    ordinal: 750,
                    members_before: 750,
                },
            };
        }
        const offset = Number(query.offset);
        return {
            snapshot: "s1",
            count: 1000,
            group_count: 1000,
            offset,
            groups: Array.from({ length: 500 }, (_, i) => ({
                key: key(offset + i),
                label: String(offset + i),
                count: 1,
                ordinal: offset + i,
                members_before: offset + i,
            })),
        };
    });
    await model.initialize();
    expect((await model.find(key(750)))?.ordinal).toBe(750);
});

test("members send the opaque key and snapshot unchanged", async () => {
    const requests: Record<string, string>[] = [];
    const model = new GroupedHistory(async (query) => {
        requests.push(query);
        const offset = Number(query.offset);
        if (query.group_key)
            return { snapshot: "s1", count: 1, offset: 0, rows: [summary(0)] };
        return {
            snapshot: "s1",
            count: 1000,
            group_count: 1000,
            offset,
            groups: Array.from({ length: 500 }, (_, i) => ({
                key: `g1:${offset + i}`,
                label: String(offset + i),
                ordinal: offset + i,
                count: 1,
                members_before: offset + i,
            })),
        };
    });
    await model.initialize();
    model.expandAll(true);
    const rows = await model.window(1999, 2000);
    expect(rows[0].row?.id).toBe(summary(0).id);
    expect(requests[requests.length - 1].group_key).toBe("g1:999");
    expect(requests.slice(1).every((query) => query.snapshot === "s1")).toBe(
        true,
    );
});

test("rejects incompatible snapshots before caching rows", async () => {
    const model = new GroupedHistory(async (query) =>
        query.group_key
            ? { snapshot: "other", count: 1, offset: 0, rows: [summary(0)] }
            : {
                  snapshot: "s1",
                  count: 1,
                  group_count: 1,
                  offset: 0,
                  groups: [
                      {
                          key: "g1:opaque",
                          label: "Host",
                          count: 1,
                          ordinal: 0,
                          members_before: 0,
                      },
                  ],
              },
    );
    await model.initialize();
    model.expandAll(true);
    await expect(model.window(1, 2)).rejects.toThrow("snapshot");
    expect(model.memberPages.size).toBe(0);
});

test("rejects changed totals within a snapshot instead of mixing page indices", async () => {
    const model = new GroupedHistory(async (query) => {
        const offset = Number(query.offset);
        return {
            snapshot: "s1",
            count: offset ? 1200 : 1000,
            group_count: offset ? 1200 : 1000,
            offset,
            groups: Array.from({ length: 500 }, (_, i) => ({
                key: `g1:${offset + i}`,
                label: String(offset + i),
                ordinal: offset + i,
                count: 1,
                members_before: offset + i,
            })),
        };
    });
    await model.initialize();
    await expect(model.window(750, 751)).rejects.toThrow("group page");
    expect(model.groups).toBe(1000);
});

test("successor initialization uses refresh once and batches exception lookups", async () => {
    const requests: Record<string, string>[] = [];
    const group = (key: string, ordinal: number) => ({
        key,
        label: key,
        ordinal,
        count: 1,
        members_before: ordinal,
    });
    const model = new GroupedHistory(async (query) => {
        requests.push(query);
        if (query.lookup_groups)
            return { snapshot: "s2", groups: [group("old", 500), null] };
        return {
            snapshot: "s2",
            count: 501,
            group_count: 501,
            offset: 0,
            groups: Array.from({ length: 500 }, (_, i) => group(`new${i}`, i)),
        };
    });
    model.exceptions.set("old", group("old", 0));
    model.exceptions.set("gone", group("gone", 1));
    await model.initialize("s1");
    expect(requests[0]).toEqual({ offset: "0", refresh: "s1" });
    expect(requests[1]).toEqual({
        snapshot: "s2",
        lookup_groups: '["old","gone"]',
    });
    expect(model.exceptions.get("old")?.ordinal).toBe(500);
    expect(model.exceptions.has("gone")).toBe(false);
});
