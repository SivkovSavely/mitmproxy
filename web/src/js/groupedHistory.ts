import type { HistoricalSummary, HistoryPage } from "./history";
import { PAGE_SIZE } from "./history";

export type GroupKey = string;
export type Group = {
    key: GroupKey;
    label: string;
    count: number;
    ordinal: number;
    members_before: number;
};
export type GroupPage = {
    snapshot: string;
    count: number;
    group_count: number;
    offset: number;
    groups: Group[];
};
export type GroupRow = {
    group: Group;
    member: number | null;
    row?: HistoricalSummary;
};

export class StaleSnapshot extends Error {
    constructor() {
        super("History snapshot expired");
    }
}

// Both caches are LRU page caches. Expansion state is independent of eviction.
function remember<T>(
    cache: Map<string | number, T>,
    key: string | number,
    value: T,
) {
    cache.delete(key);
    cache.set(key, value);
    if (cache.size > 4) cache.delete(cache.keys().next().value!);
    return value;
}

export class GroupedHistory {
    groupPages = new Map<string | number, GroupPage>();
    memberPages = new Map<string | number, HistoryPage>();
    exceptions = new Map<GroupKey, Group>();
    expanded = false;
    groups = 0;
    flows = 0;
    snapshot?: string;

    constructor(
        private fetch: (extra: Record<string, string>) => Promise<unknown>,
    ) {}

    get count() {
        let count = this.groups + (this.expanded ? this.flows : 0);
        for (const group of this.exceptions.values())
            count += (this.expanded ? -1 : 1) * group.count;
        return count;
    }

    isExpanded(group: Group) {
        return this.expanded !== this.exceptions.has(group.key);
    }

    expandAll(expanded: boolean) {
        this.expanded = expanded;
        this.exceptions.clear();
    }

    toggle(group: Group) {
        if (this.exceptions.has(group.key)) this.exceptions.delete(group.key);
        else this.exceptions.set(group.key, group);
    }

    position(group: Group) {
        let position =
            group.ordinal + (this.expanded ? group.members_before : 0);
        for (const exception of this.exceptions.values())
            if (exception.ordinal < group.ordinal)
                position += (this.expanded ? -1 : 1) * exception.count;
        return position;
    }

    async initialize(refresh?: string) {
        const keys = [...this.exceptions.keys()];
        this.groupPages.clear();
        this.memberPages.clear();
        this.exceptions.clear();
        this.snapshot = undefined;
        this.groups = this.flows = 0;
        await this.page(0, refresh);
        const cached = new Map(
            [...this.groupPages.values()].flatMap((page) =>
                page.groups.map((group) => [group.key, group] as const),
            ),
        );
        const missing = keys.filter((key) => !cached.has(key));
        for (let i = 0; i < missing.length; i += 128) {
            const batch = missing.slice(i, i + 128);
            const result = (await this.request({
                lookup_groups: JSON.stringify(batch),
            })) as unknown as { groups: (Group | null)[] };
            if (
                !Array.isArray(result.groups) ||
                result.groups.length !== batch.length
            )
                throw new Error("Invalid history group lookup");
            result.groups.forEach((group, index) => {
                if (group === null) return;
                if (!this.validGroup(group) || group.key !== batch[index])
                    throw new Error("Invalid history group lookup");
                cached.set(group.key, group);
            });
        }
        for (const key of keys) {
            const group = cached.get(key);
            if (group) this.exceptions.set(key, group);
        }
    }

    private async request(extra: Record<string, string>) {
        const result = (await this.fetch({
            ...extra,
            ...(this.snapshot ? { snapshot: this.snapshot } : {}),
        })) as GroupPage;
        if (typeof result.snapshot !== "string" || !result.snapshot)
            throw new Error("Missing history snapshot");
        if (this.snapshot && result.snapshot !== this.snapshot)
            throw new StaleSnapshot();
        this.snapshot = result.snapshot;
        return result;
    }

    private validGroup(group: Group) {
        return (
            group &&
            typeof group.key === "string" &&
            typeof group.label === "string" &&
            Number.isSafeInteger(group.ordinal) &&
            group.ordinal >= 0 &&
            group.ordinal < this.groups &&
            Number.isSafeInteger(group.count) &&
            group.count > 0 &&
            Number.isSafeInteger(group.members_before) &&
            group.members_before >= 0 &&
            group.members_before + group.count <= this.flows
        );
    }

    private async page(ordinal: number, refresh?: string) {
        const offset = Math.floor(ordinal / PAGE_SIZE) * PAGE_SIZE;
        const cached = this.groupPages.get(offset);
        if (cached) return remember(this.groupPages, offset, cached);
        const page = await this.request({
            offset: String(offset),
            ...(refresh ? { refresh } : {}),
        });
        if (
            (this.groupPages.size > 0 &&
                (page.count !== this.flows ||
                    page.group_count !== this.groups)) ||
            !Number.isSafeInteger(page.count) ||
            page.count < 0 ||
            !Number.isSafeInteger(page.group_count) ||
            page.group_count < 0 ||
            page.offset !== offset ||
            !Array.isArray(page.groups) ||
            page.groups.length !==
                Math.min(PAGE_SIZE, Math.max(0, page.group_count - offset)) ||
            page.groups.some(
                (group, i) =>
                    typeof group.key !== "string" ||
                    typeof group.label !== "string" ||
                    group.ordinal !== offset + i ||
                    !Number.isSafeInteger(group.count) ||
                    group.count <= 0 ||
                    !Number.isSafeInteger(group.members_before) ||
                    group.members_before < 0 ||
                    group.members_before + group.count > page.count,
            )
        )
            throw new Error("Invalid history group page");
        this.groups = page.group_count;
        this.flows = page.count;
        return remember(this.groupPages, offset, page);
    }

    private async group(ordinal: number) {
        const page = await this.page(ordinal);
        return page.groups[ordinal - page.offset];
    }

    async find(key: GroupKey): Promise<Group | undefined> {
        for (const page of this.groupPages.values()) {
            const cached = page.groups.find((group) => group.key === key);
            if (cached) return cached;
        }
        const result = (await this.request({
            lookup_group: key,
        })) as unknown as { group: Group | null };
        if (result.group === null) return undefined;
        if (!this.validGroup(result.group) || result.group.key !== key)
            throw new Error("Invalid history group lookup");
        return result.group;
    }

    async findFlow(id: string) {
        const result = (await this.request({ lookup_flow: id })) as unknown as {
            group: Group | null;
            member_ordinal: number | null;
        };
        if (result.group === null && result.member_ordinal === null)
            return undefined;
        if (
            !this.validGroup(result.group!) ||
            !Number.isSafeInteger(result.member_ordinal) ||
            result.member_ordinal! < 0 ||
            result.member_ordinal! >= result.group!.count
        )
            throw new Error("Invalid history flow lookup");
        return { group: result.group!, member: result.member_ordinal! };
    }

    async locate(index: number): Promise<Group | undefined> {
        for (const page of this.groupPages.values()) {
            for (const group of page.groups) {
                const position = this.position(group);
                if (
                    position <= index &&
                    index <=
                        position + (this.isExpanded(group) ? group.count : 0)
                )
                    return group;
            }
        }
        let low = 0,
            high = this.groups - 1,
            found: Group | undefined;
        while (low <= high) {
            const middle = Math.floor((low + high) / 2);
            const group = await this.group(middle);
            if (this.position(group) <= index) {
                found = group;
                low = middle + 1;
            } else high = middle - 1;
        }
        return found;
    }

    async members(group: Group, offset: number) {
        const key = JSON.stringify([group.key, offset]);
        const cached = this.memberPages.get(key);
        if (cached) return remember(this.memberPages, key, cached);
        const page = (await this.request({
            group_key: group.key,
            offset: String(offset),
        })) as unknown as HistoryPage;
        if (
            !Number.isSafeInteger(page.count) ||
            page.count !== group.count ||
            !Number.isSafeInteger(page.offset) ||
            page.offset !== offset ||
            !Array.isArray(page.rows) ||
            page.rows.length !==
                Math.min(PAGE_SIZE, Math.max(0, group.count - offset))
        )
            throw new Error("Invalid history member page");
        return remember(
            this.memberPages,
            JSON.stringify([group.key, page.offset]),
            page,
        );
    }

    async window(start: number, end: number): Promise<GroupRow[]> {
        const rows: GroupRow[] = [];
        let group = await this.locate(start);
        while (group && start < Math.min(end, this.count)) {
            const position = this.position(group);
            const last = position + (this.isExpanded(group) ? group.count : 0);
            let page: HistoryPage | undefined;
            while (start <= last && start < end) {
                const member = start === position ? null : start - position - 1;
                if (
                    member !== null &&
                    (!page || member >= page.offset + page.rows.length)
                )
                    page = await this.members(
                        group,
                        Math.floor(member / PAGE_SIZE) * PAGE_SIZE,
                    );
                rows.push({
                    group,
                    member,
                    row:
                        member === null
                            ? undefined
                            : page!.rows[member - page!.offset],
                });
                start++;
            }
            group =
                group.ordinal + 1 < this.groups
                    ? await this.group(group.ordinal + 1)
                    : undefined;
        }
        return rows;
    }
}
