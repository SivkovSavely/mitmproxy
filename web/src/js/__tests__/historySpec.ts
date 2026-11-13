import {
    HistoryCache,
    historyWindow,
    logicalToPhysical,
    physicalToLogical,
    scrollHeight,
} from "../history";
import { page } from "./historyFixtures";

describe("history scroll coordinates", () => {
    it("represents one million slots with a capped physical height and exact endpoints", () => {
        expect(scrollHeight(1000000, 320)).toBe(1000000);
        expect(logicalToPhysical(0, 1000000, 320)).toBe(0);
        expect(logicalToPhysical(31999680, 1000000, 320)).toBe(999680);
        expect(physicalToLogical(999680, 1000000, 320)).toBe(31999680);
        expect(physicalToLogical(499840, 1000000, 320)).toBe(15999840);
        expect(historyWindow(15999840, 320, 1000000)).toEqual({
            start: 499990,
            end: 500010,
        });
    });

    it("handles short lists and clamps stale positions after a count reduction", () => {
        expect(scrollHeight(2, 320)).toBe(320);
        expect(physicalToLogical(500, 2, 320)).toBe(0);
        expect(logicalToPhysical(500, 2, 320)).toBe(0);
        expect(historyWindow(500, 320, 2)).toEqual({ start: 0, end: 2 });
    });
});

describe("history page cache", () => {
    it("stores sparse metadata independent of logical count and evicts distant pages", () => {
        const cache = new HistoryCache();
        cache.add(page(999500), 999990);
        expect(cache.rows.size).toBe(500);
        expect(cache.rows.get(999999)?.sequence).toBe("900719925474999999");
        expect(cache.rows.has(0)).toBe(false);
        for (const offset of [0, 500, 1000, 1500, 2000]) {
            cache.add(page(offset), 2000);
        }
        expect(cache.rows.size).toBe(2000);
        expect(cache.pages).toHaveLength(4);
        expect(cache.rows.has(999999)).toBe(false);
        expect(cache.rows.has(0)).toBe(false);
        expect(cache.rows.get(2000)?.id).toBe("history-2000");
        cache.clear();
        expect(cache.rows.size).toBe(0);
    });
});
