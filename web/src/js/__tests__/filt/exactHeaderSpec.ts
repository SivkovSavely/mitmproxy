import {
    appendFilterClause,
    escapeRegex,
    exactHeaderClause,
    quoteFilterArgument,
} from "../../filt/exactHeader";

describe("exact header filter helper", () => {
    it("escapes regex metacharacters", () => {
        expect(escapeRegex("a.b[0]+(foo)")).toBe("a\\.b\\[0\\]\\+\\(foo\\)");
        expect(escapeRegex("x^$|?*{}")).toBe("x\\^\\$\\|\\?\\*\\{\\}");
        expect(escapeRegex("plain-value")).toBe("plain-value");
        expect(new RegExp(`^${escapeRegex("a.b")}$`).test("a.b")).toBe(true);
        expect(new RegExp(`^${escapeRegex("a.b")}$`).test("axb")).toBe(false);
    });

    it("escapes filter string quoting", () => {
        // quotes and backslashes are escaped for the filter parser
        expect(quoteFilterArgument('say "hi"')).toBe('"say \\"hi\\""');
        expect(quoteFilterArgument("back\\slash")).toBe('"back\\\\slash"');
        expect(quoteFilterArgument("plain")).toBe('"plain"');
    });

    it("builds exact request/response header clauses", () => {
        // the regex-level backslashes are themselves escaped for the
        // filter string quoting, so the backend sees ^...abc\r?$
        expect(exactHeaderClause("request", "X-Header", "abc")).toBe(
            '~hqc "^X-Header: abc\\\\r?$"',
        );
        expect(exactHeaderClause("response", "Set-Cookie", "a=b")).toBe(
            '~hsc "^Set-Cookie: a=b\\\\r?$"',
        );
    });

    it("keeps regex metacharacters literal in the clause", () => {
        const clause = exactHeaderClause("request", "X-Test", "a.b[0]+(foo)");
        expect(clause).toBe(
            '~hqc "^X-Test: a\\\\.b\\\\[0\\\\]\\\\+\\\\(foo\\\\)\\\\r?$"',
        );
        // after one round of filter-string unescaping, this is the pattern
        const pattern = "^X-Test: a\\.b\\[0\\]\\+\\(foo\\)\\r?$";
        const rex = new RegExp(pattern, "m");
        expect(rex.test("X-Test: a.b[0]+(foo)\r\nNext: v")).toBe(true);
        expect(rex.test("X-Test: axb[0]+(foo)\r\n")).toBe(false);
        expect(rex.test("prefix X-Test: a.b[0]+(foo)\r\n")).toBe(false);
    });

    it("appends to an existing filter with parentheses", () => {
        const clause = exactHeaderClause("request", "X-Header", "abc");
        expect(appendFilterClause("", clause)).toBe(clause);
        expect(appendFilterClause("  ", clause)).toBe(clause);
        expect(appendFilterClause("~d example", clause)).toBe(
            '(~d example) & ~hqc "^X-Header: abc\\\\r?$"',
        );
        // an existing `or` expression must keep its semantics
        expect(appendFilterClause("~d openrouter | ~d example", clause)).toBe(
            "(~d openrouter | ~d example) & " +
                '~hqc "^X-Header: abc\\\\r?$"',
        );
    });
});
