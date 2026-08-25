import Filt from "../../filt/filt";

const CASE_SENSITIVE_OPERATORS = [
    ["~bc", "body matches"],
    ["~bqc", "body request matches"],
    ["~bsc", "body response matches"],
    ["~tc", "content type matches"],
    ["~tqc", "req. content type matches"],
    ["~tsc", "resp. content type matches"],
    ["~hc", "header matches"],
    ["~hqc", "req. header matches"],
    ["~hsc", "resp. header matches"],
    ["~mc", "method matches"],
    ["~dc", "domain matches"],
    ["~uc", "url matches"],
    ["~srcc", "source address matches"],
    ["~dstc", "destination address matches"],
    ["~metac", "flow metadata matches"],
    ["~markerc", "marker matches"],
    ["~commentc", "comment matches"],
];

describe("case-sensitive filter variants", () => {
    it.each(CASE_SENSITIVE_OPERATORS)("parses %s", (operator, descPrefix) => {
        const parsed = Filt.parse(`${operator} foo`);
        expect(parsed.desc.startsWith(descPrefix)).toBe(true);
        // no "i" flag: case-sensitive
        expect(parsed.desc).toContain("/foo/");
        expect(parsed.desc).not.toContain("/foo/i");
    });

    it("keeps plain operators case-insensitive", () => {
        expect(Filt.parse("~u foo").desc).toBe("url matches /foo/i");
        expect(Filt.parse("~hq foo").desc).toBe("req. header matches /foo/i");
        expect(Filt.parse("~bs foo").desc).toBe("body response matches /foo/i");
    });

    it("parses longer operators before their prefixes", () => {
        expect(Filt.parse("~bqc abc").desc).toContain("body request");
        expect(Filt.parse("~hqc abc").desc).toContain("req. header");
        expect(Filt.parse("~srcc abc").desc).toContain("source address");
        // prefixes still work on their own
        expect(Filt.parse("~bq abc").desc).toBe(
            "body request matches /abc/i",
        );
    });

    it("combines with boolean operators", () => {
        expect(Filt.parse("~uc foo & ~dc bar").desc).toBe(
            "url matches /foo/ and domain matches /bar/",
        );
        expect(Filt.parse("!~hqc x").desc).toBe("not req. header matches /x/");
        expect(Filt.parse("(~uc a | ~b b)").desc).toBe(
            "(url matches /a/ or body matches /b/i)",
        );
    });
});

describe("timestamp filters", () => {
    const operators = [
        ["~dt", "any http timestamp"],
        ["~dtq", "request timestamp"],
        ["~dts", "response timestamp"],
        ["~dtqs", "request start time"],
        ["~dtqe", "request end time"],
        ["~dtss", "response start time"],
        ["~dtse", "response end time"],
    ];

    it.each(operators)("parses %s", (operator, field) => {
        expect(Filt.parse(`${operator} >= 2026-05-20`).desc).toBe(
            `${field} >= 2026-05-20`,
        );
    });

    it.each(["=", "!=", ">", ">=", "<", "<="])(
        "parses comparator %s",
        (comparator) => {
            expect(Filt.parse(`~dtqs ${comparator} 2026-05-20`).desc).toBe(
                `request start time ${comparator} 2026-05-20`,
            );
        },
    );

    it.each([
        "2026-05-20",
        "2026-05-20 00:00:00",
        "2026-05-20T00:00:00",
        "2026-05-20T12:30:45.123456",
        "2026-05-20T12:30:45Z",
        "2026-05-20 12:30:45+05:00",
        "2026-05-20T12:30:45.123456-08:00",
    ])("accepts datetime %s", (datetime) => {
        expect(Filt.parse(`~dtqs >= ${datetime}`).desc).toBe(
            `request start time >= ${datetime}`,
        );
    });

    it("accepts quoted datetimes", () => {
        expect(Filt.parse('~dtqs >= "2026-05-20 00:00:00"').desc).toBe(
            "request start time >= 2026-05-20 00:00:00",
        );
        expect(Filt.parse("~dtqs <= '2026-05-20'").desc).toBe(
            "request start time <= 2026-05-20",
        );
    });

    it("supports range queries", () => {
        expect(
            Filt.parse("~dtqs >= 2026-05-20 & ~dtqs < 2026-06-11").desc,
        ).toBe(
            "request start time >= 2026-05-20 and request start time < 2026-06-11",
        );
        expect(Filt.parse("~dtqs >= 2026-05-20 ~dtqs < 2026-06-11").desc).toBe(
            "request start time >= 2026-05-20 and request start time < 2026-06-11",
        );
    });

    it.each([
        "~dtqs 2026-05-20", // missing comparator
        "~dtqs == 2026-05-20",
        "~dtqs => 2026-05-20",
        "~dtqs > yesterday",
        "~dtqs > 2026-5-20",
        "~dtqs > 2026-05-20 25:00:00x",
        "~dt >=",
    ])("rejects invalid expression %s", (expr) => {
        expect(() => Filt.parse(expr)).toThrow();
    });
});
