// Helpers to compose mitmproxy filter expressions in the UI.

const regexMetacharacters = /[.*+?^${}()|[\]\\]/g;
const filterEscapes = /[\\"]/g;

/** Escape a literal so it can be embedded into a JavaScript RegExp. */
export function escapeRegex(literal: string): string {
    return literal.replace(regexMetacharacters, "\\$&");
}

/**
 * Quote a value for use as a filter expression argument.
 *
 * The backend filter parser treats `\` as the escape character inside quoted
 * strings, so backslashes and quotes need escaping.
 */
export function quoteFilterArgument(value: string): string {
    return `"${value.replace(filterEscapes, "\\$&")}"`;
}

/**
 * A filter matching exactly one serialized header line, e.g.
 * `~hqc "^X-Header: abc\r?$"` (backend header filters run multiline over the
 * CRLF-separated header block).
 */
export function exactHeaderClause(
    part: "request" | "response",
    name: string,
    value: string,
): string {
    const operator = part === "request" ? "~hqc" : "~hsc";
    const line = escapeRegex(`${name}: ${value}`);
    return `${operator} ${quoteFilterArgument(`^${line}\\r?$`)}`;
}

/** AND an additional clause onto the current search filter. */
export function appendFilterClause(current: string, clause: string): string {
    const trimmed = current.trim();
    return trimmed ? `(${trimmed}) & ${clause}` : clause;
}
