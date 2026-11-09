import reducer, { subscribe, receive } from "../../ducks/responsePreview";

const preview = {
    flow_id: "one",
    subscription: 1,
    seq: 1,
    start: 0,
    end: 0,
    received: 0,
    dropped: 0,
    missing: false,
    status: "waiting" as const,
    reason: null,
    text: "",
    reset: true,
    stored: false,
    max_bytes: 8,
};

test("a skipped materialized sequence marks missing data even with contiguous offsets", () => {
    let state = reducer(
        undefined,
        subscribe({ flow_id: "one", subscription: 1 }),
    );
    state = reducer(state, receive({ ...preview, text: "one", end: 3 }));
    state = reducer(
        state,
        receive({
            ...preview,
            seq: 3,
            start: 3,
            end: 6,
            text: "two",
            reset: false,
        }),
    );
    expect(state.preview?.missing).toBe(true);
    expect(state.chunks.map((chunk) => chunk.text).join("")).toBe("onetwo");
});

test("rejects stale flow, subscription and sequence without changing preview", () => {
    let state = reducer(
        undefined,
        subscribe({ flow_id: "one", subscription: 1 }),
    );
    state = reducer(state, receive(preview));
    for (const update of [
        { flow_id: "other", seq: 2 },
        { subscription: 0, seq: 2 },
        { seq: 1 },
        { seq: 0 },
    ]) {
        expect(
            reducer(state, receive({ ...preview, ...update, text: "stale" })),
        ).toBe(state);
    }
    state = reducer(state, subscribe({ flow_id: "two", subscription: 2 }));
    expect(
        reducer(state, receive({ ...preview, seq: 99, text: "old flow" })),
    ).toBe(state);
    expect(state.chunks).toEqual([]);
});

test("bounds UTF-8 bytes without splitting Unicode code points", () => {
    let state = reducer(
        undefined,
        subscribe({ flow_id: "one", subscription: 1 }),
    );
    state = reducer(state, receive({ ...preview, text: "a😀é", end: 7 }));
    state = reducer(
        state,
        receive({
            ...preview,
            seq: 2,
            reset: false,
            start: 7,
            end: 11,
            text: "tail",
            received: 11,
        }),
    );
    expect(state.chunks.map((chunk) => chunk.text).join("")).toBe("étail");
    expect(state.bytes).toBe(6);
    expect(state.capped).toBe(true);
    state = reducer(
        state,
        receive({
            ...preview,
            seq: 3,
            reset: false,
            start: 11,
            end: 11,
            text: "",
            max_bytes: 0,
        }),
    );
    expect(state.bytes).toBe(0);
    expect(state.chunks).toEqual([]);
});

test("gaps and resets discard obsolete chunks and preserve the missing notice", () => {
    let state = reducer(
        undefined,
        subscribe({ flow_id: "one", subscription: 1 }),
    );
    state = reducer(state, receive({ ...preview, text: "before", end: 6 }));
    state = reducer(
        state,
        receive({
            ...preview,
            seq: 2,
            reset: false,
            start: 20,
            end: 24,
            text: "tail",
        }),
    );
    expect(state.chunks.map((chunk) => chunk.text).join("")).toBe("tail");
    expect(state.preview?.missing).toBe(true);
    state = reducer(
        state,
        receive({
            ...preview,
            seq: 3,
            start: 24,
            end: 27,
            text: "new",
            dropped: 24,
        }),
    );
    expect(state.chunks.map((chunk) => chunk.text).join("")).toBe("new");
    expect(state.preview?.missing).toBe(true);
    expect(state.preview?.dropped).toBe(24);
    state = reducer(state, subscribe({ flow_id: null, subscription: 2 }));
    expect(state.preview).toBeNull();
    expect(state.bytes).toBe(0);
});
