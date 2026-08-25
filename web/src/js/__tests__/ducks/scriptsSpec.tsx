import reduceScripts, {
    defaultState,
    SCRIPTS_RECEIVE,
    ScriptStatusKind,
} from "../../ducks/scripts";

const tscript = {
    path: "foo.py",
    fullpath: "/tmp/foo.py",
    status: ScriptStatusKind.loaded,
    error: null,
};

describe("scripts reducer", () => {
    it("should have empty default state", () => {
        expect(defaultState).toEqual({ list: [] });
        expect(reduceScripts(undefined, { type: "@@INIT" })).toEqual(
            defaultState,
        );
    });

    it("should receive script status", () => {
        const state = reduceScripts(
            undefined,
            SCRIPTS_RECEIVE([
                tscript,
                { ...tscript, path: "err.py", status: ScriptStatusKind.error },
            ]),
        );
        expect(state.list.length).toBe(2);
        expect(state.list[1].status).toBe(ScriptStatusKind.error);
    });

    it("should ignore unrelated actions", () => {
        expect(reduceScripts(defaultState, { type: "other" })).toBe(
            defaultState,
        );
    });
});
