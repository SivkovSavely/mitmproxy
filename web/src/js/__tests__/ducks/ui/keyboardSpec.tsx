import * as flowsActions from "../../../ducks/flows";
import { onKeyDown } from "../../../ducks/ui/keyboard";
import * as modalActions from "../../../ducks/ui/modal";
import { fetchApi, runCommand } from "../../../utils";
import { TFlow, TStore } from "../tutils";
import { STATE_UPDATE } from "../../../ducks/backendState";
import { selectSummary } from "../../../ducks/history";
import {
    applyDraft,
    establishBackend,
    updateDraft,
} from "../../../ducks/timeRange";
import { FilterName } from "../../../ducks/ui/filter";
import { summary } from "../../historyFixtures";

jest.mock("../../../utils");

describe("onKeyDown", () => {
    const makeStore = () => {
        const store = TStore();
        store.dispatch(flowsActions.FLOWS_RECEIVE([]));
        for (let i = 1; i <= 12; i++) {
            store.dispatch(
                flowsActions.FLOWS_ADD({
                    flow: {
                        ...TFlow(),
                        id: i + "",
                        websocket: undefined,
                        error: undefined,
                        intercepted: true,
                        modified: true,
                    },
                    matching_filters: {},
                }),
            );
        }
        store.dispatch(flowsActions.select([store.getState().flows.list[0]]));
        return store;
    };

    const createKeyEvent = (key, ctrlKey = false) => {
        // @ts-expect-error not a real KeyboardEvent
        return onKeyDown({ key, ctrlKey, preventDefault: jest.fn() });
    };

    afterEach(() => {
        // @ts-expect-error mocking
        fetchApi.mockClear();
    });

    it("should handle cursor up/down", () => {
        const store = makeStore();
        // down
        store.dispatch(createKeyEvent("j"));
        expect(store.getState().flows.selected[0].id).toEqual("2");
        store.dispatch(createKeyEvent("ArrowDown"));
        expect(store.getState().flows.selected[0].id).toEqual("3");

        // up
        store.dispatch(createKeyEvent("k"));
        expect(store.getState().flows.selected[0].id).toEqual("2");
        store.dispatch(createKeyEvent("ArrowUp"));
        expect(store.getState().flows.selected[0].id).toEqual("1");
        store.dispatch(createKeyEvent("ArrowUp"));
        expect(store.getState().flows.selected[0].id).toEqual("1");
    });

    it("should handle scrolling", () => {
        const store = makeStore();
        store.dispatch(createKeyEvent("PageDown"));
        expect(store.getState().flows.selected[0].id).toEqual("11");
        store.dispatch(createKeyEvent("End"));
        expect(store.getState().flows.selected[0].id).toEqual("12");
        store.dispatch(createKeyEvent("PageUp"));
        expect(store.getState().flows.selected[0].id).toEqual("2");
        store.dispatch(createKeyEvent("Home"));
        expect(store.getState().flows.selected[0].id).toEqual("1");
    });

    it("should handle deselect", () => {
        const store = makeStore();
        store.dispatch(createKeyEvent("Escape"));
        expect(store.getState().flows.selected).toEqual([]);
    });

    it("should handle switch to left tab", () => {
        const store = makeStore();
        expect(store.getState().ui.flow.tab).toBe("request");
        store.dispatch(createKeyEvent("ArrowLeft"));
        expect(store.getState().ui.flow.tab).toBe("comment");
    });

    it("should handle switch to right tab", () => {
        const store = makeStore();
        expect(store.getState().ui.flow.tab).toBe("request");
        store.dispatch(createKeyEvent("Tab"));
        expect(store.getState().ui.flow.tab).toBe("response");
        store.dispatch(createKeyEvent("ArrowRight"));
        expect(store.getState().ui.flow.tab).toBe("connection");
    });

    it("should handle delete action", () => {
        const store = makeStore();
        store.dispatch(createKeyEvent("d"));
        expect(fetchApi).toHaveBeenCalledWith("/flows/1", { method: "DELETE" });
    });

    it("should handle create action", () => {
        const store = makeStore();
        store.dispatch(createKeyEvent("n"));
        expect(runCommand).toHaveBeenCalledWith(
            "view.flows.create",
            "get",
            "https://example.com/",
        );
    });

    it("should handle duplicate action", () => {
        const store = makeStore();
        store.dispatch(createKeyEvent("D"));
        expect(fetchApi).toHaveBeenCalledWith("/flows/1/duplicate", {
            method: "POST",
        });
    });

    it("should handle resume action", async () => {
        const store = makeStore();
        await store.dispatch(createKeyEvent("a"));
        expect(fetchApi).toHaveBeenCalledWith("/flows/1/resume", {
            method: "POST",
        });
    });

    it("should handle resume all action", async () => {
        const store = makeStore();
        await store.dispatch(createKeyEvent("A"));
        expect(fetchApi).toHaveBeenCalledWith("/flows/resume", {
            method: "POST",
        });
    });

    it("should handle replay action", () => {
        const store = makeStore();
        store.dispatch(createKeyEvent("r"));
        expect(fetchApi).toHaveBeenCalledWith("/flows/1/replay", {
            method: "POST",
        });
    });

    it("should handle revert action", () => {
        const store = makeStore();
        store.dispatch(createKeyEvent("v"));
        expect(fetchApi).toHaveBeenCalledWith("/flows/1/revert", {
            method: "POST",
        });
    });

    it("should handle kill action", () => {
        const store = makeStore();
        // kill all
        store.dispatch(createKeyEvent("X"));
        expect(fetchApi).toHaveBeenCalledWith("/flows/kill", {
            method: "POST",
        });
        // kill
        store.dispatch(createKeyEvent("x"));
        expect(fetchApi).toHaveBeenCalledWith("/flows/1/kill", {
            method: "POST",
        });
    });

    it("should handle clear action", () => {
        const store = makeStore();
        store.dispatch(createKeyEvent("z"));
        expect(fetchApi).toHaveBeenCalledWith("/clear", { method: "POST" });
    });

    it("should stop on some action with no flow is selected", () => {
        const store = makeStore();
        store.dispatch(flowsActions.select([]));
        store.dispatch(createKeyEvent("ArrowLeft"));
        store.dispatch(createKeyEvent("Tab"));
        store.dispatch(createKeyEvent("ArrowRight"));
        store.dispatch(createKeyEvent("D"));
        expect(fetchApi).not.toHaveBeenCalled();
    });

    it("should do nothing when Ctrl and undefined key is pressed ", () => {
        const store = makeStore();
        store.dispatch(createKeyEvent("Backspace", true));
        store.dispatch(createKeyEvent(0));
        expect(fetchApi).not.toHaveBeenCalled();
    });

    it("should close modal", () => {
        const store = makeStore();
        store.dispatch(modalActions.setActiveModal("OptionModal"));
        expect(store.getState().ui.modal.activeModal).toEqual("OptionModal");
        store.dispatch(createKeyEvent("Escape"));
        expect(store.getState().ui.modal.activeModal).toEqual(undefined);
    });

    describe("global time range", () => {
        const makeRangeStore = () => {
            const store = TStore(null);
            const flows = Array.from({ length: 12 }, (_, index) => ({
                ...TFlow(),
                id: String(index + 1),
                timestamp_created: index % 2 ? 1767225600.5 : 0,
            }));
            store.dispatch(flowsActions.FLOWS_RECEIVE(flows));
            store.dispatch(
                flowsActions.FLOWS_FILTER_UPDATE({
                    name: FilterName.Search,
                    matching_flow_ids: flows
                        .slice(0, -1)
                        .map((flow) => flow.id),
                }),
            );
            store.dispatch(establishBackend("keyboard-test"));
            store.dispatch(
                updateDraft({
                    mode: "explicit",
                    from: "2026-01-01 00:00:00",
                    until: "2026-01-01 00:00:01",
                }),
            );
            store.dispatch(applyDraft());
            return store;
        };

        beforeEach(() => localStorage.clear());

        it.each([
            ["j", "2", "4"],
            ["ArrowDown", "2", "4"],
            ["k", "4", "2"],
            ["ArrowUp", "4", "2"],
            [" ", "2", "10"],
            ["PageDown", "2", "10"],
            ["PageUp", "10", "2"],
            ["Home", "10", "2"],
            ["End", "2", "10"],
            ["ArrowUp", "3", "2"],
            ["ArrowDown", "3", "10"],
        ])(
            "%s from %s navigates only visible rows",
            (key, anchor, expected) => {
                const store = makeRangeStore();
                store.dispatch(
                    flowsActions.select([
                        store.getState().flows.byId.get(anchor)!,
                    ]),
                );
                store.dispatch(createKeyEvent(key));

                expect(
                    store.getState().flows.selected.map((flow) => flow.id),
                ).toEqual([expected]);
                expect(store.getState().flows.list).toHaveLength(12);
                expect(store.getState().flows.view).toHaveLength(11);
            },
        );

        it("does not select hidden flows when the range is empty", () => {
            const store = makeRangeStore();
            store.dispatch(
                flowsActions.FLOWS_FILTER_UPDATE({
                    name: FilterName.Search,
                    matching_flow_ids: ["1", "3"],
                }),
            );
            store.dispatch(createKeyEvent("End"));
            expect(store.getState().flows.selected).toEqual([]);
        });
    });

    it.each([
        "ArrowUp",
        "ArrowDown",
        "PageUp",
        "PageDown",
        "Home",
        "End",
        "j",
        "k",
        " ",
    ])("does not run legacy %s navigation in SQLite history mode", (key) => {
        const store = makeStore();
        store.dispatch(STATE_UPDATE({ sqlite_history: true }));
        const selected = store.getState().flows.selected;
        store.dispatch(createKeyEvent(key));
        expect(store.getState().flows.selected).toEqual(selected);
    });

    it.each([false, true])(
        "Escape closes the summary and live pane even with a focused button (live=%s)",
        async (live) => {
            localStorage.clear();
            const store = makeStore();
            store.dispatch(STATE_UPDATE({ sqlite_history: true }));
            store.dispatch(establishBackend("keyboard-test"));
            store.dispatch(updateDraft({ mode: "explicit" }));
            store.dispatch(applyDraft());
            const row = summary(1, live);
            if (live) {
                jest.mocked(fetchApi).mockResolvedValueOnce({
                    ok: true,
                    json: async () => ({ ...TFlow(), id: row.id }),
                } as Response);
            }
            await store.dispatch(selectSummary(row));
            expect(store.getState().history.selected?.id).toBe(row.id);
            expect(store.getState().flows.selected).toHaveLength(live ? 1 : 0);
            jest.mocked(fetchApi).mockClear();

            const button = document.createElement("button");
            button.addEventListener("keydown", (event) =>
                store.dispatch(onKeyDown(event)),
            );
            button.dispatchEvent(
                new KeyboardEvent("keydown", { key: "Escape" }),
            );

            expect(store.getState().history.selected).toBeNull();
            expect(store.getState().flows.selected).toEqual([]);
            expect(store.getState().flows.list).toEqual([]);
            expect(fetchApi).not.toHaveBeenCalled();
        },
    );

    it("never sends selected-flow mutations for a historical summary", async () => {
        localStorage.clear();
        const store = makeStore();
        store.dispatch(STATE_UPDATE({ sqlite_history: true }));
        store.dispatch(establishBackend("keyboard-test"));
        await store.dispatch(selectSummary(summary(1)));

        for (const key of ["Delete", "d", "D", "a", "r", "v", "x"]) {
            store.dispatch(createKeyEvent(key));
        }
        expect(store.getState().history.selected?.id).toBe("history-1");
        expect(store.getState().flows.selected).toEqual([]);
        expect(fetchApi).not.toHaveBeenCalled();
    });

    it.each(["input", "select", "textarea"])(
        "leaves keys in %s controls to the browser",
        (tag) => {
            const store = makeStore();
            const target = document.createElement(tag);
            target.addEventListener("keydown", (event) =>
                store.dispatch(onKeyDown(event as KeyboardEvent)),
            );
            for (const key of ["a", "ArrowDown", " "]) {
                const event = new KeyboardEvent("keydown", {
                    key,
                    cancelable: true,
                });
                target.dispatchEvent(event);
                expect(event.defaultPrevented).toBe(false);
            }
            expect(store.getState().flows.selected[0].id).toBe("1");
            expect(fetchApi).not.toHaveBeenCalled();
        },
    );

    it.each(["button", "a", "summary"])(
        "preserves native activation and focus keys in %s controls",
        (tag) => {
            const store = makeStore();
            const target = document.createElement(tag);
            if (tag === "a") target.setAttribute("href", "#");
            target.addEventListener("keydown", (event) =>
                store.dispatch(onKeyDown(event as KeyboardEvent)),
            );
            for (const key of ["Enter", " ", "Tab"]) {
                const event = new KeyboardEvent("keydown", {
                    key,
                    cancelable: true,
                });
                target.dispatchEvent(event);
                expect(event.defaultPrevented).toBe(false);
            }
            expect(store.getState().flows.selected[0].id).toBe("1");
            expect(fetchApi).not.toHaveBeenCalled();
        },
    );

    it("keeps live mutation shortcuts available while a button is focused", () => {
        const store = makeStore();
        const button = document.createElement("button");
        button.addEventListener("keydown", (event) =>
            store.dispatch(onKeyDown(event)),
        );
        button.dispatchEvent(new KeyboardEvent("keydown", { key: "d" }));
        expect(fetchApi).toHaveBeenCalledWith("/flows/1", { method: "DELETE" });
    });

    it("leaves keys in nested contenteditable elements to the editor", () => {
        const store = makeStore();
        const editor = document.createElement("div");
        editor.setAttribute("contenteditable", "true");
        const target = document.createElement("span");
        editor.appendChild(target);
        target.addEventListener("keydown", (event) =>
            store.dispatch(onKeyDown(event)),
        );
        const event = new KeyboardEvent("keydown", {
            key: "d",
            cancelable: true,
        });
        target.dispatchEvent(event);
        expect(event.defaultPrevented).toBe(false);
        expect(fetchApi).not.toHaveBeenCalled();
    });

    it("does not repeat a key already handled by a child table", () => {
        const store = makeStore();
        const event = new KeyboardEvent("keydown", {
            key: "ArrowDown",
            cancelable: true,
        });
        event.preventDefault();
        store.dispatch(onKeyDown(event));
        expect(store.getState().flows.selected[0].id).toBe("1");
    });
});
