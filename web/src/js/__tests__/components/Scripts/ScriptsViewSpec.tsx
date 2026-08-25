import * as React from "react";
import fetchMock from "jest-fetch-mock";
import { act, fireEvent, render, screen } from "../../test-utils";
import ScriptsView from "../../../components/Scripts/ScriptsView";
import { TStore, testState } from "../../ducks/tutils";
import { SCRIPTS_RECEIVE, ScriptStatusKind } from "../../../ducks/scripts";

// jsdom cannot measure a real CodeMirror instance; expose an edit hook.
jest.mock("@uiw/react-codemirror", () => {
    return function MockCodeMirror({
        value,
        onChange,
    }: {
        value?: string;
        onChange?: (v: string) => void;
    }) {
        return (
            <div>
                <div className="mock-codemirror">{value}</div>
                <button onClick={() => onChange?.("edited")}>mock-edit</button>
            </div>
        );
    };
});

const scriptsState = () => ({
    list: [
        {
            path: "foo.py",
            fullpath: "/tmp/foo.py",
            status: ScriptStatusKind.loaded,
            error: null,
        },
        {
            path: "err.py",
            fullpath: "/tmp/err.py",
            status: ScriptStatusKind.error,
            error: "Traceback (most recent call last):\nSyntaxError: bad code",
        },
    ],
});

function renderView() {
    const store = TStore({
        ...testState,
        options: { ...testState.options, scripts: ["foo.py", "err.py"] },
        scripts: scriptsState(),
    });
    const utils = render(<ScriptsView />, { store });
    return utils;
}

function firstFetchCall() {
    const [url, init] = fetchMock.mock.calls[0]!;
    return {
        url: String(url),
        method: init?.method ?? "GET",
        body: String(init?.body ?? ""),
    };
}

beforeEach(() => {
    fetchMock.enableMocks();
    fetchMock.resetMocks();
});

describe("Scripts workspace landing view", () => {
    it("renders a card per script with name, path and status", () => {
        renderView();
        expect(screen.getByText("foo.py")).toBeTruthy();
        expect(screen.getByTitle("/tmp/foo.py")).toBeTruthy();
        expect(screen.getByText("err.py")).toBeTruthy();
        expect(screen.getAllByText("loaded")).toHaveLength(1);
        expect(screen.getAllByText("error")).toHaveLength(1);
        // no editor yet
        expect(document.querySelector(".mock-codemirror")).toBeNull();
    });

    it("adds a script via the compact Add Script action", async () => {
        fetchMock.mockResponseOnce("");
        const { container } = renderView();
        // the path input is hidden until requested
        expect(
            screen.queryByPlaceholderText("/absolute/path/to/script.py"),
        ).toBeNull();
        await act(() => fireEvent.click(screen.getByText("Add Script")));
        fireEvent.change(
            screen.getByPlaceholderText("/absolute/path/to/script.py"),
            { target: { value: "/tmp/new.py" } },
        );
        await act(() =>
            fireEvent.click(screen.getByTitle("Add script")),
        );
        const call = firstFetchCall();
        expect(call.url).toBe("./options");
        expect(call.method).toBe("PUT");
        expect(JSON.parse(call.body)).toEqual({
            scripts: ["foo.py", "err.py", "/tmp/new.py"],
        });
        expect(container.querySelector(".scripts-add-form")).toBeNull();
    });

    it("removes a script without selecting it", async () => {
        fetchMock.mockResponseOnce("");
        renderView();
        await act(() => fireEvent.click(screen.getAllByTitle("Remove script")[0]));
        expect(fetchMock.mock.calls[0]![0]).toBe("./options");
        expect(JSON.parse(String(fetchMock.mock.calls[0]![1]?.body))).toEqual({
            scripts: ["err.py"],
        });
        expect(document.querySelector(".scripts-editor")).toBeNull();
    });
});

describe("Script editor view", () => {
    const renderEditor = async (source = "x = 1\n") => {
        fetchMock.mockResponseOnce(JSON.stringify({ source }));
        renderView();
        await act(() => fireEvent.click(screen.getByText("foo.py")));
        await screen.findByText(source.trim());
    };

    it("opens the source in an editor instead of the flow view", async () => {
        await renderEditor();
        expect(
            document.querySelectorAll(".scripts-editor .codeeditor"),
        ).toHaveLength(1);
        expect(screen.getByTitle("Save the script source to disk").hasAttribute("disabled")).toBe(true);
    });

    it("tracks dirty state and saves via PUT", async () => {
        await renderEditor();
        fetchMock.mockResponseOnce(JSON.stringify({ path: "/tmp/foo.py" }));

        await act(async () => fireEvent.click(screen.getByText("mock-edit")));
        expect(
            screen.getByTitle("Save the script source to disk").hasAttribute("disabled"),
        ).toBe(false);

        await act(async () =>
            fireEvent.click(screen.getByTitle("Save the script source to disk")),
        );
        const [, init] = fetchMock.mock.calls[1]!;
        expect(String(fetchMock.mock.calls[1]![0])).toBe(
            "./scripts/source?path=%2Ftmp%2Ffoo.py",
        );
        expect(init?.method).toBe("PUT");
        expect(String(init?.body)).toBe("edited");
        // saved: the button is disabled again
        expect(
            screen.getByTitle("Save the script source to disk").hasAttribute("disabled"),
        ).toBe(true);
    });

    it("goes back to the card list", async () => {
        await renderEditor();
        fireEvent.click(screen.getByTitle("Back to the script list"));
        expect(screen.getByText("foo.py")).toBeTruthy();
        expect(document.querySelector(".scripts-editor")).toBeNull();
    });

    it("shows the per-script console below the editor", async () => {
        fetchMock.mockResponseOnce(JSON.stringify({ source: "x = 1\n" }));
        renderView();
        await act(() => fireEvent.click(screen.getByText("err.py")));
        await screen.findByText(/SyntaxError/);
        expect(
            document.querySelector(".scripts-console-header")?.textContent,
        ).toBe("Console");
        const view = document.querySelector(".scripts-view")!;
        const children = Array.from(view.children).map((el) =>
            el.className.toString(),
        );
        // the console comes last, after the editor area
        expect(children.indexOf("scripts-console")).toBeGreaterThan(
            children.indexOf("scripts-editor"),
        );
    });

    it("updates the editor's status badge on backend broadcasts", async () => {
        const { store } = renderView();
        fetchMock.mockResponseOnce(JSON.stringify({ source: "x = 1\n" }));
        await act(() => fireEvent.click(screen.getByText("foo.py")));
        await screen.findByText(/x = 1/);
        await act(async () => {
            store.dispatch(
                SCRIPTS_RECEIVE([
                    {
                        path: "foo.py",
                        fullpath: "/tmp/foo.py",
                        status: ScriptStatusKind.loading,
                        error: null,
                    },
                ]),
            );
        });
        expect(screen.getByText("loading")).toBeTruthy();
    });
});
