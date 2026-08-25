import * as React from "react";
import fetchMock from "jest-fetch-mock";
import { act, fireEvent, render, screen } from "../../test-utils";
import ScriptsMenu from "../../../components/Header/ScriptsMenu";
import { TStore, testState } from "../../ducks/tutils";
import { SCRIPTS_RECEIVE } from "../../../ducks/scripts";
import { ScriptStatusKind } from "../../../ducks/scripts";

// jsdom cannot measure a real CodeMirror instance.
jest.mock("@uiw/react-codemirror", () => {
    return function MockCodeMirror({ value }: { value?: string }) {
        return <div className="mock-codemirror">{value}</div>;
    };
});

const scriptsState = {
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
};

function renderMenu() {
    const store = TStore({
        ...testState,
        options: { ...testState.options, scripts: ["foo.py", "err.py"] },
        scripts: scriptsState,
    });
    const utils = render(<ScriptsMenu />, { store });
    return { store, asFragment: utils.asFragment };
}

function firstFetchCall(): { url: string; body: string; method: string } {
    const [url, init] = fetchMock.mock.calls[0]!;
    return {
        url: String(url),
        method: init?.method ?? "GET",
        body: String(init?.body ?? "{}"),
    };
}

beforeEach(() => {
    fetchMock.enableMocks();
    fetchMock.mockReset();
});

describe("ScriptsMenu Component", () => {
    it("should render script rows and snapshot", () => {
        const { asFragment } = renderMenu();
        expect(screen.getByTitle("/tmp/foo.py")).toBeTruthy();
        expect(screen.getByText("loaded")).toBeTruthy();
        expect(screen.getByText("error")).toBeTruthy();
        expect(asFragment()).toMatchSnapshot();
    });

    it("should show source and error details", async () => {
        fetchMock.mockResponseOnce(JSON.stringify({ source: "x = 1\n" }));
        renderMenu();
        expect(screen.queryByText(/SyntaxError/)).toBeNull();
        await act(() => fireEvent.click(screen.getByText("err.py")));
        expect(screen.getByText(/SyntaxError/)).toBeTruthy();
        const url = String(fetchMock.mock.calls[0]![0]);
        expect(url).toBe("./scripts/source?path=%2Ftmp%2Ferr.py");
        expect(await screen.findByText("x = 1")).toBeTruthy();
        // toggling again hides the detail panel
        await act(() => fireEvent.click(screen.getByText("err.py")));
        expect(screen.queryByText(/SyntaxError/)).toBeNull();
    });

    it("should add a script via the options API", async () => {
        fetchMock.mockResponseOnce("");
        renderMenu();
        fireEvent.change(
            screen.getByPlaceholderText("/absolute/path/to/script.py"),
            { target: { value: "/tmp/new.py" } },
        );
        await act(() => fireEvent.click(screen.getByTitle("Add script")));
        const call = firstFetchCall();
        expect(call.url).toBe("./options");
        expect(call.method).toBe("PUT");
        expect(JSON.parse(call.body)).toEqual({
            scripts: ["foo.py", "err.py", "/tmp/new.py"],
        });
        // input is cleared after adding
        expect(
            (
                screen.getByPlaceholderText(
                    "/absolute/path/to/script.py",
                ) as HTMLInputElement
            ).value,
        ).toBe("");
    });

    it("should remove a script via the options API", async () => {
        fetchMock.mockResponseOnce("");
        renderMenu();
        await act(() =>
            fireEvent.click(screen.getAllByTitle("Remove script")[0]),
        );
        const call = firstFetchCall();
        expect(call.url).toBe("./options");
        expect(JSON.parse(call.body)).toEqual({
            scripts: ["err.py"],
        });
    });

    it("should update rows when the backend broadcasts", async () => {
        const { store } = renderMenu();
        await act(() => {
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
