import * as React from "react";
import { act, render } from "../../test-utils";
import CodeEditor from "../../../components/contentviews/CodeEditor";

type MockView = {
    state: {
        doc: { length: number };
        facet: () => object;
    };
};

type MockCodeMirrorProps = {
    onCreateEditor?: (view: MockView) => void;
};

let mockEditorView: MockView, mockForceParsing: jest.Mock;

jest.mock("@uiw/react-codemirror", () => {
    const react = jest.requireActual("react");
    return {
        __esModule: true,
        default: ({ onCreateEditor }: MockCodeMirrorProps) => {
            react.useEffect(() => {
                onCreateEditor?.(mockEditorView);
            }, [onCreateEditor]);
            return null;
        },
    };
});

jest.mock("@codemirror/language", () => {
    const actual = jest.requireActual("@codemirror/language");
    return {
        ...actual,
        forceParsing: (...args: unknown[]) => mockForceParsing(...args),
    };
});

function makeView(length: number): MockView {
    return {
        state: {
            doc: { length },
            facet: () => ({}),
        },
    };
}

async function flushEffects() {
    await act(async () => {
        await Promise.resolve();
    });
}

function renderEditor(eagerParse: boolean, view = mockEditorView) {
    mockEditorView = view;
    return render(
        <CodeEditor
            initialContent={"x".repeat(view.state.doc.length)}
            language="json"
            eagerParse={eagerParse}
        />,
    );
}

beforeEach(() => {
    jest.useFakeTimers();
    mockEditorView = makeView(123);
    mockForceParsing = jest.fn().mockReturnValue(true);
    Object.defineProperty(window, "requestIdleCallback", {
        configurable: true,
        value: undefined,
    });
});

afterEach(() => {
    jest.clearAllTimers();
    jest.useRealTimers();
});

test("does not force parsing when eagerParse is false", async () => {
    renderEditor(false);
    await flushEffects();

    expect(mockForceParsing).not.toHaveBeenCalled();
});

test("forces parsing through the complete document", async () => {
    renderEditor(true, makeView(321));
    await flushEffects();

    expect(mockForceParsing).toHaveBeenCalledWith(mockEditorView, 321, 100);
});

test("schedules bounded slices until parsing completes", async () => {
    mockForceParsing
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(true);
    renderEditor(true, makeView(456));
    await flushEffects();

    expect(mockForceParsing).toHaveBeenCalledTimes(1);
    act(() => jest.runOnlyPendingTimers());
    expect(mockForceParsing).toHaveBeenCalledTimes(2);
    act(() => jest.runOnlyPendingTimers());
    expect(mockForceParsing).toHaveBeenCalledTimes(3);
    act(() => jest.runOnlyPendingTimers());
    expect(mockForceParsing).toHaveBeenCalledTimes(3);
});

test("cancels scheduled work when the editor is unmounted", async () => {
    mockForceParsing.mockReturnValue(false);
    const { unmount } = renderEditor(true);
    await flushEffects();

    expect(mockForceParsing).toHaveBeenCalledTimes(1);
    unmount();
    act(() => jest.runOnlyPendingTimers());
    expect(mockForceParsing).toHaveBeenCalledTimes(1);
});
