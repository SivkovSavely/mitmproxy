import { TFlow, TStore, testState } from "../../ducks/tutils";
import * as React from "react";
import HttpMessage, {
    ViewImage,
} from "../../../components/contentviews/HttpMessage";
import { JSON_WORD_WRAP_STORAGE_KEY } from "../../../components/helpers/usePersistentBooleanPreference";
import { act, fireEvent, render, screen, waitFor } from "../../test-utils";
import { setContentViewFor } from "../../../ducks/ui/flow";
import fetchMock, { enableFetchMocks } from "jest-fetch-mock";
import { subscribe, receive } from "../../../ducks/responsePreview";

enableFetchMocks();

test("live response bypasses normal content fetching and preserves incremental SSE", () => {
    fetchMock.resetMocks();
    const flow = TFlow();
    Object.assign(flow.response, {
        stream_preview: true,
        timestamp_end: undefined,
        contentLength: 0,
    });
    const store = TStore();
    store.dispatch({
        type: "responsePreview/subscribe",
        payload: { flow_id: flow.id, subscription: 1 },
    });
    const preview = {
        flow_id: flow.id,
        subscription: 1,
        seq: 1,
        start: 0,
        end: 0,
        received: 0,
        dropped: 0,
        missing: false,
        status: "waiting",
        reason: null,
        text: "",
        reset: true,
        stored: false,
        max_bytes: 32,
    };
    store.dispatch({ type: "responsePreview/receive", payload: preview });
    render(<HttpMessage flow={flow} message={flow.response} />, { store });
    expect(screen.getByText(/Waiting for response bytes/)).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
    act(() => {
        store.dispatch({
            type: "responsePreview/receive",
            payload: {
                ...preview,
                seq: 2,
                end: 9,
                received: 9,
                status: "streaming",
                reset: false,
                text: "data: a\n\n",
            },
        });
    });
    const body = screen.getByLabelText("Live response text");
    expect(body.textContent).toBe("data: a\n\n");
    body.scrollTop = 4;
    const firstNode = body.firstChild;
    act(() => {
        store.dispatch({
            type: "responsePreview/receive",
            payload: {
                ...preview,
                seq: 3,
                start: 9,
                end: 18,
                received: 18,
                status: "streaming",
                reset: false,
                text: "data: b\n\n",
            },
        });
    });
    expect(body.textContent).toBe("data: a\n\ndata: b\n\n");
    expect(body.firstChild).toBe(firstNode);
    expect(body.scrollTop).toBe(4);
    expect(screen.getByText(/18 bytes received/)).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
});

describe("live response lifecycle", () => {
    function liveFlow() {
        const flow = TFlow();
        Object.assign(flow.response, {
            stream_preview: true,
            timestamp_end: undefined,
            contentHash: undefined,
            contentLength: 0,
        });
        return flow;
    }
    const preview = {
        flow_id: "",
        subscription: 1,
        seq: 1,
        start: 0,
        end: 4,
        received: 4,
        dropped: 0,
        missing: false,
        status: "streaming" as const,
        reason: null,
        text: "live",
        reset: true,
        stored: false,
        max_bytes: 8,
    };

    beforeEach(() => fetchMock.resetMocks());

    test("nonstored terminal preview survives canonical completion until session changes", () => {
        const flow = liveFlow();
        const store = TStore();
        store.dispatch(subscribe({ flow_id: flow.id, subscription: 1 }));
        store.dispatch(receive({ ...preview, flow_id: flow.id }));
        const view = render(
            <HttpMessage flow={flow} message={flow.response} />,
            { store },
        );
        const completed = {
            ...flow,
            response: {
                ...flow.response,
                timestamp_end: 100,
                stream_preview: false,
                contentLength: undefined,
            },
        };
        view.rerender(
            <HttpMessage flow={completed} message={completed.response} />,
        );
        expect(screen.getByLabelText("Live response text").textContent).toBe(
            "live",
        );
        act(() => {
            store.dispatch(
                receive({
                    ...preview,
                    flow_id: flow.id,
                    seq: 2,
                    status: "finished",
                    start: 4,
                    text: "",
                    reset: false,
                }),
            );
        });
        expect(
            screen.getByText("Finished response preview"),
        ).toBeInTheDocument();
        expect(screen.getByText(/Full body not retained/)).toBeInTheDocument();
        expect(screen.getByLabelText("Live response text").textContent).toBe(
            "live",
        );
        expect(fetchMock).not.toHaveBeenCalled();
    });

    test("stored terminal response returns to the existing content viewer", async () => {
        fetchMock.mockResponse(
            JSON.stringify({
                view_name: "Raw",
                description: "",
                text: "full body",
                syntax_highlight: "none",
            }),
        );
        const flow = liveFlow();
        const store = TStore();
        store.dispatch(subscribe({ flow_id: flow.id, subscription: 1 }));
        store.dispatch(receive({ ...preview, flow_id: flow.id }));
        const view = render(
            <HttpMessage flow={flow} message={flow.response} />,
            { store },
        );
        expect(fetchMock).not.toHaveBeenCalled();
        act(() => {
            store.dispatch(
                receive({
                    ...preview,
                    flow_id: flow.id,
                    seq: 2,
                    status: "finished",
                    stored: true,
                    start: 4,
                    text: "",
                    reset: false,
                }),
            );
        });
        expect(screen.getByLabelText("Live response text").textContent).toBe(
            "live",
        );
        expect(fetchMock).not.toHaveBeenCalled();
        const completed = {
            ...flow,
            response: {
                ...flow.response,
                stream_preview: false,
                timestamp_end: 100,
                contentLength: 9,
                contentHash: "complete",
            },
        };
        view.rerender(
            <HttpMessage flow={completed} message={completed.response} />,
        );
        await waitFor(() =>
            expect(screen.getByText("full body")).toBeInTheDocument(),
        );
        expect(screen.queryByLabelText("Live response text")).toBeNull();
        expect(screen.getByText("Copy")).toBeInTheDocument();
        expect(screen.getByText("Edit")).toBeInTheDocument();
    });

    test("stored aborted response keeps partial text and aborted status after canonical completion", () => {
        const flow = liveFlow();
        const store = TStore();
        store.dispatch(subscribe({ flow_id: flow.id, subscription: 1 }));
        store.dispatch(
            receive({
                ...preview,
                flow_id: flow.id,
                status: "aborted",
                stored: true,
                reason: "connection lost",
            }),
        );
        const view = render(
            <HttpMessage flow={flow} message={flow.response} />,
            { store },
        );
        const completed = {
            ...flow,
            response: {
                ...flow.response,
                stream_preview: false,
                timestamp_end: 100,
                contentHash: "partial",
                contentLength: 4,
            },
        };
        view.rerender(
            <HttpMessage flow={completed} message={completed.response} />,
        );
        expect(
            screen.getByText("Aborted response preview"),
        ).toBeInTheDocument();
        expect(screen.getByLabelText("Live response text").textContent).toBe(
            "live",
        );
        expect(fetchMock).not.toHaveBeenCalled();
    });

    test.each(["finished", "streaming"] as const)(
        "completed canonical body replaces %s nonstored preview",
        async (status) => {
            fetchMock.mockResponse(
                JSON.stringify({
                    view_name: "Raw",
                    description: "",
                    text: "replacement",
                    syntax_highlight: "none",
                }),
            );
            const flow = liveFlow();
            const store = TStore();
            store.dispatch(subscribe({ flow_id: flow.id, subscription: 1 }));
            store.dispatch(receive({ ...preview, flow_id: flow.id, status }));
            const view = render(
                <HttpMessage flow={flow} message={flow.response} />,
                { store },
            );
            expect(fetchMock).not.toHaveBeenCalled();
            const completed = {
                ...flow,
                response: {
                    ...flow.response,
                    stream_preview: false,
                    timestamp_end: 100,
                    contentHash: "replacement",
                    contentLength: 11,
                },
            };
            view.rerender(
                <HttpMessage flow={completed} message={completed.response} />,
            );
            await waitFor(() =>
                expect(screen.getByText("replacement")).toBeInTheDocument(),
            );
            expect(screen.queryByLabelText("Live response text")).toBeNull();
        },
    );

    test("stored finished preview waits for both completion and canonical body metadata", () => {
        const flow = liveFlow();
        const store = TStore();
        store.dispatch(subscribe({ flow_id: flow.id, subscription: 1 }));
        store.dispatch(
            receive({
                ...preview,
                flow_id: flow.id,
                status: "finished",
                stored: true,
            }),
        );
        const view = render(
            <HttpMessage flow={flow} message={flow.response} />,
            { store },
        );
        for (const response of [
            {
                ...flow.response,
                stream_preview: false,
                timestamp_end: undefined,
            },
            { ...flow.response, stream_preview: true, timestamp_end: 100 },
            {
                ...flow.response,
                stream_preview: false,
                timestamp_end: 100,
                contentLength: undefined,
                contentHash: undefined,
            },
        ]) {
            const updated = { ...flow, response };
            view.rerender(<HttpMessage flow={updated} message={response} />);
            expect(
                screen.getByLabelText("Live response text").textContent,
            ).toBe("live");
            expect(fetchMock).not.toHaveBeenCalled();
        }
    });

    test("reports bounded tails, missing source bytes and unsupported/aborted reasons", () => {
        const flow = liveFlow();
        const store = TStore();
        store.dispatch(subscribe({ flow_id: flow.id, subscription: 1 }));
        store.dispatch(
            receive({
                ...preview,
                flow_id: flow.id,
                start: 20,
                end: 32,
                received: 32,
                dropped: 20,
                missing: true,
                text: "abcdefghijkl",
            }),
        );
        render(<HttpMessage flow={flow} message={flow.response} />, { store });
        expect(screen.getByLabelText("Live response text").textContent).toBe(
            "efghijkl",
        );
        expect(
            screen.getByText(/Dropped source bytes: 20/),
        ).toBeInTheDocument();
        expect(
            screen.getByText(/limited to 8 UTF-8 bytes/),
        ).toBeInTheDocument();
        act(() => {
            store.dispatch(
                receive({
                    ...preview,
                    flow_id: flow.id,
                    seq: 2,
                    start: 32,
                    end: 32,
                    received: 32,
                    status: "aborted",
                    reason: "Unsupported content encoding: gzip",
                    text: "",
                    reset: false,
                }),
            );
        });
        expect(
            screen.getByText("Aborted response preview"),
        ).toBeInTheDocument();
        expect(
            screen.getByText(/Unsupported content encoding: gzip/),
        ).toBeInTheDocument();
        expect(fetchMock).not.toHaveBeenCalled();
    });

    test("a different flow or cleared session uses normal content, never stale preview", async () => {
        fetchMock.mockResponse(
            JSON.stringify({
                view_name: "Raw",
                description: "",
                text: "normal",
                syntax_highlight: "none",
            }),
        );
        const flow = liveFlow();
        const other = { ...TFlow(), id: "other" };
        const store = TStore();
        store.dispatch(subscribe({ flow_id: flow.id, subscription: 1 }));
        store.dispatch(receive({ ...preview, flow_id: flow.id }));
        const view = render(
            <HttpMessage flow={flow} message={flow.response} />,
            { store },
        );
        view.rerender(<HttpMessage flow={other} message={other.response} />);
        await waitFor(() =>
            expect(screen.getByText("normal")).toBeInTheDocument(),
        );
        expect(screen.queryByLabelText("Live response text")).toBeNull();
        act(() => {
            store.dispatch(subscribe({ flow_id: null, subscription: 2 }));
        });
        const completed = TFlow();
        view.rerender(
            <HttpMessage flow={completed} message={completed.response} />,
        );
        await waitFor(() =>
            expect(screen.getByText("normal")).toBeInTheDocument(),
        );
        expect(screen.queryByLabelText("Live response text")).toBeNull();
    });
});

type CapturedCodeEditorProps = {
    initialContent: string;
    onChange?: (content: string) => void;
    readonly?: boolean;
    language?: string | null;
    eagerParse?: boolean;
    lineWrapping?: boolean;
};

let mockUseCodeEditor = false,
    mockCapturedOnChange: ((content: string) => void) | null = null,
    mockCapturedProps: CapturedCodeEditorProps | null = null;

jest.mock("../../../components/contentviews/CodeEditor", () => {
    const actual = jest.requireActual(
        "../../../components/contentviews/CodeEditor",
    );
    return {
        __esModule: true,
        default: ({
            initialContent,
            onChange,
            readonly,
            language,
            eagerParse,
            lineWrapping,
        }: CapturedCodeEditorProps) => {
            if (!mockUseCodeEditor) {
                return actual.default({
                    initialContent,
                    onChange,
                    readonly,
                    language: language as never,
                    eagerParse,
                    lineWrapping,
                });
            }
            mockCapturedOnChange = onChange ?? null;
            mockCapturedProps = {
                initialContent,
                onChange,
                readonly,
                language,
                eagerParse,
                lineWrapping,
            };
            return (
                <textarea
                    data-testid="mock-editor"
                    defaultValue={initialContent}
                />
            );
        },
    };
});

test("HttpMessage", async () => {
    const text = "data\n".repeat(512) + "additional\n".repeat(512);

    const cvd = {
        view_name: "Raw",
        description: "",
        syntax_highlight: "none",
    };

    fetchMock.mockResponses(
        JSON.stringify({
            text: "data\n".repeat(512) + "additional\n",
            ...cvd,
        }),
        JSON.stringify({
            text,
            ...cvd,
        }),
        JSON.stringify({
            text: "rawdata\n".repeat(5),
            ...cvd,
        }),
        "raw content",
        JSON.stringify({
            text: "rawdata\n".repeat(5),
            ...cvd,
        }),
    );

    const tflow = TFlow();
    const { asFragment } = render(
        <HttpMessage flow={tflow} message={tflow.request} />,
    );
    await waitFor(() => screen.getAllByText("data"));
    expect(screen.queryByText("additional")).toBeNull();
    expect(screen.queryByRole("button", { name: "Wrap" })).toBeNull();

    fireEvent.click(screen.getByText("Show more"));
    await waitFor(() => screen.getAllByText("additional"));

    fireEvent.click(screen.getByText("auto"));
    fireEvent.click(screen.getByText("raw"));
    await waitFor(() => screen.getAllByText("rawdata"));
    expect(asFragment()).toMatchSnapshot();

    fireEvent.click(screen.getByText("Edit"));
    expect(asFragment()).toMatchSnapshot();
    fireEvent.click(screen.getByText("Cancel"));

    await waitFor(() => screen.getAllByText("rawdata"));
    expect(asFragment()).toMatchSnapshot();

    await waitFor(() => screen.getByText("Copy"));
    expect(asFragment()).toMatchSnapshot();
});

test("ViewImage", async () => {
    const flow = TFlow();
    const { asFragment } = render(
        <ViewImage flow={flow} message={flow.request} />,
    );
    expect(asFragment()).toMatchSnapshot();
});

test("ViewImage.matches", () => {
    const flow = TFlow();
    const matches = (contentType: string) => {
        flow.response.headers = [["Content-Type", contentType]];
        return ViewImage.matches(flow.response);
    };
    expect(matches("image/png")).toBe(true);
    expect(matches("image/jpeg")).toBe(true);
    expect(matches("image/jpg")).toBe(true);
    expect(matches("image/gif")).toBe(true);
    expect(matches("image/webp")).toBe(true);
    expect(matches("image/avif")).toBe(true);
    expect(matches("image/svg+xml")).toBe(true);
    expect(matches("image/vnd.microsoft.icon")).toBe(true);
    expect(matches("image/x-icon")).toBe(true);
    expect(matches("IMAGE/AVIF")).toBe(true);
    expect(matches("image/heic")).toBe(false);
    expect(matches("application/json")).toBe(false);
    expect(matches("video/mp4")).toBe(false);
});

/*
    This test differs from the one above because clicking the copy button triggers 'handleClickCopyButton'.
    In the previous test, the response contained "raw content," which caused an "invalid JSON response body" error
    when processing the following line:
    `const data: ContentViewData = await response.json()`
    since "raw content" is not valid JSON.
*/
describe("HttpMessage Copy Button", () => {
    beforeEach(() => {
        fetchMock.resetMocks();
        jest.spyOn(console, "error").mockImplementation(() => {});
    });

    test("handles successful copy action", async () => {
        jest.spyOn(console, "warn").mockImplementation(() => {});

        const text = "data\nadditional\n";
        fetchMock.mockResponse(JSON.stringify({ text, description: "Auto" }));

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.request} />);

        await waitFor(() => screen.getByText("Copy"));

        fireEvent.click(screen.getByText("Copy"));
    });

    test("handles failed fetch with non-ok response", async () => {
        fetchMock.mockResponse("", {
            status: 500,
            statusText: "Internal Server Error",
        });

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.request} />);

        await waitFor(() => screen.getByText("Copy"));
        fireEvent.click(screen.getByText("Copy"));

        await waitFor(() =>
            expect(console.error).toHaveBeenCalledWith(expect.any(Error)),
        );
    });
});

describe("HttpMessage body edit", () => {
    const cvd = { view_name: "Raw", description: "", syntax_highlight: "none" };

    beforeEach(() => {
        mockUseCodeEditor = true;
        mockCapturedOnChange = null;
        mockCapturedProps = null;
        fetchMock.resetMocks();
    });

    afterEach(() => {
        mockUseCodeEditor = false;
        mockCapturedOnChange = null;
        mockCapturedProps = null;
    });

    test("saving empty body sends empty string, not original content", async () => {
        fetchMock.mockResponses(
            JSON.stringify({ text: "original body", ...cvd }),
            "original body",
            JSON.stringify({}),
        );

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.request} />);
        await waitFor(() => screen.getAllByText("original body"));

        fireEvent.click(screen.getByText("Edit"));
        await waitFor(() => screen.getByText("Done"));

        expect(mockCapturedOnChange).not.toBeNull();
        act(() => mockCapturedOnChange!(""));

        fireEvent.click(screen.getByText("Done"));

        await waitFor(() => {
            const putCall = fetchMock.mock.calls.find(
                ([, opts]) => opts && (opts as RequestInit).method === "PUT",
            );
            expect(putCall).toBeDefined();
            const body = JSON.parse(putCall![1]!.body as string);
            expect(body.request.content).toBe("");
        });
    });

    test("saving unedited body sends original content", async () => {
        fetchMock.mockResponses(
            JSON.stringify({ text: "original body", ...cvd }),
            "original body",
            JSON.stringify({}),
        );

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.request} />);
        await waitFor(() => screen.getAllByText("original body"));

        fireEvent.click(screen.getByText("Edit"));
        await waitFor(() => {
            const editor = screen.getByTestId(
                "mock-editor",
            ) as HTMLTextAreaElement;
            expect(editor.defaultValue).toBe("original body");
        });
        fireEvent.click(screen.getByText("Done"));

        await waitFor(() => {
            const putCall = fetchMock.mock.calls.find(
                ([, opts]) => opts && (opts as RequestInit).method === "PUT",
            );
            expect(putCall).toBeDefined();
            const body = JSON.parse(putCall![1]!.body as string);
            expect(body.request.content).toBe("original body");
        });
    });
});

describe("HttpMessage JSON view", () => {
    const cvdJson = {
        view_name: "JSON",
        description: "",
        syntax_highlight: "yaml",
    };
    const expectJsonEditor = async (
        content: string,
        eagerParse = true,
        lineWrapping = false,
    ) => {
        await waitFor(() =>
            expect(mockCapturedProps).toMatchObject({
                language: "json",
                readonly: true,
                initialContent: content,
                eagerParse,
                lineWrapping,
            }),
        );
    };

    beforeEach(() => {
        mockUseCodeEditor = true;
        mockCapturedOnChange = null;
        mockCapturedProps = null;
        window.localStorage.clear();
        fetchMock.resetMocks();
    });

    afterEach(() => {
        mockUseCodeEditor = false;
        mockCapturedOnChange = null;
        mockCapturedProps = null;
        window.localStorage.clear();
    });

    test("renders request bodies read-only in json mode", async () => {
        fetchMock.mockResponse(
            JSON.stringify({ text: '{"a": [1, 2]}', ...cvdJson }),
        );

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.request} />);

        await expectJsonEditor('{"a": [1, 2]}');
        const wrap = screen.getByRole("button", { name: "Wrap" });
        expect(wrap).toHaveAttribute("title", "Toggle JSON word wrapping");
        expect(wrap).toHaveAttribute("aria-pressed", "false");
        expect(screen.queryByText("Show more")).toBeNull();
    });

    test("renders response bodies read-only in json mode", async () => {
        fetchMock.mockResponse(
            JSON.stringify({ text: '{"b": {"c": 3}}', ...cvdJson }),
        );

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.response} />);

        await expectJsonEditor('{"b": {"c": 3}}');
    });

    test("renders AI Stream view read-only in json mode", async () => {
        fetchMock.mockResponse(
            JSON.stringify({
                text: '{"protocol": "openai-chat-completions"}',
                view_name: "AI Stream",
                description: "",
                syntax_highlight: "yaml",
            }),
        );

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.response} />);

        await expectJsonEditor(
            '{"protocol": "openai-chat-completions"}',
            false,
        );
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor(
            '{"protocol": "openai-chat-completions"}',
            false,
            true,
        );
        expect(screen.queryByText("Show more")).toBeNull();
    });

    test("toggles and persists JSON wrapping immediately", async () => {
        fetchMock.mockResponse(
            JSON.stringify({ text: '{"wrapped": true}', ...cvdJson }),
        );

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.request} />);
        await expectJsonEditor('{"wrapped": true}');

        const wrap = screen.getByRole("button", { name: "Wrap" });
        fireEvent.click(wrap);
        await expectJsonEditor('{"wrapped": true}', true, true);
        expect(wrap).toHaveAttribute("aria-pressed", "true");
        expect(window.localStorage.getItem(JSON_WORD_WRAP_STORAGE_KEY)).toBe(
            "true",
        );

        fireEvent.click(wrap);
        await expectJsonEditor('{"wrapped": true}', true, false);
        expect(wrap).toHaveAttribute("aria-pressed", "false");
        expect(window.localStorage.getItem(JSON_WORD_WRAP_STORAGE_KEY)).toBe(
            "false",
        );
    });

    test("remounting a JSON viewer restores the persisted preference", async () => {
        fetchMock.mockResponse(
            JSON.stringify({ text: '{"request": true}', ...cvdJson }),
        );

        const tflow = TFlow();
        const first = render(
            <HttpMessage flow={tflow} message={tflow.request} />,
        );
        await expectJsonEditor('{"request": true}');
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor('{"request": true}', true, true);
        first.unmount();

        render(<HttpMessage flow={tflow} message={tflow.request} />);
        await waitFor(() => screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor('{"request": true}', true, true);
        expect(screen.getByRole("button", { name: "Wrap" })).toHaveAttribute(
            "aria-pressed",
            "true",
        );
    });

    test("request and response viewers share the wrapping preference", async () => {
        fetchMock.mockResponse(
            JSON.stringify({ text: '{"body": true}', ...cvdJson }),
        );

        const tflow = TFlow();
        const view = render(
            <HttpMessage flow={tflow} message={tflow.request} />,
        );
        await expectJsonEditor('{"body": true}');
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor('{"body": true}', true, true);

        view.rerender(<HttpMessage flow={tflow} message={tflow.response} />);
        await expectJsonEditor('{"body": true}', true, true);
    });

    test("switching away from JSON and back restores wrapping", async () => {
        fetchMock.mockResponses(
            JSON.stringify({
                text: "not json",
                view_name: "Raw",
                description: "",
                syntax_highlight: "none",
            }),
            JSON.stringify({ text: '{"json": true}', ...cvdJson }),
            JSON.stringify({
                text: "not json again",
                view_name: "Raw",
                description: "",
                syntax_highlight: "none",
            }),
            JSON.stringify({ text: '{"json": true}', ...cvdJson }),
        );
        const tflow = TFlow();
        const state = {
            ...testState,
            backendState: {
                ...testState.backendState,
                contentViews: [...testState.backendState.contentViews, "JSON"],
            },
            ui: {
                ...testState.ui,
                flow: {
                    ...testState.ui.flow,
                    contentViewFor: { [tflow.id + "request"]: "Raw" },
                },
            },
        };
        render(<HttpMessage flow={tflow} message={tflow.request} />, {
            store: TStore(state),
        });
        await waitFor(() => screen.getByText("not json"));
        expect(screen.queryByRole("button", { name: "Wrap" })).toBeNull();

        fireEvent.click(screen.getByText("raw"));
        fireEvent.click(screen.getByText("json"));
        await expectJsonEditor('{"json": true}');
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor('{"json": true}', true, true);

        fireEvent.click(screen.getByText("json"));
        fireEvent.click(screen.getByText("raw"));
        await waitFor(() => screen.getByText("not json again"));
        expect(screen.queryByRole("button", { name: "Wrap" })).toBeNull();

        fireEvent.click(screen.getByText("raw"));
        fireEvent.click(screen.getByText("json"));
        await expectJsonEditor('{"json": true}', true, true);
    });

    test("switching Auto -> JSON uses the json viewer despite the yaml hint", async () => {
        fetchMock.mockResponses(
            JSON.stringify({
                text: "not json\nsecond line",
                view_name: "Raw",
                description: "",
                syntax_highlight: "none",
            }),
            JSON.stringify({ text: '{"d": 4}', ...cvdJson }),
        );
        const state = {
            ...testState,
            backendState: {
                ...testState.backendState,
                contentViews: [...testState.backendState.contentViews, "JSON"],
            },
        };

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.request} />, {
            store: TStore(state),
        });
        await waitFor(() => screen.getAllByText("not json"));
        expect(mockCapturedProps).toBeNull();

        fireEvent.click(screen.getByText("auto"));
        fireEvent.click(screen.getByText("json"));

        await expectJsonEditor('{"d": 4}');
    });

    test("eager JSON fetches the complete document without a line limit", async () => {
        const text = "{\n" + '"line",\n'.repeat(600) + '"last"\n}';
        fetchMock.mockResponse(JSON.stringify({ text, ...cvdJson }));
        const tflow = TFlow();
        const state = {
            ...testState,
            backendState: {
                ...testState.backendState,
                contentViews: [...testState.backendState.contentViews, "JSON"],
            },
            ui: {
                ...testState.ui,
                flow: {
                    ...testState.ui.flow,
                    contentViewFor: { [tflow.id + "request"]: "JSON" },
                },
            },
        };

        render(<HttpMessage flow={tflow} message={tflow.request} />, {
            store: TStore(state),
        });

        await expectJsonEditor(text);
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor(text, true, true);
        expect(screen.queryByText("Show more")).toBeNull();
        const contentUrls = fetchMock.mock.calls
            .map(([url]) => String(url))
            .filter((url) => url.includes("/content/"));
        expect(contentUrls).toHaveLength(1);
        expect(contentUrls[0]).not.toContain("?lines=");
    });

    test("threshold 0 keeps the JSON cutoff behavior", async () => {
        const text = "{\n" + '"line",\n'.repeat(600) + '"last"\n}';
        fetchMock.mockResponses(
            JSON.stringify({
                text: "{\n" + '"line",\n'.repeat(600),
                ...cvdJson,
            }),
            JSON.stringify({ text, ...cvdJson }),
        );
        const tflow = TFlow();
        const state = {
            ...testState,
            options: {
                ...testState.options,
                web_json_eager_parse_max_bytes: 0,
            },
        };

        render(<HttpMessage flow={tflow} message={tflow.request} />, {
            store: TStore(state),
        });

        await expectJsonEditor(expect.any(String), false);
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor(expect.any(String), false, true);
        expect(screen.getByText("Show more")).toBeTruthy();
        const contentUrls = fetchMock.mock.calls
            .map(([url]) => String(url))
            .filter((url) => url.includes("/content/"));
        expect(contentUrls[0]).toContain("?lines=513");
    });

    test("non-JSON content below the threshold keeps the line cutoff", async () => {
        const text = "data\n".repeat(600);
        fetchMock.mockResponse(
            JSON.stringify({
                text,
                view_name: "Raw",
                description: "",
                syntax_highlight: "none",
            }),
        );
        const tflow = TFlow();

        render(<HttpMessage flow={tflow} message={tflow.request} />);

        await waitFor(() =>
            expect(screen.getAllByText("data")[0]).toBeTruthy(),
        );
        expect(screen.queryByTestId("mock-editor")).toBeNull();
        expect(screen.getByText("Show more")).toBeTruthy();
        const contentUrls = fetchMock.mock.calls
            .map(([url]) => String(url))
            .filter((url) => url.includes("/content/"));
        expect(contentUrls[0]).toContain("?lines=513");
    });

    test("Auto JSON is refetched without a line limit", async () => {
        const bounded = "{\n" + '"line",\n'.repeat(20);
        const full = bounded + '"last"\n}';
        fetchMock.mockResponses(
            JSON.stringify({ text: bounded, ...cvdJson }),
            JSON.stringify({ text: full, ...cvdJson }),
        );
        const tflow = TFlow();
        const state = {
            ...testState,
            ui: {
                ...testState.ui,
                flow: {
                    ...testState.ui.flow,
                    contentViewFor: { [tflow.id + "request"]: "auto" },
                },
            },
        };

        render(<HttpMessage flow={tflow} message={tflow.request} />, {
            store: TStore(state),
        });

        await expectJsonEditor(full);
        expect(screen.queryByText("Show more")).toBeNull();
        const contentUrls = fetchMock.mock.calls
            .map(([url]) => String(url))
            .filter((url) => url.includes("/content/"));
        expect(contentUrls).toHaveLength(2);
        expect(contentUrls[0]).toContain("?lines=513");
        expect(contentUrls[1]).not.toContain("?lines=");
    });

    test("does not reuse eager content across flows or content views", async () => {
        const flow1 = TFlow();
        const flow2 = TFlow();
        flow2.id = "second-flow";
        const json1 = '{"flow": 1}';
        const json2 = '{"flow": 2}';
        const raw = "raw second flow";
        fetchMock.mockResponses(
            JSON.stringify({ text: json1, ...cvdJson }),
            JSON.stringify({ text: json1, ...cvdJson }),
            JSON.stringify({ text: json2, ...cvdJson }),
            JSON.stringify({ text: json2, ...cvdJson }),
            JSON.stringify({
                text: raw,
                view_name: "Raw",
                description: "",
                syntax_highlight: "none",
            }),
        );
        const store = TStore();
        const { rerender } = render(
            <HttpMessage flow={flow1} message={flow1.request} />,
            { store },
        );

        await expectJsonEditor(json1);
        rerender(<HttpMessage flow={flow2} message={flow2.request} />);
        await expectJsonEditor(json2);
        expect(mockCapturedProps?.initialContent).not.toBe(json1);

        act(() => {
            store.dispatch(
                setContentViewFor({
                    messageId: flow2.id + "request",
                    contentView: "Raw",
                }),
            );
        });
        await waitFor(() => expect(screen.getByText(raw)).toBeTruthy());
        expect(screen.queryByTestId("mock-editor")).toBeNull();
        expect(screen.queryByText(json2)).toBeNull();
    });

    test("does not reuse eager content after the body hash changes", async () => {
        const flow = TFlow();
        const first = '{"hash": 1}';
        const second = '{"hash": 2}';
        fetchMock.mockResponses(
            JSON.stringify({ text: first, ...cvdJson }),
            JSON.stringify({ text: second, ...cvdJson }),
        );
        const state = {
            ...testState,
            ui: {
                ...testState.ui,
                flow: {
                    ...testState.ui.flow,
                    contentViewFor: { [flow.id + "request"]: "JSON" },
                },
            },
        };
        const { rerender } = render(
            <HttpMessage flow={flow} message={flow.request} />,
            { store: TStore(state) },
        );

        await expectJsonEditor(first);
        flow.request.contentHash = "new-content-hash";
        rerender(<HttpMessage flow={flow} message={flow.request} />);
        await expectJsonEditor(second);
        expect(mockCapturedProps?.initialContent).not.toBe(first);
    });

    test("truncated json shows maxLines lines plus Show more", async () => {
        const fullJson = "{\n" + '"line",\n'.repeat(512) + '"last"\n}';
        const truncatedJson = "{\n" + '"line",\n'.repeat(512);
        fetchMock.mockResponses(
            JSON.stringify({ text: truncatedJson, ...cvdJson }),
            JSON.stringify({ text: fullJson, ...cvdJson }),
        );

        const tflow = TFlow();
        tflow.request.contentLength = 2097153;
        render(<HttpMessage flow={tflow} message={tflow.request} />);

        await expectJsonEditor(expect.any(String), false);
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor(expect.any(String), false, true);
        const editor = screen.getByTestId("mock-editor") as HTMLTextAreaElement;
        expect(editor.defaultValue.split("\n")).toHaveLength(512);
        expect(editor.defaultValue).not.toContain('"last"');

        const contentUrls = () =>
            fetchMock.mock.calls
                .map(([url]) => String(url))
                .filter((url) => url.includes("/content/"));
        expect(contentUrls()).toEqual([expect.stringContaining("?lines=513")]);

        fireEvent.click(screen.getByText("Show more"));
        await waitFor(() => {
            const grown = screen.getByTestId(
                "mock-editor",
            ) as HTMLTextAreaElement;
            expect(grown.defaultValue).toContain('"last"');
            expect(mockCapturedProps?.lineWrapping).toBe(true);
        });
        expect(screen.queryByText("Show more")).toBeNull();

        const urlsAfter = contentUrls();
        expect(urlsAfter).toHaveLength(2);
        expect(urlsAfter[1]).toContain("?lines=10513");
    });
});
