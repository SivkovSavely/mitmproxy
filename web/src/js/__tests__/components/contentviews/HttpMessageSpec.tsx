import { TFlow, TStore, testState } from "../../ducks/tutils";
import * as React from "react";
import HttpMessage, {
    ViewImage,
} from "../../../components/contentviews/HttpMessage";
import { JSON_WORD_WRAP_STORAGE_KEY } from "../../../components/helpers/usePersistentBooleanPreference";
import { act, fireEvent, render, screen, waitFor } from "../../test-utils";
import fetchMock, { enableFetchMocks } from "jest-fetch-mock";

enableFetchMocks();

type CapturedCodeEditorProps = {
    initialContent: string;
    onChange?: (content: string) => void;
    readonly?: boolean;
    language?: string | null;
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
            lineWrapping,
        }: CapturedCodeEditorProps) => {
            if (!mockUseCodeEditor) {
                return actual.default({
                    initialContent,
                    onChange,
                    readonly,
                    language: language as never,
                    lineWrapping,
                });
            }
            mockCapturedOnChange = onChange ?? null;
            mockCapturedProps = {
                initialContent,
                onChange,
                readonly,
                language,
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
    const expectJsonEditor = async (content: string, lineWrapping = false) => {
        await waitFor(() =>
            expect(mockCapturedProps).toMatchObject({
                language: "json",
                readonly: true,
                initialContent: content,
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

        await expectJsonEditor('{"protocol": "openai-chat-completions"}');
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor(
            '{"protocol": "openai-chat-completions"}',
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
        await expectJsonEditor('{"wrapped": true}', true);
        expect(wrap).toHaveAttribute("aria-pressed", "true");
        expect(window.localStorage.getItem(JSON_WORD_WRAP_STORAGE_KEY)).toBe(
            "true",
        );

        fireEvent.click(wrap);
        await expectJsonEditor('{"wrapped": true}', false);
        expect(wrap).toHaveAttribute("aria-pressed", "false");
        expect(window.localStorage.getItem(JSON_WORD_WRAP_STORAGE_KEY)).toBe(
            "false",
        );
    });

    test("remounting a JSON viewer restores the persisted preference", async () => {
        fetchMock.mockResponses(
            JSON.stringify({ text: '{"request": true}', ...cvdJson }),
            JSON.stringify({ text: '{"request": false}', ...cvdJson }),
        );

        const tflow = TFlow();
        const first = render(
            <HttpMessage flow={tflow} message={tflow.request} />,
        );
        await expectJsonEditor('{"request": true}');
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor('{"request": true}', true);
        first.unmount();

        render(<HttpMessage flow={tflow} message={tflow.request} />);
        await expectJsonEditor('{"request": false}', true);
        expect(screen.getByRole("button", { name: "Wrap" })).toHaveAttribute(
            "aria-pressed",
            "true",
        );
    });

    test("request and response viewers share the wrapping preference", async () => {
        fetchMock.mockResponses(
            JSON.stringify({ text: '{"request": true}', ...cvdJson }),
            JSON.stringify({ text: '{"response": true}', ...cvdJson }),
        );

        const tflow = TFlow();
        const view = render(
            <HttpMessage flow={tflow} message={tflow.request} />,
        );
        await expectJsonEditor('{"request": true}');
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor('{"request": true}', true);

        view.rerender(<HttpMessage flow={tflow} message={tflow.response} />);
        await expectJsonEditor('{"response": true}', true);
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
        await waitFor(() => screen.getByText("not json"));
        expect(screen.queryByRole("button", { name: "Wrap" })).toBeNull();

        fireEvent.click(screen.getByText("auto"));
        fireEvent.click(screen.getByText("json"));
        await expectJsonEditor('{"json": true}');
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor('{"json": true}', true);

        fireEvent.click(screen.getByText("json"));
        fireEvent.click(screen.getByText("raw"));
        await waitFor(() => screen.getByText("not json again"));
        expect(screen.queryByRole("button", { name: "Wrap" })).toBeNull();

        fireEvent.click(screen.getByText("raw"));
        fireEvent.click(screen.getByText("json"));
        await expectJsonEditor('{"json": true}', true);
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

    test("truncated json shows maxLines lines plus Show more", async () => {
        const fullJson = '{\n' + '"line",\n'.repeat(512) + '"last"\n}';
        const truncatedJson = '{\n' + '"line",\n'.repeat(512);
        fetchMock.mockResponses(
            JSON.stringify({ text: truncatedJson, ...cvdJson }),
            JSON.stringify({ text: fullJson, ...cvdJson }),
        );

        const tflow = TFlow();
        render(<HttpMessage flow={tflow} message={tflow.request} />);

        await expectJsonEditor(expect.any(String));
        fireEvent.click(screen.getByRole("button", { name: "Wrap" }));
        await expectJsonEditor(expect.any(String), true);
        const editor = screen.getByTestId("mock-editor") as HTMLTextAreaElement;
        expect(editor.defaultValue.split("\n")).toHaveLength(512);
        expect(editor.defaultValue).not.toContain('"last"');

        const contentUrls = () =>
            fetchMock.mock.calls
                .map(([url]) => String(url))
                .filter((url) => url.includes("/content/"));
        expect(contentUrls()).toEqual([
            expect.stringContaining("?lines=513"),
        ]);

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
