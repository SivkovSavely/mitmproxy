import * as React from "react";
import { EditorView } from "@codemirror/view";
import CodeEditor from "../../../components/contentviews/CodeEditor";
import { render } from "../../test-utils";

type MockCodeMirrorProps = {
    extensions?: readonly unknown[];
};

let mockExtensions: readonly unknown[] = [];

jest.mock("@uiw/react-codemirror", () => ({
    __esModule: true,
    default: ({ extensions }: MockCodeMirrorProps) => {
        mockExtensions = extensions ?? [];
        return <div data-testid="mock-code-mirror" />;
    },
}));

test("CodeEditor does not enable line wrapping by default", () => {
    render(<CodeEditor initialContent="foo" />);

    expect(mockExtensions).not.toContain(EditorView.lineWrapping);
});

test("CodeEditor adds and removes native line wrapping dynamically", () => {
    const { rerender } = render(
        <CodeEditor initialContent="foo" lineWrapping={false} />,
    );
    expect(mockExtensions).not.toContain(EditorView.lineWrapping);

    rerender(<CodeEditor initialContent="foo" lineWrapping />);
    expect(mockExtensions).toContain(EditorView.lineWrapping);

    rerender(<CodeEditor initialContent="foo" lineWrapping={false} />);
    expect(mockExtensions).not.toContain(EditorView.lineWrapping);
});
