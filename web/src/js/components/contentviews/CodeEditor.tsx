import * as React from "react";
import { useCallback, useMemo } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { css } from "@codemirror/lang-css";
import { html } from "@codemirror/lang-html";
import { javascript } from "@codemirror/lang-javascript";
import { json } from "@codemirror/lang-json";
import { python } from "@codemirror/lang-python";
import { yaml } from "@codemirror/lang-yaml";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";
import { SyntaxHighlight } from "../../backends/consts";
import { useResolvedTheme } from "../helpers/useTheme";

type CodeEditorLanguage = SyntaxHighlight | "json" | "python";

type CodeEditorProps = {
    initialContent: string;
    onChange?: (content: string) => void;
    readonly?: boolean;
    language?: CodeEditorLanguage | null;
    lineWrapping?: boolean;
};

export default function CodeEditor({
    initialContent,
    onChange,
    language,
    readonly = false,
    lineWrapping = false,
}: CodeEditorProps) {
    const resolvedTheme = useResolvedTheme();
    const stopPropagation = useCallback(
        (e: React.KeyboardEvent<HTMLDivElement>) => e.stopPropagation(),
        [],
    );
    const extensions = useMemo(() => {
        let languageExtensions;
        switch (language) {
            case SyntaxHighlight.YAML:
                languageExtensions = [yaml()];
                break;
            case SyntaxHighlight.XML:
                languageExtensions = [html()];
                break;
            case SyntaxHighlight.JAVASCRIPT:
                languageExtensions = [javascript()];
                break;
            case "json":
                languageExtensions = [json()];
                break;
            case "python":
                languageExtensions = [python()];
                break;
            case SyntaxHighlight.CSS:
                languageExtensions = [css()];
                break;
            case undefined:
            case null:
            case SyntaxHighlight.NONE:
            case SyntaxHighlight.ERROR:
                languageExtensions = [];
                break;
            /* istanbul ignore next @preserve */
            default: {
                const unexpected: never = language;
                console.error(
                    "Unexpected syntax highlighting language: ",
                    unexpected,
                );
                languageExtensions = [];
                break;
            }
        }
        return lineWrapping
            ? [...languageExtensions, EditorView.lineWrapping]
            : languageExtensions;
    }, [language, lineWrapping]);
    return (
        <div className="codeeditor" onKeyDown={stopPropagation}>
            <CodeMirror
                value={initialContent}
                onChange={onChange}
                readOnly={readonly}
                extensions={extensions}
                theme={resolvedTheme === "dark" ? oneDark : "light"}
            />
        </div>
    );
}
