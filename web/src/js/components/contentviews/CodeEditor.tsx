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
import { SyntaxHighlight } from "../../backends/consts";
import { useResolvedTheme } from "../helpers/useTheme";

type CodeEditorLanguage = SyntaxHighlight | "json" | "python";

type CodeEditorProps = {
    initialContent: string;
    onChange?: (content: string) => void;
    readonly?: boolean;
    language?: CodeEditorLanguage | null;
};

export default function CodeEditor({
    initialContent,
    onChange,
    language,
    readonly = false,
}: CodeEditorProps) {
    const resolvedTheme = useResolvedTheme();
    const stopPropagation = useCallback(
        (e: React.KeyboardEvent<HTMLDivElement>) => e.stopPropagation(),
        [],
    );
    const extensions = useMemo(() => {
        switch (language) {
            case SyntaxHighlight.YAML:
                return [yaml()];
            case SyntaxHighlight.XML:
                return [html()];
            case SyntaxHighlight.JAVASCRIPT:
                return [javascript()];
            case "json":
                return [json()];
            case "python":
                return [python()];
            case SyntaxHighlight.CSS:
                return [css()];
            case undefined:
            case null:
            case SyntaxHighlight.NONE:
            case SyntaxHighlight.ERROR:
                return [];
            /* istanbul ignore next @preserve */
            default: {
                const unexpected: never = language;
                console.error(
                    "Unexpected syntax highlighting language: ",
                    unexpected,
                );
                return [];
            }
        }
    }, [language]);
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
