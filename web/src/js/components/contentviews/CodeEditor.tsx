import * as React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import CodeMirror, {
    type EditorView,
    type ViewUpdate,
} from "@uiw/react-codemirror";
import { forceParsing, language as languageFacet } from "@codemirror/language";
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
    eagerParse?: boolean;
};

const EAGER_PARSE_SLICE_MS = 100;
const EAGER_PARSE_IDLE_TIMEOUT_MS = 100;

type IdleCallbackWindow = Window & {
    requestIdleCallback?: (
        callback: () => void,
        options?: { timeout: number },
    ) => number;
    cancelIdleCallback?: (handle: number) => void;
};

function scheduleEagerParse(callback: () => void): () => void {
    const idleWindow = window as IdleCallbackWindow;
    if (idleWindow.requestIdleCallback) {
        const handle = idleWindow.requestIdleCallback(callback, {
            timeout: EAGER_PARSE_IDLE_TIMEOUT_MS,
        });
        return () => idleWindow.cancelIdleCallback?.(handle);
    }

    const handle = window.setTimeout(callback, 0);
    return () => window.clearTimeout(handle);
}

export default function CodeEditor({
    initialContent,
    onChange,
    language,
    readonly = false,
    eagerParse = false,
}: CodeEditorProps) {
    const resolvedTheme = useResolvedTheme();
    const [editorView, setEditorView] = useState<EditorView>();
    const editorViewRef = useRef<EditorView | undefined>(undefined);
    const [documentVersion, setDocumentVersion] = useState(0);
    const stopPropagation = useCallback(
        (e: React.KeyboardEvent<HTMLDivElement>) => e.stopPropagation(),
        [],
    );
    const onCreateEditor = useCallback((view: EditorView) => {
        editorViewRef.current = view;
        setEditorView(view);
    }, []);
    const onUpdate = useCallback((update: ViewUpdate) => {
        if (update.docChanged) {
            setDocumentVersion((version) => version + 1);
        }
    }, []);

    useEffect(() => {
        if (!eagerParse || !editorView || editorView.state.doc.length === 0) {
            return;
        }

        let cancelled = false,
            cancelScheduled: (() => void) | undefined;
        const documentAtStart = editorView.state.doc;
        const parse = () => {
            cancelScheduled = undefined;
            if (
                cancelled ||
                editorViewRef.current !== editorView ||
                editorView.state.doc !== documentAtStart
            ) {
                return;
            }
            if (editorView.state.facet(languageFacet) === null) {
                return;
            }
            if (
                forceParsing(
                    editorView,
                    editorView.state.doc.length,
                    EAGER_PARSE_SLICE_MS,
                )
            ) {
                return;
            }
            if (
                !cancelled &&
                editorViewRef.current === editorView &&
                editorView.state.doc === documentAtStart
            ) {
                cancelScheduled = scheduleEagerParse(parse);
            }
        };

        // Start immediately, then yield between bounded parsing slices.
        parse();
        return () => {
            cancelled = true;
            cancelScheduled?.();
        };
    }, [documentVersion, eagerParse, editorView, initialContent, language]);

    useEffect(() => {
        return () => {
            editorViewRef.current = undefined;
        };
    }, []);

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
                onCreateEditor={onCreateEditor}
                onUpdate={eagerParse ? onUpdate : undefined}
            />
        </div>
    );
}
