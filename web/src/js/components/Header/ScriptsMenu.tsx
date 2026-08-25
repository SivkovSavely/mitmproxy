import * as React from "react";
import { useEffect, useState } from "react";
import classnames from "classnames";
import Button from "../common/Button";
import CodeEditor from "../contentviews/CodeEditor";
import HideInStatic from "../common/HideInStatic";
import * as optionsActions from "../../ducks/options";
import type { ScriptStatus } from "../../ducks/scripts";
import { fetchApi } from "../../utils";
import { useAppDispatch, useAppSelector } from "../../ducks";

ScriptsMenu.title = "Scripts";

function basename(path: string): string {
    const idx = path.lastIndexOf("/");
    return idx === -1 ? path : path.slice(idx + 1);
}

function ScriptRow({ script }: { script: ScriptStatus }) {
    const dispatch = useAppDispatch();
    const configuredScripts = useAppSelector((state) => state.options.scripts);
    const [expanded, setExpanded] = useState(false);
    const [source, setSource] = useState<string | null>(null);

    useEffect(() => {
        if (!expanded || source !== null) {
            return;
        }
        let cancelled = false;
        fetchApi(`/scripts/source?path=${encodeURIComponent(script.fullpath)}`)
            .then(async (res) => {
                if (!res.ok) {
                    throw new Error(`status ${res.status}`);
                }
                return res.json();
            })
            .then(
                (json: { source?: string }) =>
                    !cancelled && setSource(json.source ?? ""),
            )
            .catch(() => !cancelled && setSource(""));
        return () => {
            cancelled = true;
        };
    }, [expanded, script.fullpath, source]);

    return (
        <>
            <div className="scripts-row">
                <button
                    className="script-name"
                    title={script.fullpath}
                    onClick={() => setExpanded(!expanded)}
                >
                    {basename(script.path)}
                </button>
                <span
                    className={classnames(
                        "script-status",
                        `script-status-${script.status}`,
                    )}
                >
                    {script.status}
                </span>
                <Button
                    className="btn-sm scripts-remove"
                    title="Remove script"
                    icon="close"
                    iconClassName="text-danger"
                    onClick={() =>
                        dispatch(
                            optionsActions.update(
                                "scripts",
                                configuredScripts.filter(
                                    (path) => path !== script.path,
                                ),
                            ),
                        )
                    }
                />
            </div>
            {expanded && (
                <div className="scripts-detail">
                    {script.error && (
                        <pre className="scripts-error">{script.error}</pre>
                    )}
                    {source === null ? (
                        <div className="scripts-source-status">
                            Loading source...
                        </div>
                    ) : (
                        <CodeEditor
                            initialContent={source}
                            readonly
                            language="python"
                        />
                    )}
                </div>
            )}
        </>
    );
}

export default function ScriptsMenu() {
    const dispatch = useAppDispatch();
    const scripts = useAppSelector((state) => state.scripts.list);
    const configuredScripts = useAppSelector((state) => state.options.scripts);
    const [inputValue, setInputValue] = useState("");

    const addScript = () => {
        const value = inputValue.trim();
        if (!value) {
            return;
        }
        dispatch(optionsActions.update("scripts", [...configuredScripts, value]));
        setInputValue("");
    };

    return (
        <HideInStatic>
            <div className="menu-group scripts-menu">
                <div className="menu-content">
                    {scripts.map((script) => (
                        <ScriptRow key={script.path} script={script} />
                    ))}
                    <div
                        className="scripts-add"
                        onKeyDown={(e) => e.stopPropagation()}
                    >
                        <input
                            type="text"
                            placeholder="/absolute/path/to/script.py"
                            title="Path of the Python script to load"
                            value={inputValue}
                            onChange={(e) => setInputValue(e.target.value)}
                            onKeyDown={(e) => {
                                e.stopPropagation();
                                if (e.key === "Enter") {
                                    addScript();
                                }
                            }}
                        />
                        <Button
                            className="btn-sm"
                            title="Add script"
                            onClick={addScript}
                            disabled={!inputValue.trim()}
                        >
                            Add
                        </Button>
                    </div>
                </div>
                <div className="menu-legend">Python Scripts</div>
            </div>
        </HideInStatic>
    );
}
