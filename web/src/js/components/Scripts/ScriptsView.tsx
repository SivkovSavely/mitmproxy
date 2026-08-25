import * as React from "react";
import { useEffect, useState } from "react";
import classnames from "classnames";

import { useAppDispatch, useAppSelector } from "../../ducks";
import type { ScriptStatus } from "../../ducks/scripts";
import { ScriptStatusKind } from "../../ducks/scripts";
import * as optionsActions from "../../ducks/options";
import { fetchApi } from "../../utils";
import Button from "../common/Button";
import HideInStatic from "../common/HideInStatic";
import Icon from "../common/Icon";
import CodeEditor from "../contentviews/CodeEditor";

function basename(path: string): string {
    const idx = path.lastIndexOf("/");
    return idx === -1 ? path : path.slice(idx + 1);
}

function StatusBadge({ status }: { status: ScriptStatus["status"] }) {
    return (
        <span className={classnames("script-status", `script-status-${status}`)}>
            {status}
        </span>
    );
}

function AddScriptForm({ onDone }: { onDone?: () => void }) {
    const dispatch = useAppDispatch();
    const configuredScripts = useAppSelector((state) => state.options.scripts);
    const [inputValue, setInputValue] = useState("");

    const addScript = () => {
        const value = inputValue.trim();
        if (!value) {
            return;
        }
        dispatch(
            optionsActions.update("scripts", [...configuredScripts, value]),
        );
        setInputValue("");
        onDone?.();
    };

    return (
        <div
            className="scripts-add-form"
            onKeyDown={(e) => e.stopPropagation()}
        >
            <input
                autoFocus
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
                    if (e.key === "Escape") {
                        setInputValue("");
                        onDone?.();
                    }
                }}
            />
            <Button
                title="Add script"
                onClick={addScript}
                disabled={!inputValue.trim()}
            >
                Add
            </Button>
        </div>
    );
}

function ScriptCard({
    script,
    onSelect,
}: {
    script: ScriptStatus;
    onSelect: (script: ScriptStatus) => void;
}) {
    const dispatch = useAppDispatch();
    const configuredScripts = useAppSelector((state) => state.options.scripts);

    return (
        <div
            className="script-card"
            role="button"
            tabIndex={0}
            onClick={() => onSelect(script)}
            onKeyDown={(e) => {
                if (e.key === "Enter") {
                    onSelect(script);
                }
            }}
        >
            <div className="script-card-main">
                <div className="script-card-name">{basename(script.path)}</div>
                <div className="script-card-path" title={script.fullpath}>
                    {script.fullpath}
                </div>
            </div>
            {script.status === ScriptStatusKind.error && (
                <Icon name="warning" className="text-danger" />
            )}
            <StatusBadge status={script.status} />
            {/* Isolate the remove action from the card's select handling. */}
            <span
                className="script-card-remove"
                onClick={(e) => e.stopPropagation()}
                onMouseDown={(e) => e.stopPropagation()}
            >
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
            </span>
        </div>
    );
}

function ScriptList({ onSelect }: { onSelect: (s: ScriptStatus) => void }) {
    const scripts = useAppSelector((state) => state.scripts.list);
    const [adding, setAdding] = useState(false);

    return (
        <>
            <div className="scripts-toolbar">
                <h2>Python Scripts</h2>
                <div className="flex-spacer" />
                {!adding && (
                    <Button
                        title="Add a script by its absolute file path"
                        icon="addSquare"
                        onClick={() => setAdding(true)}
                    >
                        Add Script
                    </Button>
                )}
            </div>
            {adding && (
                <AddScriptForm
                    onDone={() => {
                        setAdding(false);
                    }}
                />
            )}
            <div className="scripts-cards">
                {scripts.length === 0 && (
                    <p className="scripts-empty">
                        No scripts configured. Use Add Script to load one.
                    </p>
                )}
                {scripts.map((script) => (
                    <ScriptCard
                        key={script.path}
                        script={script}
                        onSelect={onSelect}
                    />
                ))}
            </div>
        </>
    );
}

function ScriptConsole({ script }: { script: ScriptStatus }) {
    return (
        <div className="scripts-console">
            <div className="scripts-console-header">Console</div>
            {script.error ? (
                <pre className="scripts-error">{script.error}</pre>
            ) : (
                <pre className="scripts-console-empty">no output</pre>
            )}
        </div>
    );
}

function ScriptEditor({
    script,
    onBack,
}: {
    script: ScriptStatus;
    onBack: () => void;
}) {
    const [source, setSource] = useState<string | null>(null);
    const [saved, setSaved] = useState<string | null>(null);
    const [loadError, setLoadError] = useState<string | null>(null);
    const [saveError, setSaveError] = useState<string | null>(null);
    const [saving, setSaving] = useState(false);

    useEffect(() => {
        let cancelled = false;
        setSource(null);
        setSaved(null);
        setLoadError(null);
        setSaveError(null);
        fetchApi(`/scripts/source?path=${encodeURIComponent(script.fullpath)}`)
            .then(async (res) => {
                if (!res.ok) {
                    throw new Error(`status ${res.status}`);
                }
                return res.json();
            })
            .then((json: { source?: string }) => {
                if (cancelled) {
                    return;
                }
                const content = json.source ?? "";
                setSource(content);
                setSaved(content);
            })
            .catch(() => {
                if (!cancelled) {
                    setLoadError("Could not load the script source.");
                }
            });
        return () => {
            cancelled = true;
        };
    }, [script.fullpath]);

    const dirty = source !== null && source !== saved;

    const save = async () => {
        if (!dirty || saving || source === null) {
            return;
        }
        setSaving(true);
        setSaveError(null);
        try {
            const res = await fetchApi(
                `/scripts/source?path=${encodeURIComponent(script.fullpath)}`,
                { method: "PUT", body: source },
            );
            if (!res.ok) {
                throw new Error(`status ${res.status}`);
            }
            setSaved(source);
        } catch {
            setSaveError("Could not save the script source.");
        } finally {
            setSaving(false);
        }
    };

    return (
        <>
            <div className="scripts-toolbar">
                <Button
                    title="Back to the script list"
                    icon="arrowLeft"
                    onClick={onBack}
                >
                    Scripts
                </Button>
                <div className="script-card-name" title={script.fullpath}>
                    {basename(script.path)}
                </div>
                <StatusBadge status={script.status} />
                <div className="flex-spacer" />
                {saveError && <span className="text-danger">{saveError}</span>}
                <Button
                    title="Save the script source to disk"
                    icon="save"
                    onClick={() => save()}
                    disabled={!dirty || saving}
                >
                    Save
                </Button>
            </div>
            <div className="scripts-editor">
                {loadError ? (
                    <p className="scripts-empty">{loadError}</p>
                ) : source === null ? (
                    <p className="scripts-empty">Loading source...</p>
                ) : (
                    <CodeEditor
                        key={script.path}
                        initialContent={source}
                        onChange={setSource}
                        language="python"
                    />
                )}
            </div>
            <ScriptConsole script={script} />
        </>
    );
}

export default function ScriptsView() {
    const scripts = useAppSelector((state) => state.scripts.list);
    const [selectedPath, setSelectedPath] = useState<string | null>(null);
    const selected =
        scripts.find((script) => script.path === selectedPath) ?? null;

    return (
        <HideInStatic>
            <div className="scripts-view">
                {selected ? (
                    <ScriptEditor
                        script={selected}
                        onBack={() => setSelectedPath(null)}
                    />
                ) : (
                    <ScriptList onSelect={(s) => setSelectedPath(s.path)} />
                )}
            </div>
        </HideInStatic>
    );
}
