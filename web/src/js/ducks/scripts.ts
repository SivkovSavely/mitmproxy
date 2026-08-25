import type { UnknownAction } from "@reduxjs/toolkit";
import { createAction } from "@reduxjs/toolkit";

export const SCRIPTS_RECEIVE = createAction<ScriptStatus[]>("SCRIPTS_RECEIVE");

export enum ScriptStatusKind {
    loaded = "loaded",
    error = "error",
    loading = "loading",
}

export interface ScriptStatus {
    path: string;
    fullpath: string;
    status: ScriptStatusKind;
    error: string | null;
}

interface ScriptsState {
    list: ScriptStatus[];
}

export const defaultState: ScriptsState = {
    list: [],
};

export default function scriptsReducer(
    state = defaultState,
    action: UnknownAction,
): ScriptsState {
    if (SCRIPTS_RECEIVE.match(action)) {
        return {
            ...state,
            list: action.payload,
        };
    }
    return state;
}
