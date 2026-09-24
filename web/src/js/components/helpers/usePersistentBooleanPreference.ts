import { useCallback, useRef, useState } from "react";

export const JSON_WORD_WRAP_STORAGE_KEY = "mitmweb.jsonWordWrap";

function readBooleanPreference(key: string, defaultValue: boolean): boolean {
    try {
        const stored = window.localStorage.getItem(key);
        return stored === null ? defaultValue : stored === "true";
    } catch {
        return defaultValue;
    }
}

function writeBooleanPreference(key: string, value: boolean): void {
    try {
        window.localStorage.setItem(key, value ? "true" : "false");
    } catch {
        // Preferences should not prevent the editor from rendering.
    }
}

export function usePersistentBooleanPreference(
    key: string,
    defaultValue = false,
): [boolean, () => void] {
    const [value, setValue] = useState<boolean>(() =>
        readBooleanPreference(key, defaultValue),
    );
    const valueRef = useRef(value);

    const toggle = useCallback(() => {
        const nextValue = !valueRef.current;
        valueRef.current = nextValue;
        setValue(nextValue);
        writeBooleanPreference(key, nextValue);
    }, [key]);

    return [value, toggle];
}
