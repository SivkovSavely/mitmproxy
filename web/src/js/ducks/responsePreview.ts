import { createSlice } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";

export interface ResponsePreview {
    flow_id: string;
    subscription: number;
    seq: number;
    start: number;
    end: number;
    received: number;
    dropped: number;
    missing: boolean;
    status: "waiting" | "streaming" | "finished" | "aborted";
    reason: string | null;
    text: string;
    reset: boolean;
    stored: boolean;
    max_bytes: number;
}

export interface PreviewState {
    flow_id: string | null;
    subscription: number;
    preview: Omit<ResponsePreview, "text"> | null;
    chunks: { id: number; text: string; bytes: number }[];
    bytes: number;
    capped: boolean;
}

export const defaultState: PreviewState = {
    flow_id: null,
    subscription: 0,
    preview: null,
    chunks: [],
    bytes: 0,
    capped: false,
};

function utf8Length(text: string): number {
    let bytes = 0;
    for (const char of text) {
        const point = char.codePointAt(0)!;
        bytes +=
            point <= 0x7f ? 1 : point <= 0x7ff ? 2 : point <= 0xffff ? 3 : 4;
    }
    return bytes;
}

const slice = createSlice({
    name: "responsePreview",
    initialState: defaultState,
    reducers: {
        subscribe(
            _state,
            action: PayloadAction<{
                flow_id: string | null;
                subscription: number;
            }>,
        ) {
            return { ...defaultState, ...action.payload };
        },
        receive(state, action: PayloadAction<ResponsePreview>) {
            const update = action.payload;
            if (
                update.flow_id !== state.flow_id ||
                update.subscription !== state.subscription ||
                update.seq <= (state.preview?.seq ?? -1)
            ) {
                return;
            }
            const offsetGap =
                state.preview !== null && update.start !== state.preview.end;
            const gap =
                offsetGap ||
                (state.preview !== null &&
                    update.seq !== state.preview.seq + 1);
            if (update.reset || offsetGap) {
                state.chunks = [];
                state.bytes = 0;
            }
            const { text, ...metadata } = update;
            state.preview = {
                ...metadata,
                missing:
                    update.missing || gap || (state.preview?.missing ?? false),
            };
            if (text) {
                const bytes = utf8Length(text);
                state.chunks.push({ id: update.seq, text, bytes });
                state.bytes += bytes;
            }
            const cap = Math.max(0, update.max_bytes);
            while (state.bytes > cap && state.chunks.length) {
                state.capped = true;
                const first = state.chunks[0];
                const excess = state.bytes - cap;
                if (first.bytes <= excess) {
                    state.bytes -= first.bytes;
                    state.chunks.shift();
                } else {
                    let removed = 0,
                        offset = 0;
                    for (const char of first.text) {
                        removed += utf8Length(char);
                        offset += char.length;
                        if (removed >= excess) break;
                    }
                    first.text = first.text.slice(offset);
                    first.bytes -= removed;
                    state.bytes -= removed;
                }
            }
        },
    },
});

export const { subscribe, receive } = slice.actions;
export default slice.reducer;
