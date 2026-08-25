import { copyToClipboard, runCommand } from "../utils";
import type { Flow } from "../flow";

// Separator between the exports of consecutive flows.
const separators: Partial<Record<string, string>> = {
    raw: "",
    raw_request: "",
    raw_response: "",
    curl: "\n",
    httpie: "\n",
};

export const copy = async (flows: Flow[], format: string): Promise<void> => {
    // Safari: We need to call copyToClipboard _right away_ with a promise,
    // otherwise we're loosing user intent and can't copy anymore.
    const formatted = (async () => {
        const sep = separators[format] ?? "\n";
        // Promise.all keeps input order even if individual exports resolve out of order.
        const exported = await Promise.all(
            flows.map(async (flow) => {
                const ret = await runCommand("export", format, `@${flow.id}`);
                if (ret.value) {
                    return ret.value;
                } else if (ret.error) {
                    throw ret.error;
                } else {
                    throw ret;
                }
            }),
        );
        return exported.join(sep);
    })();
    try {
        await copyToClipboard(formatted);
    } catch (err) {
        alert(err);
    }
};
