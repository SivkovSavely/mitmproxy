import { useEffect, useMemo, useState } from "react";
import { fetchApi } from "../../utils";

type ContentState = {
    key: object;
    text: string;
};

export function useContent(
    url: string,
    hash?: string,
    enabled = true,
): string | undefined {
    const requestKey = useMemo(() => ({}), [enabled, url, hash]);
    const [content, setContent] = useState<ContentState>();

    useEffect(() => {
        if (!enabled) {
            return;
        }

        const controller = new AbortController();
        fetchApi(url, { signal: controller.signal })
            .then((response) => {
                if (!response.ok)
                    throw `${response.status} ${response.statusText}`.trim();
                return response.text();
            })
            .then((text) => {
                if (!controller.signal.aborted) {
                    setContent({ key: requestKey, text });
                }
            })
            .catch((e) => {
                if (controller.signal.aborted) {
                    return;
                }
                setContent({
                    key: requestKey,
                    text: `Error getting content: ${e}.`,
                });
            });

        return () => {
            if (!controller.signal.aborted) controller.abort();
        };
    }, [enabled, requestKey, url]);

    if (!enabled || content?.key !== requestKey) {
        return undefined;
    }
    return content.text;
}
