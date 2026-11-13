export function summary(index: number, live = false) {
    return {
        id: `history-${index}`,
        sequence: `900719925474${String(index).padStart(6, "0")}`,
        timestamp_created: 1700000000 + index,
        type: "http",
        method: "GET",
        host: "example.com",
        path: `/history/${index}`,
        status_code: 200,
        request_size: 10,
        response_size: 20,
        payload_version: 2,
        live,
        highlighted: false,
        cursor: [
            1700000000 + index,
            `900719925474${index}`,
            `history-${index}`,
        ] as [number, string, string],
    };
}

export function page(offset: number, count = 1000000, length = 500) {
    return {
        instance_id: "instance-a",
        count,
        offset,
        rows: Array.from({ length: Math.min(length, count - offset) }, (_, i) =>
            summary(offset + i),
        ),
        anchor_position: null as number | null,
        selected_eligible: null as boolean | null,
        lower: null,
        upper: null,
        historical_search_unsupported: false,
        historical_highlight_unsupported: false,
    };
}
