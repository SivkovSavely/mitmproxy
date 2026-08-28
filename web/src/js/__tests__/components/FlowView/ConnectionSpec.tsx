import React from "react";
import Connection, {
    ConnectionInfo,
    formatAddress,
} from "../../../components/FlowView/Connection";
import { THTTPFlow } from "../../ducks/_tflow";
import { render, screen } from "../../test-utils";

describe("formatAddress", () => {
    it("should handle IPv4 addresses", () => {
        let { asFragment } = render(
            <table>
                <tbody>{formatAddress("Address", ["8.8.8.8", 53])}</tbody>
            </table>,
        );
        expect(asFragment()).toHaveTextContent("8.8.8.8:53");
    });
    it("should handle IPv6 addresses", () => {
        let { asFragment } = render(
            <table>
                <tbody>{formatAddress("Address", ["::1", 53, 0, 0])}</tbody>
            </table>,
        );
        expect(asFragment()).toHaveTextContent("[::1]:53");
    });
    it("should handle missing addresses", () => {
        let { asFragment } = render(
            <table>
                <tbody>{formatAddress("Address", undefined)}</tbody>
            </table>,
        );
        expect(asFragment()).not.toHaveTextContent("Address");
    });
});

test("Connection renders client and server connection IDs", () => {
    const flow = THTTPFlow();
    flow.client_conn.id = "client-connection-id";
    flow.server_conn.id = "server-connection-id";

    render(<Connection flow={flow} />);

    expect(screen.getByText("Client Connection")).toBeInTheDocument();
    expect(screen.getByText("Server Connection")).toBeInTheDocument();
    expect(screen.getByText("client-connection-id")).toBeInTheDocument();
    expect(screen.getByText("server-connection-id")).toBeInTheDocument();
    expect(screen.getByText("client-connection-id")).not.toEqual(
        screen.getByText("server-connection-id"),
    );
});

test("ConnectionInfo keeps rendering connection fields", () => {
    const flow = THTTPFlow();

    render(
        <>
            <ConnectionInfo conn={flow.client_conn} />
            <ConnectionInfo conn={flow.server_conn} />
        </>,
    );

    expect(screen.getByText("127.0.0.1:22")).toBeInTheDocument();
    expect(screen.getByText("192.168.0.1:22")).toBeInTheDocument();
    expect(screen.getByText("http/1.1")).toBeInTheDocument();
    expect(screen.getAllByText("TLSv1.2")).toHaveLength(2);
    expect(screen.getByText("cipher")).toBeInTheDocument();
    expect(screen.getAllByText("address")).toHaveLength(2);
});
