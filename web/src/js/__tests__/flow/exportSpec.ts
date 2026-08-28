import { copy } from "../../flow/export";
import { copyToClipboard, runCommand } from "../../utils";
import { TFlow } from "../ducks/tutils";

jest.mock("../../utils");

const mockedRunCommand = jest.mocked(runCommand);
const mockedCopyToClipboard = jest.mocked(copyToClipboard);

type Result = { value?: string; error?: string };

const flowA = { ...TFlow(), id: "a" };
const flowB = { ...TFlow(), id: "b" };
const flowC = { ...TFlow(), id: "c" };

/** Mock the backend export command, keyed by the `@flow.id` selector. */
const mockExports = (values: Record<string, string>) => {
    mockedRunCommand.mockImplementation(
        (_cmd: string, _format: string, selector: string) =>
            Promise.resolve<Result>({ value: values[selector] }),
    );
};

const clipboardPromise = (): Promise<string> =>
    mockedCopyToClipboard.mock.calls.at(-1)![0];

beforeEach(() => {
    jest.clearAllMocks();
    // Mirror the real contract: copyToClipboard never throws unless the
    // promised text rejects.
    mockedCopyToClipboard.mockImplementation((textPromise: Promise<string>) =>
        textPromise.then(() => undefined),
    );
});

describe("copy", () => {
    it("exports a single flow unchanged", async () => {
        mockExports({ "@a": "GET /\n" });
        await copy([flowA], "raw");

        expect(mockedRunCommand).toHaveBeenCalledTimes(1);
        expect(mockedRunCommand).toHaveBeenCalledWith("export", "raw", "@a");
        expect(mockedCopyToClipboard).toHaveBeenCalledTimes(1);
        await expect(clipboardPromise()).resolves.toBe("GET /\n");
    });

    it.each([
        "raw",
        "raw_request",
        "raw_response",
        "raw_request_body",
        "raw_response_body",
        "raw_bodies",
        "redacted_request",
        "redacted_response",
        "redacted",
    ])(
        "concatenates multiple %s exports exactly, without separators",
        async (format) => {
            mockExports({ "@a": "A\n", "@b": "B\n\n", "@c": "C" });
            await copy([flowA, flowB, flowC], format);

            expect(mockedRunCommand).toHaveBeenCalledTimes(3);
            await expect(clipboardPromise()).resolves.toBe("A\nB\n\nC");
        },
    );

    it("joins cURL commands with a single newline", async () => {
        mockExports({ "@a": "curl 'http://a'", "@b": "curl 'http://b'" });
        await copy([flowA, flowB], "curl");
        await expect(clipboardPromise()).resolves.toBe(
            "curl 'http://a'\ncurl 'http://b'",
        );
    });

    it("joins HTTPie commands with a single newline", async () => {
        mockExports({ "@a": "http GET a", "@b": "http GET b" });
        await copy([flowA, flowB], "httpie");
        await expect(clipboardPromise()).resolves.toBe("http GET a\nhttp GET b");
    });

    it("accepts an empty export result", async () => {
        mockExports({ "@a": "" });
        await copy([flowA], "raw_request_body");
        await expect(clipboardPromise()).resolves.toBe("");
    });

    it("rejects malformed export results", async () => {
        const alertMock = jest
            .spyOn(window, "alert")
            .mockImplementation(() => {});
        mockedRunCommand.mockResolvedValue({ value: null });

        await copy([flowA], "raw_request_body");

        await expect(clipboardPromise()).rejects.toEqual({ value: null });
        expect(alertMock).toHaveBeenCalledWith({ value: null });
        alertMock.mockRestore();
    });

    it("keeps input order when backend calls resolve out of order", async () => {
        mockedRunCommand.mockImplementation(
            (_cmd: string, _format: string, selector: string) =>
                new Promise<Result>((resolve) =>
                    setTimeout(
                        () =>
                            resolve({
                                value: selector === "@a" ? "AAA" : "BBB",
                            }),
                        selector === "@a" ? 30 : 0,
                    ),
                ),
        );
        await copy([flowA, flowB], "curl");
        await expect(clipboardPromise()).resolves.toBe("AAA\nBBB");
    });

    it("rejects the aggregate operation if any export fails", async () => {
        const alertMock = jest
            .spyOn(window, "alert")
            .mockImplementation(() => {});
        mockedRunCommand.mockImplementation(
            (_cmd: string, _format: string, selector: string) =>
                selector === "@b"
                    ? Promise.resolve<Result>({ error: "cannot export" })
                    : Promise.resolve<Result>({ value: "AAA" }),
        );

        await copy([flowA, flowB], "raw");

        // no successful subset is copied, the aggregate promise rejects.
        await expect(clipboardPromise()).rejects.toBe("cannot export");
        expect(alertMock).toHaveBeenCalledWith("cannot export");
        alertMock.mockRestore();
    });

    it("hands the aggregate promise to copyToClipboard immediately", async () => {
        const resolvers: ((r: Result) => void)[] = [];
        mockedRunCommand.mockImplementation(
            () =>
                new Promise<Result>((res) => {
                    resolvers.push(res);
                }),
        );

        const done = copy([flowA, flowB], "raw");
        // Safari user-intent semantics: no awaiting of the exports before this point.
        expect(mockedCopyToClipboard).toHaveBeenCalledTimes(1);

        resolvers.forEach((resolve) => resolve({ value: "X" }));
        await done;
        await expect(clipboardPromise()).resolves.toBe("XX");
    });
});
