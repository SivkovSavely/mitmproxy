import * as React from "react";
import { usePersistentBooleanPreference } from "../../../components/helpers/usePersistentBooleanPreference";
import { fireEvent, render, screen } from "../../test-utils";

const storageKey = "test.persistentBoolean";

function PreferenceControl() {
    const [value, toggle] = usePersistentBooleanPreference(storageKey, false);
    return (
        <button data-testid="preference" onClick={toggle}>
            {String(value)}
        </button>
    );
}

describe("usePersistentBooleanPreference", () => {
    beforeEach(() => {
        jest.restoreAllMocks();
        window.localStorage.clear();
    });

    afterEach(() => {
        jest.restoreAllMocks();
        window.localStorage.clear();
    });

    test("defaults to false when the key is missing", () => {
        render(<PreferenceControl />);

        expect(screen.getByTestId("preference")).toHaveTextContent("false");
    });

    test.each([
        ["true", "true"],
        ["false", "false"],
        ["not-a-boolean", "false"],
    ])("reads %s as %s", (stored, expected) => {
        window.localStorage.setItem(storageKey, stored);
        render(<PreferenceControl />);

        expect(screen.getByTestId("preference")).toHaveTextContent(expected);
    });

    test("persists both enabled and disabled values when toggled", () => {
        render(<PreferenceControl />);
        const button = screen.getByTestId("preference");

        fireEvent.click(button);
        expect(button).toHaveTextContent("true");
        expect(window.localStorage.getItem(storageKey)).toBe("true");

        fireEvent.click(button);
        expect(button).toHaveTextContent("false");
        expect(window.localStorage.getItem(storageKey)).toBe("false");
    });

    test("falls back safely when reading localStorage fails", () => {
        jest.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
            throw new Error("read failed");
        });

        render(<PreferenceControl />);

        expect(screen.getByTestId("preference")).toHaveTextContent("false");
    });

    test("keeps state changes when writing localStorage fails", () => {
        const setItem = jest
            .spyOn(Storage.prototype, "setItem")
            .mockImplementation(() => {
                throw new Error("write failed");
            });

        render(<PreferenceControl />);
        fireEvent.click(screen.getByTestId("preference"));

        expect(screen.getByTestId("preference")).toHaveTextContent("true");
        expect(setItem).toHaveBeenCalledWith(storageKey, "true");
    });
});
