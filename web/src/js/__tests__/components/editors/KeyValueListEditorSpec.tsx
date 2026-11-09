import * as React from "react";
import { fireEvent, render, screen } from "../../test-utils";
import KeyValueListEditor from "../../../components/editors/KeyValueListEditor";

describe("KeyValueListEditor row actions", () => {
    const data: [string, string][] = [
        ["X-Test", "a.b[0]+(foo)"],
        ["Other", "value"],
    ];

    it("does not render an action when the prop is absent", () => {
        const { container } = render(
            <KeyValueListEditor data={data} onChange={() => {}} />,
        );
        expect(container.querySelectorAll(".kv-row-action")).toHaveLength(0);
    });

    it("renders one action per row, isolated from row editing", () => {
        const onChange = jest.fn();
        const { container } = render(
            <KeyValueListEditor
                data={data}
                onChange={onChange}
                rowAction={([name, value]) => (
                    <button title={`act-${name}-${value}`} />
                )}
            />,
        );

        expect(screen.getByTitle("act-X-Test-a.b[0]+(foo)")).toBeTruthy();
        expect(screen.getByTitle("act-Other-value")).toBeTruthy();
        expect(onChange).not.toHaveBeenCalled();

        // Clicking the action neither starts editing nor adds an empty row.
        fireEvent.click(screen.getByTitle("act-X-Test-a.b[0]+(foo)"));
        expect(container.querySelectorAll(".kv-row").length).toBe(2);
        expect(
            document.querySelectorAll('span[contenteditable="true"]'),
        ).toHaveLength(0);
        expect(onChange).not.toHaveBeenCalled();
    });

    it("keeps header text together and the action outside its scroll content", () => {
        const { container } = render(
            <KeyValueListEditor
                className="headers"
                data={[["X-Long", "a".repeat(500)]]}
                onChange={() => {}}
                rowAction={() => <button>filter</button>}
            />,
        );
        const row = container.querySelector(".kv-row")!;
        const content = row.querySelector(".kv-row-content")!;
        const action = row.querySelector(".kv-row-action")!;
        expect(content.textContent).toBe(`X-Long:\u00a0${"a".repeat(500)}`);
        expect(content.parentElement).toBe(row);
        expect(action.parentElement).toBe(row);
        expect(content.contains(action)).toBe(false);
        const value = content.querySelector<HTMLSpanElement>(".kv-value")!;
        fireEvent.focus(value);
        expect(value.contentEditable).toBe("true");
        expect(value.textContent).toBe("a".repeat(500));
    });

    it("invokes the row's own item on click", () => {
        const clicks = jest.fn();
        render(
            <KeyValueListEditor
                data={data}
                onChange={jest.fn()}
                rowAction={([name]) => (
                    <button onClick={() => clicks(name)}>go</button>
                )}
            />,
        );
        fireEvent.click(screen.getAllByText("go")[1]!);
        expect(clicks).toHaveBeenCalledWith("Other");
        fireEvent.click(screen.getAllByText("go")[0]!);
        expect(clicks).toHaveBeenCalledWith("X-Test");
    });
});
