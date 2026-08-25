import json

import pytest

from mitmproxy.contentviews import Metadata
from mitmproxy.contentviews import ai_stream
from mitmproxy.contentviews._view_aistream import parse_sse


CHAT_STREAM = (
    ": OPENROUTER PROCESSING\n"
    "\n"
    ": OPENROUTER PROCESSING\n"
    "\n"
    'data: {"id":"gen-1","object":"chat.completion.chunk","created":1787595459,'
    '"model":"stealth/ox-alpha","provider":"Stealth","choices":[{"index":0,'
    '"delta":{"role":"assistant","content":""},"finish_reason":null}]}\n'
    "\n"
    'data: {"id":"gen-1","created":1787595459,"choices":[{"index":0,'
    '"delta":{"content":"Hello"}}]}\n'
    "\n"
    'data: {"id":"gen-1","created":1787595460,"choices":[{"index":0,'
    '"delta":{"reasoning":"thinking"}}]}\n'
    "\n"
    'data: {"id":"gen-1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,'
    '"id":"call_1","type":"function","function":{"name":"bash",'
    '"arguments":"{\\"co"}}]}},{"index":1,"delta":{"content":"other choice"}}]}\n'
    "\n"
    'data: {"id":"gen-1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,'
    '"function":{"arguments":"mmand\\":\\"ls\\"}"}}]},"finish_reason":"tool_calls",'
    '"native_finish_reason":"stop"}]}\n'
    "\n"
    'data: {"id":"gen-1","choices":[{"index":1,"delta":{},"finish_reason":"stop"}],'
    '"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}\n'
    "\n"
    "data: [DONE]\n"
    "\n"
)

RESPONSES_STREAM = (
    "event: response.created\n"
    'data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-x"}}\n'
    "\n"
    "event: response.output_item.added\n"
    'data: {"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1",'
    '"type":"message","role":"assistant","content":[]}}\n'
    "\n"
    'data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,'
    '"content_index":0,"delta":"Hello"}\n'
    "\n"
    'data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,'
    '"content_index":0,"delta":", world"}\n'
    "\n"
    'data: {"type":"response.output_text.done","item_id":"msg_1","output_index":0,'
    '"content_index":0,"text":"Hello, world"}\n'
    "\n"
    "event: response.output_item.added\n"
    'data: {"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1",'
    '"type":"function_call","call_id":"call_1","name":"bash","arguments":""}}\n'
    "\n"
    'data: {"type":"response.function_call_arguments.delta","item_id":"fc_1",'
    '"output_index":1,"delta":"{\\"command\\": \\"ls\\"}"}\n'
    "\n"
    "event: response.output_item.done\n"
    'data: {"type":"response.output_item.done","output_index":1,"item":{"id":"fc_1",'
    '"type":"function_call","call_id":"call_1","name":"bash",'
    '"arguments":"{\\"command\\": \\"ls\\"}"}}\n'
    "\n"
    'data: {"type":"response.completed","response":{"id":"resp_1","model":"gpt-x",'
    '"usage":{"input_tokens":3,"output_tokens":7,"total_tokens":10}}}\n'
    "\n"
)


def sse_meta() -> Metadata:
    return Metadata(content_type="text/event-stream")


def test_parse_sse():
    events, comments = parse_sse(
        ": heartbeat\r\nevent: foo\r\ndata: one\r\ndata: two\n\ndata: three\n"
    )
    assert comments == 1
    assert events == [
        ("foo", "one\ntwo"),
        (None, "three"),
    ]


def test_reduce_chat_completions():
    out = json.loads(ai_stream.prettify(CHAT_STREAM.encode(), sse_meta()))
    assert out["protocol"] == "openai-chat-completions"
    assert out["status"] == "completed"
    assert out["ids"] == ["gen-1"]
    assert out["created"] == [1787595459, 1787595460]
    assert out["model"] == "stealth/ox-alpha"
    assert out["provider"] == "Stealth"
    assert len(out["choices"]) == 2
    c0 = out["choices"][0]
    assert c0["index"] == 0
    assert c0["role"] == "assistant"
    assert c0["content"] == "Hello"
    assert c0["reasoning"] == "thinking"
    tc = c0["tool_calls"][0]
    assert tc["id"] == "call_1"
    assert tc["type"] == "function"
    assert tc["name"] == "bash"
    assert tc["arguments"] == '{"command":"ls"}'
    assert tc["arguments_json"] == {"command": "ls"}
    assert c0["finish_reason"] == "tool_calls"
    assert c0["native_finish_reason"] == "stop"
    assert out["choices"][1]["content"] == "other choice"
    assert out["choices"][1]["finish_reason"] == "stop"
    assert out["usage"] == {
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "total_tokens": 15,
    }
    assert out["stream"] == {
        "events": 6,
        "heartbeats": 2,
        "done": True,
        "errors": 0,
        "complete": True,
    }


def test_reduce_chat_completions_error_in_200():
    stream = (
        'data: {"id":"gen-1","choices":[{"index":0,"delta":{"content":"Hi"}}]}\n'
        "\n"
        'data: {"error":{"message":"Provider returned error","code":429,'
        '"metadata":{"raw":"rate limited"}}}\n'
        "\n"
    )
    out = json.loads(ai_stream.prettify(stream.encode(), sse_meta()))
    assert out["status"] == "error"
    assert out["error"]["code"] == 429
    assert out["stream"]["complete"] is False
    assert out["choices"][0]["content"] == "Hi"


def test_reduce_chat_completions_truncated():
    stream = (
        'data: {"id":"gen-1","choices":[{"index":0,"delta":{"content":"partial"}}]}\n'
        "\n"
    )
    out = json.loads(ai_stream.prettify(stream.encode(), sse_meta()))
    assert out["status"] == "incomplete"
    assert out["stream"]["done"] is False
    assert out["stream"]["complete"] is False


def test_reduce_responses():
    out = json.loads(ai_stream.prettify(RESPONSES_STREAM.encode(), sse_meta()))
    assert out["protocol"] == "openai-responses"
    assert out["status"] == "completed"
    assert out["id"] == "resp_1"
    assert out["model"] == "gpt-x"
    # The done snapshot is authoritative and must not be appended to the deltas.
    assert out["text"] == "Hello, world"
    tc = out["tool_calls"][0]
    assert tc["type"] == "function_call"
    assert tc["name"] == "bash"
    assert tc["call_id"] == "call_1"
    assert tc["arguments_json"] == {"command": "ls"}
    assert out["usage"]["total_tokens"] == 10
    assert out["stream"]["complete"] is True


def test_reduce_responses_failed():
    stream = (
        'data: {"type":"response.created","response":{"id":"resp_1"}}\n'
        "\n"
        'data: {"type":"response.failed","response":{"id":"resp_1","error":'
        '{"code":"server_error","message":"boom"}}}\n'
        "\n"
    )
    out = json.loads(ai_stream.prettify(stream.encode(), sse_meta()))
    assert out["status"] == "failed"
    assert out["error"]["code"] == "server_error"


def test_render_priority():
    assert ai_stream.render_priority(CHAT_STREAM.encode(), sse_meta()) > 0
    assert ai_stream.render_priority(RESPONSES_STREAM.encode(), sse_meta()) > 0
    assert not ai_stream.render_priority(b"data: hello\n\n", sse_meta())
    assert not ai_stream.render_priority(CHAT_STREAM.encode(), Metadata())
    assert not ai_stream.render_priority(
        CHAT_STREAM.encode(), Metadata(content_type="application/json")
    )
    assert not ai_stream.render_priority(b"", sse_meta())


def test_prettify_rejects_non_ai_sse():
    with pytest.raises(ValueError):
        ai_stream.prettify(b"event: ping\ndata: {}\n\n", sse_meta())


def test_name_and_highlight():
    assert ai_stream.name == "AI Stream"
    assert ai_stream.syntax_highlight == "yaml"


CHAT_KITCHEN_SINK = (
    'data: {"object":"chat.completion.chunk","id":"gen-2","model":"m",'
    '"service_tier":"default","user_id":"u1","choices":["bogus"]}\n'
    "\n"
    "data: {invalid\n"
    "\n"
    'data: "plain-string"\n'
    "\n"
    "data: \n"
    "\n"
    'data: {"id":"gen-2","choices":[{"index":0,"delta":{"role":"assistant",'
    '"refusal":"no ","reasoning_details":[{"type":"reasoning.text","text":"abc",'
    '"index":0}],"annotations":[{"a":1}],"images":[{"i":1}],'
    '"tool_calls":[{"index":0,"id":"c9","function":{"name":"f"}},'
    '{"index":1,"id":"c10","function":{"name":"g","arguments":"{\\"x\\":"}},'
    '"bogus"],"function_call":{"name":"legacy","arguments":"{\\"y\\":"},'
    '"custom_field":42},"logprobs":{"tokens":["a"]}},'
    '{"index":1,"message":{"content":"msg"},"finish_reason":"stop"},'
    '{"index":2,"finish_reason":"length"}]}\n'
    "\n"
    'data: {"id":"gen-2","choices":[{"index":0,"delta":{"reasoning_details":'
    '[{"type":"reasoning.text","text":"def","index":0},'
    '{"type":"reasoning.encrypted","data":"zz","index":1},"junk"],'
    '"tool_calls":[{"index":1,"function":{"arguments":"3}"}}],'
    '"function_call":{"arguments":"1}"}}},'
    '{"index":3,"delta":{"reasoning_details":[],"tool_calls":['
    '{"index":2,"id":"c11","type":"function",'
    '"function":{"name":"h","arguments":"{brok"}}]}},'
    '{"index":4,"delta":{"content":" tail"},"finish_reason":"length"}]}\n'
    "\n"
    "data: [DONE]\n"
    "\n"
)


def test_reduce_chat_completions_kitchen_sink():
    out = json.loads(ai_stream.prettify(CHAT_KITCHEN_SINK.encode(), sse_meta()))
    assert out["ids"] == ["gen-2"]
    assert out["service_tier"] == "default"
    assert out["extra_top_level_fields"]["user_id"] == "u1"
    assert out["stream"]["events"] == 4
    assert out["stream"]["done"] is True

    c0 = out["choices"][0]
    assert c0["refusal"] == "no "
    assert c0["reasoning_details"] == [
        {"type": "reasoning.text", "text": "abcdef", "index": 0},
        {"type": "reasoning.encrypted", "data": "zz", "index": 1},
    ]
    assert c0["annotations"] == [{"a": 1}]
    assert c0["images"] == [{"i": 1}]
    assert c0["tool_calls"][0]["id"] == "c9"
    assert c0["tool_calls"][1]["arguments_json"] == {"x": 3}
    assert c0["function_call"] == {
        "name": "legacy",
        "arguments": '{"y":1}',
    }
    assert c0["extra_delta_fields"]["custom_field"] == 42
    assert c0["logprobs"] == {"tokens": ["a"]}
    assert out["choices"][1]["content"] == "msg"
    assert out["choices"][1]["finish_reason"] == "stop"
    assert out["choices"][2]["finish_reason"] == "length"
    # empty reasoning_details and unparsable tool call arguments are handled
    c3 = out["choices"][3]
    assert "reasoning_details" not in c3
    assert c3["tool_calls"][0]["arguments"] == "{brok"
    assert "arguments_json" not in c3["tool_calls"][0]
    assert out["choices"][4]["content"] == " tail"

    # Detection also works via the object type alone.
    assert ai_stream.render_priority(
        b'data: {"object":"chat.completion.chunk"}\n\n', sse_meta()
    ) > 0


RESPONSES_KITCHEN_SINK = (
    'data: "plain-string"\n'
    "\n"
    'data: {"type":"response.created","response":{"id":"resp_2","model":"m",'
    '"usage":{"total_tokens":99}}}\n'
    "\n"
    'data: {"type":"response.output_item.added","output_index":0,"item":'
    '{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":""}]}}\n'
    "\n"
    'data: {"type":"response.reasoning_summary_text.delta","item_id":"rs_1",'
    '"output_index":0,"summary_index":0,"delta":"why "}\n'
    "\n"
    'data: {"type":"response.reasoning_summary_text.done","item_id":"rs_1",'
    '"output_index":0,"summary_index":0,"text":"why because"}\n'
    "\n"
    'data: {"type":"response.reasoning_text.delta","item_id":"rs_1",'
    '"output_index":0,"content_index":0,"delta":"raw"}\n'
    "\n"
    'data: {"type":"response.output_item.added","output_index":1,"item":'
    '{"id":"msg_2","type":"message","role":"assistant","content":[]}}\n'
    "\n"
    'data: {"type":"response.content_part.added","item_id":"msg_2",'
    '"output_index":1,"content_index":0,"part":{"type":"output_text",'
    '"text":"","annotations":[]}}\n'
    "\n"
    'data: {"type":"response.output_text.delta","item_id":"msg_2",'
    '"content_index":0,"delta":"Hi"}\n'
    "\n"
    'data: {"type":"response.refusal.delta","item_id":"msg_2","output_index":1,'
    '"content_index":1,"delta":"never!"}\n'
    "\n"
    'data: {"type":"response.output_item.added","output_index":2,"item":'
    '{"id":"ctc_1","type":"custom_tool_call","call_id":"call_c",'
    '"name":"apply_patch","input":""}}\n'
    "\n"
    'data: {"type":"response.custom_tool_call_input.delta","item_id":"ctc_1",'
    '"output_index":2,"delta":"*** Begin Patch"}\n'
    "\n"
    'data: {"type":"response.custom_tool_call_input.done","item_id":"ctc_1",'
    '"output_index":2,"input":"*** Begin Patch\\n*** End Patch"}\n'
    "\n"
    'data: {"type":"response.output_item.added","output_index":3,"item":'
    '{"id":"fc_bad","type":"function_call","call_id":"call_b","name":"bash",'
    '"arguments":""}}\n'
    "\n"
    'data: {"type":"response.function_call_arguments.delta","item_id":"fc_bad",'
    '"output_index":3,"delta":"{oops"}\n'
    "\n"
    'data: {"type":"response.function_call_arguments.done","item_id":"fc_bad",'
    '"output_index":3,"arguments":"{oops"}\n'
    "\n"
    'data: {"type":"response.output_text.delta","item_id":"ghost","delta":"lost"}\n'
    "\n"
    'data: {"type":"response.custom_event","foo":1}\n'
    "\n"
    'data: {"type":"response.output_item.added","output_index":4,"item":'
    '{"id":"msg_3","type":"message","role":"assistant","content":[null]}}\n'
    "\n"
    'data: {"type":"response.output_text.delta","item_id":"msg_3",'
    '"output_index":4,"content_index":0,"delta":"x"}\n'
    "\n"
    'data: {"type":"error","error":{"code":"overloaded"}}\n'
    "\n"
    'data: {"type":"response.incomplete","response":{"id":"resp_2","output":['
    '{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text",'
    '"text":"why because"}],"content":[{"type":"reasoning_text","text":"raw"}]},'
    '{"id":"msg_2","type":"message","role":"assistant","content":['
    '{"type":"output_text","text":"Hi"},{"type":"refusal","refusal":"never!"}]},'
    '{"id":"ctc_1","type":"custom_tool_call","call_id":"call_c",'
    '"name":"apply_patch","input":"*** Begin Patch\\n*** End Patch"},'
    '{"id":"fc_bad","type":"function_call","call_id":"call_b","name":"bash",'
    '"arguments":"{oops"},'
    '"not-a-dict"]}}\n'
    "\n"
)


def test_reduce_responses_kitchen_sink():
    out = json.loads(ai_stream.prettify(RESPONSES_KITCHEN_SINK.encode(), sse_meta()))
    assert out["protocol"] == "openai-responses"
    assert out["status"] == "incomplete"
    assert out["stream"]["complete"] is False
    # usage comes from the created snapshot since the final event has none.
    assert out["usage"] == {"total_tokens": 99}
    # deltas addressed to an unknown item are dropped.
    assert out["text"] == "Hinever!"
    assert out["reasoning"] == "why becauseraw"
    assert out["unhandled_event_types"] == ["response.custom_event"]

    calls = {tc["type"]: tc for tc in out["tool_calls"]}
    assert calls["custom_tool_call"]["input"] == "*** Begin Patch\n*** End Patch"
    assert calls["function_call"]["arguments"] == "{oops"
    assert "arguments_json" not in calls["function_call"]


def test_render_priority_parse_error(monkeypatch):
    def boom(self, data):
        raise RuntimeError("boom")

    monkeypatch.setattr(ai_stream, "_parse", boom)
    assert ai_stream.render_priority(b"data: {}", sse_meta()) == 0
