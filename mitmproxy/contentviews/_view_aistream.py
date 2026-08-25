"""
Contentview that reduces Server-Sent Events emitted by LLM APIs (OpenAI
Chat Completions style chunks, as proxied by e.g. OpenRouter, and OpenAI
Responses style event streams) into a single JSON document summarizing the
reconstructed messages: text, reasoning, tool calls, finish reasons and
token usage.
"""

import json
import typing
from typing import Any

from mitmproxy.contentviews._api import Contentview
from mitmproxy.contentviews._api import Metadata


def parse_sse(data: str) -> tuple[list[tuple[str | None, str]], int]:
    """
    Split an SSE body into `(event name, concatenated data)` tuples.
    Multiple `data:` lines of one event are joined with newlines.
    Also returns the number of comment/heartbeat lines.
    """
    events: list[tuple[str | None, str]] = []
    event_name: str | None = None
    data_lines: list[str] = []
    comments = 0

    def flush() -> None:
        nonlocal event_name, data_lines
        if data_lines:
            events.append((event_name, "\n".join(data_lines)))
        event_name = None
        data_lines = []

    for line in data.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if not line:
            flush()
        elif line.startswith(":"):
            comments += 1
        elif ":" in line:
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "event":
                event_name = value
            elif field == "data":
                data_lines.append(value)
        # Other SSE fields (id, retry) are ignored.
    flush()
    return events, comments


def _payloads(events: list[tuple[str | None, str]]) -> tuple[list[Any], bool]:
    """Parse all JSON data payloads, ignoring comments and the [DONE] sentinel."""
    out = []
    saw_done = False
    for _, data in events:
        stripped = data.strip()
        if stripped == "[DONE]":
            saw_done = True
            continue
        if not stripped:
            continue
        try:
            out.append(json.loads(stripped))
        except ValueError:
            pass
    return out, saw_done


def _looks_like_ai_stream(payloads: list[Any]) -> bool:
    for obj in payloads:
        if not isinstance(obj, dict):
            continue
        if isinstance(obj.get("choices"), list):
            return True
        if obj.get("object") in ("chat.completion.chunk", "chat.completion"):
            return True
        if isinstance(obj.get("type"), str) and obj["type"].startswith("response."):
            return True
    return False


def _setdefault_choice(choices: dict[int, dict], index: Any) -> dict:
    key = index if isinstance(index, int) else len(choices)
    return choices.setdefault(key, {"index": key})


def _merge_reasoning_details(
    existing: list[dict], incoming: list[Any]
) -> None:
    """Merge consecutive reasoning detail fragments of the same (type, index)."""
    for detail in incoming:
        if not isinstance(detail, dict):
            continue
        prev = next(
            (
                e
                for e in reversed(existing)
                if e.get("type") == detail.get("type")
                and e.get("index") == detail.get("index")
                and e.get("type") in ("reasoning.text", "reasoning.summary")
            ),
            None,
        )
        field = (
            "summary"
            if detail.get("type") == "reasoning.summary"
            else "text"
        )
        if prev is not None and isinstance(detail.get(field), str):
            prev[field] = prev.get(field, "") + detail[field]
        else:
            existing.append(json.loads(json.dumps(detail)))


def _finalize_tool_call(tc: dict) -> dict:
    out = {k: v for k, v in tc.items() if k not in ("function",)}
    fn = tc.get("function") or {}
    if "name" in fn:
        out["name"] = fn["name"]
    if "arguments" in fn:
        out["arguments"] = fn["arguments"]
        try:
            out["arguments_json"] = json.loads(fn["arguments"])
        except ValueError:
            pass
    return out


def reduce_chat_completions(
    payloads: list[Any], comments: int, saw_done: bool
) -> dict[str, Any]:
    """Reduce OpenAI Chat Completions style chunks into one summary object."""
    ids: list[str] = []
    created: list[int] = []
    model = provider = service_tier = None
    usage = None
    error = None
    extra: dict[str, Any] = {}
    choices: dict[int, dict] = {}
    n_errors = 0

    def remember(seq: list, value: Any) -> None:
        if value is not None and value not in seq:
            seq.append(value)

    for obj in payloads:
        if not isinstance(obj, dict):
            continue
        remember(ids, obj.get("id"))
        remember(created, obj.get("created"))
        model = obj.get("model") or model
        provider = obj.get("provider") or provider
        service_tier = obj.get("service_tier") or service_tier
        if isinstance(obj.get("usage"), dict):
            usage = obj["usage"]
        if "error" in obj:
            error = obj["error"]
            n_errors += 1
        for key, value in obj.items():
            if key not in (
                "id",
                "created",
                "model",
                "provider",
                "service_tier",
                "usage",
                "error",
                "choices",
                "object",
            ):
                extra.setdefault(key, value)

        for choice in obj.get("choices") or []:
            if not isinstance(choice, dict):
                continue
            c = _setdefault_choice(choices, choice.get("index"))
            delta = choice.get("delta")
            if delta is None:
                delta = choice.get("message") or {}
            if "role" in delta:
                c.setdefault("role", delta["role"])
            if isinstance(delta.get("content"), str):
                c["content"] = c.get("content", "") + delta["content"]
            if isinstance(delta.get("refusal"), str):
                c["refusal"] = c.get("refusal", "") + delta["refusal"]
            if isinstance(delta.get("reasoning"), str):
                c["reasoning"] = c.get("reasoning", "") + delta["reasoning"]
            if isinstance(delta.get("reasoning_details"), list):
                details = c.setdefault("reasoning_details", [])
                _merge_reasoning_details(details, delta["reasoning_details"])
            for field in ("annotations", "images"):
                if isinstance(delta.get(field), list):
                    c.setdefault(field, []).extend(delta[field])
            for tc in delta.get("tool_calls") or []:
                if not isinstance(tc, dict):
                    continue
                idx = tc.get("index")
                key = idx if isinstance(idx, int) else len(c.get("tool_calls", []))
                calls = c.setdefault("tool_calls", [])
                call = calls[key] if key < len(calls) else {}
                if key >= len(calls):
                    calls.append(call)
                for frag_key in ("id", "type"):
                    if tc.get(frag_key) is not None:
                        call[frag_key] = tc[frag_key]
                fn = tc.get("function") or {}
                if fn.get("name") is not None:
                    call.setdefault("function", {})["name"] = fn["name"]
                if isinstance(fn.get("arguments"), str):
                    f = call.setdefault("function", {})
                    f["arguments"] = f.get("arguments", "") + fn["arguments"]
            fc = delta.get("function_call")
            if isinstance(fc, dict):
                legacy = c.setdefault("function_call", {})
                if fc.get("name") is not None:
                    legacy["name"] = fc["name"]
                if isinstance(fc.get("arguments"), str):
                    legacy["arguments"] = legacy.get("arguments", "") + fc["arguments"]
            known_delta = {
                "role",
                "content",
                "refusal",
                "reasoning",
                "reasoning_details",
                "annotations",
                "images",
                "tool_calls",
                "function_call",
            }
            for key, value in delta.items():
                if key not in known_delta and value is not None:
                    c.setdefault("extra_delta_fields", {}).setdefault(key, value)
            for field in ("logprobs",):
                if choice.get(field) is not None:
                    c[field] = choice[field]
            if choice.get("finish_reason") is not None:
                c["finish_reason"] = choice["finish_reason"]
            if choice.get("native_finish_reason") is not None:
                c["native_finish_reason"] = choice["native_finish_reason"]

    result: dict[str, Any] = {
        "protocol": "openai-chat-completions",
        "ids": ids,
        "created": sorted(created),
        "choices": [
            _finalize_tool_call_choice(c) for _, c in sorted(choices.items())
        ],
        "stream": {
            "events": len(payloads),
            "heartbeats": comments,
            "done": saw_done,
            "errors": n_errors,
            "complete": saw_done and not error,
        },
    }
    if model:
        result["model"] = model
    if provider:
        result["provider"] = provider
    if service_tier:
        result["service_tier"] = service_tier
    if usage:
        result["usage"] = usage
    if error is not None:
        result["error"] = error
        result["status"] = "error"
    elif saw_done:
        result["status"] = "completed"
    else:
        result["status"] = "incomplete"
    if extra:
        result["extra_top_level_fields"] = extra
    return result


def _finalize_tool_call_choice(choice: dict) -> dict:
    """Post-process one reduced choice: finalize tool calls, drop empty fields."""
    out: dict[str, Any] = {}
    for key, value in choice.items():
        if key == "tool_calls":
            out["tool_calls"] = [_finalize_tool_call(tc) for tc in value if tc]
        elif key == "reasoning_details" and not value:
            continue
        else:
            out[key] = value
    return out


def _find_item(items: dict[Any, dict], obj: dict) -> dict | None:
    idx = obj.get("output_index")
    if idx in items:
        return items[idx]
    item_id = obj.get("item_id")
    for item in items.values():
        if item.get("id") == item_id:
            return item
    return None


def _append_indexed(item: dict, field: str, index: Any, part_type: str) -> dict:
    parts = item.setdefault(field, [])
    while len(parts) <= index:
        parts.append({})
    part = parts[index]
    if not isinstance(part, dict):
        part = {}
        parts[index] = part
    part.setdefault("type", part_type)
    return part


_DONE_SNAPSHOTS = {
    "response.output_text.done": ("content", "text"),
    "response.reasoning_summary_text.done": ("summary", "text"),
    "response.reasoning_text.done": ("content", "text"),
    "response.refusal.done": ("content", "refusal"),
}
_DELTAS = {
    "response.output_text.delta": ("content", "text", "output_text"),
    "response.reasoning_summary_text.delta": ("summary", "text", "summary_text"),
    "response.reasoning_text.delta": ("content", "text", "reasoning_text"),
    "response.refusal.delta": ("content", "refusal", "refusal"),
}


def reduce_responses(payloads: list[Any], comments: int) -> dict[str, Any]:
    """Reduce OpenAI Responses API style events into one summary object."""
    meta: dict[str, Any] = {}
    final: dict[str, Any] | None = None
    items: dict[Any, dict] = {}
    status: str | None = None
    error: Any = None
    unhandled: set[str] = set()

    for obj in payloads:
        if not isinstance(obj, dict):
            continue
        etype = obj.get("type")
        if etype in ("response.created", "response.in_progress", "response.queued"):
            if isinstance(obj.get("response"), dict):
                for key, value in obj["response"].items():
                    meta.setdefault(key, value)
                status = status or "in_progress"
        elif etype == "response.output_item.added":
            idx = obj.get("output_index")
            if isinstance(obj.get("item"), dict):
                items[idx if idx is not None else obj["item"].get("id")] = json.loads(
                    json.dumps(obj["item"])
                )
        elif etype == "response.output_item.done":
            idx = obj.get("output_index")
            if isinstance(obj.get("item"), dict):
                items[idx if idx is not None else obj["item"].get("id")] = json.loads(
                    json.dumps(obj["item"])
                )
        elif etype in _DELTAS:
            field, key, part_type = _DELTAS[etype]
            item = _find_item(items, obj)
            if item is not None:
                ci = obj.get("content_index") or obj.get("summary_index") or 0
                part = _append_indexed(item, field, ci, part_type)
                if isinstance(obj.get("delta"), str):
                    part[key] = part.get(key, "") + obj["delta"]
        elif etype in _DONE_SNAPSHOTS:
            field, key = _DONE_SNAPSHOTS[etype]
            item = _find_item(items, obj)
            if item is not None:
                ci = obj.get("content_index") or obj.get("summary_index") or 0
                part = _append_indexed(item, field, ci, "")
                if obj.get(key) is not None:
                    part[key] = obj[key]
        elif etype == "response.content_part.added":
            item = _find_item(items, obj)
            if item is not None and isinstance(obj.get("part"), dict):
                ci = obj.get("content_index") or 0
                parts = item.setdefault("content", [])
                while len(parts) <= ci:
                    parts.append({})
                parts[ci] = json.loads(json.dumps(obj["part"]))
        elif etype in (
            "response.function_call_arguments.delta",
            "response.custom_tool_call_input.delta",
        ):
            item = _find_item(items, obj)
            key = "arguments" if "function_call" in etype else "input"
            if item is not None and isinstance(obj.get("delta"), str):
                item[key] = item.get(key, "") + obj["delta"]
        elif etype in (
            "response.function_call_arguments.done",
            "response.custom_tool_call_input.done",
        ):
            item = _find_item(items, obj)
            key = "arguments" if "function_call" in etype else "input"
            if item is not None and obj.get(key) is not None:
                item[key] = obj[key]
        elif etype == "response.completed":
            final = obj.get("response")
            status = "completed"
        elif etype == "response.incomplete":
            final = obj.get("response")
            status = "incomplete"
        elif etype == "response.failed":
            final = obj.get("response")
            status = "failed"
            error = (final or {}).get("error")
        elif etype == "error":
            error = obj.get("error") or obj
            status = "error"
        elif isinstance(etype, str):
            unhandled.add(etype)

    source = final if isinstance(final, dict) else {}
    output = source.get("output")
    if not isinstance(output, list):
        output = [items[k] for k in sorted(items, key=lambda x: (str(type(x)), x))]

    tool_calls = []
    text_parts: list[str] = []
    reasoning_parts: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "message":
            for part in item.get("content") or []:
                if isinstance(part, dict) and part.get("type") == "output_text":
                    text_parts.append(part.get("text") or "")
                elif isinstance(part, dict) and part.get("type") == "refusal":
                    text_parts.append(part.get("refusal") or "")
        elif item.get("type") == "reasoning":
            for part in item.get("summary") or []:
                if isinstance(part, dict):
                    reasoning_parts.append(part.get("text") or "")
            for part in item.get("content") or []:
                if isinstance(part, dict) and part.get("type") == "reasoning_text":
                    reasoning_parts.append(part.get("text") or "")
        elif item.get("type") in ("function_call", "custom_tool_call"):
            tc = {
                "type": item["type"],
                "name": item.get("name"),
            }
            if item.get("call_id") is not None:
                tc["call_id"] = item["call_id"]
            if item.get("id") is not None:
                tc["id"] = item["id"]
            raw = item.get("arguments") if "arguments" in item else item.get("input")
            tc["arguments" if "arguments" in item else "input"] = raw
            if isinstance(raw, str) and "arguments" in item:
                try:
                    tc["arguments_json"] = json.loads(raw)
                except ValueError:
                    pass
            tool_calls.append(tc)

    result: dict[str, Any] = {
        "protocol": "openai-responses",
        "stream": {
            "events": len(payloads),
            "heartbeats": comments,
            "complete": status == "completed",
        },
        "output_items": output,
    }
    rid = source.get("id") or meta.get("id")
    if rid:
        result["id"] = rid
    model = source.get("model") or meta.get("model")
    if model:
        result["model"] = model
    result["status"] = status or "incomplete"
    joined_text = "".join(text_parts)
    if joined_text:
        result["text"] = joined_text
    joined_reasoning = "".join(reasoning_parts)
    if joined_reasoning:
        result["reasoning"] = joined_reasoning
    if tool_calls:
        result["tool_calls"] = tool_calls
    if isinstance(source.get("usage"), dict):
        result["usage"] = source["usage"]
    elif isinstance(meta.get("usage"), dict):
        result["usage"] = meta["usage"]
    if error is not None:
        result["error"] = error
    if unhandled:
        result["unhandled_event_types"] = sorted(unhandled)
    return result


class AIStreamContentview(Contentview):
    name = "AI Stream"

    @property
    def syntax_highlight(self) -> str:
        return "yaml"

    def _parse(self, data: bytes) -> tuple[list[Any], int, bool]:
        events, comments = parse_sse(data.decode("utf-8", "replace"))
        return (*_payloads(events), comments)

    def prettify(self, data: bytes, metadata: Metadata) -> str:
        payloads, saw_done, comments = self._parse(data)
        if not _looks_like_ai_stream(payloads):
            raise ValueError("Not a recognized AI event stream.")
        if any(
            isinstance(o, dict)
            and isinstance(o.get("type"), str)
            and o["type"].startswith("response.")
            for o in payloads
        ):
            reduced = reduce_responses(payloads, comments)
        else:
            reduced = reduce_chat_completions(payloads, comments, saw_done)
        return json.dumps(reduced, indent=4, ensure_ascii=False)

    def render_priority(self, data: bytes, metadata: Metadata) -> float:
        if not data:
            return 0
        content_type = (metadata.content_type or "").split(";")[0].strip().lower()
        if content_type != "text/event-stream":
            return 0
        try:
            payloads, _, _ = self._parse(data)
        except Exception:
            return 0
        return 2 if _looks_like_ai_stream(payloads) else 0


ai_stream = AIStreamContentview()
