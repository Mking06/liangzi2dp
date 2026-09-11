from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from starlette.requests import Request

from app.dataplane.proxy.reverse import ReverseProxy
from app.dataplane.proxy.liangzidp_client import LiangzidpClient
from app.dataplane.proxy.tool_calls import (
    ToolProtocolError,
    ToolSieve,
    build_tool_repair_request,
    convert_openai_tool_response_with_repair,
    extract_tool_aware_output,
    is_continuation_only,
    openai_to_anthropic_stream,
    openai_tool_stream,
    resolve_tool_policy,
    stream_with_keepalive,
)
from app.products.anthropic.router import (
    _convert_anthropic_to_openai,
    _convert_openai_to_anthropic,
)


def bash_body(**extra):
    body = {
        "model": "deepseek-v4-flash",
        "tools": [{
            "type": "function",
            "function": {
                "name": "Bash",
                "description": "Run a shell command",
                "parameters": {
                    "type": "object",
                    "properties": {"command": {"type": "string"}},
                    "required": ["command"],
                    "additionalProperties": False,
                },
            },
        }],
    }
    body.update(extra)
    return body


def tool_xml(command: str = "dir") -> str:
    args = json.dumps({"command": command}, ensure_ascii=False)
    return (
        "<tool_calls><tool_call><tool_name>Bash</tool_name>"
        f"<parameters>{args}</parameters></tool_call></tool_calls>"
    )


async def async_source(items, delay: float = 0):
    for item in items:
        if delay:
            await asyncio.sleep(delay)
        yield item


async def collect(source):
    return [item async for item in source]


def parse_openai_sse(parts):
    events = []
    for part in parts:
        for block in str(part).split("\n\n"):
            data = "\n".join(line[5:].strip() for line in block.splitlines() if line.startswith("data:"))
            if data and data != "[DONE]":
                events.append(json.loads(data))
    return events


class ToolPolicyTests(unittest.TestCase):
    def test_policy_modes(self):
        self.assertEqual(resolve_tool_policy(bash_body()).mode, "auto")
        self.assertEqual(resolve_tool_policy(bash_body(tool_choice="none")).mode, "none")
        self.assertEqual(resolve_tool_policy(bash_body(tool_choice="required")).mode, "required")
        forced = resolve_tool_policy(bash_body(tool_choice={
            "type": "function", "function": {"name": "Bash"},
        }))
        self.assertEqual(forced.mode, "forced")
        self.assertEqual(forced.forced_name, "Bash")

    def test_invalid_forced_tool_is_rejected(self):
        with self.assertRaises(ToolProtocolError):
            resolve_tool_policy(bash_body(tool_choice={
                "type": "function", "function": {"name": "Missing"},
            }))


class ToolParsingTests(unittest.TestCase):
    def test_short_patch_is_not_double_applied_and_snapshot_prefix_is_kept(self):
        xml_tail = "_calls><tool_call><tool_name>Bash</tool_name><parameters>{\"command\":\"dir\"}</parameters></tool_call></tool_calls>"
        events = [
            {"op": "SET", "value": {"response": {
                "message_id": 2, "parent_id": 1, "status": "WIP",
                "fragments": [{"type": "RESPONSE", "content": "<"}],
            }}},
            {"p": "response/fragments/-1/content", "o": "APPEND", "v": "tool"},
            {"v": xml_tail},
            {"p": "response/status", "o": "SET", "v": "FINISHED"},
        ]
        raw = "".join("data: " + json.dumps(event) + "\n\n" for event in events) + "data: [DONE]\n\n"
        response = LiangzidpClient()._parse_sse_response(raw, response_model="deepseek-v4-flash")
        self.assertEqual(response["choices"][0]["message"]["content"], "<tool" + xml_tail)
        converted = __import__(
            "app.dataplane.proxy.tool_calls", fromlist=["convert_openai_tool_response"]
        ).convert_openai_tool_response(response, bash_body())
        self.assertIsNone(converted["choices"][0]["message"]["content"])
        self.assertEqual(
            json.loads(converted["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]),
            {"command": "dir"},
        )

    def test_mixed_text_and_tool_call(self):
        content, calls = extract_tool_aware_output("before" + tool_xml("ping 127.0.0.1") + "after", bash_body())
        self.assertEqual(content, "beforeafter")
        self.assertEqual(calls[0]["function"]["name"], "Bash")
        self.assertEqual(json.loads(calls[0]["function"]["arguments"]), {"command": "ping 127.0.0.1"})

    def test_fragmented_tool_call(self):
        sieve = ToolSieve(resolve_tool_policy(bash_body()))
        events = []
        for fragment in ("pre<tool_", "calls><tool_call><tool_name>Ba", "sh</tool_name><parameters>{\"command\":\"dir\"}</parameters></tool_call></tool_calls>post"):
            events.extend(sieve.push(fragment))
        events.extend(sieve.flush())
        self.assertEqual([event["type"] for event in events], ["text", "tool_calls", "text"])
        self.assertEqual(events[0]["text"], "pre")
        self.assertEqual(events[2]["text"], "post")

    def test_xml_inside_code_fence_is_not_executed(self):
        raw = "```xml\n" + tool_xml() + "\n```"
        content, calls = extract_tool_aware_output(raw, bash_body())
        self.assertEqual(content, raw)
        self.assertEqual(calls, [])

    def test_missing_required_argument_is_rejected(self):
        invalid = "<tool_calls><tool_call><tool_name>Bash</tool_name><parameters>{}</parameters></tool_call></tool_calls>"
        with self.assertRaisesRegex(ToolProtocolError, "missing required fields: command"):
            extract_tool_aware_output(invalid, bash_body())

    def test_final_guard_repeats_required_argument_names(self):
        body = bash_body()
        body["tools"][0]["function"].update({
            "name": "WebFetch",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                    "prompt": {"type": "string"},
                },
                "required": ["url", "prompt"],
                "additionalProperties": False,
            },
        })
        prompt = ReverseProxy()._convert_request({
            **body,
            "messages": [{"role": "user", "content": "inspect https://example.com"}],
        })["prompt"]
        self.assertIn("WebFetch(url, prompt)", prompt)
        self.assertIn("Never emit an empty parameters object", prompt)
        self.assertIn("without a following Tool result message", prompt)

    def test_repair_request_preserves_history_and_adds_validator_feedback(self):
        body = bash_body(messages=[
            {"role": "user", "content": "list files"},
            {"role": "assistant", "content": "working"},
            {"role": "user", "content": "continue"},
        ])
        response = {
            "choices": [{"message": {"content": (
                "<tool_calls><tool_call><tool_name>Bash</tool_name>"
                "<parameters>{}</parameters></tool_call></tool_calls>"
            )}}],
        }
        repaired = build_tool_repair_request(
            body,
            response,
            ToolProtocolError("Bash.arguments is missing required fields: command"),
        )
        self.assertEqual(repaired["messages"][0], body["messages"][0])
        self.assertEqual(repaired["messages"][-2]["role"], "assistant")
        self.assertIn("missing required fields: command", repaired["messages"][-1]["content"])
        self.assertIn("Current task context:\nlist files", repaired["messages"][-1]["content"])
        self.assertFalse(repaired["stream"])

    def test_continuation_only_variants_are_detected(self):
        for value in ("continue", "Continue.", "go on", "\u7ee7\u7eed", "\u7ee7\u7eed\u554a", "\u8bf7\u7ee7\u7eed\uff01"):
            self.assertTrue(is_continuation_only(value), value)
        self.assertFalse(is_continuation_only("\u7ee7\u7eed\u521b\u5efa\u4f7f\u7528\u8bf4\u660e\u6587\u4ef6"))

    def test_clean_repair_keeps_real_task_instead_of_continue(self):
        body = bash_body(messages=[
            {"role": "system", "content": "Use tools for file operations."},
            {"role": "user", "content": "Create requirements.txt and README.md"},
            {"role": "assistant", "content": "requirements complete"},
            {"role": "user", "content": "\u7ee7\u7eed\u554a"},
        ])
        response = {
            "choices": [{"message": {"content": "I will create the README next."}}],
        }
        repaired = build_tool_repair_request(
            body,
            response,
            ToolProtocolError("continuation stalled"),
            clean=True,
        )
        transcript = "\n".join(str(item.get("content") or "") for item in repaired["messages"])
        self.assertIn("Create requirements.txt and README.md", transcript)
        self.assertNotIn("\u7ee7\u7eed\u554a", transcript)
        self.assertTrue(repaired["_force_fresh_upstream_session"])

    def test_progress_only_continuation_is_repaired_not_returned_as_success(self):
        body = bash_body(messages=[
            {"role": "user", "content": "Create requirements.txt and README.md"},
            {"role": "assistant", "content": "requirements complete"},
            {"role": "user", "content": "\u7ee7\u7eed"},
        ])
        with self.assertRaisesRegex(ToolProtocolError, "progress narration"):
            extract_tool_aware_output("I will create the README next.", body)
        with self.assertRaisesRegex(ToolProtocolError, "progress narration"):
            extract_tool_aware_output("\u7ee7\u7eed\u521b\u5efa\u4f7f\u7528\u8bf4\u660e\u6587\u4ef6", body)
        content, calls = extract_tool_aware_output("README.md is complete and verified.", body)
        self.assertEqual(content, "README.md is complete and verified.")
        self.assertEqual(calls, [])

    def test_nonstream_tool_protocol_retries_once(self):
        invalid = {
            "choices": [{"message": {"content": (
                "<tool_calls><tool_call><tool_name>Bash</tool_name>"
                "<parameters>{}</parameters></tool_call></tool_calls>"
            )}}],
        }
        valid = {
            "choices": [{"message": {"content": tool_xml("dir")}, "finish_reason": "stop"}],
        }
        seen = []

        async def retry(request):
            seen.append(request)
            return valid

        converted, used_request = asyncio.run(convert_openai_tool_response_with_repair(
            invalid,
            bash_body(messages=[{"role": "user", "content": "list files"}]),
            retry,
        ))
        self.assertEqual(len(seen), 1)
        self.assertIs(used_request, seen[0])
        self.assertEqual(
            json.loads(converted["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]),
            {"command": "dir"},
        )

    def test_single_nested_parameters_wrapper_is_safely_unwrapped(self):
        wrapped = tool_xml().replace(
            '{"command": "dir"}',
            '{"parameters": {"command": "dir"}}',
        )
        _, calls = extract_tool_aware_output(wrapped, bash_body())
        self.assertEqual(
            json.loads(calls[0]["function"]["arguments"]),
            {"command": "dir"},
        )

    def test_missing_inner_tool_call_wrapper_is_recovered(self):
        raw = (
            "<tool_calls><tool_name>Bash</tool_name>"
            '<parameters>{"command":"dir"}</parameters></tool_calls>'
        )
        _, calls = extract_tool_aware_output(raw, bash_body())
        self.assertEqual(calls[0]["function"]["name"], "Bash")
        self.assertEqual(
            json.loads(calls[0]["function"]["arguments"]),
            {"command": "dir"},
        )

    def test_json_object_inside_tool_calls_is_recovered(self):
        raw = (
            '<tool_calls>{"name":"Bash","arguments":'
            '{"command":"dir"}}</tool_calls>'
        )
        _, calls = extract_tool_aware_output(raw, bash_body())
        self.assertEqual(calls[0]["function"]["name"], "Bash")
        self.assertEqual(
            json.loads(calls[0]["function"]["arguments"]),
            {"command": "dir"},
        )

    def test_second_protocol_repair_uses_fresh_clean_session(self):
        invalid = {
            "choices": [{"message": {"content": (
                "<tool_calls><tool_call><tool_name>Bash</tool_name>"
                "<parameters>{}</parameters></tool_call></tool_calls>"
            )}}],
        }
        valid = {
            "choices": [{"message": {"content": tool_xml("dir")}, "finish_reason": "stop"}],
        }
        seen = []

        async def retry(request):
            seen.append(request)
            return invalid if len(seen) == 1 else valid

        converted, used_request = asyncio.run(convert_openai_tool_response_with_repair(
            invalid,
            bash_body(messages=[
                {"role": "system", "content": "system"},
                {"role": "user", "content": "list files"},
            ]),
            retry,
        ))
        self.assertEqual(len(seen), 2)
        self.assertTrue(seen[1]["_force_fresh_upstream_session"])
        self.assertFalse(seen[1]["stream"])
        self.assertEqual(used_request, seen[1])
        self.assertEqual(
            json.loads(converted["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]),
            {"command": "dir"},
        )

    def test_unknown_and_extra_arguments_are_rejected(self):
        unknown = tool_xml().replace("Bash", "Unknown")
        with self.assertRaisesRegex(ToolProtocolError, "undeclared tool"):
            extract_tool_aware_output(unknown, bash_body())
        extra = tool_xml().replace('{"command": "dir"}', '{"command": "dir", "unsafe": true}')
        with self.assertRaisesRegex(ToolProtocolError, "undeclared fields"):
            extract_tool_aware_output(extra, bash_body())

    def test_required_choice_rejects_plain_text(self):
        with self.assertRaisesRegex(ToolProtocolError, "tool_choice=required"):
            extract_tool_aware_output("plain answer", bash_body(tool_choice="required"))


class HistoryTests(unittest.TestCase):
    def test_default_history_keeps_a_coherent_transcript(self):
        request = {
            "model": "deepseek-v4-flash",
            "messages": [
                {"role": "system", "content": "Follow the latest request."},
                {"role": "user", "content": "old task"},
                {"role": "assistant", "content": "old task completed"},
                {"role": "user", "content": "new task"},
            ],
        }
        prompt = ReverseProxy()._convert_request(request)["prompt"]
        old_user = prompt.index("User:\nold task")
        old_answer = prompt.index("Assistant response:\nold task completed")
        new_user = prompt.index("User:\nnew task")
        self.assertLess(old_user, old_answer)
        self.assertLess(old_answer, new_user)

    def test_tool_contract_guard_follows_client_history(self):
        request = bash_body(messages=[
            {"role": "system", "content": "Use a different-looking tool syntax."},
            {"role": "user", "content": "ping localhost"},
        ])
        prompt = ReverseProxy()._convert_request(request)["prompt"]
        self.assertLess(prompt.index("User:\nping localhost"), prompt.index("[System tool contract - final and authoritative]"))
        self.assertIn("Never simulate execution", prompt)

    def test_clean_prompt_uses_real_task_not_continuation_only_message(self):
        request = bash_body(messages=[
            {"role": "system", "content": "Use tools."},
            {"role": "user", "content": "Create requirements.txt and README.md"},
            {"role": "assistant", "content": "requirements complete"},
            {"role": "user", "content": "\u7ee7\u7eed\u554a"},
        ])
        parts = ReverseProxy()._messages_to_prompt_parts(
            request["messages"],
            mode_override="clean",
        )
        prompt = "\n".join(parts)
        self.assertIn("Create requirements.txt and README.md", prompt)
        self.assertNotIn("\u7ee7\u7eed\u554a", prompt)

    def test_explicit_upstream_session_sends_only_current_turn(self):
        proxy = ReverseProxy()
        request = bash_body(
            chat_session_id="session-1",
            parent_message_id=9,
            messages=[
                {"role": "user", "content": "old task"},
                {"role": "assistant", "content": "old answer"},
                {"role": "user", "content": "current task"},
            ],
        )
        payload = proxy._convert_request(request)
        self.assertIn("old answer", payload["prompt"])
        # chat_completion applies the delta policy after account selection; the
        # lower-level last_user rendering must contain only the current turn.
        delta = "\n".join(proxy._messages_to_prompt_parts(request["messages"], mode_override="last_user"))
        self.assertNotIn("old task", delta)
        self.assertNotIn("old answer", delta)
        self.assertIn("current task", delta)

    def test_delta_after_tool_call_contains_tool_result_not_stale_user(self):
        proxy = ReverseProxy()
        messages = [
            {"role": "user", "content": "old task"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "Bash", "arguments": '{"command":"dir"}'},
                }],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "tool output"},
        ]
        delta = "\n".join(proxy._messages_to_prompt_parts(messages, mode_override="delta"))
        self.assertIn("tool output", delta)
        self.assertNotIn("old task", delta)

    def test_pro_url_does_not_silently_downgrade_model(self):
        payload = ReverseProxy()._convert_request({
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "inspect https://example.com"}],
        })
        self.assertEqual(payload["model_type"], "expert")
        self.assertEqual(payload["response_model"], "deepseek-v4-pro")

    def test_openai_tool_history_is_preserved_in_prompt(self):
        request = bash_body(messages=[
            {"role": "user", "content": "inspect"},
            {
                "role": "assistant",
                "content": None,
                "reasoning_content": "need a command",
                "tool_calls": [{
                    "id": "call_1", "type": "function",
                    "function": {"name": "Bash", "arguments": '{"command":"dir"}'},
                }],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "ok"},
        ])
        prompt = ReverseProxy()._convert_request(request)["prompt"]
        self.assertIn("Assistant reasoning:\nneed a command", prompt)
        self.assertIn("<call_id>call_1</call_id>", prompt)
        self.assertIn("Tool result (name=tool, call_id=call_1):\nok", prompt)
        self.assertIn("Available tools:", prompt)

    def test_anthropic_history_keeps_thinking_and_tool_ids(self):
        body = {
            "model": "deepseek-v4-pro[1m]",
            "messages": [
                {"role": "user", "content": "inspect"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "need shell"},
                    {"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {"command": "dir"}},
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok"},
                ]},
            ],
            "tools": [{
                "name": "Bash",
                "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]},
            }],
        }
        converted = _convert_anthropic_to_openai(body)
        self.assertEqual(converted["model"], "deepseek-v4-pro")
        self.assertEqual(converted["messages"][1]["reasoning_content"], "need shell")
        self.assertEqual(converted["messages"][1]["tool_calls"][0]["id"], "toolu_1")
        self.assertEqual(converted["messages"][2]["tool_call_id"], "toolu_1")

    def test_openai_response_converts_to_anthropic_tool_use(self):
        response = {
            "id": "chatcmpl-1", "choices": [{"finish_reason": "tool_calls", "message": {
                "role": "assistant", "reasoning_content": "need shell", "content": None,
                "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "Bash", "arguments": '{"command":"dir"}'}}],
            }}], "usage": {},
        }
        converted = _convert_openai_to_anthropic(response, "deepseek-v4-flash")
        self.assertEqual(converted["stop_reason"], "tool_use")
        self.assertEqual(converted["content"][0]["type"], "thinking")
        self.assertEqual(converted["content"][1]["input"], {"command": "dir"})


class StreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_anthropic_stream_repairs_arguments_before_emitting_tool_use(self):
        invalid = {
            "id": "chatcmpl-invalid",
            "model": "deepseek-v4-flash",
            "choices": [{"finish_reason": "stop", "message": {"role": "assistant", "content": (
                "<tool_calls><tool_call><tool_name>Bash</tool_name>"
                "<parameters>{}</parameters></tool_call></tool_calls>"
            )}}],
            "usage": {},
        }
        valid = {
            "id": "chatcmpl-valid",
            "model": "deepseek-v4-flash",
            "choices": [{"finish_reason": "stop", "message": {
                "role": "assistant", "content": tool_xml("ping 127.0.0.1"),
            }}],
            "usage": {},
        }

        class FakeProxy:
            def __init__(self):
                self.requests = []

            async def chat_completion(self, **kwargs):
                self.requests.append(kwargs["request_data"])
                return invalid if len(self.requests) == 1 else valid

        class FakeStore:
            def __init__(self):
                self.metrics = []

            async def increment_metric(self, name):
                self.metrics.append(name)

            async def update_conversation(self, *args, **kwargs):
                return None

        class FakeOrchestrator:
            def __init__(self):
                self.store = FakeStore()
                self.calls = []

            def session_key(self, body):
                return "session", "test"

            async def run_singleflight(self, body, invoke):
                return await invoke()

            async def register_tool_calls(self, body, calls):
                self.calls.extend(calls)

        body = {
            "model": "deepseek-v4-flash",
            "stream": True,
            "messages": [{"role": "user", "content": "ping localhost"}],
            "tools": [{
                "name": "Bash",
                "description": "Run a shell command",
                "input_schema": bash_body()["tools"][0]["function"]["parameters"],
            }],
        }
        raw_body = json.dumps(body).encode()
        delivered = False

        async def receive():
            nonlocal delivered
            if delivered:
                return {"type": "http.disconnect"}
            delivered = True
            return {"type": "http.request", "body": raw_body, "more_body": False}

        app = FastAPI()
        orchestrator = FakeOrchestrator()
        app.state.repository = object()
        app.state.orchestrator = orchestrator
        request = Request({
            "type": "http",
            "method": "POST",
            "path": "/v1/messages",
            "headers": [],
            "app": app,
        }, receive)
        fake_proxy = FakeProxy()

        with patch("app.products.anthropic.router._proxy", fake_proxy):
            from app.products.anthropic.router import anthropic_messages

            response = await anthropic_messages(request)
            output = [part.decode() if isinstance(part, bytes) else part async for part in response.body_iterator]

        payloads = [
            json.loads(line[6:])
            for part in output
            for line in part.splitlines()
            if line.startswith("data: ")
        ]
        start = next(
            item for item in payloads
            if item.get("type") == "content_block_start"
            and item.get("content_block", {}).get("type") == "tool_use"
        )
        delta = next(
            item for item in payloads
            if item.get("type") == "content_block_delta"
            and item.get("delta", {}).get("type") == "input_json_delta"
        )
        self.assertEqual(start["content_block"]["input"], {})
        self.assertEqual(json.loads(delta["delta"]["partial_json"]), {"command": "ping 127.0.0.1"})
        self.assertEqual(len(fake_proxy.requests), 2)
        self.assertFalse(fake_proxy.requests[0]["stream"])
        self.assertFalse(fake_proxy.requests[1]["stream"])
        self.assertEqual(orchestrator.store.metrics, ["tool_protocol_repairs"])
        self.assertEqual(len(orchestrator.calls), 1)

    async def test_openai_stream_emits_complete_arguments_once(self):
        chunks = [
            'data: {"id":"x","model":"deepseek-v4-flash","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}\n\n',
            'data: {"id":"x","model":"deepseek-v4-flash","choices":[{"index":0,"delta":{"content":"<tool_calls><tool_call><tool_name>B"},"finish_reason":null}]}\n\n',
            'data: {"id":"x","model":"deepseek-v4-flash","choices":[{"index":0,"delta":{"content":"ash</tool_name><parameters>{\\"command\\":\\"dir\\"}</parameters></tool_call></tool_calls>"},"finish_reason":null}]}\n\n',
            'data: {"id":"x","model":"deepseek-v4-flash","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n',
            'data: [DONE]\n\n',
        ]
        output = await collect(openai_tool_stream(async_source(chunks), bash_body()))
        events = parse_openai_sse(output)
        calls = [call for event in events for call in ((event.get("choices") or [{}])[0].get("delta") or {}).get("tool_calls", [])]
        self.assertEqual(len(calls), 1)
        self.assertEqual(json.loads(calls[0]["function"]["arguments"]), {"command": "dir"})
        self.assertEqual(events[-1]["choices"][0]["finish_reason"], "tool_calls")
        self.assertNotIn("<tool_calls>", "".join(output))

    async def test_anthropic_stream_uses_standard_empty_start_input(self):
        openai = [
            'data: {"id":"x","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}\n\n',
            'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"Bash","arguments":"{\\"command\\":\\"dir\\"}"}}]},"finish_reason":null}]}\n\n',
            'data: {"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}\n\n',
            'data: [DONE]\n\n',
        ]
        output = await collect(openai_to_anthropic_stream(async_source(openai), "deepseek-v4-flash"))
        payloads = [json.loads(line[6:]) for part in output for line in part.splitlines() if line.startswith("data: ")]
        start = next(item for item in payloads if item.get("type") == "content_block_start")
        delta = next(item for item in payloads if item.get("type") == "content_block_delta")
        message_delta = next(item for item in payloads if item.get("type") == "message_delta")
        self.assertEqual(start["content_block"]["input"], {})
        self.assertEqual(json.loads(delta["delta"]["partial_json"]), {"command": "dir"})
        self.assertEqual(message_delta["delta"]["stop_reason"], "tool_use")

    async def test_keepalive_does_not_cancel_slow_source(self):
        output = await collect(stream_with_keepalive(async_source(["done"], delay=0.04), interval=0.01))
        self.assertIn(None, output)
        self.assertEqual(output[-1], "done")


if __name__ == "__main__":
    unittest.main()
