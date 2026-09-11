"""Liangzidp Web API 协议逆向客户端。



请求格式（从浏览器抓包确认）:

  POST /api/v0/chat/completion

  {

    "chat_session_id": "uuid",

    "parent_message_id": 12,

    "model_type": null,

    "prompt": "用户输入",

    "ref_file_ids": [],

    "thinking_enabled": false,

    "search_enabled": true,

    "action": null,

    "preempt": false

  }



响应格式（SSE + JSON Patch）:

  event: ready

  data: {"request_message_id":13,"response_message_id":14,"model_type":"default"}



  event: update_session

  data: {"updated_at":...}



  data: {"v":{"response":{...,"fragments":[{"id":2,"type":"RESPONSE","content":"..."}]}}}

  data: {"p":"response/fragments/-1/content","o":"APPEND","v":"追加的文本"}

  data: {"p":"response","o":"BATCH","v":[{"p":"accumulated_token_usage","v":1562},{"p":"quasi_status","v":"FINISHED"}]}

  data: {"p":"response/status","o":"SET","v":"FINISHED"}

  event: close

"""



from __future__ import annotations



import asyncio

import base64

import json

import os

import queue

import threading

import time

import uuid

from typing import Any, AsyncGenerator



import curl_cffi.requests



from app.dataplane.proxy.liangzidp_events import (

    LiangzidpEventReducer,

    parse_sse_blocks,

)

from app.dataplane.proxy.protocol_trace import ProtocolTrace





LIANGZIDP_BASE = "https://chat.deepseek.com"

CHAT_URL = f"{LIANGZIDP_BASE}/api/v0/chat/completion"

CHAT_SESSION_CREATE_URL = f"{LIANGZIDP_BASE}/api/v0/chat_session/create"

MODELS_URL = f"{LIANGZIDP_BASE}/api/v0/models"

CHALLENGE_URL = f"{LIANGZIDP_BASE}/api/v0/chat/create_pow_challenge"

USER_URL = f"{LIANGZIDP_BASE}/api/v0/users/current"

SETTINGS_URL = f"{LIANGZIDP_BASE}/api/v0/client/settings"

POW_SERVICE_URL = os.getenv("POW_SERVICE_URL", "http://127.0.0.1:8787").rstrip("/")



DEFAULT_HEADERS = {

    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",

    "Accept": "*/*",

    "Accept-Encoding": "gzip, deflate, br, zstd",

    "Accept-Language": "zh-CN,zh;q=0.9",

    "Content-Type": "application/json",

    "Origin": "https://chat.deepseek.com",

    "Referer": "https://chat.deepseek.com/",

    "Sec-Fetch-Dest": "empty",

    "Sec-Fetch-Mode": "cors",

    "Sec-Fetch-Site": "same-origin",

    "X-Client-Bundle-Id": "com.deepseek.chat",

    "X-Client-Locale": "zh_CN",

    "X-Client-Platform": "web",

    "X-Client-Timezone-Offset": "28800",

    "X-Client-Version": "2.2.0",

}





class LiangzidpAPIError(Exception):

    """Error from Liangzidp API."""



    def __init__(self, message: str, status_code: int = 500, code: str = "error"):

        self.message = message

        self.status_code = status_code

        self.code = code

        super().__init__(message)



    @classmethod

    def from_response(cls, resp) -> "LiangzidpAPIError":

        try:

            data = resp.json()

            msg = data.get("msg", data.get("message", resp.text[:200]))

            code = data.get("code", "api_error")

        except Exception:

            msg = resp.text[:200] or f"HTTP {resp.status_code}"

            code = "api_error"

        return cls.normalized(str(msg), resp.status_code, str(code))



    @classmethod

    def normalized(

        cls,

        message: str,

        status_code: int = 502,

        code: str = "upstream_error",

    ) -> "LiangzidpAPIError":

        """Map Liangzidp Web prose errors to stable API semantics."""

        lowered = message.lower()

        normalized_code = str(code or "").strip().lower()

        if normalized_code in {

            "context_length",

            "context_length_exceeded",

            "content_too_long",

        } or any(

            marker in lowered

            for marker in (

                "content too long",

                "context length",

                "内容超长",

                "达到对话长度上限",

                "对话长度上限",

                "请开启新对话",

            )

        ):

            return cls(message, 413, "context_length_exceeded")

        if any(

            marker in lowered

            for marker in ("服务器繁忙", "server busy", "快速模式", "quick mode")

        ):

            return cls(message, 503, "upstream_busy")

        if any(marker in lowered for marker in ("too frequent", "rate limit", "频繁")):

            return cls(message, 429, "rate_limit_exceeded")

        if any(

            marker in lowered

            for marker in (

                "有消息正在生成",

                "message is being generated",

                "message still wip",

                "parallel chat",

            )

        ):

            return cls(message, 409, "parallel_chat_limit")

        if any(

            marker in lowered

            for marker in ("invalid chat session id", "invalid message id")

        ):

            return cls(message, 409, "session_state_invalid")

        return cls(message, status_code, code)





class LiangzidpClient:

    """Client for Liangzidp Web API.



    认证方式: Bearer token (从浏览器 localStorage 获取的 memoryToken)

    POW 验证: 需要 X-Ds-Pow-Response header

    """



    def __init__(

        self,

        bearer_token: str = "",

        session_token: str = "",

        csrf_token: str = "",

        pow_response: str = "",

        proxy: str = "",

        timeout: int = 60,

        impersonate: str = "chrome136",

    ):

        # Liangzidp Web deployments seen in the wild use a browser memory token

        # in Authorization: Bearer.  The rest of this project historically named

        # that value session_token, so accept both names and normalize here.

        self._bearer_token = bearer_token or session_token

        self._session_token = session_token or bearer_token

        self._csrf_token = csrf_token

        self._pow_response = pow_response

        self._proxy = proxy or None

        self._timeout = timeout

        self._impersonate = impersonate



    def _get_headers(self, extra: dict | None = None) -> dict:

        headers = {**DEFAULT_HEADERS}

        if self._bearer_token:

            headers["Authorization"] = f"Bearer {self._bearer_token}"

        if self._csrf_token:

            headers["X-CSRF-Token"] = self._csrf_token

        if self._pow_response:

            headers["X-Ds-Pow-Response"] = self._pow_response

        if extra:

            headers.update(extra)

        return headers



    def _get_session_kwargs(self) -> dict:

        kwargs = {"impersonate": self._impersonate, "timeout": self._timeout}

        if self._proxy:

            kwargs["proxies"] = {"http": self._proxy, "https": self._proxy}

        return kwargs



    @staticmethod

    def _json_body(payload: dict) -> bytes:

        """Serialize request JSON as ASCII escapes to avoid upstream mojibake."""

        return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode(

            "ascii"

        )



    async def chat_completion(

        self,

        prompt: str,

        chat_session_id: str = "",

        parent_message_id: int | None = None,

        model_type: str | None = None,

        response_model: str = "",

        ref_file_ids: list | None = None,

        thinking_enabled: bool = False,

        search_enabled: bool = True,

        action: str | None = None,

        preempt: bool = False,

        stream: bool = True,

        pow_response: str = "",

    ) -> dict | AsyncGenerator[str, None]:

        """Send a chat completion request.



        Args:

            prompt: 用户输入的文本

            chat_session_id: 会话 ID（首次可留空，服务器会创建）

            parent_message_id: 上一条消息 ID

            model_type: 模型类型（null=default, "reasoner"=深度思考）

            stream: 是否流式返回

            pow_response: POW 验证响应（base64 encoded JSON）

        """

        if not chat_session_id:

            chat_session_id = await self.create_chat_session()



        payload = {

            "chat_session_id": chat_session_id,

            "parent_message_id": parent_message_id,

            "model_type": model_type,

            "prompt": prompt,

            "ref_file_ids": ref_file_ids or [],

            "thinking_enabled": thinking_enabled,

            "search_enabled": search_enabled,

            "action": action,

            "preempt": preempt,

        }



        trace = ProtocolTrace(

            {

                "chat_session": ProtocolTrace.digest(chat_session_id),

                "parent_message_id": parent_message_id,

                "model_type": model_type,

                "response_model": response_model,

                "thinking_enabled": thinking_enabled,

                "search_enabled": search_enabled,

                "stream": stream,

                "prompt_chars": len(prompt),

            }

        )



        headers = self._get_headers()

        if pow_response:

            headers["X-Ds-Pow-Response"] = pow_response

        elif not self._pow_response and os.getenv(

            "LIANGZIDP_AUTO_POW", "true"

        ).lower() in {"1", "true", "yes", "on"}:

            headers["X-Ds-Pow-Response"] = await self.build_pow_response(

                "/api/v0/chat/completion"

            )



        if stream:

            return self._stream_chat(payload, headers, response_model, trace)

        else:

            return await self._do_chat(payload, headers, response_model, trace)



    async def _do_chat(

        self,

        payload: dict,

        headers: dict,

        response_model: str = "",

        trace: ProtocolTrace | None = None,

    ) -> dict:

        """Non-streaming chat completion."""



        def request_once() -> tuple[int, dict[str, str], bytes]:

            # curl_cffi AsyncSession cleanup is unstable when an ASGI client

            # disconnect cancels a non-streaming request on Windows/Python 3.14.

            # A worker thread lets libcurl finish and release its handle without

            # emitting repeated cleanup callback exceptions.

            with curl_cffi.requests.Session(**self._get_session_kwargs()) as session:

                resp = session.post(

                    CHAT_URL,

                    headers=headers,

                    data=self._json_body(payload),

                )

                return resp.status_code, dict(resp.headers), bytes(resp.content)



        status_code, response_headers, response_body = await asyncio.to_thread(

            request_once

        )

        text = response_body.decode("utf-8", errors="replace")

        if trace:

            trace.record(

                "http_response",

                {

                    "status_code": status_code,

                    "content_type": response_headers.get("content-type", ""),

                    "body_chars": len(text),

                },

            )



        if status_code in (403, 40301):

            raise LiangzidpAPIError("INVALID_POW_RESPONSE", 403, "invalid_pow")

        if status_code in (401, 40003):

            raise LiangzidpAPIError("INVALID_TOKEN", 401, "invalid_api_key")

        if status_code == 429:

            raise LiangzidpAPIError("Rate limited", 429, "rate_limit_exceeded")

        if status_code != 200:

            try:

                error_data = json.loads(text)

                error_message = error_data.get(

                    "msg", error_data.get("message", text[:200])

                )

                error_code = error_data.get("code", "api_error")

            except Exception:

                error_message = text[:200] or f"HTTP {status_code}"

                error_code = "api_error"

            raise LiangzidpAPIError.normalized(

                str(error_message), status_code, str(error_code)

            )



        content_type = (response_headers.get("content-type") or "").lower()

        if "application/json" in content_type or text.lstrip().startswith("{"):

            try:

                data = json.loads(text)

            except Exception:

                data = None

            if isinstance(data, dict):

                code = data.get("code")

                if code not in (None, 0):

                    message = f"Chat rejected: code={code} msg={data.get('msg') or '-'}"

                    raise LiangzidpAPIError.normalized(

                        message,

                        401 if str(code) in {"40003", "401"} else 500,

                        "invalid_api_key"

                        if str(code) in {"40003", "401"}

                        else "chat_error",

                    )

                inner = data.get("data")

                if isinstance(inner, dict):

                    biz_code = inner.get("biz_code")

                    if biz_code not in (None, 0):

                        message = (

                            f"Chat rejected: biz_code={biz_code} "

                            f"msg={inner.get('biz_msg') or '-'}"

                        )

                        raise LiangzidpAPIError.normalized(

                            message,

                            500,

                            "chat_error",

                        )

            raise LiangzidpAPIError.normalized(

                f"Chat returned JSON instead of SSE: {text[:300]}",

                500,

                "chat_error",

            )



        return self._parse_sse_response(

            text,

            chat_session_id=str(payload.get("chat_session_id") or ""),

            prompt_chars=len(str(payload.get("prompt") or "")),

            response_model=response_model,

            trace=trace,

        )



    async def _stream_chat(

        self,

        payload: dict,

        headers: dict,

        response_model: str = "",

        trace: ProtocolTrace | None = None,

    ) -> AsyncGenerator[str, None]:

        """Translate the browser SSE stream through the canonical reducer."""

        headers["Accept"] = "text/event-stream"

        stream_id = f"chatcmpl-{uuid.uuid4().hex}"

        created = int(time.time())

        model = response_model or (
            "deepseek-v4-pro"
            if payload.get("model_type") == "expert"
            else (
                "deepseek-v4-vision"
                if payload.get("model_type") == "vision"
                else "deepseek-v4-flash"
            )
        )
        bridge: queue.Queue[tuple[str, Any]] = queue.Queue()

        stop_worker = threading.Event()

        reducer = LiangzidpEventReducer()



        def chunk(

            delta: dict[str, Any],

            finish_reason: str | None = None,

            liangzidp: dict[str, Any] | None = None,

        ) -> str:

            body: dict[str, Any] = {

                "id": stream_id,

                "object": "chat.completion.chunk",

                "created": created,

                "model": str(model),

                "choices": [

                    {"index": 0, "delta": delta, "finish_reason": finish_reason}

                ],

            }

            if liangzidp:

                body["liangzidp"] = liangzidp

            return (

                "data: "

                + json.dumps(body, ensure_ascii=False, separators=(",", ":"))

                + "\n\n"

            )



        def stream_worker() -> None:

            try:

                with curl_cffi.requests.Session(

                    **self._get_session_kwargs()

                ) as session:

                    response = session.post(

                        CHAT_URL,

                        headers=headers,

                        data=self._json_body(payload),

                        stream=True,

                    )

                    if response.status_code in (403, 40301):

                        raise LiangzidpAPIError(

                            "INVALID_POW_RESPONSE", 403, "invalid_pow"

                        )

                    if response.status_code in (401, 40003):

                        raise LiangzidpAPIError("INVALID_TOKEN", 401, "invalid_api_key")

                    if response.status_code == 429:

                        raise LiangzidpAPIError(

                            "Rate limited", 429, "rate_limit_exceeded"

                        )

                    if response.status_code != 200:

                        raise LiangzidpAPIError.from_response(response)

                    if trace:

                        trace.record(

                            "http_response",

                            {

                                "status_code": response.status_code,

                                "content_type": response.headers.get(

                                    "content-type", ""

                                ),

                            },

                        )

                    bridge.put(("ready", None))

                    for raw_line in response.iter_lines():

                        if stop_worker.is_set():

                            break

                        bridge.put(("line", raw_line))

            except BaseException as exc:

                bridge.put(("error", exc))

            finally:

                bridge.put(("done", None))



        def decode_line(raw: Any) -> str:

            if raw is None:

                return ""

            if isinstance(raw, bytes):

                return raw.decode("utf-8", errors="replace")

            return str(raw)



        worker = threading.Thread(

            target=stream_worker,

            name=f"liangzidp-stream-{stream_id[-8:]}",

            daemon=True,

        )

        worker.start()



        event_name = "message"

        event_data: list[str] = []

        terminal_sent = False

        transport_reason = "eof"



        async def emit_frame(name: str, raw_data: str):

            nonlocal terminal_sent, transport_reason

            if raw_data == "[DONE]":

                reducer.feed("done", None)

                transport_reason = "done_marker"

                return

            try:

                event = json.loads(raw_data)

            except json.JSONDecodeError:

                if trace:

                    trace.record(

                        "malformed_sse", {"event": name, "data": raw_data[:1000]}

                    )

                return

            if trace:

                trace.record("raw_sse", {"event": name, "data": event})

            for semantic in reducer.feed(name, event):

                if semantic.kind == "text_delta" and semantic.text:

                    yield chunk({"content": semantic.text})

                elif semantic.kind == "reasoning_delta" and semantic.text:

                    yield chunk({"reasoning_content": semantic.text})

                elif semantic.kind == "metadata":

                    yield chunk(

                        {},

                        liangzidp={

                            "chat_session_id": str(

                                payload.get("chat_session_id") or ""

                            ),

                            **semantic.metadata,

                        },

                    )

                elif semantic.kind == "terminal" and not terminal_sent:

                    terminal_sent = True

                    yield chunk(

                        {},

                        semantic.finish_reason,

                        {

                            "chat_session_id": str(

                                payload.get("chat_session_id") or ""

                            ),

                            "status": semantic.status,

                            "usage_total": reducer.usage_total,

                            "error_code": reducer.error_code,

                            "error_message": reducer.error_message,

                            **reducer.metadata,

                        },

                    )



        try:

            role_metadata = {

                "chat_session_id": str(payload.get("chat_session_id") or "")

            }

            yield chunk({"role": "assistant"}, liangzidp=role_metadata)

            done = False

            while not done:

                kind, value = await asyncio.to_thread(bridge.get)

                if kind == "error":

                    raise value

                if kind == "ready":

                    continue

                if kind == "done":

                    done = True

                    continue



                line = decode_line(value).rstrip("\r")

                if line == "":

                    if event_data:

                        raw_data = "\n".join(event_data).strip()

                        event_data = []

                        async for rendered in emit_frame(event_name, raw_data):

                            yield rendered

                        if raw_data == "[DONE]":

                            done = True

                        event_name = "message"

                    continue

                if line.startswith("event:"):

                    event_name = line[6:].strip() or "message"

                elif line.startswith("data:"):

                    event_data.append(line[5:].lstrip())



            if event_data:

                async for rendered in emit_frame(

                    event_name, "\n".join(event_data).strip()

                ):

                    yield rendered



            if not terminal_sent:

                terminal = reducer.finalize(transport_reason)

                if trace:

                    trace.record(

                        "synthetic_terminal",

                        {

                            "status": terminal.status,

                            "finish_reason": terminal.finish_reason,

                            **terminal.metadata,

                        },

                    )

                yield chunk(

                    {},

                    terminal.finish_reason,

                    {

                        "chat_session_id": str(payload.get("chat_session_id") or ""),

                        "status": terminal.status,

                        "usage_total": reducer.usage_total,

                        "error_code": reducer.error_code,

                        "error_message": reducer.error_message,

                        **reducer.metadata,

                    },

                )

            yield "data: [DONE]\n\n"

        except asyncio.CancelledError:

            transport_reason = "client_cancelled"

            if trace:

                trace.record("client_cancelled", {})

            raise

        finally:

            stop_worker.set()



    def _parse_sse_response(

        self,

        sse_text: str,

        chat_session_id: str = "",

        prompt_chars: int = 0,

        response_model: str = "",

        trace: ProtocolTrace | None = None,

    ) -> dict:

        """Parse a complete SSE body through the canonical event reducer."""

        reducer = LiangzidpEventReducer()

        raw_events: list[dict[str, Any]] = []

        for event_name, payload in parse_sse_blocks(sse_text):

            if trace:

                trace.record("raw_sse", {"event": event_name, "data": payload})

            if isinstance(payload, dict):

                raw_events.append(payload)

            reducer.feed(event_name, payload)



        terminal = (

            reducer.finalize("buffered_eof") if not reducer.terminal_emitted else None

        )

        finish_reason = terminal.finish_reason if terminal else reducer.finish_reason

        if finish_reason == "error":

            if trace:

                trace.record(

                    "incomplete_stream",

                    {

                        "status": reducer.status,

                        "content_chars": len(reducer.content),

                        "reasoning_chars": len(reducer.reasoning_content),

                    },

                )

            if reducer.error_code == "rate_limit_exceeded":

                raise LiangzidpAPIError.normalized(

                    reducer.error_message or "Liangzidp Web rate limit reached",

                    429,

                    "rate_limit_exceeded",

                )

            raise LiangzidpAPIError.normalized(

                reducer.error_message

                or f"Liangzidp stream ended without a successful terminal status ({reducer.status})",

                502,

                reducer.error_code or "incomplete_stream",

            )



        message: dict[str, Any] = {"role": "assistant", "content": reducer.content}

        if reducer.reasoning_content:

            message["reasoning_content"] = reducer.reasoning_content

        request_msg_id = reducer.metadata.get("request_message_id")

        response_msg_id = reducer.metadata.get("response_message_id")

        return {

            "id": f"chatcmpl-{response_msg_id or uuid.uuid4().hex}",

            "object": "chat.completion",

            "created": 0,

            "model": response_model or "deepseek-v4-flash",

            "choices": [

                {

                    "index": 0,

                    "message": message,

                    "finish_reason": finish_reason or "stop",

                }

            ],

            "usage": {

                "prompt_tokens": 0,

                "completion_tokens": 0,

                "total_tokens": reducer.usage_total,

            },

            "liangzidp": {

                "chat_session_id": chat_session_id,

                "request_message_id": request_msg_id,

                "response_message_id": response_msg_id,

                "status": reducer.status,

                "prompt_chars": prompt_chars,

                "prompt_tokens_estimate": max(1, (prompt_chars + 2) // 3)

                if prompt_chars

                else 0,

            },

            "raw_events": raw_events[

                -int(os.getenv("LIANGZIDP_DEBUG_RAW_EVENT_LIMIT", "200")) :

            ]

            if os.getenv("LIANGZIDP_DEBUG_RAW_EVENTS", "").lower()

            in {"1", "true", "yes", "on"}

            else [],

        }



    def _parse_sse_response_legacy(

        self,

        sse_text: str,

        prefer_response_snapshot: bool = False,

        chat_session_id: str = "",

        prompt_chars: int = 0,

        response_model: str = "",

    ) -> dict:

        """Parse Liangzidp SSE response into a unified format."""

        fragments = []

        reasoning_fragments = []

        initial_fragments = []

        initial_reasoning_fragments = []

        current_fragment_type = "RESPONSE"

        token_usage = 0

        status = "unknown"

        request_msg_id = None

        response_msg_id = None

        raw_events = []



        def repair_text(value: str) -> str:

            """Fix occasional UTF-8-as-latin1 mojibake from streaming decode."""

            if not isinstance(value, str):

                return value

            if any(mark in value for mark in ("ä", "å", "æ", "ç", "è", "é", "ï")):

                try:

                    return value.encode("latin1").decode("utf-8")

                except Exception:

                    return value

            return value



        for line in sse_text.split("\n"):

            line = line.strip()

            if not line.startswith("data:"):

                continue

            data_str = line[5:].strip()

            if data_str == "[DONE]":

                break

            try:

                event = json.loads(data_str)

            except json.JSONDecodeError:

                continue

            raw_events.append(event)



            # Current response text commonly arrives as bare {"v":"..."} events.

            # These are the authoritative answer deltas; prefer them over the

            # initial full-response snapshot to avoid duplicate/stale fragments.

            if set(event.keys()) == {"v"} and isinstance(event.get("v"), str):

                fragments.append(repair_text(event["v"]))

                current_fragment_type = "RESPONSE"

                continue



            # Current Liangzidp Web SSE uses JSON-patch-like events in the shape:

            # {"op":"SET","value":{"response":{...}}}

            # {"op":"APPEND","path":"response/fragments/-1/content","value":"..."}

            # Older captures used short keys: {"o":"APPEND","p":"...","v":"..."}.

            op = event.get("op", event.get("o", ""))

            path = event.get("path", event.get("p", ""))

            value = event.get("value", event.get("v", ""))



            if isinstance(value, dict) and "response" in value:

                resp_data = value.get("response") or {}

                request_msg_id = resp_data.get("parent_id", request_msg_id)

                response_msg_id = resp_data.get("message_id", response_msg_id)

                status = (

                    resp_data.get("status") or resp_data.get("quasi_status") or status

                )

                token_usage = (

                    resp_data.get("accumulated_token_usage", token_usage) or token_usage

                )

                fragments_list = resp_data.get("fragments", []) or []

                if fragments_list:

                    last = fragments_list[-1]

                    if isinstance(last, dict):

                        current_fragment_type = str(

                            last.get("type") or current_fragment_type

                        )

                for frag in fragments_list:

                    if isinstance(frag, dict):

                        content = frag.get("content")

                        frag_type = str(

                            frag.get("type") or current_fragment_type

                        ).upper()

                        if isinstance(content, str):

                            if frag_type == "THINK":

                                initial_reasoning_fragments.append(repair_text(content))

                            elif frag_type in (

                                "RESPONSE",

                                "TEMPLATE_RESPONSE",

                                "NONE",

                                "",

                            ):

                                initial_fragments.append(repair_text(content))



            if isinstance(value, dict) and "type" in value and "fragments" in str(path):

                current_fragment_type = str(value.get("type") or current_fragment_type)

                content = value.get("content")

                if isinstance(content, str) and content:

                    if current_fragment_type.upper() == "THINK":

                        reasoning_fragments.append(repair_text(content))

                    else:

                        fragments.append(repair_text(content))



            if op == "APPEND" and path.endswith("/content") and isinstance(value, str):

                if current_fragment_type.upper() == "THINK":

                    reasoning_fragments.append(repair_text(value))

                else:

                    fragments.append(repair_text(value))

            elif (

                op == "SET"

                and path in ("response/status", "response/quasi_status")

                and value

            ):

                status = str(value)

            elif op == "BATCH" and path == "response" and isinstance(value, list):

                for item in value:

                    if not isinstance(item, dict):

                        continue

                    item_path = item.get("path", item.get("p", ""))

                    item_value = item.get("value", item.get("v", ""))

                    if item_path == "accumulated_token_usage":

                        token_usage = item_value

                    elif item_path in ("quasi_status", "status"):

                        status = item_value



            # Ready/update metadata events do not carry answer text.

            if "request_message_id" in event:

                request_msg_id = event["request_message_id"]

                response_msg_id = event["response_message_id"]



            if "updated_at" in event:

                continue



            # Short-key p/o/v patches were already normalized into path/op/value

            # above. Handling them a second time duplicated tokens such as

            # `<tool` into `<tooltool` and corrupted tool XML.



        def merge_snapshot_and_deltas(initial: list[str], deltas: list[str]) -> str:

            """Join an initial response snapshot with later deltas once.



            Fresh Liangzidp streams can snapshot the first character/token and

            then continue with patch/bare deltas. Reconnects may replay that

            prefix, so use the largest boundary overlap instead of blindly

            concatenating or discarding the snapshot.

            """

            prefix = repair_text("".join(initial))

            suffix = repair_text("".join(deltas))

            if not prefix:

                return suffix

            if not suffix:

                return prefix

            if suffix.startswith(prefix):

                return suffix

            overlap = min(len(prefix), len(suffix))

            while overlap and prefix[-overlap:] != suffix[:overlap]:

                overlap -= 1

            return prefix + suffix[overlap:]



        # For expert/pro responses Liangzidp may emit THINK deltas as bare

        # {"v":"..."} while the final answer is only available in the final

        # response snapshot. Prefer explicit RESPONSE fragments when requested.

        if prefer_response_snapshot and initial_fragments:

            content = repair_text("".join(initial_fragments))

            reasoning_content = merge_snapshot_and_deltas(

                initial_reasoning_fragments, reasoning_fragments or fragments

            )

        else:

            content = merge_snapshot_and_deltas(initial_fragments, fragments)

            reasoning_content = merge_snapshot_and_deltas(

                initial_reasoning_fragments, reasoning_fragments

            )

        message = {

            "role": "assistant",

            "content": content,

        }

        if reasoning_content:

            message["reasoning_content"] = reasoning_content

        return {

            "id": f"chatcmpl-{response_msg_id or uuid.uuid4().hex}",

            "object": "chat.completion",

            "created": 0,

            "model": response_model or "deepseek-v4-flash",

            "choices": [

                {

                    "index": 0,

                    "message": message,

                    "finish_reason": status.lower() if status else "stop",

                }

            ],

            "usage": {

                # Liangzidp Web does not expose a clean prompt/completion split.

                # Keep token counts conservative and expose exact prompt_chars

                # under `liangzidp` for capacity/debugging.

                "prompt_tokens": 0,

                "completion_tokens": 0,

                "total_tokens": token_usage,

            },

            "liangzidp": {

                "chat_session_id": chat_session_id,

                "request_message_id": request_msg_id,

                "response_message_id": response_msg_id,

                "prompt_chars": prompt_chars,

                "prompt_tokens_estimate": max(1, (prompt_chars + 2) // 3)

                if prompt_chars

                else 0,

            },

            "raw_events": raw_events[

                -int(os.getenv("LIANGZIDP_DEBUG_RAW_EVENT_LIMIT", "200")) :

            ]

            if os.getenv("LIANGZIDP_DEBUG_RAW_EVENTS", "").lower()

            in {"1", "true", "yes", "on"}

            else [],

        }



    async def create_pow_challenge(

        self, target_path: str = "/api/v0/chat/completion"

    ) -> dict:

        """Get a POW challenge from Liangzidp."""

        async with curl_cffi.requests.AsyncSession(

            **self._get_session_kwargs()

        ) as session:

            resp = await session.post(

                CHALLENGE_URL,

                headers=self._get_headers(),

                data=self._json_body({"target_path": target_path}),

            )

            if resp.status_code != 200:

                raise LiangzidpAPIError.from_response(resp)

            try:

                data = resp.json()

            except Exception:

                raise LiangzidpAPIError(

                    f"POW challenge returned non-JSON: {resp.text[:200]}",

                    resp.status_code,

                    "invalid_pow",

                )



            # Liangzidp often returns HTTP 200 for application-level auth errors:

            # {"code":40003,"msg":"Authorization Failed (invalid token)","data":null}

            app_code = data.get("code") if isinstance(data, dict) else None

            app_msg = data.get("msg") if isinstance(data, dict) else ""

            if app_code not in (None, 0):

                status = 401 if str(app_code) in {"40003", "401"} else 500

                raise LiangzidpAPIError(

                    f"POW challenge rejected: code={app_code} msg={app_msg or '-'}",

                    status,

                    "invalid_api_key" if status == 401 else "pow_challenge_error",

                )



            inner = data.get("data") if isinstance(data, dict) else None

            if isinstance(inner, dict):

                biz_code = inner.get("biz_code")

                if biz_code not in (None, 0):

                    raise LiangzidpAPIError(

                        f"POW challenge rejected: biz_code={biz_code} msg={inner.get('biz_msg') or app_msg or '-'}",

                        401 if str(biz_code) in {"40003", "401"} else 500,

                        "pow_challenge_error",

                    )



            return data



    async def create_chat_session(self) -> str:

        """Create a real Liangzidp chat session and return its id."""

        async with curl_cffi.requests.AsyncSession(

            **self._get_session_kwargs()

        ) as session:

            resp = await session.post(

                CHAT_SESSION_CREATE_URL,

                headers=self._get_headers(),

                data=self._json_body({}),

            )

            if resp.status_code != 200:

                raise LiangzidpAPIError.from_response(resp)

            try:

                data = resp.json()

            except Exception:

                raise LiangzidpAPIError(

                    f"Create chat session returned non-JSON: {resp.text[:200]}",

                    resp.status_code,

                    "chat_session_error",

                )



        if not isinstance(data, dict):

            raise LiangzidpAPIError(

                "Create chat session returned invalid payload",

                500,

                "chat_session_error",

            )

        if data.get("code") not in (None, 0):

            raise LiangzidpAPIError(

                f"Create chat session rejected: code={data.get('code')} msg={data.get('msg') or '-'}",

                401 if str(data.get("code")) in {"40003", "401"} else 500,

                "invalid_api_key"

                if str(data.get("code")) in {"40003", "401"}

                else "chat_session_error",

            )



        payload = data.get("data") or {}

        if isinstance(payload, dict) and payload.get("biz_code") not in (None, 0):

            raise LiangzidpAPIError(

                f"Create chat session rejected: biz_code={payload.get('biz_code')} msg={payload.get('biz_msg') or '-'}",

                500,

                "chat_session_error",

            )

        biz_data = payload.get("biz_data") if isinstance(payload, dict) else None

        chat_session = (

            (biz_data or {}).get("chat_session") if isinstance(biz_data, dict) else None

        )

        session_id = chat_session.get("id") if isinstance(chat_session, dict) else None

        if not session_id:

            raise LiangzidpAPIError(

                f"Create chat session missing id: keys={list(data.keys())}",

                500,

                "chat_session_error",

            )

        return str(session_id)



    @staticmethod

    def _challenge_payload(raw: dict) -> dict:

        """Normalize Liangzidp challenge responses with data/result wrappers."""

        if not isinstance(raw, dict):

            raise LiangzidpAPIError("Invalid POW challenge response", 500, "invalid_pow")



        def unwrap(obj: Any) -> dict | None:

            if not isinstance(obj, dict):

                return None



            for key in ("data", "biz_data", "result", "payload"):

                nested = obj.get(key)

                if isinstance(nested, dict):

                    found = unwrap(nested)

                    if found:

                        return found



            for key in ("challenge", "pow_challenge", "proof_of_work"):

                nested = obj.get(key)

                if isinstance(nested, dict):

                    return {**obj, **nested}



            has_fields = all(

                obj.get(k) not in (None, "")

                for k in ("challenge", "salt", "difficulty")

            )

            if has_fields:

                return obj

            return None



        data = unwrap(raw)

        if data:

            return data



        if raw.get("data") is None:

            raise LiangzidpAPIError(

                f"Invalid POW challenge payload: code={raw.get('code')} msg={raw.get('msg') or '-'} data=null",

                401 if str(raw.get("code")) in {"40003", "401"} else 500,

                "invalid_api_key"

                if str(raw.get("code")) in {"40003", "401"}

                else "invalid_pow",

            )

        raise LiangzidpAPIError(

            f"Invalid POW challenge payload: keys={list(raw.keys())}",

            500,

            "invalid_pow",

        )



    @staticmethod

    def _first_present(data: dict, *names: str, default: Any = None) -> Any:

        for name in names:

            if name in data and data[name] not in (None, ""):

                return data[name]

        return default



    async def solve_pow(self, challenge: dict) -> int:

        """Solve a normalized POW challenge via the local Node microservice."""

        payload = {

            "algorithm": self._first_present(

                challenge, "algorithm", default="LiangzidpHashV1"

            ),

            "challenge": self._first_present(challenge, "challenge", "hash", "target"),

            "salt": self._first_present(challenge, "salt"),

            "difficulty": self._first_present(challenge, "difficulty"),

            "expireAt": self._first_present(

                challenge, "expireAt", "expire_at", "expiresAt", "expires_at"

            ),

        }

        missing = [key for key, value in payload.items() if value in (None, "")]

        if missing:

            raise LiangzidpAPIError(

                f"POW challenge missing fields: {', '.join(missing)}",

                500,

                "invalid_pow",

            )



        try:

            async with curl_cffi.requests.AsyncSession(

                timeout=self._timeout

            ) as session:

                resp = await session.post(f"{POW_SERVICE_URL}/solve", json=payload)

        except Exception as exc:

            raise LiangzidpAPIError(

                "Local POW service unavailable",

                503,

                "pow_service_unavailable",

            ) from exc

        if resp.status_code != 200:

            raise LiangzidpAPIError(

                f"POW solver failed: HTTP {resp.status_code} {resp.text[:200]}",

                503,

                "pow_solver_error",

            )

        data = resp.json()

        if not data.get("ok") or data.get("answer") is None:

            raise LiangzidpAPIError(

                f"POW solver returned no answer: {data}",

                503,

                "pow_solver_no_answer",

            )

        return int(data["answer"])



    async def build_pow_response(

        self, target_path: str = "/api/v0/chat/completion"

    ) -> str:

        """Create the X-Ds-Pow-Response header value.



        The server-side format has changed across Liangzidp bundle versions.  To

        avoid throwing away fields the browser challenge returns, preserve the

        normalized challenge object and add answer/target_path.  The header is

        compact JSON encoded as base64, matching the observed web header style.

        """

        raw = await self.create_pow_challenge(target_path)

        challenge = self._challenge_payload(raw)

        answer = await self.solve_pow(challenge)

        response = {

            **challenge,

            "algorithm": self._first_present(

                challenge, "algorithm", default="LiangzidpHashV1"

            ),

            "answer": answer,

            "target_path": target_path,

        }

        encoded = json.dumps(

            response, separators=(",", ":"), ensure_ascii=False

        ).encode("utf-8")

        self._pow_response = base64.b64encode(encoded).decode("ascii")

        return self._pow_response



    async def get_models(self) -> list[dict]:

        """Fetch available models."""

        async with curl_cffi.requests.AsyncSession(

            **self._get_session_kwargs()

        ) as session:

            resp = await session.get(

                MODELS_URL,

                headers=self._get_headers(),

            )

            if resp.status_code != 200:

                raise LiangzidpAPIError.from_response(resp)

            data = resp.json()

            models = []

            for m in data.get("data", []):

                models.append(

                    {

                        "id": m.get("id", ""),

                        "object": "model",

                        "owned_by": "liangzidp",

                    }

                )

            return models



    async def verify_session_detail(self) -> dict[str, Any]:

        """Verify credentials while preserving auth versus infrastructure failures."""

        started = time.perf_counter()



        def result(

            ok: bool, status: str, *, http_status: int = 0, error: str = ""

        ) -> dict[str, Any]:

            return {

                "ok": ok,

                "status": status,

                "http_status": http_status,

                "latency_ms": round((time.perf_counter() - started) * 1000),

                "error": error[:500],

            }



        try:

            async with curl_cffi.requests.AsyncSession(

                **self._get_session_kwargs()

            ) as session:

                resp = await session.get(

                    USER_URL,

                    headers=self._get_headers(),

                    timeout=10,

                )

                if resp.status_code in {401, 403}:

                    return result(

                        False,

                        "invalid_token",

                        http_status=resp.status_code,

                        error=f"session verification returned HTTP {resp.status_code}",

                    )

                if resp.status_code != 200:

                    return result(

                        False,

                        "upstream_error",

                        http_status=resp.status_code,

                        error=f"session verification returned HTTP {resp.status_code}",

                    )

                try:

                    data = resp.json()

                except Exception:

                    # Keep a POW challenge fallback for deployments whose

                    # current-user endpoint is temporarily routed to the SPA.

                    try:

                        await self.create_pow_challenge("/api/v0/chat/completion")

                        return result(True, "healthy", http_status=resp.status_code)

                    except LiangzidpAPIError as exc:

                        status = (

                            "invalid_token"

                            if exc.status_code in {401, 403}

                            else "upstream_error"

                        )

                        return result(

                            False,

                            status,

                            http_status=exc.status_code,

                            error=exc.message,

                        )

                if isinstance(data, dict) and data.get("code") not in (None, 0):

                    code = data.get("code")

                    return result(

                        False,

                        "invalid_token",

                        http_status=resp.status_code,

                        error=f"session verification returned code {code}",

                    )

                if not isinstance(data, dict):

                    return result(

                        False,

                        "upstream_error",

                        http_status=resp.status_code,

                        error="session verification returned an invalid payload",

                    )

                return result(True, "healthy", http_status=resp.status_code)

        except Exception as exc:

            return result(False, "network_error", error=str(exc))



    async def verify_session(self) -> bool:

        """Verify that the current session token is valid."""

        return bool((await self.verify_session_detail())["ok"])



    async def get_web_model_configs(self) -> list[dict[str, Any]]:

        """Read the account-scoped Web model capabilities from official settings."""

        async with curl_cffi.requests.AsyncSession(

            **self._get_session_kwargs()

        ) as session:

            resp = await session.get(

                SETTINGS_URL,

                params={"did": str(uuid.uuid4()), "scope": "model"},

                headers=self._get_headers(),

                timeout=10,

            )

        if resp.status_code != 200:

            raise LiangzidpAPIError.from_response(resp)

        try:

            payload = resp.json()

            data = payload.get("data") or {}

            if payload.get("code") != 0 or data.get("biz_code") != 0:

                raise LiangzidpAPIError(

                    str(

                        data.get("biz_msg")

                        or payload.get("msg")

                        or "model settings failed"

                    ),

                    502,

                    "model_settings_error",

                )

            settings = (data.get("biz_data") or {}).get("settings") or {}

            models = (settings.get("model_configs") or {}).get("value") or []

            return [item for item in models if isinstance(item, dict)]

        except LiangzidpAPIError:

            raise

        except Exception as exc:

            raise LiangzidpAPIError(

                "Invalid model settings payload",

                502,

                "model_settings_error",

            ) from exc



    def update_credentials(

        self,

        bearer_token: str = "",

        session_token: str = "",

        csrf_token: str = "",

        pow_response: str = "",

    ) -> None:

        """Update authentication credentials."""

        token = bearer_token or session_token

        if token:

            self._bearer_token = token

            self._session_token = token

        if csrf_token:

            self._csrf_token = csrf_token

        if pow_response:

            self._pow_response = pow_response

