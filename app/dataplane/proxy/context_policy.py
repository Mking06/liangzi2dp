"""Token-aware context and tool-result governance."""

from __future__ import annotations

import hashlib
import os
from functools import lru_cache


@lru_cache(maxsize=1)
def _encoding():
    try:
        import tiktoken

        return tiktoken.get_encoding("cl100k_base")
    except Exception:
        return None


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    encoding = _encoding()
    if encoding is None:
        return max(1, (len(text) + 2) // 3)
    return len(encoding.encode(text, disallowed_special=()))


def truncate_tokens(text: str, limit: int) -> str:
    if limit <= 0:
        return ""
    if estimate_tokens(text) <= limit:
        return text
    encoding = _encoding()
    if encoding is None:
        chars = max(1, limit * 3)
        head = chars // 2
        return text[:head] + "\n...[truncated]...\n" + text[-head:]
    tokens = encoding.encode(text, disallowed_special=())
    marker = "\n...[truncated]...\n"
    marker_tokens = encoding.encode(marker, disallowed_special=())
    keep = max(0, limit - len(marker_tokens))
    head = keep // 2
    tail = keep - head
    output = encoding.decode(tokens[:head]) + marker
    if tail:
        output += encoding.decode(tokens[-tail:])
    encoded = encoding.encode(output, disallowed_special=())
    return encoding.decode(encoded[:limit])


def govern_tool_result(text: str) -> str:
    limit = int(os.getenv("LIANGZIDP_TOOL_RESULT_TOKEN_LIMIT", "32000"))
    tokens = estimate_tokens(text)
    if tokens <= limit:
        return text
    digest = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
    body = truncate_tokens(text, max(256, limit - 128))
    return (
        f"[Tool result truncated: original_tokens={tokens}, original_chars={len(text)}, "
        f"sha256={digest}]\n{body}"
    )


def fit_rendered_parts(parts: list[str], limit: int | None = None) -> list[str]:
    budget = limit or int(os.getenv("LIANGZIDP_INPUT_TOKEN_LIMIT", "900000"))
    if budget <= 0:
        return parts
    costs = [estimate_tokens(part) for part in parts]
    if sum(costs) <= budget:
        return parts

    system_indexes = {
        index for index, part in enumerate(parts) if part.startswith("System instruction:\n")
    }
    selected: set[int] = set()
    used = 0

    latest_index = next(
        (index for index in range(len(parts) - 1, -1, -1) if index not in system_indexes),
        len(parts) - 1,
    )
    latest_reserve = min(costs[latest_index], max(1, budget * 3 // 4)) if parts else 0
    system_allowance = max(0, budget - latest_reserve)

    for index in sorted(system_indexes):
        remaining = system_allowance - used
        if remaining <= 0:
            break
        if costs[index] <= remaining:
            selected.add(index)
            used += costs[index]
        elif not selected:
            parts[index] = truncate_tokens(parts[index], remaining)
            costs[index] = estimate_tokens(parts[index])
            selected.add(index)
            used += costs[index]

    if parts and latest_index not in selected:
        remaining = budget - used
        if costs[latest_index] > remaining:
            parts[latest_index] = truncate_tokens(parts[latest_index], remaining)
            costs[latest_index] = estimate_tokens(parts[latest_index])
        if parts[latest_index]:
            selected.add(latest_index)
            used += costs[latest_index]

    for index in range(len(parts) - 1, -1, -1):
        if index in selected:
            continue
        remaining = budget - used
        if remaining <= 0:
            break
        if costs[index] <= remaining:
            selected.add(index)
            used += costs[index]
        elif not any(candidate > index for candidate in selected):
            parts[index] = truncate_tokens(parts[index], remaining)
            selected.add(index)
            break
    return [part for index, part in enumerate(parts) if index in selected]
