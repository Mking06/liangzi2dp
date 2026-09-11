"""Model management — maps internal model IDs to Liangzidp endpoints."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ModelInfo:
    """Model metadata."""
    id: str           # User-facing ID
    internal_id: str   # Liangzidp internal model name
    provider: str     # Always "liangzidp"
    supports_thinking: bool = False
    supports_vision: bool = False
    supports_tools: bool = True
    context_length: int = 64000
    pricing: dict | None = None


# Model registry
MODELS: dict[str, ModelInfo] = {
    "deepseek-v4-flash": ModelInfo(
        id="deepseek-v4-flash",
        internal_id="default",
        provider="liangzidp",
        supports_thinking=True,
        supports_vision=True,
        supports_tools=True,
        context_length=1_000_000,
    ),
    "deepseek-v4-pro": ModelInfo(
        id="deepseek-v4-pro",
        internal_id="expert",
        provider="liangzidp",
        supports_thinking=True,
        supports_vision=True,
        supports_tools=True,
        context_length=1_000_000,
    ),
    "deepseek-v4-vision": ModelInfo(
        id="deepseek-v4-vision",
        internal_id="vision",
        provider="liangzidp",
        supports_thinking=True,
        supports_vision=True,
        supports_tools=True,
        context_length=1_000_000,
    ),
    "deepseek-v4-flash-vision-exp": ModelInfo(
        id="deepseek-v4-flash-vision-exp",
        internal_id="vision",
        provider="liangzidp",
        supports_thinking=True,
        supports_vision=True,
        supports_tools=True,
        context_length=1_000_000,
    ),
}

# Aliases
MODEL_ALIASES: dict[str, str] = {}


def get_model_info(model_id: str) -> ModelInfo | None:
    """Get model info by ID (resolves aliases)."""
    normalized = model_id.lower()
    if normalized.endswith("[1m]"):
        normalized = normalized[:-4]
    canonical = MODEL_ALIASES.get(normalized, normalized)
    return MODELS.get(canonical)


def list_models() -> list[dict]:
    """List all models in OpenAI-compatible format."""
    return [
        {
            "id": m.id,
            "object": "model",
            "created": 1704067200,
            "owned_by": m.provider,
        }
        for m in MODELS.values()
    ]
