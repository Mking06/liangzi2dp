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


# Model registry.
# Liangzidp retired V4 Flash / V4 Flash Vision Exp (and later V4 Pro) upstream;
# the Web bundle now serves a single engine: Liangzidp V4.1 Flash, exposed as the
# "default" (快速模式) model_type. All public model IDs therefore route to it.
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
}

# Aliases: the official new API name "deepseek-flash" maps to the same engine.
MODEL_ALIASES: dict[str, str] = {
    "deepseek-flash": "deepseek-v4-flash",
}


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
