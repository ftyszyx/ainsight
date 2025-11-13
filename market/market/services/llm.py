from __future__ import annotations

import json
from typing import Dict, List, Optional

import httpx

from market.config import get_settings


class LLMUnavailable(RuntimeError):
    pass


def summarize_report(content: str) -> Dict[str, Optional[str]]:
    settings = get_settings()
    if not settings.llm_endpoint or not settings.llm_api_key:
        raise LLMUnavailable("LLM_ENDPOINT or LLM_API_KEY not configured.")
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {
                "role": "system",
                "content": "你是证券研报分析助手，请返回JSON，字段包括summary(300字内)、sentiment(-1到1浮点)、risks(数组)、highlights(数组)。"
            },
            {"role": "user", "content": content},
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }
    headers = {
        "Authorization": f"Bearer {settings.llm_api_key}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=60.0) as client:
        response = client.post(settings.llm_endpoint, headers=headers, json=payload)
        response.raise_for_status()
    data = response.json()
    message = data.get("choices", [{}])[0].get("message", {})
    content_raw = message.get("content", "{}")
    parsed = json.loads(content_raw)
    return {
        "summary": parsed.get("summary"),
        "sentiment": parsed.get("sentiment"),
        "risks": _as_list(parsed.get("risks")),
        "highlights": _as_list(parsed.get("highlights")),
    }


def _as_list(value) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]

