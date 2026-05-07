"""Segunda passagem opcional após score calibrado: regex (deterministic) ou LLM (HTTP).

SPEC: docs/motor-nlp/doc-llm-router-v0.md
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

def _as_mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def _as_str_list(raw: Any) -> list[str]:
    if not isinstance(raw, (list, tuple)):
        return []
    return [str(x) for x in raw if str(x).strip()]


def _clip01(value: float) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if v != v:
        return 0.0
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


def _in_band(v: float, lo: float, hi: float) -> bool:
    return lo <= v <= hi


def llm_router_mode(nlp_config: Mapping[str, Any]) -> str:
    cfg = _as_mapping(nlp_config.get("llm_router"))
    raw = str(cfg.get("mode", "deterministic")).strip().lower()
    if raw in ("llm", "deterministic"):
        return raw
    return "deterministic"


def llm_uncertainty_band(nlp_config: Mapping[str, Any]) -> tuple[float, float]:
    cfg = _as_mapping(nlp_config.get("llm_router"))
    band = cfg.get("uncertainty_band")
    if isinstance(band, (list, tuple)) and len(band) == 2:
        lo = _clip01(float(band[0]))
        hi = _clip01(float(band[1]))
        if lo > hi:
            lo, hi = hi, lo
        return lo, hi
    return (0.35, 0.65)


def decide_deterministic_regex(
    *,
    treated: str,
    current_fl: int,
    calibrated_score: float,
    nlp_config: Mapping[str, Any],
) -> tuple[int, str, bool]:
    """Regex na banda de incerteza — sem chamada externa."""
    lo, hi = llm_uncertainty_band(nlp_config)
    if not _in_band(calibrated_score, lo, hi):
        return current_fl, "hybrid_calibrated", False

    cfg = _as_mapping(nlp_config.get("llm_router"))
    text = (treated or "").lower()
    blockers = _as_str_list(cfg.get("negative_context_patterns")) or [
        r"sem\s+c[aá]lculos",
        r"aus[êe]ncia\s+de\s+c[aá]lculos",
        r"n[aã]o\s+h[áa]\s+c[aá]lculos",
    ]
    positives = _as_str_list(cfg.get("positive_context_patterns")) or [
        r"colelit[ií]ase",
        r"microlit[ií]ase",
        r"barro\s+biliar|lama\s+biliar",
    ]
    if any(re.search(p, text, re.IGNORECASE) for p in blockers):
        return 0, "llm_router_block", True
    if any(re.search(p, text, re.IGNORECASE) for p in positives):
        return 1, "llm_router_promote", True
    return current_fl, "llm_router_no_change", True


def _truncate(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars]


def _parse_llm_json(content: str) -> tuple[int | None, str]:
    """Devolve (fl ou None se abstain), decision_suffix."""
    raw = (content or "").strip()
    if not raw:
        return None, "abstain_empty"
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{[^{}]*\}", raw, re.DOTALL)
        if not m:
            return None, "abstain_invalid_json"
        try:
            obj = json.loads(m.group(0))
        except json.JSONDecodeError:
            return None, "abstain_invalid_json"
    if not isinstance(obj, Mapping):
        return None, "abstain_not_object"
    if "relevante" in obj:
        rel = obj.get("relevante")
        if isinstance(rel, bool):
            return (1 if rel else 0), "llm_relevante"
        if str(rel).lower() in ("1", "true", "yes"):
            return 1, "llm_relevante"
        if str(rel).lower() in ("0", "false", "no"):
            return 0, "llm_relevante"
        return None, "abstain_relevante"
    act = str(obj.get("action", "")).strip().lower()
    if act == "promote":
        return 1, "llm_action"
    if act == "block":
        return 0, "llm_action"
    if act == "abstain":
        return None, "llm_abstain"
    return None, "abstain_unknown_keys"


@dataclass(frozen=True)
class LlmExtras:
    llm_router_mode: str
    llm_called: bool
    llm_model: str
    llm_error: str


def _prompt_placeholders(
    cfg: Mapping[str, Any],
    user_text: str,
    *,
    specialty_id: str,
) -> dict[str, str]:
    return {
        "text": user_text,
        "specialty_context": str(cfg.get("specialty_context") or "").strip(),
        "specialty_id": (specialty_id or "").strip(),
    }


def _safe_format_template(tmpl: str, mapping: Mapping[str, str]) -> str:
    try:
        return str(tmpl).format(**dict(mapping))
    except (KeyError, ValueError):
        out = str(tmpl)
        for k, v in mapping.items():
            out = out.replace("{" + k + "}", v)
        return out


def _build_messages(
    cfg: Mapping[str, Any],
    user_text: str,
    *,
    specialty_id: str = "",
) -> list[dict[str, str]]:
    sys_default = (
        "Respond only with a single JSON object. Task: decide if the excerpt "
        "supports marking the clinical routing flag as relevant (true) or not (false). "
        'Schema: {"relevante": boolean}'
    )
    ph = _prompt_placeholders(cfg, user_text, specialty_id=specialty_id)
    sys_msg = str(cfg.get("prompt_system") or sys_default).strip() or sys_default
    sys_msg = _safe_format_template(sys_msg, ph)
    tmpl = str(cfg.get("prompt_user_template") or "{text}").strip() or "{text}"
    user_body = _safe_format_template(tmpl, ph)
    return [
        {"role": "system", "content": sys_msg},
        {"role": "user", "content": user_body},
    ]


def call_openai_compatible_chat(
    *,
    cfg: Mapping[str, Any],
    messages: list[dict[str, str]],
) -> tuple[str, str | None]:
    """POST chat/completions; returns (content, error_short). Requires httpx."""
    try:
        import httpx
    except ImportError:
        return "", "httpx_missing_install_llm_extra"

    base = str(cfg.get("base_url") or "").rstrip("/")
    model = str(cfg.get("model") or "").strip()
    key_env = str(cfg.get("api_key_env") or "NLP_ENGINE_LLM_API_KEY").strip()
    api_key = os.environ.get(key_env, "").strip()
    timeout_s = float(cfg.get("timeout_s") or 30.0)

    if not base or not model:
        return "", "missing_base_url_or_model"
    if not api_key:
        return "", f"missing_env:{key_env}"

    url = f"{base}/chat/completions"
    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": float(cfg.get("temperature") or 0.0),
    }
    if str(cfg.get("json_response_format") or "").lower() in ("1", "true", "yes"):
        payload["response_format"] = {"type": "json_object"}

    try:
        with httpx.Client(timeout=timeout_s) as client:
            r = client.post(
                url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        if r.status_code >= 400:
            return "", f"http_{r.status_code}"
        data = r.json()
        choices = data.get("choices") or []
        if not choices:
            return "", "no_choices"
        msg = choices[0].get("message") or {}
        content = str(msg.get("content") or "").strip()
        return content, None
    except Exception as exc:
        return "", str(exc)[:120]


def decide_llm_http(
    *,
    treated: str,
    current_fl: int,
    calibrated_score: float,
    nlp_config: Mapping[str, Any],
    specialty_id: str = "",
) -> tuple[int, str, bool, LlmExtras]:
    lo, hi = llm_uncertainty_band(nlp_config)
    if not _in_band(calibrated_score, lo, hi):
        ex = LlmExtras("llm", False, "", "")
        return current_fl, "hybrid_calibrated", False, ex

    cfg = _as_mapping(nlp_config.get("llm_router"))
    max_chars = int(cfg.get("max_input_chars") or 8000)
    excerpt = _truncate(treated or "", max_chars)
    messages = _build_messages(cfg, excerpt, specialty_id=specialty_id)
    content, err = call_openai_compatible_chat(cfg=cfg, messages=messages)
    model = str(cfg.get("model") or "")

    if err:
        ex = LlmExtras("llm", True, model, err)
        return current_fl, "llm_router_llm_fallback", True, ex

    fl_new, suffix = _parse_llm_json(content)
    if fl_new is None:
        src = f"llm_router_llm_{suffix}"
        ex = LlmExtras("llm", True, model, "")
        return current_fl, src, True, ex

    verdict_src = "llm_router_llm_positive" if fl_new == 1 else "llm_router_llm_negative"
    ex = LlmExtras("llm", True, model, "")
    return fl_new, verdict_src, True, ex


def llm_router_step(
    *,
    treated: str,
    current_fl: int,
    calibrated_score: float,
    nlp_config: Mapping[str, Any],
    specialty_id: str = "",
) -> tuple[int, str, bool, dict[str, Any]]:
    """Delega deterministic vs llm; extras são merged em exm_laudo_resultado."""
    mode = llm_router_mode(nlp_config)
    if mode == "llm":
        fl, src, band, extras = decide_llm_http(
            treated=treated,
            current_fl=current_fl,
            calibrated_score=calibrated_score,
            nlp_config=nlp_config,
            specialty_id=specialty_id,
        )
        payload = {
            "llm_router_mode": extras.llm_router_mode,
            "llm_called": extras.llm_called,
            "llm_model": extras.llm_model,
            "llm_error": extras.llm_error,
        }
        return fl, src, band, payload

    fl, src, band = decide_deterministic_regex(
        treated=treated,
        current_fl=current_fl,
        calibrated_score=calibrated_score,
        nlp_config=nlp_config,
    )
    payload = {
        "llm_router_mode": "deterministic",
        "llm_called": False,
        "llm_model": "",
        "llm_error": "",
    }
    return fl, src, band, payload
