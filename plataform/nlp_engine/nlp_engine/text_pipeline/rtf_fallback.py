"""Fallback RTF -> texto quando striprtf/pypandoc falham (notebook biliar)."""

from __future__ import annotations

import re
import unicodedata


def _undo_py_escapes(s: str) -> str:
    return s.replace("\x0c", r"\f").replace("\t", r"\tab")


def _strip_rtf_metadata_top(s: str) -> str:
    head = s[:8192]
    tail = s[8192:]
    patterns = [
        r"\{\\\*\\fonttbl[^{}]*\}",
        r"\{\\\*\\colortbl[^{}]*\}",
        r"\{\\\*\\stylesheet[^{}]*\}",
        r"\{\\\*\\listtable[^{}]*\}",
        r"\{\\\*\\listoverridetable[^{}]*\}",
        r"\{\\\*\\generator[^{}]*\}",
        r"\{\\\*\\info[^{}]*\}",
    ]
    for pat in patterns:
        head = re.sub(pat, " ", head, flags=re.IGNORECASE)
    return head + tail


def _hex_sub_cp1252(m: re.Match[str]) -> str:
    try:
        return bytes([int(m.group(1), 16)]).decode("cp1252", errors="ignore")
    except Exception:
        return ""


def _u_sub_unicode(m: re.Match[str]) -> str:
    try:
        code = int(m.group(1))
        if code < 0:
            code += 65536
        return chr(code)
    except Exception:
        return ""


def _drop_rtf_meta_lines(text: str) -> str:
    out = []
    only_words_semicolon_rx = re.compile(r"^[A-Za-z0-9 /+\-\u00C0-\u017F]+;\s$")
    font_tokens = (
        r"(unicode|opensymbol|wingdings|monospaced|serif|sans|arial|calibri|times|courier)"
    )
    font_line_rx = re.compile(
        rf"^\s*[A-Za-z0-9 \-\u00C0-\u017F]+(?:\s\\?\\s)?(?:{font_tokens})(?:\s*[;:]?)\s*$",
        re.IGNORECASE,
    )
    tiny_meta_rx = re.compile(
        r"^\s*(default|\\jword2|\\ generator|\* info)\s*[;:]?\s*$", re.IGNORECASE
    )

    for ln in text.splitlines():
        lns = ln.strip()
        if not lns:
            out.append(ln)
            continue
        if only_words_semicolon_rx.match(lns):
            continue
        if font_line_rx.match(lns):
            continue
        if tiny_meta_rx.match(lns):
            continue
        if ";" in lns and re.search(font_tokens, lns, re.IGNORECASE) and len(lns) <= 80:
            continue
        out.append(ln)
    return "\n".join(out).strip()


def rtf_to_text_fallback(rtf: str, debug: bool = False) -> str:
    if not rtf:
        return ""

    s = _undo_py_escapes(rtf)
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    if not s.lstrip().startswith("{\\rtf"):
        return s.strip()

    s = _strip_rtf_metadata_top(s)
    if debug:
        print("--- after strip_top ---")
        print(s[:1000])

    s = re.sub(r"\\'([0-9a-fA-F]{2})", _hex_sub_cp1252, s)
    s = re.sub(r"\\u(-?\d+)\??", _u_sub_unicode, s)

    s = re.sub(r"\\par[d]?\b", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"\\line\b", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"\\tab\b", "\t", s, flags=re.IGNORECASE)

    s = re.sub(
        r"\{\\\*\\(fonttbl|colortbl|stylesheet|info|generator|listtable|listoverridetable)[^{}]*\}",
        " ",
        s,
        flags=re.IGNORECASE,
    )

    s = re.sub(r"\\[a-zA-Z]+-?\d*\s?", " ", s)

    s = s.replace("\\{", "{").replace("\\}", "}").replace("\\\\", "\\")
    s = re.sub(r"[{}]", " ", s)

    s = re.sub(r"(?<!\\)'([0-9a-fA-F]{2})", _hex_sub_cp1252, s)

    s = re.sub(
        r"\b(?:rtf\d*|ansi|ansicpg\d+|deftab\d+|paper[wh]\d+|marg[tlrb]\d+|headery\d+|"
        r"footery\d+|colsx?\d+|snext\d+|fs\d+|cf\d+|pard|plain|qc|ql|itap\d+|viewkind\d+|"
        r"uc\d+|sa\d+|sl\d+|slmult\d+|lang\d+|kerning\d+|ulnone|b0|i0|f\d+|deff\d+|"
        r"colortbl|fonttbl|stylesheet|info|generator|listtable|listoverridetable)\b",
        " ",
        s,
        flags=re.IGNORECASE,
    )

    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u200b", "").replace("\u200c", "").replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()

    s = _drop_rtf_meta_lines(s)

    if not s:
        s = bytes(rtf, "latin1", errors="ignore").decode("cp1252", errors="ignore").strip()

    return s
