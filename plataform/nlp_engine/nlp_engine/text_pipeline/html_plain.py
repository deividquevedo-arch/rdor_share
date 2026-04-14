"""HTML -> texto plano (lxml + html2text), alinhado ao notebook biliar."""

from __future__ import annotations

import html as _html
import re
import unicodedata

import html2text
import lxml.html
from lxml.html import fromstring


def html_to_plain(s: str) -> str:
    if not s:
        return ""

    s = s.replace("\r\n", "\n").replace("\r", "\n")

    try:
        doc = fromstring(s)
        for bad in doc.xpath("//script|//style|//noscript"):
            bad.drop_tree()

        body = doc.find("body")
        node = body if body is not None else doc
        cleaned_html = lxml.html.tostring(node, encoding="unicode", method="html")
    except Exception:
        cleaned_html = s

    h = html2text.HTML2Text()
    h.body_width = 0
    h.single_line_break = True
    h.ignore_links = True
    h.ignore_images = True
    h.ignore_emphasis = True
    h.unicode_snob = True

    try:
        s = h.handle(cleaned_html)
    except Exception:
        try:
            s = fromstring(cleaned_html).text_content()
        except Exception:
            s = cleaned_html

    s = _html.unescape(s)
    s = s.replace("\u200b", "").replace("\u200c", "").replace("\xa0", " ")
    s = unicodedata.normalize("NFKC", s)

    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r" ?/ ?", " / ", s)
    s = re.sub(r"\n[ \t]+", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)

    lines = [ln.rstrip() for ln in s.splitlines()]
    s = "\n".join(lines).strip()

    s = s.replace("Hospita\u200bl", "Hospital")
    s = re.sub(r"\bColonosocopia\b", "Colonoscopia", s, flags=re.IGNORECASE)
    s = re.sub(r"\bredundancia\b", "redundância", s, flags=re.IGNORECASE)

    return s
