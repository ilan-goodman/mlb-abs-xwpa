from __future__ import annotations

import textwrap
from pathlib import Path

from .models import Article
from .textutil import ascii_text


PAGE_WIDTH = 612
PAGE_HEIGHT = 792
MARGIN = 58


def pdf_escape(value: str) -> str:
    value = ascii_text(value)
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def wrap_lines(text: str, font_size: int, width: int) -> list[str]:
    approx_char_width = max(font_size * 0.48, 7)
    max_chars = max(int(width / approx_char_width), 24)
    lines: list[str] = []
    for paragraph in text.splitlines():
        paragraph = paragraph.strip()
        if not paragraph:
            lines.append("")
            continue
        lines.extend(textwrap.wrap(paragraph, width=max_chars, break_long_words=False))
    return lines


def build_pages(article: Article, font_size: int = 18) -> list[list[tuple[int, str]]]:
    body_width = PAGE_WIDTH - (MARGIN * 2)
    pages: list[list[tuple[int, str]]] = [[]]
    y_budget = PAGE_HEIGHT - (MARGIN * 2)

    def add_line(size: int, text: str, extra_after: int = 0) -> None:
        nonlocal y_budget
        line_height = int(size * 1.35)
        needed = line_height + extra_after
        if y_budget < needed:
            pages.append([])
            y_budget = PAGE_HEIGHT - (MARGIN * 2)
        pages[-1].append((size, text))
        y_budget -= needed

    for line in wrap_lines(article.title, 24, body_width):
        add_line(24, line)
    add_line(12, f"{article.source_name} | {article.url}", extra_after=8)
    if article.byline:
        add_line(12, f"By {article.byline}", extra_after=8)
    for line in wrap_lines(article.text, font_size, body_width):
        if line:
            add_line(font_size, line)
        else:
            add_line(font_size, "", extra_after=int(font_size * 0.6))
    return pages


def make_pdf(article: Article, output_path: Path, font_size: int = 18) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pages = build_pages(article, font_size=font_size)
    objects: list[bytes] = []

    def obj(data: str | bytes) -> int:
        if isinstance(data, str):
            data = data.encode("latin-1", errors="replace")
        objects.append(data)
        return len(objects)

    catalog_id = obj("PLACEHOLDER")
    pages_id = obj("PLACEHOLDER")
    font_id = obj("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    page_ids: list[int] = []
    content_ids: list[int] = []

    for page in pages:
        commands: list[str] = []
        y = PAGE_HEIGHT - MARGIN
        for size, line in page:
            if line:
                commands.append(
                    f"BT /F1 {size} Tf 1 0 0 1 {MARGIN} {y} Tm ({pdf_escape(line)}) Tj ET"
                )
            y -= int(size * 1.35)
        stream = "\n".join(commands).encode("latin-1", errors="replace")
        content_id = obj(
            b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream"
        )
        content_ids.append(content_id)
        page_id = obj("PLACEHOLDER")
        page_ids.append(page_id)

    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    objects[catalog_id - 1] = f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode("ascii")
    objects[pages_id - 1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode("ascii")
    for page_id, content_id in zip(page_ids, content_ids):
        objects[page_id - 1] = (
            f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 {PAGE_WIDTH} {PAGE_HEIGHT}] "
            f"/Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_id} 0 R >>"
        ).encode("ascii")

    output = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for idx, data in enumerate(objects, start=1):
        offsets.append(len(output))
        output.extend(f"{idx} 0 obj\n".encode("ascii"))
        output.extend(data)
        output.extend(b"\nendobj\n")
    xref_start = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    output.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_start}\n%%EOF\n"
        ).encode("ascii")
    )
    output_path.write_bytes(output)
    return output_path
