"""
将 PDF 文件转换为 Markdown。
默认处理仓库下的 doc 目录，也支持传入单个 PDF 文件或目录路径。
"""
from __future__ import annotations

import hashlib
import html
import sys
import re
import shutil
import unicodedata
from contextlib import contextmanager
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DEFAULT_SOURCE = Path(__file__).resolve().parents[1] / "doc"
DEFAULT_ENGINE = "pymupdf4llm"

# CJK 康熙部首 (U+2F00-2FDF) → 标准汉字
_CJK_RADICAL = {
    "⼀": "一", "⼁": "丨", "⼂": "丶", "⼃": "丿", "⼄": "乙",
    "⼅": "亅", "⼆": "二", "⼇": "亠", "⼈": "人", "⼉": "儿",
    "⼊": "入", "⼋": "八", "⼌": "冂", "⼍": "冖", "⼎": "冫",
    "⼏": "几", "⼐": "凵", "⼑": "刀", "⼒": "力", "⼓": "勹",
    "⼔": "匕", "⼕": "匚", "⼖": "匸", "⼗": "十", "⼘": "卜",
    "⼙": "卩", "⼚": "厂", "⼛": "厶", "⼜": "又", "⼝": "口",
    "⼞": "囗", "⼟": "土", "⼠": "士", "⼡": "夂", "⼢": "夊",
    "⼣": "夕", "⼤": "大", "⼥": "女", "⼦": "子", "⼧": "宀",
    "⼨": "寸", "⼩": "小", "⼪": "尢", "⼫": "尸", "⼬": "屮",
    "⼭": "山", "⼮": "巛", "⼯": "工", "⼰": "己", "⼱": "巾",
    "⼲": "干", "⼳": "幺", "⼴": "广", "⼵": "廴", "⼶": "廾",
    "⼷": "弋", "⼸": "弓", "⼹": "彐", "⼺": "彡", "⼻": "彳",
    "⼼": "心", "⼽": "戈", "⼾": "户", "⼿": "手", "⽀": "支",
    "⽁": "攴", "⽂": "文", "⽃": "斗", "⽄": "斤", "⽅": "方",
    "⽆": "无", "⽇": "日", "⽈": "曰", "⽉": "月", "⽊": "木",
    "⽋": "欠", "⽌": "止", "⽍": "歹", "⽎": "殳", "⽏": "毋",
    "⽐": "比", "⽑": "毛", "⽒": "氏", "⽓": "气", "⽔": "水",
    "⽕": "火", "⽖": "爪", "⽗": "父", "⽘": "爻", "⽙": "爿",
    "⽚": "片", "⽛": "牙", "⽜": "牛", "⽝": "犬", "⽞": "玄",
    "⽟": "玉", "⽠": "瓜", "⽡": "瓦", "⽢": "甘", "⽣": "生",
    "⽤": "用", "⽥": "田", "⽦": "疋", "⽧": "疒", "⽨": "癶",
    "⽩": "白", "⽪": "皮", "⽫": "皿", "⽬": "目", "⽭": "矛",
    "⽮": "矢", "⽯": "石", "⽰": "示", "⽱": "禸", "⽲": "禾",
    "⽳": "穴", "⽴": "立", "⽵": "竹", "⽶": "米", "⽷": "糸",
    "⽸": "缶", "⽹": "网", "⽺": "羊", "⽻": "羽", "⽼": "老",
    "⽽": "而", "⽾": "耒", "⽿": "耳", "⾀": "聿", "⾁": "肉",
    "⾂": "臣", "⾃": "自", "⾄": "至", "⾅": "臼", "⾆": "舌",
    "⾇": "舛", "⾈": "舟", "⾉": "艮", "⾊": "色", "⾋": "艸",
    "⾌": "虍", "⾍": "虫", "⾎": "血", "⾏": "行", "⾐": "衣",
    "⾑": "襾", "⾒": "见", "⾓": "角", "⾔": "言", "⾕": "谷",
    "⾖": "豆", "⾗": "豕", "⾘": "豸", "⾙": "贝", "⾚": "赤",
    "⾛": "走", "⾜": "足", "⾝": "身", "⾞": "车", "⾟": "辛",
    "⾠": "辰", "⾡": "辵", "⾢": "邑", "⾣": "酉", "⾤": "釆",
    "⾥": "里", "⾦": "金", "⾧": "长", "⾨": "门", "⾩": "阜",
    "⾪": "隶", "⾫": "隹", "⾬": "雨", "⾭": "青", "⾮": "非",
    "⾯": "面", "⾰": "革", "⾱": "韦", "⾲": "韭", "⾳": "音",
    "⾴": "页", "⾵": "风", "⾶": "飞", "⾷": "食", "⾸": "首",
    "⾹": "香", "⾺": "马", "⾻": "骨", "⾼": "高", "⾽": "髟",
    "⾾": "鬥", "⾿": "鬯", "⿀": "鬲", "⿁": "鬼", "⿂": "鱼",
    "⿃": "鸟", "⿄": "卤", "⿅": "鹿", "⿆": "麦", "⿇": "麻",
    "⿈": "黄", "⿉": "黍", "⿊": "黑", "⿋": "黹", "⿌": "黾",
    "⿍": "鼎", "⿎": "鼓", "⿏": "鼠", "⿐": "鼻", "⿑": "齐",
    "⿒": "齿", "⿓": "龙", "⿔": "龟", "⿕": "龠",
    # OCR 常见部件字
    "⻛": "风", "⻚": "页", "⻥": "鱼", "⻢": "马",
    "⻓": "长", "⻔": "门", "⻋": "车", "⻝": "食", "⻅": "见",
}


def fix_cjk(text: str) -> str:
    """NFKC 规范化 + CJK 康熙部首修复"""
    text = unicodedata.normalize("NFKC", text)
    return "".join(_CJK_RADICAL.get(ch, ch) for ch in text)


def _clean_picture_text_lines(block_text: str) -> list[str]:
    lines = []
    for raw_line in block_text.split("<br>"):
        line = raw_line.strip()
        line = re.sub(r"^[•◦▪]+", "", line).strip()
        if not line or line in {"🎯", "“ ”"}:
            continue
        lines.append(line)
    return lines


def _extract_metadata_from_picture_lines(lines: list[str]) -> str | None:
    joined = " ".join(line.strip() for line in lines)
    pattern = re.compile(
        r"需求负责人\s*(?P<owner>.*?)\s+需求提出方\s*(?P<requester>.*?)\s+预计上线时间\s*(?P<eta>.*?)\s+关联项目流程链接\s*(?P<link>.+)$"
    )
    match = pattern.search(joined)
    if not match:
        return None

    owner = match.group("owner").strip()
    requester = match.group("requester").strip()
    eta = match.group("eta").strip()
    link = match.group("link").strip()
    return (
        f"- 需求负责人: {owner}\n"
        f"- 需求提出方: {requester}\n"
        f"- 预计上线时间: {eta}\n"
        f"- 关联项目流程链接: {link}\n"
    )


def _picture_text_replacement(match: re.Match[str]) -> str:
    block_text = match.group("content")
    lines = _clean_picture_text_lines(block_text)
    if not lines:
        return "\n"

    metadata_block = _extract_metadata_from_picture_lines(lines)
    if metadata_block:
        return f"\n{metadata_block}\n"

    if len(lines) == 1 and len(lines[0]) <= 40 and re.match(r"^\d+(?:\.\d+)*\s*", lines[0]):
        return f"\n## {lines[0]}\n"

    return "\n"


def normalize_chinese_quotes(text: str) -> str:
    text = re.sub(r'"([^"\n]*[\u4e00-\u9fff][^"\n]*)"', r"“\1”", text)
    text = re.sub(r"'([^'\n]*[\u4e00-\u9fff][^'\n]*)'", r"“\1”", text)
    return text


def normalize_chinese_spacing(text: str) -> str:
    hspace = r"[ \t\u00A0\u3000]+"
    text = re.sub(rf"([\u4e00-\u9fff]){hspace}([\u4e00-\u9fff])", r"\1\2", text)
    text = re.sub(rf"([\u4e00-\u9fff]){hspace}([，。；：！？、）】》”’\)])", r"\1\2", text)
    text = re.sub(rf"([（【《“‘\(]){hspace}([\u4e00-\u9fff])", r"\1\2", text)
    text = re.sub(rf"([”’）】》\)]){hspace}([\u4e00-\u9fff])", r"\1\2", text)
    return text


def _is_plain_text_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith(("#", "- ", "* ", "|", "![", "```")):
        return False
    if re.match(r"^\d+\.\s", stripped):
        return False
    return True


def _should_join_lines(previous: str, current: str) -> bool:
    prev = previous.strip()
    curr = current.strip()
    if not prev or not curr:
        return False
    if not _is_plain_text_line(previous) or not _is_plain_text_line(current):
        return False
    if prev.endswith(("。", "！", "？", "!", "?", "|")):
        return False
    if len(prev) < 12 and len(curr) < 12:
        return False
    return True


def merge_soft_wrapped_lines(text: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text

    merged: list[str] = []
    for line in lines:
        if merged and _should_join_lines(merged[-1], line):
            merged[-1] = merged[-1].rstrip() + line.lstrip()
        else:
            merged.append(line)
    return "\n".join(merged)


def restructure_top_metadata_block(text: str) -> str:
    pattern = re.compile(
        r"^\s*需求负责人\s*\n(?P<owner>.+?)\n+需求提出方\s*\n(?P<requester>.+?)\n+预计上线时间\s*\n(?P<eta>.+?)\n+关联项目流程链接\s*(?:\n)?(?P<link>.+?)\n+(?=## )",
        flags=re.MULTILINE | re.DOTALL,
    )

    def repl(match: re.Match[str]) -> str:
        owner = match.group("owner").strip()
        requester = match.group("requester").strip()
        eta = match.group("eta").strip()
        link = match.group("link").strip()
        return (
            f"- 需求负责人: {owner}\n"
            f"- 需求提出方: {requester}\n"
            f"- 预计上线时间: {eta}\n"
            f"- 关联项目流程链接: {link}\n\n"
        )

    return pattern.sub(repl, text, count=1)


def normalize_bullets(text: str) -> str:
    text = re.sub(r"^[ \t]*[•◦▪]\s*", "- ", text, flags=re.MULTILINE)
    text = re.sub(r"^([ \t]*-)\s*[•◦▪]\s*", r"\1 ", text, flags=re.MULTILINE)
    return text


def remove_empty_placeholder_items(text: str) -> str:
    return re.sub(r'^[ \t]*-\s*[“"]?\s*[”"]?\s*$\n?', "", text, flags=re.MULTILINE)


def remove_placeholder_section_items(text: str) -> str:
    return re.sub(r"^[ \t]*-\s*(?:具体内容|风险与应对)\s*$\n?", "", text, flags=re.MULTILINE)


def _parse_table_row_cells(line: str) -> list[str] | None:
    stripped = line.strip()
    if not stripped.startswith("|") or stripped.count("|") < 2:
        return None
    body = stripped[1:]
    if body.endswith("|"):
        body = body[:-1]
    return body.split("|")


def _clean_table_cell(cell: str) -> str:
    cleaned = cell.strip()
    cleaned = re.sub(r"^(?:[•◦▪]\s*<br>\s*)+", "", cleaned)
    cleaned = re.sub(r"^[•◦▪]\s*", "", cleaned)
    return cleaned.strip()


def _is_separator_table_row(cells: list[str]) -> bool:
    non_empty = [cell.strip() for cell in cells if cell.strip()]
    if not non_empty:
        return False
    return all(re.fullmatch(r":?-{3,}:?", cell) for cell in non_empty)


def normalize_table_rows(text: str) -> str:
    normalized_lines: list[str] = []
    expected_width: int | None = None

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|") or stripped.count("|") < 2:
            normalized_lines.append(line)
            expected_width = None
            continue

        cells = _parse_table_row_cells(stripped)
        if cells is None:
            normalized_lines.append(line)
            expected_width = None
            continue

        if _is_separator_table_row(cells):
            normalized = [cell.strip() for cell in cells]
            while len(normalized) > 1 and not normalized[-1]:
                normalized.pop()
            non_empty = [cell for cell in normalized if cell]
            if not non_empty:
                continue
            if expected_width:
                normalized = [non_empty[0]] * expected_width
            elif len(set(non_empty)) == 1:
                normalized = [non_empty[0]]
            normalized_lines.append(f"|{'|'.join(normalized)}|")
            continue

        normalized = [_clean_table_cell(cell) for cell in cells]
        while len(normalized) > 1 and not normalized[-1]:
            normalized.pop()
        non_empty = [cell for cell in normalized if cell]
        if len(normalized) == 3 and len(non_empty) == 2 and "" in normalized:
            normalized = non_empty
            non_empty = normalized
        if not non_empty:
            continue
        if len(set(non_empty)) == 1:
            normalized = [non_empty[0]]
        elif len(non_empty) == 1:
            normalized = [non_empty[0]]
        expected_width = len(normalized)
        normalized_lines.append(f"|{'|'.join(normalized)}|")

    return "\n".join(normalized_lines)


def remove_redundant_table_title_rows(text: str) -> str:
    return re.sub(r"^\|风险与应对\|\n(?=\|风险\|应对方案\|)", "", text, flags=re.MULTILINE)


def insert_missing_table_separators(text: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text

    normalized_lines: list[str] = []
    for index, line in enumerate(lines):
        normalized_lines.append(line)
        current_cells = _parse_table_row_cells(line)
        if current_cells is None or _is_separator_table_row(current_cells):
            continue

        if index + 1 >= len(lines):
            continue
        next_cells = _parse_table_row_cells(lines[index + 1])
        if next_cells is None or _is_separator_table_row(next_cells):
            continue

        current_non_empty = [_clean_table_cell(cell) for cell in current_cells if _clean_table_cell(cell)]
        next_non_empty = [_clean_table_cell(cell) for cell in next_cells if _clean_table_cell(cell)]
        if len(current_non_empty) < 2 or len(current_non_empty) != len(next_non_empty):
            continue
        if any("<br>" in cell for cell in current_non_empty):
            continue
        if not all(len(cell) <= 12 for cell in current_non_empty):
            continue

        normalized_lines.append(f"|{'|'.join(['---'] * len(current_non_empty))}|")

    return "\n".join(normalized_lines)


def expand_inline_bullet_markers(text: str) -> str:
    expanded_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("|") or not any(marker in line for marker in ("◦", "▪", "•")):
            expanded_lines.append(line)
            continue

        line = re.sub(r"\s+[◦▪•]\s*(?=阶段[一二三四五六七八九十\d])", "\n- ", line)
        bullet_prefix = "  - " if stripped.startswith("- ") else "- "
        line = re.sub(r"\s+[◦▪•]\s*(?=\S)", "\n" + bullet_prefix, line)
        expanded_lines.extend(line.splitlines())

    return "\n".join(expanded_lines)


def post_process_markdown(text: str) -> str:
    text = html.unescape(text)
    text = text.replace("\ufffd", "")
    text = text.replace("\x01", "")
    text = re.sub(r"\[([^\]]+)\]\(javascript:void\(0\)\)", r"\1", text)
    text = re.sub(r"^\s*<!-- image -->\s*$\n?", "", text, flags=re.MULTILINE)
    text = re.sub(
        r"\n?\*\*==> picture \[[^\]]+\] intentionally omitted <==\*\*\n?",
        "\n",
        text,
    )
    text = re.sub(
        r"\n?\*\*----- Start of picture text -----\*\*<br>\n(?P<content>.*?)\*\*----- End of picture text -----\*\*<br>\n?",
        _picture_text_replacement,
        text,
        flags=re.DOTALL,
    )
    text = re.sub(r"^##\s*[•◦▪·]\s*", "- ", text, flags=re.MULTILINE)
    text = re.sub(r"^##\s*([a-zA-Z]\.)\s*", r"- \1 ", text, flags=re.MULTILINE)
    text = re.sub(
        r"^##\s*(1\.\s+[^\n]+)\n+(?=2\.\s+)",
        r"\1\n\n",
        text,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"([;；])\s*(\d+(?:\.\d+)+\s+[^\n]{1,40})\s*(?=\n)",
        r"\1\n\n## \2",
        text,
    )
    text = re.sub(r"^\|+\s*$\n?", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\|(?:---\|)+\s*$\n(?=\|{2,})", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\|{2,}", "|", text, flags=re.MULTILINE)
    text = normalize_chinese_quotes(text)
    text = normalize_chinese_spacing(text)
    text = expand_inline_bullet_markers(text)
    text = merge_soft_wrapped_lines(text)
    text = restructure_top_metadata_block(text)
    text = normalize_bullets(text)
    text = remove_empty_placeholder_items(text)
    text = remove_placeholder_section_items(text)
    text = normalize_table_rows(text)
    text = remove_redundant_table_title_rows(text)
    text = insert_missing_table_separators(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() + "\n"


def collect_pdf_files(source: Path) -> list[Path]:
    if source.is_file():
        return [source] if source.suffix.lower() == ".pdf" else []
    if source.is_dir():
        return sorted(path for path in source.glob("*.pdf") if path.is_file())
    return []


def output_markdown_path(pdf_path: Path) -> Path:
    return pdf_path.with_suffix(".md")


def output_image_dir(pdf_path: Path) -> Path:
    return pdf_path.with_name(f"{pdf_path.stem}_images")


@contextmanager
def prepare_docling_input(pdf_path: Path):
    if pdf_path.name.isascii():
        yield pdf_path
        return

    digest = hashlib.sha1(str(pdf_path).encode("utf-8")).hexdigest()[:10]
    temp_root = pdf_path.parent / ".docling-temp"
    temp_root.mkdir(exist_ok=True)
    prepared = temp_root / f"input-{digest}.pdf"
    try:
        shutil.copyfile(pdf_path, prepared)
        yield prepared
    finally:
        if prepared.exists():
            prepared.unlink()


def load_docling():
    try:
        from docling.document_converter import DocumentConverter
    except ModuleNotFoundError as exc:
        print("缺少依赖 docling，请先执行: pip install docling", file=sys.stderr)
        raise SystemExit(1) from exc
    return DocumentConverter


def load_pymupdf4llm():
    try:
        import pymupdf4llm
    except ModuleNotFoundError as exc:
        print("缺少依赖 pymupdf4llm，请先执行: pip install pymupdf4llm", file=sys.stderr)
        raise SystemExit(1) from exc
    return pymupdf4llm


def convert_pdf_with_docling(pdf_path: Path) -> str:
    DocumentConverter = load_docling()
    converter = DocumentConverter()
    with prepare_docling_input(pdf_path) as prepared_pdf:
        result = converter.convert(prepared_pdf)
    return result.document.export_to_markdown()


def convert_pdf_with_pymupdf4llm(pdf_path: Path) -> str:
    pymupdf4llm = load_pymupdf4llm()
    image_dir = output_image_dir(pdf_path)
    return pymupdf4llm.to_markdown(
        str(pdf_path),
        write_images=True,
        image_path=str(image_dir),
        show_progress=True,
    )


def convert_pdf(pdf_path: Path, engine: str = DEFAULT_ENGINE) -> Path:
    output_md = output_markdown_path(pdf_path)

    print(f"提取: {pdf_path.name}")
    print(f"使用 {engine} ...")

    if engine == "docling":
        md_text = convert_pdf_with_docling(pdf_path)
    elif engine == "pymupdf4llm":
        md_text = convert_pdf_with_pymupdf4llm(pdf_path)
    else:
        print(f"不支持的引擎: {engine}")
        raise SystemExit(1)

    md_text = fix_cjk(md_text)
    md_text = post_process_markdown(md_text)
    output_md.write_text(md_text, encoding="utf-8")

    print(f"完成! 输出: {output_md.name} ({len(md_text)} 字符)")
    return output_md


def parse_args(args: list[str]) -> tuple[str, Path]:
    engine = DEFAULT_ENGINE
    source_arg: str | None = None
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--engine":
            if index + 1 >= len(args):
                print("--engine 缺少参数")
                raise SystemExit(1)
            engine = args[index + 1]
            index += 2
            continue
        if source_arg is None:
            source_arg = arg
            index += 1
            continue
        print(f"无法识别的参数: {arg}")
        raise SystemExit(1)

    source = Path(source_arg).resolve() if source_arg else DEFAULT_SOURCE
    return engine, source


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    engine, source = parse_args(args)
    pdf_files = collect_pdf_files(source)

    if not pdf_files:
        print(f"未找到 PDF 文件: {source}")
        return 1

    for index, pdf_path in enumerate(pdf_files):
        if index:
            print()
        convert_pdf(pdf_path, engine=engine)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
