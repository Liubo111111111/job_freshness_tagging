"""文本清洗模块 — 对 WideRow 中的原始文本字段进行降噪、展平、去重。

在 snippet_recall 之前执行，纯规则处理，不调用 LLM。
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata

from job_freshness.schemas import WideRow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 语气词 / 填充词列表
# ---------------------------------------------------------------------------

_FILLER_WORDS: list[str] = [
    "喂", "嗯", "啊", "那个", "那啥", "这个", "就是", "的话",
    "吧", "呢", "呀", "哦", "欸", "哈", "吗", "了", "嘛",
    "你好", "您好", "对", "诶", "哎",
]

# 按长度降序排列，避免短词先匹配导致长词残留
_FILLER_WORDS_SORTED = sorted(_FILLER_WORDS, key=len, reverse=True)

# 构建语气词正则：匹配独立的语气词（呃 支持连续多个）
_FILLER_RE = re.compile(
    r"呃+|" + "|".join(re.escape(w) for w in _FILLER_WORDS_SORTED)
)

# 短重复片段折叠：1-4 字符重复 2 次以上 → 保留 1 次
_REPEAT_RE = re.compile(r"(.{1,4})\1+")

# 行首标点
_LEADING_PUNCT_RE = re.compile(r"^[\s,，.。;；:：!！?？、\-—…·]+")

# 行尾句号/点号
_TRAILING_DOT_RE = re.compile(r"[。.]+$")

# 纯标点行判定
_PURE_PUNCT_RE = re.compile(
    r"^[\s\p{P}\p{S}]+$" if False else  # unicodedata fallback below
    r"^[\s!\"#$%&\'()*+,\-./:;<=>?@\[\\\]^_`{|}~"
    r"，。、；：？！…—·''""《》【】（）〈〉〔〕｛｝"
    r"～﹏﹑﹔﹖﹗﹐﹒﹕﹔]+$"
)

# 中文数字字符
_CN_DIGITS = set("零一二三四五六七八九十百千万亿〇壹贰叁肆伍陆柒捌玖拾佰仟")

# IM 固定问候语集合
_GREETINGS: set[str] = {
    "你好",
    "您好",
    "你好！",
    "您好！",
    "你好，请问有什么可以帮您？",
    "您好，请问有什么可以帮您？",
    "你好，有什么可以帮你的吗？",
    "您好，有什么可以帮你的吗？",
    "你好，请问有什么可以帮助您的？",
    "您好，请问有什么可以帮助您的？",
    "你好，我是智能助手，请问有什么可以帮您？",
    "您好，我是智能助手，请问有什么可以帮您？",
    "你好，欢迎咨询",
    "您好，欢迎咨询",
    "你好，欢迎咨询！",
    "您好，欢迎咨询！",
    "您好！很高兴为您服务",
    "你好！很高兴为您服务",
    "您好！很高兴为您服务！",
    "你好！很高兴为您服务！",
    "您好，很高兴为您服务",
    "你好，很高兴为您服务",
    "您好，很高兴为您服务！",
    "你好，很高兴为您服务！",
    "您好，请问您需要什么帮助？",
    "你好，请问你需要什么帮助？",
    "你好呀",
    "您好呀",
    "在吗",
    "在吗？",
    "在的",
    "在的亲",
    "亲，在的",
    "在呢",
    "嗯嗯",
    "好的",
    "好的呢",
    "好的哦",
    "收到",
    "嗯",
    "哦",
    "哈喽",
    "hello",
    "Hello",
    "hi",
    "Hi",
    "HI",
    "HELLO",
}

# IM 行解析：uid: content
_IM_LINE_RE = re.compile(r"^(\d+):\s*(.*)")


# ---------------------------------------------------------------------------
# denoise_text — 通用降噪
# ---------------------------------------------------------------------------


def denoise_text(text: str) -> str:
    """通用文本降噪：去语气词、折叠重复、去标点行、去短纯数字行、相邻去重。"""
    if not text:
        return ""

    lines = text.splitlines()
    cleaned: list[str] = []
    prev_line: str | None = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 跳过纯标点行
        if _PURE_PUNCT_RE.match(line):
            continue

        # 跳过短纯数字行（< 11 字符，含中文数字）— 在降噪前判断
        if _is_short_number_line(line):
            continue

        # 去语气词
        line = _FILLER_RE.sub("", line)

        # 折叠短重复片段（跳过纯数字行，避免破坏电话号码等）
        if not _is_pure_number(line):
            line = _REPEAT_RE.sub(r"\1", line)

        # 去行首标点
        line = _LEADING_PUNCT_RE.sub("", line)

        # 去行尾句号/点号
        line = _TRAILING_DOT_RE.sub("", line)

        line = line.strip()
        if not line:
            continue

        # 跳过纯标点行（降噪后再检查一次）
        if _PURE_PUNCT_RE.match(line):
            continue

        # 跳过清洗后只剩符号的行（如 "。"、"，"、"、" 等）
        if len(line) <= 2 and all(not c.isalnum() for c in line):
            continue

        # 相邻行去重
        if line == prev_line:
            continue

        cleaned.append(line)
        prev_line = line

    return "\n".join(cleaned)


def _is_short_number_line(line: str) -> bool:
    """判断是否为短纯数字行（长度 < 11，仅含阿拉伯数字和中文数字）。"""
    if len(line) >= 11:
        return False
    return all(c.isdigit() or c in _CN_DIGITS for c in line)


def _is_pure_number(line: str) -> bool:
    """判断是否为纯数字行（仅含阿拉伯数字和中文数字）。"""
    return bool(line) and all(c.isdigit() or c in _CN_DIGITS for c in line)


# ---------------------------------------------------------------------------
# flatten_asr — ASR JSON 展平
# ---------------------------------------------------------------------------


# 通话标记行正则：【通话N 时间】
_CALL_HEADER_RE = re.compile(r"^【通话\d+\s+[^】]*】$")


def flatten_asr(raw: str) -> str:
    """将 ASR 数据展平为纯文本。

    支持两种输入格式：
    1. 纯 JSON 数组：[{"speaker":0,"begin":740,"text":"你好"}, ...]
    2. 带通话标记的多段格式：
       【通话1 2026-04-14 08:07:53】
       [{"speaker":0,"begin":740,"text":"你好"}, ...]
       【通话2 2026-04-15 10:00:00】
       [{"speaker":1,"begin":100,"text":"明天来"}, ...]

    输出仅保留 text 字段内容，丢弃 speaker/begin/end，保留通话标记行。
    """
    if not raw or raw.strip() in ("", "None", "null", "[]"):
        return ""

    raw = raw.strip()

    # 如果包含通话标记，按标记分段处理
    if "【通话" in raw:
        return _flatten_multi_call_asr(raw)

    # 尝试直接解析为 JSON 数组
    return _flatten_single_json_asr(raw)


def _flatten_single_json_asr(raw: str) -> str:
    """解析单个 JSON 数组，提取 text 字段。"""
    try:
        items = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return denoise_text(raw)

    if not isinstance(items, list):
        return denoise_text(raw)

    try:
        items.sort(key=lambda x: x.get("begin", 0))
    except (AttributeError, TypeError):
        pass

    texts: list[str] = []
    for item in items:
        if isinstance(item, dict):
            t = str(item.get("text", "")).strip()
            if t:
                texts.append(t)

    return denoise_text("\n".join(texts))


def _flatten_multi_call_asr(raw: str) -> str:
    """处理带【通话N 时间】标记的多段 ASR 数据。"""
    output_parts: list[str] = []
    current_header: str = ""
    current_json_lines: list[str] = []

    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        if _CALL_HEADER_RE.match(stripped):
            # 先处理上一段
            if current_header and current_json_lines:
                text = _extract_texts_from_json_lines(current_json_lines)
                if text:
                    output_parts.append(f"{current_header}\n{text}")
            current_header = stripped
            current_json_lines = []
        else:
            current_json_lines.append(stripped)

    # 处理最后一段
    if current_header and current_json_lines:
        text = _extract_texts_from_json_lines(current_json_lines)
        if text:
            output_parts.append(f"{current_header}\n{text}")

    return denoise_text("\n".join(output_parts))


def _extract_texts_from_json_lines(lines: list[str]) -> str:
    """从 JSON 行中提取 text 字段。支持完整 JSON 数组或逐行 JSON 对象。"""
    joined = "\n".join(lines)

    # 尝试解析为完整 JSON 数组
    try:
        items = json.loads(joined)
        if isinstance(items, list):
            texts = []
            for item in items:
                if isinstance(item, dict):
                    t = str(item.get("text", "")).strip()
                    if t:
                        texts.append(t)
            return "\n".join(texts)
    except (json.JSONDecodeError, TypeError):
        pass

    # 尝试修复：可能是逗号分隔的 JSON 对象（缺少外层方括号）
    try:
        items = json.loads(f"[{joined}]")
        if isinstance(items, list):
            texts = []
            for item in items:
                if isinstance(item, dict):
                    t = str(item.get("text", "")).strip()
                    if t:
                        texts.append(t)
            return "\n".join(texts)
    except (json.JSONDecodeError, TypeError):
        pass

    # 逐行尝试解析
    texts = []
    for line in lines:
        line = line.strip().rstrip(",")
        try:
            item = json.loads(line)
            if isinstance(item, dict):
                t = str(item.get("text", "")).strip()
                if t:
                    texts.append(t)
        except (json.JSONDecodeError, TypeError):
            # 非 JSON 行，如果不是纯标点/空白就保留
            if line and not _PURE_PUNCT_RE.match(line):
                texts.append(line)

    return "\n".join(texts)


# ---------------------------------------------------------------------------
# clean_im_text — IM 清洗（去问候语 + 去重）
# ---------------------------------------------------------------------------


def clean_im_text(text: str) -> str:
    """去除 IM 固定问候语，行级去重。"""
    if not text:
        return ""

    lines = text.splitlines()
    seen: set[str] = set()
    cleaned: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # 提取消息内容（可能带 uid: 前缀）
        m = _IM_LINE_RE.match(stripped)
        content = m.group(2).strip() if m else stripped

        # 去问候语
        if content in _GREETINGS:
            continue

        # 行级去重
        if stripped in seen:
            continue
        seen.add(stripped)

        cleaned.append(stripped)

    return "\n".join(cleaned)


# ---------------------------------------------------------------------------
# flatten_im — IM 展平（清洗 + 分轮 + 截断 + 降噪）
# ---------------------------------------------------------------------------


def flatten_im(text: str) -> str:
    """IM 文本展平：清洗 → 按 uid 分轮 → 截断 → 降噪。"""
    if not text:
        return ""

    # 先清洗
    cleaned = clean_im_text(text)
    if not cleaned:
        return ""

    # 解析为 (uid, content) 元组
    lines = cleaned.splitlines()
    messages: list[tuple[str, str]] = []
    for line in lines:
        m = _IM_LINE_RE.match(line)
        if m:
            messages.append((m.group(1), m.group(2).strip()))
        else:
            # 无 uid 前缀的行，用空字符串作为 uid
            messages.append(("", line.strip()))

    # 按连续相同 uid 分轮
    turns: list[list[str]] = []
    current_uid: str | None = None
    current_contents: list[str] = []

    for uid, content in messages:
        if uid != current_uid:
            if current_contents:
                turns.append(current_contents)
            current_uid = uid
            current_contents = [content]
        else:
            current_contents.append(content)

    if current_contents:
        turns.append(current_contents)

    # 超过 30 轮时截断：跳过第一轮（问候），取 turns[1:30]
    if len(turns) > 30:
        turns = turns[1:30]

    # 拼接所有内容
    all_contents: list[str] = []
    for turn in turns:
        all_contents.extend(turn)

    joined = "\n".join(all_contents)
    return denoise_text(joined)


# ---------------------------------------------------------------------------
# clean_wide_row_texts — 主入口
# ---------------------------------------------------------------------------


def clean_wide_row_texts(wide_row: WideRow) -> WideRow:
    """清洗 WideRow 中的文本字段，返回新的 WideRow 实例。

    - asr_result → flatten_asr
    - im_text → flatten_im
    - complaint_content → denoise_text（轻度清洗）
    - job_detail → 保持原样（源头已清洗）
    """
    asr_raw = _normalize_empty(wide_row.asr_result)
    im_raw = _normalize_empty(wide_row.im_text)
    complaint_raw = _normalize_empty(wide_row.complaint_content)

    return wide_row.model_copy(
        update={
            "asr_result": flatten_asr(asr_raw),
            "im_text": flatten_im(im_raw),
            "complaint_content": denoise_text(complaint_raw),
        }
    )


def _normalize_empty(value: str) -> str:
    """将 None / "None" / 空白值统一为空字符串。"""
    if not value or value.strip() in ("", "None", "null"):
        return ""
    return value
