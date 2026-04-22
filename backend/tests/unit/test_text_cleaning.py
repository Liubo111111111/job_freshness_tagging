"""text_cleaning 模块单元测试。"""

from __future__ import annotations

import json

import pytest

from job_freshness.schemas import WideRow
from job_freshness.text_cleaning import (
    clean_im_text,
    clean_wide_row_texts,
    denoise_text,
    flatten_asr,
    flatten_im,
)


# ---------------------------------------------------------------------------
# denoise_text
# ---------------------------------------------------------------------------


class TestDenoiseText:
    """通用降噪函数测试。"""

    def test_denoise_removes_fillers(self) -> None:
        """语气词（嗯、啊等）应被移除。"""
        text = "嗯啊那个就是明天来上班"
        result = denoise_text(text)
        assert "嗯" not in result
        assert "啊" not in result
        assert "那个" not in result
        assert "就是" not in result
        assert "明天来上班" in result

    def test_denoise_collapses_repeats(self) -> None:
        """短重复片段（1-4 字符重复）应折叠为 1 次。"""
        # "好好好好" = "好好" × 2 → "好好"
        assert denoise_text("好好好好") == "好好"
        # "来来来来来来" = "来来" × 3 → "来来" 或 "来来来" × 2 → "来来来"
        result = denoise_text("来来来来来来")
        assert len(result) < len("来来来来来来")
        # 多字符重复
        assert denoise_text("你说你说你说") == "你说"

    def test_denoise_removes_punct_lines(self) -> None:
        """纯标点行应被移除。"""
        text = "正常内容\n。。。\n---\n另一行正常内容"
        result = denoise_text(text)
        assert "。。。" not in result
        assert "---" not in result
        assert "正常内容" in result
        assert "另一行正常内容" in result

    def test_denoise_removes_short_numbers(self) -> None:
        """短纯数字行（< 11 字符）应被移除，11 位电话号码应保留。"""
        text = "123\n13800138000\n正常内容"
        result = denoise_text(text)
        assert "123" not in result
        assert "13800138000" in result
        assert "正常内容" in result

    def test_denoise_removes_chinese_number_lines(self) -> None:
        """短中文数字行也应被移除。"""
        text = "三百\n正常内容"
        result = denoise_text(text)
        assert "三百" not in result
        assert "正常内容" in result

    def test_denoise_deduplicates_adjacent_lines(self) -> None:
        """相邻重复行应去重。"""
        text = "明天来\n明天来\n明天来\n再见"
        result = denoise_text(text)
        lines = result.splitlines()
        # "明天来" 只出现一次（相邻去重）
        assert lines.count("明天来") == 1
        assert "再见" in result

    def test_denoise_strips_trailing_dots(self) -> None:
        """行尾句号/点号应被移除。"""
        text = "明天来上班。"
        result = denoise_text(text)
        assert not result.endswith("。")
        assert "明天来上班" in result

    def test_denoise_strips_leading_punct(self) -> None:
        """行首标点应被移除。"""
        text = "，明天来上班"
        result = denoise_text(text)
        assert result.startswith("明天")

    def test_denoise_empty_input(self) -> None:
        """空输入返回空字符串。"""
        assert denoise_text("") == ""
        assert denoise_text("   ") == ""

    def test_denoise_removes_uh_variants(self) -> None:
        """呃+ 连续多个应被移除。"""
        text = "呃呃呃明天来"
        result = denoise_text(text)
        assert "呃" not in result
        assert "明天来" in result


# ---------------------------------------------------------------------------
# flatten_asr
# ---------------------------------------------------------------------------


class TestFlattenAsr:
    """ASR JSON 展平测试。"""

    def test_flatten_asr_sorts_and_cleans(self) -> None:
        """JSON 数组按 begin 排序后提取 text，并降噪。"""
        asr_json = json.dumps([
            {"begin": 200, "text": "后天到岗"},
            {"begin": 100, "text": "嗯那个"},
            {"begin": 300, "text": "好的好的好的"},
        ], ensure_ascii=False)
        result = flatten_asr(asr_json)
        lines = result.splitlines()
        # begin=100 的 "嗯那个" 应被降噪掉（纯语气词）
        # begin=200 的 "后天到岗" 应在前
        # begin=300 的 "好的好的好的" 应折叠为 "好的"
        assert "后天到岗" in result
        assert "好的" in result
        # 排序验证：后天到岗 在 好的 之前
        idx_ht = result.index("后天到岗")
        idx_hd = result.index("好的")
        assert idx_ht < idx_hd

    def test_flatten_asr_empty(self) -> None:
        """空/null/None 输入返回空字符串。"""
        assert flatten_asr("") == ""
        assert flatten_asr("None") == ""
        assert flatten_asr("null") == ""
        assert flatten_asr("[]") == ""

    def test_flatten_asr_invalid_json(self) -> None:
        """非 JSON 格式当作纯文本降噪。"""
        result = flatten_asr("这是一段普通文本")
        assert "这是一段普通文本" in result


# ---------------------------------------------------------------------------
# clean_im_text
# ---------------------------------------------------------------------------


class TestCleanImText:
    """IM 清洗测试。"""

    def test_removes_greetings(self) -> None:
        """固定问候语应被移除。"""
        text = "你好\n你好，请问有什么可以帮您？\n明天来上班\n好的"
        result = clean_im_text(text)
        assert "你好，请问有什么可以帮您？" not in result
        assert "明天来上班" in result

    def test_removes_greeting_with_uid(self) -> None:
        """带 uid 前缀的问候语也应被移除。"""
        text = "12345: 你好\n67890: 明天来上班"
        result = clean_im_text(text)
        assert "明天来上班" in result

    def test_deduplicates_lines(self) -> None:
        """重复行应去重。"""
        text = "12345: 明天来\n12345: 明天来\n67890: 好的收到"
        result = clean_im_text(text)
        assert result.count("明天来") == 1


# ---------------------------------------------------------------------------
# flatten_im
# ---------------------------------------------------------------------------


class TestFlattenIm:
    """IM 展平测试。"""

    def test_flatten_im_removes_greetings(self) -> None:
        """问候语应被移除。"""
        text = "12345: 你好\n67890: 你好，请问有什么可以帮您？\n12345: 明天来上班\n67890: 好的收到"
        result = flatten_im(text)
        assert "你好，请问有什么可以帮您？" not in result
        assert "明天来上班" in result

    def test_flatten_im_caps_turns(self) -> None:
        """超过 30 轮时应截断。"""
        # 构造 35 轮对话（交替 uid）
        lines: list[str] = []
        for i in range(35):
            uid = "111" if i % 2 == 0 else "222"
            lines.append(f"{uid}: 消息{i}")
        text = "\n".join(lines)
        result = flatten_im(text)
        # 第一轮（消息0）应被跳过，保留 turns[1:30]
        assert "消息0" not in result
        # 消息1 应保留（第二轮）
        assert "消息1" in result

    def test_flatten_im_empty(self) -> None:
        """空输入返回空字符串。"""
        assert flatten_im("") == ""

    def test_flatten_im_applies_denoise(self) -> None:
        """展平后应应用降噪。"""
        text = "12345: 嗯嗯嗯\n67890: 明天来上班。"
        result = flatten_im(text)
        assert "明天来上班" in result


# ---------------------------------------------------------------------------
# clean_wide_row_texts
# ---------------------------------------------------------------------------


class TestCleanWideRowTexts:
    """WideRow 文本清洗主入口测试。"""

    def test_clean_wide_row_texts(self) -> None:
        """验证各字段的清洗策略。"""
        asr_json = json.dumps([
            {"begin": 100, "text": "嗯那个明天来"},
            {"begin": 200, "text": "好的好的好的"},
        ], ensure_ascii=False)

        im_text = "12345: 你好\n67890: 你好，请问有什么可以帮您？\n12345: 后天到岗"

        wide_row = WideRow(
            user_id="u1",
            info_id="i1",
            job_detail="这是职位详情",
            asr_result=asr_json,
            im_text=im_text,
            complaint_content="嗯啊投诉内容。",
        )

        cleaned = clean_wide_row_texts(wide_row)

        # job_detail 保持不变
        assert cleaned.job_detail == "这是职位详情"

        # asr_result 已展平降噪
        assert "明天来" in cleaned.asr_result
        assert "好的" in cleaned.asr_result

        # im_text 已清洗
        assert "你好，请问有什么可以帮您？" not in cleaned.im_text
        assert "后天到岗" in cleaned.im_text

        # complaint_content 已轻度降噪
        assert "投诉内容" in cleaned.complaint_content
        assert "嗯" not in cleaned.complaint_content

    def test_clean_wide_row_none_values(self) -> None:
        """None/"None"/空值应统一为空字符串。"""
        wide_row = WideRow(
            user_id="u1",
            info_id="i1",
            asr_result="None",
            im_text="",
            complaint_content="null",
        )

        cleaned = clean_wide_row_texts(wide_row)
        assert cleaned.asr_result == ""
        assert cleaned.im_text == ""
        assert cleaned.complaint_content == ""

    def test_clean_wide_row_returns_valid_wide_row(self) -> None:
        """返回值应是合法的 WideRow（extra=forbid 不报错）。"""
        wide_row = WideRow(
            user_id="u1",
            info_id="i1",
            job_detail="详情",
            asr_result="",
            im_text="",
            complaint_content="",
        )
        cleaned = clean_wide_row_texts(wide_row)
        assert isinstance(cleaned, WideRow)
        # 验证 model_dump 不报错
        cleaned.model_dump()
