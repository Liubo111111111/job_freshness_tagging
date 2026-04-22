import io
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
import uuid
from pathlib import Path
from unittest import mock

from script.extract_sop_pymupdf4llm import (
    fix_cjk,
    load_docling,
    post_process_markdown,
    prepare_docling_input,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "script" / "extract_sop_pymupdf4llm.py"
TEST_TEMP_ROOT = REPO_ROOT / ".tmp-tests"


class WorkspaceTempDir:
    def __init__(self) -> None:
        self.path = TEST_TEMP_ROOT / f"tmp-{uuid.uuid4().hex}"

    def __enter__(self) -> str:
        self.path.mkdir(parents=True, exist_ok=False)
        return str(self.path)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.path.exists():
            for child in sorted(self.path.rglob("*"), reverse=True):
                if child.is_file():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            self.path.rmdir()


def make_tempdir() -> WorkspaceTempDir:
    TEST_TEMP_ROOT.mkdir(exist_ok=True)
    return WorkspaceTempDir()


class ExtractSopPyMuPdf4LlmCliTests(unittest.TestCase):
    def test_converts_all_pdfs_in_input_directory_to_markdown_with_default_pymupdf_engine(self) -> None:
        with make_tempdir() as tmpdir:
            temp_root = Path(tmpdir)
            source_dir = temp_root / "doc"
            source_dir.mkdir()

            pdf_path = source_dir / "example.pdf"
            pdf_path.write_bytes(b"%PDF-1.4 fake")

            fake_module = temp_root / "pymupdf4llm.py"
            fake_module.write_text(
                textwrap.dedent(
                    """
                    from pathlib import Path

                    def to_markdown(pdf_path, write_images, image_path, show_progress):
                        Path(image_path).mkdir(parents=True, exist_ok=True)
                        return f"# Converted\\n\\nengine=pymupdf4llm\\nsource={Path(pdf_path).name}"
                    """
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = (
                f"{temp_root}{os.pathsep}{env['PYTHONPATH']}"
                if env.get("PYTHONPATH")
                else str(temp_root)
            )

            result = subprocess.run(
                [sys.executable, str(SCRIPT_PATH), str(source_dir)],
                cwd=REPO_ROOT,
                env=env,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

            output_md = source_dir / "example.md"
            self.assertTrue(output_md.exists(), result.stdout)
            self.assertIn("engine=pymupdf4llm", output_md.read_text(encoding="utf-8"))
            self.assertIn("source=example.pdf", output_md.read_text(encoding="utf-8"))

    def test_uses_docling_only_when_explicit_engine_is_requested(self) -> None:
        with make_tempdir() as tmpdir:
            temp_root = Path(tmpdir)
            source_dir = temp_root / "doc"
            source_dir.mkdir()

            pdf_path = source_dir / "example.pdf"
            pdf_path.write_bytes(b"%PDF-1.4 fake")

            fake_package = temp_root / "docling"
            fake_package.mkdir()
            (fake_package / "__init__.py").write_text("", encoding="utf-8")
            fake_module = fake_package / "document_converter.py"
            fake_module.write_text(
                textwrap.dedent(
                    """
                    from types import SimpleNamespace

                    class DocumentConverter:
                        def convert(self, pdf_path):
                            return SimpleNamespace(
                                document=SimpleNamespace(
                                    export_to_markdown=lambda: f"# Converted\\n\\nengine=docling\\nsource={pdf_path.name}"
                                )
                            )
                    """
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = (
                f"{temp_root}{os.pathsep}{env['PYTHONPATH']}"
                if env.get("PYTHONPATH")
                else str(temp_root)
            )

            result = subprocess.run(
                [sys.executable, str(SCRIPT_PATH), "--engine", "docling", str(source_dir)],
                cwd=REPO_ROOT,
                env=env,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

            output_md = source_dir / "example.md"
            self.assertTrue(output_md.exists(), result.stdout)
            self.assertIn("engine=docling", output_md.read_text(encoding="utf-8"))
            self.assertIn("source=example.pdf", output_md.read_text(encoding="utf-8"))

    def test_shows_helpful_message_when_docling_is_missing(self) -> None:
        real_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "docling.document_converter":
                raise ModuleNotFoundError("No module named 'docling'")
            return real_import(name, globals, locals, fromlist, level)

        with (
            mock.patch("builtins.__import__", side_effect=fake_import),
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
        ):
            with self.assertRaises(SystemExit) as error:
                load_docling()

        self.assertEqual(error.exception.code, 1)
        self.assertIn("pip install docling", stderr.getvalue())


class MarkdownPostProcessTests(unittest.TestCase):
    def test_repairs_common_ocr_component_characters(self) -> None:
        source = "⻥泡直聘 / ⻛险与应对 / ⻢上开工 / 首⻚ / 可⻅\n"

        result = fix_cjk(source)

        self.assertEqual(result, "鱼泡直聘 / 风险与应对 / 马上开工 / 首页 / 可见\n")

    def test_removes_picture_ocr_blocks_and_placeholder_lines(self) -> None:
        source = textwrap.dedent(
            """
            ## 标题

            **==> picture [100 x 30] intentionally omitted <==**

            **----- Start of picture text -----**<br>
            图片里的说明文字<br>
            **----- End of picture text -----**<br>

            正文段落
            """
        )

        result = post_process_markdown(source)

        self.assertNotIn("Start of picture text", result)
        self.assertNotIn("intentionally omitted", result)
        self.assertIn("正文段落", result)

    def test_downgrades_false_headings_created_from_bullets(self) -> None:
        source = "## • 有效期为明确时间时:预估截止时间=具体的时间\n"

        result = post_process_markdown(source)

        self.assertEqual(result.strip(), "- 有效期为明确时间时:预估截止时间=具体的时间")

    def test_downgrades_false_headings_created_from_bullet_and_alpha_markers(self) -> None:
        source = "## · 核心业务流程\n## a. 识别来源:\n"

        result = post_process_markdown(source)

        self.assertIn("- 核心业务流程", result)
        self.assertIn("- a. 识别来源:", result)

    def test_removes_empty_table_scaffolding(self) -> None:
        source = textwrap.dedent(
            """
            |触达渠道|触发条件|频次|文案|
            |---|---|---|---|
            |Push+站内信|立刻触发|不限制|标题:职位管理提醒|

            ||||||
            |---|---|---|---|---|
            ||短信|立刻触发|每天每人1次|短信文案|
            """
        )

        result = post_process_markdown(source)

        self.assertNotIn("||||||", result)
        self.assertNotIn("||短信", result)
        self.assertIn("|短信|立刻触发|每天每人1次|短信文案|", result)

    def test_removes_replacement_characters(self) -> None:
        source = "预计上线时间 4月30日�19:00\n"

        result = post_process_markdown(source)

        self.assertNotIn("�", result)

    def test_removes_docling_control_characters_and_image_placeholders(self) -> None:
        source = "标题\x01\n\n<!-- image -->\n\n正文\n"

        result = post_process_markdown(source)

        self.assertNotIn("\x01", result)
        self.assertNotIn("<!-- image -->", result)

    def test_simplifies_javascript_links_and_html_entities(self) -> None:
        source = "[罗显俨 ](javascript:void(0)) &amp; 背景\n"

        result = post_process_markdown(source)

        self.assertNotIn("javascript:void(0)", result)
        self.assertIn("罗显俨  & 背景", result)

    def test_keeps_heading_like_text_from_picture_ocr_blocks(self) -> None:
        source = textwrap.dedent(
            """
            ![](demo.png)

            **----- Start of picture text -----**<br>
            2.1 过期提醒与自动下架<br>
            **----- End of picture text -----**<br>

            后续正文
            """
        )

        result = post_process_markdown(source)

        self.assertIn("## 2.1 过期提醒与自动下架", result)
        self.assertNotIn("Start of picture text", result)

    def test_preserves_metadata_from_picture_ocr_blocks(self) -> None:
        source = textwrap.dedent(
            """
            **----- Start of picture text -----**<br>
            需求负责人 罗显俨 需求提出方 罗显俨 预计上线时间 4月30日19:00<br>
            关联项目流程链接 [需求]【新增】智能识别订单类职位时效性标签<br>
            **----- End of picture text -----**<br>
            """
        )

        result = post_process_markdown(source)

        self.assertIn("- 需求负责人: 罗显俨", result)
        self.assertIn("- 需求提出方: 罗显俨", result)
        self.assertIn("- 预计上线时间: 4月30日19:00", result)
        self.assertIn("- 关联项目流程链接: [需求]【新增】智能识别订单类职位时效性标签", result)

    def test_promotes_inline_numbered_section_titles(self) -> None:
        source = "上一段说明结束; 2.2 流量分发优化 \n后续正文\n"

        result = post_process_markdown(source)

        self.assertIn("## 2.2 流量分发优化", result)

    def test_downgrades_numbered_headings_when_they_are_part_of_a_list(self) -> None:
        source = "## 1. 零工类职位“已招满”投诉占比\n\n2. 有效期标签准确度\n\n3. 有效期标签覆盖率\n"

        result = post_process_markdown(source)

        self.assertIn("1. 零工类职位“已招满”投诉占比", result)
        self.assertNotIn("## 1. 零工类职位“已招满”投诉占比", result)

    def test_normalizes_chinese_quotes_and_spacing(self) -> None:
        source = "## 一 、目标(必填)\n当前平台职位投诉中,'已招满''电话打不通' 两类占比高达72%。\n- ▪ IM聊天记录(如B端回复 明天下午开工 )\n"

        result = post_process_markdown(source)

        self.assertIn("## 一、目标(必填)", result)
        self.assertIn("当前平台职位投诉中,“已招满”“电话打不通”两类占比高达72%。", result)
        self.assertIn("- IM聊天记录(如B端回复明天下午开工)", result)

    def test_joins_soft_wrapped_paragraph_lines(self) -> None:
        source = (
            "当前平台职位投诉中,“已招满”“电话打不通”\n"
            "两类占比高达72%,核心原因之一是职位信息\n"
            "时效性过时。\n\n"
            "## 下节\n"
        )

        result = post_process_markdown(source)

        self.assertIn(
            "当前平台职位投诉中,“已招满”“电话打不通”两类占比高达72%,核心原因之一是职位信息时效性过时。",
            result,
        )
        self.assertIn("\n\n## 下节\n", result)

    def test_restructures_top_metadata_block(self) -> None:
        source = (
            "需求负责人\n"
            "罗显俨\n\n"
            "需求提出方\n"
            "罗显俨\n\n"
            "预计上线时间\n"
            "4月30日19:00\n\n"
            "关联项目流程链接\n"
            "[需求]【新增】智能识别订单类职位时效性标签\n\n"
            "## 一、目标(必填)\n"
        )

        result = post_process_markdown(source)

        self.assertIn("- 需求负责人: 罗显俨", result)
        self.assertIn("- 需求提出方: 罗显俨", result)
        self.assertIn("- 预计上线时间: 4月30日19:00", result)
        self.assertIn("- 关联项目流程链接: [需求]【新增】智能识别订单类职位时效性标签", result)

    def test_removes_empty_placeholder_list_items(self) -> None:
        source = "- “ ”\n- 合法内容\n"

        result = post_process_markdown(source)

        self.assertNotIn("- “ ”", result)
        self.assertIn("- 合法内容", result)

    def test_normalizes_mixed_bullet_markers(self) -> None:
        source = "• 建议目标参考SMART原则\n- ▪ IM聊天记录(如B端回复明天下午开工)\n"

        result = post_process_markdown(source)

        self.assertIn("- 建议目标参考SMART原则", result)
        self.assertIn("- IM聊天记录(如B端回复明天下午开工)", result)

    def test_collapses_duplicate_single_value_table_rows(self) -> None:
        source = (
            "|•<br>有效期覆盖节假日期间|•<br>有效期覆盖节假日期间|•<br>有效期覆盖节假日期间|\n"
            "|---|---|---|\n"
            "|•<br>或职位内容中包含节假日关键词||| \n"
        )

        result = post_process_markdown(source)

        self.assertIn("|有效期覆盖节假日期间|", result)
        self.assertIn("|---|", result)
        self.assertIn("|或职位内容中包含节假日关键词|", result)
        self.assertNotIn("|•<br>有效期覆盖节假日期间|•<br>有效期覆盖节假日期间|", result)

    def test_trims_trailing_empty_table_columns(self) -> None:
        source = (
            "|动作|说明||\n"
            "|专区搭建|说明内容||\n"
            "|⻛险与应对|||\n"
        )

        result = post_process_markdown(source)

        self.assertIn("|动作|说明|", result)
        self.assertIn("|专区搭建|说明内容|", result)
        self.assertIn("|⻛险与应对|", result)
        self.assertNotIn("|动作|说明||", result)

    def test_keeps_table_separator_width_for_multi_column_tables(self) -> None:
        source = (
            "|人群|执行动作|动作说明|AB实验|\n"
            "|---|---|---|---|\n"
            "|A|B|C|D|\n"
        )

        result = post_process_markdown(source)

        self.assertIn("|人群|执行动作|动作说明|AB实验|", result)
        self.assertIn("|---|---|---|---|", result)
        self.assertIn("|A|B|C|D|", result)

    def test_repairs_missing_table_header_separator(self) -> None:
        source = (
            "|动作|说明|\n"
            "|专区搭建|说明内容|\n"
        )

        result = post_process_markdown(source)

        self.assertIn("|动作|说明|", result)
        self.assertIn("|---|---|", result)
        self.assertIn("|专区搭建|说明内容|", result)

    def test_repairs_risk_table_with_empty_middle_columns(self) -> None:
        source = (
            "|风险与应对|\n"
            "|风险||应对方案|\n"
            "|职位量不足||扩大筛选范围|\n"
        )

        result = post_process_markdown(source)

        self.assertNotIn("|风险与应对|", result)
        self.assertIn("|风险|应对方案|", result)
        self.assertIn("|---|---|", result)
        self.assertIn("|职位量不足|扩大筛选范围|", result)

    def test_expands_inline_bullet_markers_into_lists(self) -> None:
        source = (
            "- 阶段二:说明文字 ▪ 若已招满,则关闭 • ps:先做人审 ◦ 阶段三:继续处理 ▪ 若在招,则开启职位\n"
        )

        result = post_process_markdown(source)

        self.assertIn("- 阶段二:说明文字", result)
        self.assertIn("  - 若已招满,则关闭", result)
        self.assertIn("  - ps:先做人审", result)
        self.assertIn("- 阶段三:继续处理", result)
        self.assertIn("  - 若在招,则开启职位", result)

    def test_removes_placeholder_section_items(self) -> None:
        source = "- 具体内容\n- 风险与应对\n- 正常条目\n"

        result = post_process_markdown(source)

        self.assertNotIn("- 具体内容", result)
        self.assertNotIn("- 风险与应对", result)
        self.assertIn("- 正常条目", result)


class DoclingInputPreparationTests(unittest.TestCase):
    def test_copies_non_ascii_pdf_to_ascii_temp_path(self) -> None:
        with make_tempdir() as tmpdir:
            original = Path(tmpdir) / "中文文件.pdf"
            original.write_bytes(b"%PDF-1.4 fake")

            with prepare_docling_input(original) as prepared:
                self.assertNotEqual(prepared, original)
                self.assertTrue(prepared.exists())
                self.assertEqual(prepared.read_bytes(), original.read_bytes())
                self.assertTrue(prepared.name.isascii())


if __name__ == "__main__":
    unittest.main()
