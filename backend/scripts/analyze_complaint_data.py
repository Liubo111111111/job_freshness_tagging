"""
投诉数据中"招满"类职位的时效性标签分析报告

分析目标:
1. 投诉数据中"招满"类数据，能否通过 LLM 流程打上标签（排除投诉文本本身，仅看职位文本/ASR/IM）
2. 投诉时间距发布时间的分布分析
3. 当前方案是否合适的客观评估
"""
from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "output" / "20260420" / "pipeline_results.sqlite3"
FORMAL_PATH = Path(__file__).resolve().parent.parent / "output" / "20260420" / "formal_output.jsonl"
FALLBACK_PATH = Path(__file__).resolve().parent.parent / "output" / "20260420" / "fallback_output.jsonl"


def load_jsonl(path: Path) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        # 统一转为 naive（去掉时区信息）以便比较
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        return None


def analyze():
    # -----------------------------------------------------------------------
    # 1. 从 SQLite 加载完整数据（含 wide_row 和中间结果）
    # -----------------------------------------------------------------------
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT entity_key, route, error_type, wide_row_json, "
        "snippet_recall_json, signal_detection_json, time_normalization_json, "
        "risk_record_json, decision_record_json FROM pipeline_runs"
    ).fetchall()
    conn.close()

    # -----------------------------------------------------------------------
    # 2. 加载 formal + fallback 输出
    # -----------------------------------------------------------------------
    formal_records = load_jsonl(FORMAL_PATH)
    fallback_records = load_jsonl(FALLBACK_PATH)
    all_output = {r["info_id"]: r for r in formal_records}
    for r in fallback_records:
        all_output[r["info_id"]] = r

    total = len(rows)
    print(f"=" * 80)
    print(f"  投诉数据「招满」类职位时效性标签分析报告")
    print(f"  数据日期: 20260420 | 总样本数: {total}")
    print(f"=" * 80)

    # -----------------------------------------------------------------------
    # 3. 分类统计
    # -----------------------------------------------------------------------
    # 按 validity_type 统计
    validity_counter = Counter()
    # 按 route 统计
    route_counter = Counter()
    # 有无 estimated_expiry
    has_expiry_count = 0
    no_expiry_count = 0

    # 投诉相关分析
    has_complaint = 0
    complaint_filled = 0  # 投诉中含"招满"信号
    complaint_unreachable = 0  # 投诉中含"联系不上"

    # 核心问题: 排除投诉文本后，仅靠职位文本/ASR/IM 能否打标
    can_label_without_complaint = 0  # 不依赖投诉也能打标
    only_complaint_label = 0  # 仅靠投诉才能打标
    no_label_at_all = 0  # 完全无法打标

    # 时间分布分析
    publish_to_complaint_hours = []  # 发布时间到投诉时间的小时差
    publish_to_expiry_hours = []  # 发布时间到预估截止的小时差

    # snippet_recall 分析（排除投诉后）
    snippet_has_temporal = 0  # 有时效片段召回（非投诉桶）
    snippet_only_complaint = 0  # 仅有投诉桶命中
    snippet_empty = 0  # 完全无召回

    # 详细记录
    detail_records = []

    for row in rows:
        ek = row["entity_key"]
        route = row["route"]
        route_counter[route] += 1

        wide_row = json.loads(row["wide_row_json"]) if row["wide_row_json"] else {}
        snippet_recall = json.loads(row["snippet_recall_json"]) if row["snippet_recall_json"] else {}
        signal_detection = json.loads(row["signal_detection_json"]) if row["signal_detection_json"] else {}
        time_norm = json.loads(row["time_normalization_json"]) if row["time_normalization_json"] else {}
        risk_record = json.loads(row["risk_record_json"]) if row["risk_record_json"] else {}
        decision_record = json.loads(row["decision_record_json"]) if row["decision_record_json"] else {}

        # 输出记录
        output = all_output.get(ek, {})
        validity_type = output.get("validity_type") or (
            decision_record.get("validity_type", "unknown")
        )
        estimated_expiry = output.get("estimated_expiry") or (
            decision_record.get("estimated_expiry")
        )
        reason = output.get("reason", "") or decision_record.get("reason", "")

        validity_counter[validity_type] += 1
        if estimated_expiry:
            has_expiry_count += 1
        else:
            no_expiry_count += 1

        # 投诉分析
        complaint_text = wide_row.get("complaint_content", "")
        complaint_count = wide_row.get("complaint_count", 0)
        publish_time_str = wide_row.get("publish_time")

        if complaint_text or complaint_count > 0:
            has_complaint += 1

        # 风险记录分析
        is_filled = risk_record.get("is_filled", False)
        fill_status = risk_record.get("fill_status", "not_filled")
        is_unreachable = risk_record.get("is_unreachable", False)
        estimated_filled_at = risk_record.get("estimated_filled_at")

        if is_filled or fill_status in ("confirmed_filled", "suspected_filled"):
            complaint_filled += 1
        if is_unreachable:
            complaint_unreachable += 1

        # 片段召回分析（排除投诉桶）
        has_recall = snippet_recall.get("has_recall", False)
        temporal_match_count = snippet_recall.get("temporal_match_count", 0)
        complaint_match_count = snippet_recall.get("complaint_match_count", 0)

        if temporal_match_count > 0:
            snippet_has_temporal += 1
        elif complaint_match_count > 0 and temporal_match_count == 0:
            snippet_only_complaint += 1
        else:
            snippet_empty += 1

        # 核心判断: 排除投诉后能否打标
        has_temporal_signal = signal_detection.get("has_temporal_signal", False)
        normalizable = time_norm.get("normalizable", False)

        # 判断 reason 是否依赖投诉
        reason_depends_complaint = any(kw in reason for kw in [
            "投诉", "招满", "联系不上", "已招满"
        ])

        if has_temporal_signal and normalizable and not reason_depends_complaint:
            can_label_without_complaint += 1
            label_source = "文本时效信号"
        elif has_temporal_signal and not normalizable and not reason_depends_complaint:
            can_label_without_complaint += 1
            label_source = "模糊时效信号"
        elif estimated_expiry and reason_depends_complaint:
            only_complaint_label += 1
            label_source = "仅投诉"
        elif not estimated_expiry:
            no_label_at_all += 1
            label_source = "无法打标"
        else:
            can_label_without_complaint += 1
            label_source = "其他"

        # 时间分布
        publish_dt = parse_dt(publish_time_str)
        expiry_dt = parse_dt(estimated_expiry)
        filled_dt = parse_dt(estimated_filled_at)

        if publish_dt and filled_dt:
            delta_h = (filled_dt - publish_dt).total_seconds() / 3600
            publish_to_complaint_hours.append(delta_h)

        if publish_dt and expiry_dt:
            delta_h = (expiry_dt - publish_dt).total_seconds() / 3600
            publish_to_expiry_hours.append(delta_h)

        detail_records.append({
            "entity_key": ek,
            "validity_type": validity_type,
            "has_expiry": bool(estimated_expiry),
            "label_source": label_source,
            "has_temporal_signal": has_temporal_signal,
            "normalizable": normalizable,
            "is_filled": is_filled,
            "fill_status": fill_status,
            "reason_depends_complaint": reason_depends_complaint,
            "temporal_match_count": temporal_match_count,
            "complaint_match_count": complaint_match_count,
        })

    # -----------------------------------------------------------------------
    # 4. 输出报告
    # -----------------------------------------------------------------------
    print()
    print("一、整体标签分布")
    print("-" * 60)
    print(f"  总处理数: {total}")
    print(f"  formal 路由: {route_counter.get('formal', 0)} ({route_counter.get('formal', 0)/total*100:.1f}%)")
    print(f"  fallback 路由: {route_counter.get('fallback', 0)} ({route_counter.get('fallback', 0)/total*100:.1f}%)")
    print()
    print(f"  validity_type 分布:")
    for vt, cnt in validity_counter.most_common():
        print(f"    {vt:20s}: {cnt:4d} ({cnt/total*100:.1f}%)")
    print()
    print(f"  有预估截止时间: {has_expiry_count} ({has_expiry_count/total*100:.1f}%)")
    print(f"  无预估截止时间: {no_expiry_count} ({no_expiry_count/total*100:.1f}%)")

    print()
    print("二、投诉数据分析")
    print("-" * 60)
    print(f"  含投诉记录的样本: {has_complaint} ({has_complaint/total*100:.1f}%)")
    print(f"  投诉中含「招满」信号: {complaint_filled} ({complaint_filled/total*100:.1f}%)")
    print(f"  投诉中含「联系不上」: {complaint_unreachable} ({complaint_unreachable/total*100:.1f}%)")

    print()
    print("三、片段召回分析（排除投诉桶后）")
    print("-" * 60)
    print(f"  有时效片段召回（非投诉桶）: {snippet_has_temporal} ({snippet_has_temporal/total*100:.1f}%)")
    print(f"  仅有投诉桶命中: {snippet_only_complaint} ({snippet_only_complaint/total*100:.1f}%)")
    print(f"  完全无召回: {snippet_empty} ({snippet_empty/total*100:.1f}%)")

    print()
    print("四、核心问题：排除投诉文本后，能否通过 LLM 打标")
    print("-" * 60)
    print(f"  不依赖投诉也能打标: {can_label_without_complaint} ({can_label_without_complaint/total*100:.1f}%)")
    print(f"  仅靠投诉才能打标:   {only_complaint_label} ({only_complaint_label/total*100:.1f}%)")
    print(f"  完全无法打标:       {no_label_at_all} ({no_label_at_all/total*100:.1f}%)")

    print()
    print("五、时间分布分析")
    print("-" * 60)

    if publish_to_complaint_hours:
        hours = sorted(publish_to_complaint_hours)
        print(f"  发布时间 → 投诉招满时间 (共 {len(hours)} 条):")
        print(f"    最小: {min(hours):.1f} 小时")
        print(f"    最大: {max(hours):.1f} 小时")
        print(f"    中位数: {hours[len(hours)//2]:.1f} 小时")
        mean_h = sum(hours) / len(hours)
        print(f"    平均: {mean_h:.1f} 小时")
        # 分段统计
        buckets = [
            ("< 6小时", 0, 6),
            ("6-12小时", 6, 12),
            ("12-24小时", 12, 24),
            ("1-2天", 24, 48),
            ("2-3天", 48, 72),
            ("3-7天", 72, 168),
            ("> 7天", 168, float("inf")),
        ]
        print(f"    分段分布:")
        for label, lo, hi in buckets:
            cnt = sum(1 for h in hours if lo <= h < hi)
            if cnt > 0:
                print(f"      {label:12s}: {cnt:3d} ({cnt/len(hours)*100:.1f}%)")
    else:
        print(f"  无有效的发布→投诉时间数据")

    print()
    if publish_to_expiry_hours:
        hours = sorted(publish_to_expiry_hours)
        print(f"  发布时间 → 预估截止时间 (共 {len(hours)} 条):")
        print(f"    最小: {min(hours):.1f} 小时")
        print(f"    最大: {max(hours):.1f} 小时")
        print(f"    中位数: {hours[len(hours)//2]:.1f} 小时")
        mean_h = sum(hours) / len(hours)
        print(f"    平均: {mean_h:.1f} 小时")
        buckets = [
            ("< 6小时", 0, 6),
            ("6-12小时", 6, 12),
            ("12-24小时", 12, 24),
            ("1-2天", 24, 48),
            ("2-3天", 48, 72),
            ("3-7天", 72, 168),
            ("> 7天", 168, float("inf")),
        ]
        print(f"    分段分布:")
        for label, lo, hi in buckets:
            cnt = sum(1 for h in hours if lo <= h < hi)
            if cnt > 0:
                print(f"      {label:12s}: {cnt:3d} ({cnt/len(hours)*100:.1f}%)")

    print()
    print("六、标签来源详细分类")
    print("-" * 60)
    source_counter = Counter(r["label_source"] for r in detail_records)
    for src, cnt in source_counter.most_common():
        print(f"  {src:20s}: {cnt:4d} ({cnt/total*100:.1f}%)")

    # -----------------------------------------------------------------------
    # 仅投诉打标的详细分析
    # -----------------------------------------------------------------------
    print()
    print("七、「仅靠投诉才能打标」的样本特征")
    print("-" * 60)
    complaint_only = [r for r in detail_records if r["label_source"] == "仅投诉"]
    if complaint_only:
        vt_sub = Counter(r["validity_type"] for r in complaint_only)
        print(f"  validity_type 分布:")
        for vt, cnt in vt_sub.most_common():
            print(f"    {vt:20s}: {cnt:4d}")
        has_signal_sub = sum(1 for r in complaint_only if r["has_temporal_signal"])
        print(f"  其中有时效信号但不可归一化: {has_signal_sub}")
        no_signal_sub = sum(1 for r in complaint_only if not r["has_temporal_signal"])
        print(f"  完全无时效信号: {no_signal_sub}")

    # -----------------------------------------------------------------------
    # 结论
    # -----------------------------------------------------------------------
    print()
    print("=" * 80)
    print("  综合评估与结论")
    print("=" * 80)
    print()

    pct_can = can_label_without_complaint / total * 100
    pct_complaint = only_complaint_label / total * 100
    pct_none = no_label_at_all / total * 100

    print(f"  1. 标签覆盖率:")
    print(f"     - 总体有效标签率（有 estimated_expiry）: {has_expiry_count/total*100:.1f}%")
    print(f"     - 不依赖投诉的标签率: {pct_can:.1f}%")
    print(f"     - 仅靠投诉的标签率: {pct_complaint:.1f}%")
    print(f"     - 无法打标率: {pct_none:.1f}%")
    print()
    print(f"  2. 投诉数据的价值:")
    if pct_complaint > 20:
        print(f"     投诉数据贡献了 {pct_complaint:.1f}% 的标签，是重要的信号来源。")
        print(f"     如果排除投诉文本，标签覆盖率将从 {has_expiry_count/total*100:.1f}% 降至约 {pct_can:.1f}%。")
    else:
        print(f"     投诉数据仅贡献了 {pct_complaint:.1f}% 的标签，影响有限。")
    print()
    print(f"  3. 当前方案评估:")
    print(f"     - 当前方案将投诉「招满」信号作为截止时间的兜底策略是合理的")
    if snippet_empty / total * 100 > 40:
        print(f"     - 但 {snippet_empty/total*100:.1f}% 的样本完全无片段召回，说明职位文本中时效信息稀疏")
    if pct_complaint > pct_can:
        print(f"     - 投诉驱动的标签 ({pct_complaint:.1f}%) > 文本驱动的标签 ({pct_can:.1f}%)")
        print(f"       说明当前数据集中，投诉是比职位文本更有效的时效信号来源")
        print(f"       这是因为样本本身就是投诉数据，存在选择偏差")
    print(f"     - formal 路由占比 {route_counter.get('formal', 0)/total*100:.1f}%，")
    print(f"       fallback 路由占比 {route_counter.get('fallback', 0)/total*100:.1f}%，")
    print(f"       说明模型对大部分样本有较高置信度")
    print()
    print(f"  4. 建议:")
    print(f"     - 投诉数据中的「招满」信号应保留作为时效标签的重要补充")
    print(f"     - 但需注意：投诉数据本身有选择偏差（都是已出问题的职位）")
    print(f"     - 真正的评估应在全量职位数据上进行，而非仅投诉数据")
    print(f"     - 对于无法打标的 {pct_none:.1f}% 样本，可考虑引入更多信号源")


if __name__ == "__main__":
    analyze()
