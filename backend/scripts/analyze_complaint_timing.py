"""
投诉时间与发布时间间隔分布的详细分析

重点分析:
1. 每条投诉的时间戳与发布时间的间隔（小时）
2. 招满投诉时间与发布时间的间隔
3. 按小时粒度的分布直方图
4. 排除投诉文本后 LLM 打标能力评估
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "output" / "20260420" / "pipeline_results.sqlite3"

# 从投诉文本中提取所有投诉时间戳
_COMPLAINT_TS_RE = re.compile(r"【投诉\d+\s+([\d-]+\s+[\d:]+)】")


def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def extract_complaint_timestamps(text: str) -> list[datetime]:
    """从投诉文本中提取所有时间戳"""
    dts = []
    for m in _COMPLAINT_TS_RE.finditer(text):
        dt = parse_dt(m.group(1))
        if dt:
            dts.append(dt)
    return dts


def hour_bucket(h: float) -> str:
    if h < 0:
        return "< 0h (异常)"
    if h < 1:
        return "0-1h"
    if h < 2:
        return "1-2h"
    if h < 3:
        return "2-3h"
    if h < 6:
        return "3-6h"
    if h < 12:
        return "6-12h"
    if h < 24:
        return "12-24h"
    if h < 48:
        return "1-2天"
    if h < 72:
        return "2-3天"
    if h < 168:
        return "3-7天"
    return "> 7天"


BUCKET_ORDER = [
    "< 0h (异常)", "0-1h", "1-2h", "2-3h", "3-6h", "6-12h",
    "12-24h", "1-2天", "2-3天", "3-7天", "> 7天",
]


def print_histogram(hours: list[float], title: str, total_ref: int | None = None):
    """打印小时分布直方图"""
    if not hours:
        print(f"  无数据")
        return
    ref = total_ref or len(hours)
    counter = Counter(hour_bucket(h) for h in hours)
    print(f"  样本数: {len(hours)}")
    print(f"  最小: {min(hours):.2f}h | 最大: {max(hours):.2f}h | 中位数: {sorted(hours)[len(hours)//2]:.2f}h | 平均: {sum(hours)/len(hours):.2f}h")
    print()
    max_bar = 50
    max_cnt = max(counter.values()) if counter else 1
    for bucket in BUCKET_ORDER:
        cnt = counter.get(bucket, 0)
        if cnt == 0:
            continue
        bar_len = int(cnt / max_cnt * max_bar)
        bar = "█" * bar_len
        print(f"    {bucket:14s} | {bar:50s} {cnt:4d} ({cnt/len(hours)*100:5.1f}%)")
    print()


def analyze():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT entity_key, route, wide_row_json, risk_record_json, "
        "snippet_recall_json, signal_detection_json, time_normalization_json, "
        "decision_record_json FROM pipeline_runs"
    ).fetchall()
    conn.close()

    total = len(rows)

    # 收集各类时间差
    all_complaint_deltas = []        # 所有投诉时间 - 发布时间
    first_complaint_deltas = []      # 第一条投诉时间 - 发布时间
    filled_complaint_deltas = []     # 招满投诉时间 - 发布时间
    complaint_counts_per_job = []    # 每个职位的投诉条数

    # 按投诉条数分组的招满率
    complaint_count_filled = Counter()  # {投诉条数: 招满数}
    complaint_count_total = Counter()   # {投诉条数: 总数}

    # 招满 vs 非招满的时间差对比
    filled_first_deltas = []         # 招满样本的第一条投诉时间差
    not_filled_first_deltas = []     # 非招满样本的第一条投诉时间差

    # 排除投诉后的打标分析（更细致）
    label_analysis = []

    for row in rows:
        ek = row["entity_key"]
        wr = json.loads(row["wide_row_json"]) if row["wide_row_json"] else {}
        rr = json.loads(row["risk_record_json"]) if row["risk_record_json"] else {}
        sr = json.loads(row["snippet_recall_json"]) if row["snippet_recall_json"] else {}
        sd = json.loads(row["signal_detection_json"]) if row["signal_detection_json"] else {}
        tn = json.loads(row["time_normalization_json"]) if row["time_normalization_json"] else {}
        dr = json.loads(row["decision_record_json"]) if row["decision_record_json"] else {}

        publish_time = parse_dt(wr.get("publish_time"))
        complaint_text = wr.get("complaint_content", "")
        complaint_count = wr.get("complaint_count", 0)
        is_filled = rr.get("is_filled", False)
        estimated_filled_at = parse_dt(rr.get("estimated_filled_at"))

        # 提取投诉时间戳
        complaint_dts = extract_complaint_timestamps(complaint_text)
        complaint_counts_per_job.append(len(complaint_dts))

        # 投诉条数分组
        complaint_count_total[len(complaint_dts)] += 1
        if is_filled:
            complaint_count_filled[len(complaint_dts)] += 1

        if publish_time and complaint_dts:
            # 所有投诉时间差
            for cdt in complaint_dts:
                delta_h = (cdt - publish_time).total_seconds() / 3600
                all_complaint_deltas.append(delta_h)

            # 第一条投诉时间差
            first_dt = min(complaint_dts)
            delta_h = (first_dt - publish_time).total_seconds() / 3600
            first_complaint_deltas.append(delta_h)

            if is_filled:
                filled_first_deltas.append(delta_h)
            else:
                not_filled_first_deltas.append(delta_h)

        # 招满投诉时间差
        if publish_time and estimated_filled_at:
            delta_h = (estimated_filled_at - publish_time).total_seconds() / 3600
            filled_complaint_deltas.append(delta_h)

        # 打标分析
        has_temporal = sd.get("has_temporal_signal", False)
        temporal_status = sd.get("temporal_status", "no_signal")
        signal_type = sd.get("signal_type", "no_signal")
        normalizable = tn.get("normalizable", False)
        recruitment_valid_until = tn.get("recruitment_valid_until")
        temporal_match_count = sr.get("temporal_match_count", 0)
        complaint_match_count = sr.get("complaint_match_count", 0)

        reason = dr.get("reason", "")
        validity_type = dr.get("validity_type", "unknown")
        estimated_expiry = dr.get("estimated_expiry")
        reason_uses_complaint = any(kw in reason for kw in ["投诉", "招满", "联系不上", "已招满"])

        label_analysis.append({
            "entity_key": ek,
            "has_temporal": has_temporal,
            "temporal_status": temporal_status,
            "signal_type": signal_type,
            "normalizable": normalizable,
            "has_rvt": bool(recruitment_valid_until),
            "temporal_match_count": temporal_match_count,
            "complaint_match_count": complaint_match_count,
            "is_filled": is_filled,
            "reason_uses_complaint": reason_uses_complaint,
            "validity_type": validity_type,
            "has_expiry": bool(estimated_expiry),
            "route": row["route"],
        })

    # ===================================================================
    # 输出报告
    # ===================================================================
    print("=" * 80)
    print("  投诉时间与发布时间间隔分布 · 详细分析报告")
    print(f"  数据日期: 20260420 | 总样本: {total}")
    print("=" * 80)

    print()
    print("一、每个职位的投诉条数分布")
    print("-" * 60)
    cc = Counter(complaint_counts_per_job)
    for n in sorted(cc.keys()):
        print(f"  {n} 条投诉: {cc[n]:4d} 个职位 ({cc[n]/total*100:.1f}%)")

    print()
    print("二、所有投诉时间 vs 发布时间间隔（每条投诉单独计算）")
    print("-" * 60)
    print_histogram(all_complaint_deltas, "所有投诉")

    print()
    print("三、第一条投诉时间 vs 发布时间间隔（每个职位取最早投诉）")
    print("-" * 60)
    print_histogram(first_complaint_deltas, "第一条投诉")

    print()
    print("四、招满投诉时间 vs 发布时间间隔（仅 estimated_filled_at 有值的样本）")
    print("-" * 60)
    print_histogram(filled_complaint_deltas, "招满投诉")

    print()
    print("五、招满 vs 非招满样本的第一条投诉时间差对比")
    print("-" * 60)
    print(f"  [招满样本] (n={len(filled_first_deltas)})")
    print_histogram(filled_first_deltas, "招满")
    print(f"  [非招满样本] (n={len(not_filled_first_deltas)})")
    print_histogram(not_filled_first_deltas, "非招满")

    print()
    print("六、按投诉条数分组的招满率")
    print("-" * 60)
    for n in sorted(complaint_count_total.keys()):
        t = complaint_count_total[n]
        f = complaint_count_filled.get(n, 0)
        pct = f / t * 100 if t else 0
        print(f"  {n} 条投诉: 总 {t:3d} | 招满 {f:3d} | 招满率 {pct:5.1f}%")

    # ===================================================================
    # 排除投诉后的 LLM 打标能力细分
    # ===================================================================
    print()
    print("=" * 80)
    print("  排除投诉文本后 LLM 打标能力详细分析")
    print("=" * 80)

    print()
    print("七、时效信号检测结果分布（signal_detection 节点）")
    print("-" * 60)
    ts_counter = Counter(r["temporal_status"] for r in label_analysis)
    for ts, cnt in ts_counter.most_common():
        print(f"  {ts:25s}: {cnt:4d} ({cnt/total*100:.1f}%)")

    print()
    print("八、信号类型分布（signal_type）")
    print("-" * 60)
    st_counter = Counter(r["signal_type"] for r in label_analysis)
    for st, cnt in st_counter.most_common():
        print(f"  {st:25s}: {cnt:4d} ({cnt/total*100:.1f}%)")

    print()
    print("九、时间归一化结果（time_normalization 节点）")
    print("-" * 60)
    has_temporal_total = sum(1 for r in label_analysis if r["has_temporal"])
    normalizable_total = sum(1 for r in label_analysis if r["normalizable"])
    has_rvt_total = sum(1 for r in label_analysis if r["has_rvt"])
    print(f"  有时效信号 (has_temporal_signal=true): {has_temporal_total} ({has_temporal_total/total*100:.1f}%)")
    print(f"  可归一化 (normalizable=true):          {normalizable_total} ({normalizable_total/total*100:.1f}%)")
    print(f"  有 recruitment_valid_until:             {has_rvt_total} ({has_rvt_total/total*100:.1f}%)")
    print()
    # 有信号但不可归一化的原因
    has_signal_not_norm = [r for r in label_analysis if r["has_temporal"] and not r["normalizable"]]
    print(f"  有信号但不可归一化: {len(has_signal_not_norm)} 条")
    if has_signal_not_norm:
        st_sub = Counter(r["signal_type"] for r in has_signal_not_norm)
        for st, cnt in st_sub.most_common():
            print(f"    {st:25s}: {cnt:4d}")

    print()
    print("十、排除投诉后的打标能力矩阵")
    print("-" * 60)
    print()
    print("  分类逻辑:")
    print("    A. 纯文本打标: 有时效信号 + 可归一化 + reason 不依赖投诉")
    print("    B. 文本信号但模糊: 有时效信号 + 不可归一化 + reason 不依赖投诉")
    print("    C. 投诉兜底打标: 有 expiry 但 reason 依赖投诉")
    print("    D. 无信号无投诉: 无时效信号 + 无投诉招满 → 无法打标")
    print("    E. 无信号有投诉: 无时效信号 + 投诉招满 → 仅投诉打标")
    print()

    cat_a = [r for r in label_analysis if r["has_temporal"] and r["normalizable"] and not r["reason_uses_complaint"]]
    cat_b = [r for r in label_analysis if r["has_temporal"] and not r["normalizable"] and not r["reason_uses_complaint"]]
    cat_c = [r for r in label_analysis if r["has_expiry"] and r["reason_uses_complaint"]]
    cat_d = [r for r in label_analysis if not r["has_temporal"] and not r["is_filled"] and not r["has_expiry"]]
    cat_e = [r for r in label_analysis if not r["has_temporal"] and r["is_filled"]]
    # 其他未分类
    classified = set()
    for cat in [cat_a, cat_b, cat_c, cat_d, cat_e]:
        for r in cat:
            classified.add(r["entity_key"])
    cat_other = [r for r in label_analysis if r["entity_key"] not in classified]

    cats = [
        ("A. 纯文本打标", cat_a),
        ("B. 文本信号但模糊", cat_b),
        ("C. 投诉兜底打标", cat_c),
        ("D. 无信号无投诉", cat_d),
        ("E. 无信号有投诉", cat_e),
        ("F. 其他", cat_other),
    ]
    for label, cat in cats:
        cnt = len(cat)
        print(f"  {label:25s}: {cnt:4d} ({cnt/total*100:.1f}%)")

    print()
    print("  → 如果完全排除投诉数据:")
    pure_text = len(cat_a) + len(cat_b)
    complaint_dep = len(cat_c) + len(cat_e)
    no_label = len(cat_d)
    print(f"    可打标（A+B）: {pure_text} ({pure_text/total*100:.1f}%)")
    print(f"    将丢失（C+E）: {complaint_dep} ({complaint_dep/total*100:.1f}%)")
    print(f"    本就无标（D）: {no_label} ({no_label/total*100:.1f}%)")

    # ===================================================================
    # 结论
    # ===================================================================
    print()
    print("=" * 80)
    print("  综合结论")
    print("=" * 80)
    print()
    print(f"  1. 投诉响应速度极快:")
    if first_complaint_deltas:
        median_h = sorted(first_complaint_deltas)[len(first_complaint_deltas)//2]
        pct_6h = sum(1 for h in first_complaint_deltas if h < 6) / len(first_complaint_deltas) * 100
        print(f"     第一条投诉中位数距发布仅 {median_h:.1f} 小时，{pct_6h:.0f}% 在 6 小时内")
    if filled_complaint_deltas:
        median_h = sorted(filled_complaint_deltas)[len(filled_complaint_deltas)//2]
        pct_6h = sum(1 for h in filled_complaint_deltas if h < 6) / len(filled_complaint_deltas) * 100
        print(f"     招满投诉中位数距发布 {median_h:.1f} 小时，{pct_6h:.0f}% 在 6 小时内")
    print()
    print(f"  2. LLM 纯文本打标能力有限:")
    print(f"     仅 {pure_text/total*100:.1f}% 可通过职位文本/ASR/IM 独立打标")
    print(f"     {complaint_dep/total*100:.1f}% 的标签依赖投诉信号")
    print()
    print(f"  3. 当前方案适用性:")
    print(f"     对于投诉数据集，投诉「招满」是最有效的时效信号")
    print(f"     但此结论不能推广到全量数据 — 投诉数据天然偏向短时效职位")
    print(f"     建议在非投诉数据上单独评估 LLM 打标覆盖率")


if __name__ == "__main__":
    analyze()
