"""
投诉时间与发布时间间隔分布 — 可视化图表
生成多张子图，保存为 PNG
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import rcParams

# 中文字体
rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
rcParams["axes.unicode_minus"] = False

DB_PATH = Path(__file__).resolve().parent.parent / "output" / "20260420" / "pipeline_results.sqlite3"
OUT_DIR = Path(__file__).resolve().parent.parent / "output" / "20260420"

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
    dts = []
    for m in _COMPLAINT_TS_RE.finditer(text):
        dt = parse_dt(m.group(1))
        if dt:
            dts.append(dt)
    return dts


def load_data():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT entity_key, route, wide_row_json, risk_record_json, "
        "snippet_recall_json, signal_detection_json, time_normalization_json, "
        "decision_record_json FROM pipeline_runs"
    ).fetchall()
    conn.close()

    all_complaint_deltas = []
    first_complaint_deltas = []
    filled_complaint_deltas = []
    filled_first_deltas = []
    not_filled_first_deltas = []

    # 打标分类
    cat_counts = Counter()

    for row in rows:
        wr = json.loads(row["wide_row_json"]) if row["wide_row_json"] else {}
        rr = json.loads(row["risk_record_json"]) if row["risk_record_json"] else {}
        sd = json.loads(row["signal_detection_json"]) if row["signal_detection_json"] else {}
        tn = json.loads(row["time_normalization_json"]) if row["time_normalization_json"] else {}
        dr = json.loads(row["decision_record_json"]) if row["decision_record_json"] else {}

        publish_time = parse_dt(wr.get("publish_time"))
        complaint_text = wr.get("complaint_content", "")
        is_filled = rr.get("is_filled", False)
        estimated_filled_at = parse_dt(rr.get("estimated_filled_at"))
        complaint_dts = extract_complaint_timestamps(complaint_text)

        if publish_time and complaint_dts:
            for cdt in complaint_dts:
                delta_h = (cdt - publish_time).total_seconds() / 3600
                all_complaint_deltas.append(delta_h)
            first_dt = min(complaint_dts)
            delta_h = (first_dt - publish_time).total_seconds() / 3600
            first_complaint_deltas.append(delta_h)
            if is_filled:
                filled_first_deltas.append(delta_h)
            else:
                not_filled_first_deltas.append(delta_h)

        if publish_time and estimated_filled_at:
            delta_h = (estimated_filled_at - publish_time).total_seconds() / 3600
            filled_complaint_deltas.append(delta_h)

        # 打标分类
        has_temporal = sd.get("has_temporal_signal", False)
        normalizable = tn.get("normalizable", False)
        reason = dr.get("reason", "")
        estimated_expiry = dr.get("estimated_expiry")
        reason_uses_complaint = any(kw in reason for kw in ["投诉", "招满", "联系不上", "已招满"])

        if has_temporal and normalizable and not reason_uses_complaint:
            cat_counts["A. 纯文本打标"] += 1
        elif has_temporal and not normalizable and not reason_uses_complaint:
            cat_counts["B. 文本信号(模糊)"] += 1
        elif estimated_expiry and reason_uses_complaint:
            cat_counts["C. 投诉兜底打标"] += 1
        elif not has_temporal and is_filled:
            cat_counts["E. 无信号+有投诉"] += 1
        elif not has_temporal and not is_filled and not estimated_expiry:
            cat_counts["D. 无信号无投诉"] += 1
        else:
            cat_counts["F. 其他"] += 1

    # signal_type
    signal_types = Counter()
    validity_types = Counter()
    for row in rows:
        sd = json.loads(row["signal_detection_json"]) if row["signal_detection_json"] else {}
        dr = json.loads(row["decision_record_json"]) if row["decision_record_json"] else {}
        signal_types[sd.get("signal_type", "no_signal")] += 1
        validity_types[dr.get("validity_type", "unknown")] += 1

    return {
        "all_complaint_deltas": all_complaint_deltas,
        "first_complaint_deltas": first_complaint_deltas,
        "filled_complaint_deltas": filled_complaint_deltas,
        "filled_first_deltas": filled_first_deltas,
        "not_filled_first_deltas": not_filled_first_deltas,
        "cat_counts": cat_counts,
        "signal_types": signal_types,
        "validity_types": validity_types,
        "total": len(rows),
    }


def plot_all(data: dict):
    fig, axes = plt.subplots(3, 2, figsize=(18, 22))
    fig.suptitle("投诉数据「招满」类职位时效性标签分析\n数据日期: 20260420 | 总样本: {}".format(data["total"]),
                 fontsize=16, fontweight="bold", y=0.98)
    plt.subplots_adjust(hspace=0.38, wspace=0.28, top=0.93, bottom=0.05)

    colors_blue = "#4C72B0"
    colors_orange = "#DD8452"
    colors_green = "#55A868"

    # ---------------------------------------------------------------
    # 图1: 所有投诉时间 vs 发布时间间隔
    # ---------------------------------------------------------------
    ax = axes[0, 0]
    hrs = [h for h in data["all_complaint_deltas"] if -5 <= h <= 150]
    bins = list(range(-5, 155, 2))
    ax.hist(hrs, bins=bins, color=colors_blue, edgecolor="white", alpha=0.85)
    median_v = sorted(data["all_complaint_deltas"])[len(data["all_complaint_deltas"]) // 2]
    ax.axvline(median_v, color="red", linestyle="--", linewidth=1.5, label=f"中位数 {median_v:.1f}h")
    ax.set_xlabel("间隔（小时）", fontsize=11)
    ax.set_ylabel("投诉条数", fontsize=11)
    ax.set_title("① 所有投诉时间 vs 发布时间间隔\n（每条投诉单独计算, n={}）".format(len(data["all_complaint_deltas"])),
                 fontsize=12)
    ax.legend(fontsize=10)
    ax.set_xlim(-5, 80)

    # ---------------------------------------------------------------
    # 图2: 第一条投诉时间 vs 发布时间间隔
    # ---------------------------------------------------------------
    ax = axes[0, 1]
    hrs = [h for h in data["first_complaint_deltas"] if -25 <= h <= 150]
    bins = list(range(-25, 130, 2))
    ax.hist(hrs, bins=bins, color=colors_green, edgecolor="white", alpha=0.85)
    median_v = sorted(data["first_complaint_deltas"])[len(data["first_complaint_deltas"]) // 2]
    ax.axvline(median_v, color="red", linestyle="--", linewidth=1.5, label=f"中位数 {median_v:.1f}h")
    ax.set_xlabel("间隔（小时）", fontsize=11)
    ax.set_ylabel("职位数", fontsize=11)
    ax.set_title("② 第一条投诉 vs 发布时间间隔\n（每个职位取最早投诉, n={}）".format(len(data["first_complaint_deltas"])),
                 fontsize=12)
    ax.legend(fontsize=10)
    ax.set_xlim(-25, 80)

    # ---------------------------------------------------------------
    # 图3: 招满投诉时间 vs 发布时间间隔
    # ---------------------------------------------------------------
    ax = axes[1, 0]
    hrs = [h for h in data["filled_complaint_deltas"] if 0 <= h <= 150]
    bins = list(range(0, 130, 2))
    ax.hist(hrs, bins=bins, color=colors_orange, edgecolor="white", alpha=0.85)
    median_v = sorted(data["filled_complaint_deltas"])[len(data["filled_complaint_deltas"]) // 2]
    ax.axvline(median_v, color="red", linestyle="--", linewidth=1.5, label=f"中位数 {median_v:.1f}h")
    ax.set_xlabel("间隔（小时）", fontsize=11)
    ax.set_ylabel("职位数", fontsize=11)
    ax.set_title("③ 招满投诉时间 vs 发布时间间隔\n（estimated_filled_at 有值, n={}）".format(
        len(data["filled_complaint_deltas"])), fontsize=12)
    ax.legend(fontsize=10)
    ax.set_xlim(0, 80)

    # ---------------------------------------------------------------
    # 图4: 招满 vs 非招满 第一条投诉时间差对比
    # ---------------------------------------------------------------
    ax = axes[1, 1]
    bins = list(range(-25, 130, 3))
    ax.hist([h for h in data["filled_first_deltas"] if -25 <= h <= 130],
            bins=bins, color=colors_orange, edgecolor="white", alpha=0.7,
            label=f"招满 (n={len(data['filled_first_deltas'])})")
    ax.hist([h for h in data["not_filled_first_deltas"] if -25 <= h <= 130],
            bins=bins, color=colors_blue, edgecolor="white", alpha=0.5,
            label=f"非招满 (n={len(data['not_filled_first_deltas'])})")
    ax.set_xlabel("间隔（小时）", fontsize=11)
    ax.set_ylabel("职位数", fontsize=11)
    ax.set_title("④ 招满 vs 非招满：第一条投诉时间差对比", fontsize=12)
    ax.legend(fontsize=10)
    ax.set_xlim(-25, 80)

    # ---------------------------------------------------------------
    # 图5: 排除投诉后 LLM 打标能力饼图
    # ---------------------------------------------------------------
    ax = axes[2, 0]
    cat = data["cat_counts"]
    labels = []
    sizes = []
    pie_colors = ["#55A868", "#8DC77B", "#DD8452", "#C44E52", "#8172B2", "#CCB974"]
    order = ["A. 纯文本打标", "B. 文本信号(模糊)", "C. 投诉兜底打标",
             "D. 无信号无投诉", "E. 无信号+有投诉", "F. 其他"]
    for o in order:
        if cat.get(o, 0) > 0:
            labels.append(f"{o}\n({cat[o]}, {cat[o]/data['total']*100:.1f}%)")
            sizes.append(cat[o])
    wedges, texts = ax.pie(sizes, labels=labels, colors=pie_colors[:len(sizes)],
                           startangle=90, textprops={"fontsize": 9})
    ax.set_title("⑤ 排除投诉后 LLM 打标能力分类", fontsize=12)

    # ---------------------------------------------------------------
    # 图6: 分段柱状图 — 投诉时间间隔分段统计
    # ---------------------------------------------------------------
    ax = axes[2, 1]
    bucket_labels = ["<1h", "1-2h", "2-3h", "3-6h", "6-12h", "12-24h", "1-2天", "2-3天", "3-7天", ">7天"]
    bucket_ranges = [(0, 1), (1, 2), (2, 3), (3, 6), (6, 12), (12, 24), (24, 48), (48, 72), (72, 168), (168, 9999)]

    def count_buckets(hours):
        counts = []
        for lo, hi in bucket_ranges:
            counts.append(sum(1 for h in hours if lo <= h < hi))
        return counts

    first_counts = count_buckets(data["first_complaint_deltas"])
    filled_counts = count_buckets(data["filled_complaint_deltas"])

    x = range(len(bucket_labels))
    width = 0.35
    bars1 = ax.bar([i - width/2 for i in x], first_counts, width, label="第一条投诉", color=colors_blue, alpha=0.85)
    bars2 = ax.bar([i + width/2 for i in x], filled_counts, width, label="招满投诉", color=colors_orange, alpha=0.85)
    ax.set_xticks(list(x))
    ax.set_xticklabels(bucket_labels, fontsize=9, rotation=30)
    ax.set_ylabel("职位数", fontsize=11)
    ax.set_title("⑥ 投诉时间间隔分段统计（对比）", fontsize=12)
    ax.legend(fontsize=10)

    # 柱顶标注
    for bar in bars1:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width()/2, h + 0.5, str(int(h)),
                    ha="center", va="bottom", fontsize=8)
    for bar in bars2:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width()/2, h + 0.5, str(int(h)),
                    ha="center", va="bottom", fontsize=8)

    out_path = OUT_DIR / "complaint_timing_analysis.png"
    fig.savefig(str(out_path), dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"图表已保存: {out_path}")


if __name__ == "__main__":
    data = load_data()
    plot_all(data)
