-- 从 ads_freshness_candidates 宽表读取已聚合的候选数据
-- 建表/写入逻辑见 fetch_freshness_candidates.sql（调度任务用）
-- 本 SQL 仅用于流水线运行时读取

SELECT
    user_id,
    info_id,
    job_detail,
    occupation_id,
    sub_id,
    asr_result,
    im_text,
    complaint_content,
    im_message_count,
    call_record_count,
    complaint_count,
    publish_time
FROM yuapo_dev.ads_freshness_candidates
WHERE pt = '${bizdate}'
