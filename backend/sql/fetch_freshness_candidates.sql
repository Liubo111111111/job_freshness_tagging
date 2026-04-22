-- 职位新鲜度候选数据提取（按 info_id 聚合版）
-- 同一 info_id 的多条 IM 会话和多条 ASR 通话记录在 SQL 层聚合为一行
-- IM: "【会话1】\n...\n【会话2】\n..." 格式
-- ASR: "【通话1】\n...\n【通话2】\n..." 格式
-- 投诉: 多条用"；"分隔

-- 1. 建表
CREATE TABLE IF NOT EXISTS yuapo_dev.ads_freshness_candidates (
    user_id               STRING    COMMENT '用户ID',
    info_id               STRING    COMMENT '关联info_id',
    job_detail            STRING    COMMENT '招工详情',
    occupation_id         STRING    COMMENT '工种ID',
    sub_id                STRING    COMMENT '中间号（多个用逗号分隔）',
    asr_result            STRING    COMMENT 'ASR识别结果（多条通话用【通话N】分隔）',
    im_text               STRING    COMMENT 'IM聊天文本（多个会话用【会话N】分隔）',
    complaint_content     STRING    COMMENT '投诉内容（多条用；分隔）',
    im_message_count      BIGINT    COMMENT 'IM会话总次数',
    call_record_count     BIGINT    COMMENT '通话记录总次数',
    complaint_count       BIGINT    COMMENT '投诉总次数',
    publish_time          STRING    COMMENT '职位发布时间'
)
PARTITIONED BY (
    pt STRING COMMENT '日期分区 yyyyMMdd'
)
LIFECYCLE 120;

-- 2. 数据写入
INSERT OVERWRITE TABLE yuapo_dev.ads_freshness_candidates PARTITION (pt = '${bdp.system.bizdate}')

WITH users AS (
    SELECT om.id, COUNT(jc.id) AS jc_cnt
    FROM yuapo.ods_member om
    JOIN yuapo.ods_job_jc_gczdw jc ON jc.user_id = om.id
    WHERE om.pt = '20210807'
      AND jc.pt = '20210807'
      AND jc.is_check = 2
      AND TO_DATE(FROM_UNIXTIME(jc.add_time)) > TO_DATE(DATEADD(TO_DATE('${bdp.system.bizdate}', 'yyyyMMdd'), -7, 'dd'))
    GROUP BY om.id
    HAVING COUNT(jc.id) >= 1
),

-- ═══════════════════════════════════════════════════════════════════════════
-- ASR 通话路：每个 info_id 最多取 5 条通话，按时间排序后聚合
-- 输出格式: "【通话1】\nasr文本\n【通话2】\nasr文本\n..."
-- ═══════════════════════════════════════════════════════════════════════════

call_tel AS (
    SELECT
        gc.id AS info_id,
        gc.user_id,
        gc.detail,
        gc.occupations_v2,
        call_log.sub_id,
        log_media.asr_result,
        log_media.created_at AS call_time,
        gc.add_time,
        ROW_NUMBER() OVER(PARTITION BY gc.id ORDER BY log_media.created_at DESC) AS rn
    FROM users
    JOIN yuapo.ods_job_jc_gczdw gc ON gc.user_id = users.id AND gc.pt = '20210807'
    JOIN yuapo.ods_reach_privacy_tel_call_log call_log
        ON call_log.info_id = gc.id AND call_log.call_duration > 0
    JOIN yuapo.ods_privacy_tel_call_log_media log_media
        ON call_log.id = log_media.call_log_id AND log_media.pt = '20210807'
        AND log_media.is_asr = 1
        AND TO_DATE(log_media.created_at) > TO_DATE(DATEADD(TO_DATE('${bdp.system.bizdate}', 'yyyyMMdd'), -7, 'dd'))
    WHERE call_log.pt = '20210807'
),

-- 按 info_id 聚合多条 ASR 为一个字段（只取前5条用于展示）
asr_agg AS (
    SELECT
        info_id,
        user_id,
        MAX(detail) AS detail,
        MAX(occupations_v2) AS occupations_v2,
        MAX(add_time) AS add_time,
        -- 多个中间号用逗号分隔
        CONCAT_WS(',', COLLECT_SET(sub_id)) AS sub_ids,
        -- 多条 ASR 用【通话N 时间】标记分隔
        CONCAT_WS('\n', COLLECT_LIST(
            CONCAT('【通话', CAST(rn AS STRING), ' ', CAST(call_time AS STRING), '】\n', COALESCE(asr_result, ''))
        )) AS asr_merged
    FROM call_tel
    WHERE rn <= 10
    GROUP BY info_id, user_id
),

-- 通话记录总数（不限 rn，统计全部）
call_count AS (
    SELECT
        info_id,
        COUNT(*) AS call_record_count
    FROM call_tel
    GROUP BY info_id
),

-- ═══════════════════════════════════════════════════════════════════════════
-- IM 聊天路：每个 info_id 最多取 5 个会话，每个会话内消息拼接后聚合
-- 输出格式: "【会话1】\nuid: msg\nuid: msg\n【会话2】\n..."
-- ═══════════════════════════════════════════════════════════════════════════

im_chat_record AS (
    SELECT
        gc.id AS info_id,
        gc.user_id,
        chat_records.conversation_id,
        MIN(chat_records.msg_time) AS conv_start_time,
        ROW_NUMBER() OVER(PARTITION BY gc.id ORDER BY COUNT(DISTINCT chat_records.msg_body) DESC) AS conv_rn
    FROM users
    JOIN yuapo.ods_job_jc_gczdw gc ON gc.user_id = users.id AND gc.pt = '20210807'
    JOIN yuapo.ods_im_chat_records chat_records ON gc.id = chat_records.info_id
        AND chat_records.is_admin = 0
        AND TO_DATE(chat_records.pt, 'yyyyMMdd')
            BETWEEN DATEADD(TO_DATE('${bdp.system.bizdate}', 'yyyyMMdd'), -7, 'dd')
                AND TO_DATE('${bdp.system.bizdate}', 'yyyyMMdd')
    WHERE GET_JSON_OBJECT(chat_records.msg_body, '$[0].MsgType') = 'TIMTextElem'
    GROUP BY gc.id, gc.user_id, chat_records.conversation_id
    HAVING COUNT(DISTINCT chat_records.msg_body) > 4
),

-- 每个会话内的消息拼接（带时间）
im_conv_messages AS (
    SELECT
        icr.info_id,
        icr.conv_rn,
        icr.conv_start_time,
        CONCAT_WS('\n', COLLECT_LIST(
            CONCAT(chat_records.from_user, ': ',
                   COALESCE(GET_JSON_OBJECT(msg_item, '$.MsgContent.Text'), ''))
        )) AS conv_text
    FROM im_chat_record icr
    JOIN yuapo.ods_im_chat_records chat_records
        ON icr.conversation_id = chat_records.conversation_id
        AND TO_DATE(chat_records.pt, 'yyyyMMdd')
            BETWEEN DATEADD(TO_DATE('${bdp.system.bizdate}', 'yyyyMMdd'), -7, 'dd')
                AND TO_DATE('${bdp.system.bizdate}', 'yyyyMMdd')
    LATERAL VIEW EXPLODE(
        SPLIT(
            REGEXP_REPLACE(REGEXP_REPLACE(chat_records.msg_body, '^\\[', ''), '\\]$', ''),
            '\\},\\{'
        )
    ) msg_t AS msg_item
    WHERE icr.conv_rn <= 10
      AND GET_JSON_OBJECT(msg_item, '$.MsgType') = 'TIMTextElem'
      AND GET_JSON_OBJECT(msg_item, '$.MsgContent.Text') IS NOT NULL
      AND GET_JSON_OBJECT(msg_item, '$.MsgContent.Text') != ''
    GROUP BY icr.info_id, icr.conv_rn, icr.conv_start_time
),

-- 按 info_id 聚合多个会话为一个字段（带会话开始时间）
im_agg AS (
    SELECT
        info_id,
        -- 多个会话用【会话N 时间】标记分隔
        CONCAT_WS('\n', COLLECT_LIST(
            CONCAT('【会话', CAST(conv_rn AS STRING), ' ', FROM_UNIXTIME(conv_start_time), '】\n', conv_text)
        )) AS im_merged
    FROM im_conv_messages
    GROUP BY info_id
),

-- ═══════════════════════════════════════════════════════════════════════════
-- IM 会话总次数（满足条件的会话数，不限 rn）
-- ═══════════════════════════════════════════════════════════════════════════

im_msg_count AS (
    SELECT
        info_id,
        COUNT(*) AS im_message_count
    FROM im_chat_record
    GROUP BY info_id
),

-- ═══════════════════════════════════════════════════════════════════════════
-- 投诉聚合（带【投诉N 时间】格式）
-- ═══════════════════════════════════════════════════════════════════════════

complaint_detail AS (
    SELECT
        ca.target_info_id,
        a.complaint_content,
        ca.created_at AS complaint_time,
        ROW_NUMBER() OVER(PARTITION BY ca.target_info_id ORDER BY ca.created_at DESC) AS rn
    FROM yuapo.ods_complaint_record ca
    LEFT JOIN yuapo.ods_complaint_record_detail a
        ON a.complaint_id = ca.id AND a.pt = '20210807'
    WHERE ca.pt = '20210807'
      AND a.complaint_content IS NOT NULL
),

complaint AS (
    SELECT
        target_info_id,
        CONCAT_WS('\n', COLLECT_LIST(
            CONCAT('【投诉', CAST(rn AS STRING), ' ', complaint_time, '】\n', COALESCE(complaint_content, ''))
        )) AS complaint_content,
        COUNT(*) AS complaint_count
    FROM complaint_detail
    GROUP BY target_info_id
),

-- ═══════════════════════════════════════════════════════════════════════════
-- 职位基础信息（去重，每个 info_id 一行）
-- ═══════════════════════════════════════════════════════════════════════════

job_base AS (
    SELECT
        gc.user_id,
        gc.id AS info_id,
        gc.detail AS job_detail,
        gc.occupations_v2 AS occupation_id,
        gc.add_time,
        ROW_NUMBER() OVER(PARTITION BY gc.id ORDER BY gc.add_time DESC) AS rn
    FROM users
    JOIN yuapo.ods_job_jc_gczdw gc ON gc.user_id = users.id AND gc.pt = '20210807'
    WHERE gc.is_check = 2
      AND TO_DATE(FROM_UNIXTIME(gc.add_time)) > TO_DATE(DATEADD(TO_DATE('${bdp.system.bizdate}', 'yyyyMMdd'), -7, 'dd'))
)

-- ═══════════════════════════════════════════════════════════════════════════
-- 最终输出：每个 info_id 一行，多条记录已聚合
-- ═══════════════════════════════════════════════════════════════════════════

SELECT
    j.user_id,
    j.info_id,
    j.job_detail,
    j.occupation_id,
    COALESCE(a.sub_ids, '')                     AS sub_id,
    COALESCE(a.asr_merged, '')                  AS asr_result,
    COALESCE(im.im_merged, '')                  AS im_text,
    COALESCE(com.complaint_content, '')         AS complaint_content,
    COALESCE(imc.im_message_count, 0)          AS im_message_count,
    COALESCE(cc.call_record_count, 0)          AS call_record_count,
    COALESCE(com.complaint_count, 0)           AS complaint_count,
    FROM_UNIXTIME(j.add_time)                  AS publish_time
FROM job_base j
LEFT JOIN asr_agg a         ON a.info_id = j.info_id
LEFT JOIN call_count cc     ON cc.info_id = j.info_id
LEFT JOIN im_agg im         ON im.info_id = j.info_id
LEFT JOIN im_msg_count imc  ON imc.info_id = j.info_id
LEFT JOIN complaint com     ON com.target_info_id = j.info_id
WHERE j.rn = 1
;
