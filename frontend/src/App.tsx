import React, { useState, useEffect, useCallback, useRef } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import {
  LayoutDashboard,
  ClipboardList,
  AlertCircle,
  Settings,
  CheckCircle2,
  Clock,
  Database,
  Activity,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  Loader2,
  ArrowLeft,
  X,
  RefreshCw,
  Save,
  Send,
  XCircle,
  Search,
  Sparkles,
  ShieldAlert,
  Timer,
  FileText,
  Eraser,
  LogOut,
  Tag,
} from 'lucide-react';
import { api } from './api/client';
import type {
  AuthSession,
  DateEntry,
  StatsResponse,
  RunSummary,
  PaginatedResponse,
  RunDetail,
  SettingsResponse,
  FallbackRecord,
  SnippetRecallRecord,
  SignalDetectionRecord,
  TimeNormalizationRecord,
  SearchResult,
  OnlineQueryResponse,
  AnnotationRecord,
} from './api/types';
import DateSelector from './components/DateSelector';
import DateRangeView from './components/DateRangeView';

// --- Utility ---
function formatDateTime(value?: string): string {
  if (!value) return '-';
  const trimmed = value.trim();
  // 宽表时间已经是北京时间，不加 Z（UTC），直接显示
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(trimmed)) {
    return trimmed;
  }
  // ISO 8601 格式直接解析
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) return value ?? '-';
  return parsed.toLocaleString('zh-CN', { hour12: false, timeZone: 'Asia/Shanghai' });
}

function formatConfidence(score?: number | null): string {
  if (score == null || Number.isNaN(score)) return '-';
  return `${(score * 100).toFixed(0)}%`;
}

function getLatestAnnotation(
  annotations?: AnnotationRecord[] | null
): AnnotationRecord | null {
  if (!annotations || annotations.length === 0) return null;
  return annotations[annotations.length - 1];
}

function getFillStatusLabel(fillStatus?: string | null): string {
  if (fillStatus === 'confirmed_filled') return '明确招满';
  if (fillStatus === 'suspected_filled') return '疑似招满';
  return '未招满';
}

function getFillStatusBadgeClass(fillStatus?: string | null): string {
  if (fillStatus === 'confirmed_filled') return 'bg-orange-50 text-orange-700 border-orange-200';
  if (fillStatus === 'suspected_filled') return 'bg-amber-50 text-amber-700 border-amber-200';
  return 'bg-slate-50 text-slate-600 border-slate-200';
}

const getConfidenceBadgeClass = (score: number) => (
  score >= 0.8
    ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
    : score >= 0.5
      ? 'bg-amber-50 text-amber-700 border-amber-200'
      : 'bg-red-50 text-red-600 border-red-200'
);

// --- Small Components ---
const Skeleton: React.FC<{ className?: string }> = ({ className = '' }) => (
  <div className={`animate-pulse bg-slate-200 rounded ${className}`} />
);

const SidebarItem: React.FC<{ icon: any; label: string; active?: boolean; onClick?: () => void }> = ({ icon: Icon, label, active = false, onClick }) => (
  <div
    onClick={onClick}
    className={`flex items-center gap-3 px-4 py-3 rounded-xl cursor-pointer transition-all duration-200 ${active ? 'bg-blue-600 text-white shadow-md shadow-blue-500/20' : 'text-slate-400 hover:bg-slate-800 hover:text-slate-200'}`}
  >
    <Icon size={20} className={active ? 'text-white' : 'text-slate-400'} />
    <span className="font-medium text-sm">{label}</span>
  </div>
);

const StatusBadge: React.FC<{ status: string }> = ({ status }) => {
  const isFormal = status === 'formal';
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${
      isFormal
        ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
        : 'bg-amber-50 text-amber-700 border-amber-200'
    }`}>
      {isFormal ? <CheckCircle2 size={12} /> : <AlertCircle size={12} />}
      {isFormal ? '正式输出' : '待审核'}
    </span>
  );
};

const DetailSummaryBadge: React.FC<{
  route: string;
  validityType?: string | null;
  confidence?: number | null;
  estimatedExpiry?: string | null;
}> = ({ route, validityType, confidence, estimatedExpiry }) => {
  const isFormal = route === 'formal';
  const badgeClass = isFormal
    ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
    : 'bg-amber-50 text-amber-700 border-amber-200';

  return (
    <span className={`inline-flex max-w-full items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium ${badgeClass}`}>
      {isFormal ? <CheckCircle2 size={12} /> : <AlertCircle size={12} />}
      <span className="truncate">{VALIDITY_TYPE_LABELS[validityType || ''] || validityType || '-'}</span>
      <span className="text-current/50">|</span>
      <span className="whitespace-nowrap">置信度 {formatConfidence(confidence)}</span>
      <span className="text-current/50">|</span>
      <span className="truncate">截止 {formatDateTime(estimatedExpiry ?? undefined)}</span>
    </span>
  );
};

const ErrorBox = ({ message, onRetry }: { message: string; onRetry: () => void }) => (
  <div className="flex flex-col items-center justify-center py-12 text-center">
    <XCircle size={40} className="text-red-400 mb-3" />
    <p className="text-sm text-red-600 mb-4">{message}</p>
    <button onClick={onRetry} className="flex items-center gap-2 px-4 py-2 bg-blue-50 text-blue-600 hover:bg-blue-100 rounded-lg text-sm font-medium transition-colors">
      <RefreshCw size={14} /> 重试
    </button>
  </div>
);

const FullScreenState = ({
  title,
  description,
  action,
}: {
  title: string;
  description: string;
  action?: React.ReactNode;
}) => (
  <div className="min-h-screen bg-[radial-gradient(circle_at_top,_rgba(59,130,246,0.14),_transparent_50%),linear-gradient(180deg,#f8fbff_0%,#eef4ff_100%)] flex items-center justify-center p-6">
    <div className="w-full max-w-lg rounded-[28px] border border-slate-200 bg-white/90 backdrop-blur-xl shadow-[0_30px_80px_-40px_rgba(15,23,42,0.45)] p-10 text-center">
      <div className="mx-auto mb-5 flex h-14 w-14 items-center justify-center rounded-2xl bg-gradient-to-br from-blue-500 to-indigo-600 shadow-lg shadow-blue-500/25">
        <Clock size={26} className="text-white" />
      </div>
      <h1 className="text-2xl font-bold text-slate-900">{title}</h1>
      <p className="mt-3 text-sm leading-6 text-slate-600">{description}</p>
      {action ? <div className="mt-8">{action}</div> : null}
    </div>
  </div>
);

// --- Batch Confirm Dialog ---
const BatchConfirmDialog = ({ open, onClose, onSuccess, showToast }: { open: boolean; onClose: () => void; onSuccess: () => void; showToast: (msg: string, isError?: boolean) => void }) => {
  const [pt, setPt] = useState(() => {
    const d = new Date(); return `${d.getFullYear()}${String(d.getMonth()+1).padStart(2,'0')}${String(d.getDate()).padStart(2,'0')}`;
  });
  const [inputPath, setInputPath] = useState('');
  const [workerCount, setWorkerCount] = useState(4);
  const [submitting, setSubmitting] = useState(false);

  if (!open) return null;

  const handleConfirm = async () => {
    setSubmitting(true);
    try {
      const result = await api.triggerBatch({ pt, inputPath, workerCount });
      showToast(`批量任务已提交: ${result.taskId}`);
      onSuccess();
      onClose();
    } catch (e: any) {
      showToast(e.message || '批量任务触发失败', true);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm" onClick={onClose}>
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.95 }}
        className="bg-white rounded-2xl shadow-2xl border border-slate-200 w-full max-w-md p-6"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-lg font-bold text-slate-900">确认触发批量识别</h3>
          <button onClick={onClose} className="p-1 text-slate-400 hover:text-slate-600"><X size={20} /></button>
        </div>
        <div className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">业务日期 (pt)</label>
            <input value={pt} onChange={e => setPt(e.target.value)} className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none" placeholder="yyyymmdd" />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">数据源路径 (input_path)</label>
            <input value={inputPath} onChange={e => setInputPath(e.target.value)} className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none" placeholder="/path/to/input.csv" />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">并发工作线程数 (worker_count)</label>
            <input type="number" value={workerCount} onChange={e => setWorkerCount(Number(e.target.value))} min={1} max={32} className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none" />
          </div>
        </div>
        <div className="flex gap-3 mt-6">
          <button onClick={onClose} className="flex-1 py-2.5 border border-slate-200 text-slate-700 rounded-xl text-sm font-medium hover:bg-slate-50 transition-colors">取消</button>
          <button onClick={handleConfirm} disabled={submitting || !pt || !inputPath} className="flex-1 py-2.5 bg-slate-900 text-white rounded-xl text-sm font-medium hover:bg-slate-800 disabled:bg-slate-400 transition-colors flex items-center justify-center gap-2">
            {submitting ? <><Loader2 size={14} className="animate-spin" /> 提交中...</> : <><Send size={14} /> 确认执行</>}
          </button>
        </div>
      </motion.div>
    </div>
  );
};

// --- Timeline Step Component ---
const TimelineStep: React.FC<{
  icon: React.ReactNode;
  title: string;
  bgColor: string;
  defaultOpen?: boolean;
  isLast?: boolean;
  children: React.ReactNode;
}> = ({ icon, title, bgColor, defaultOpen = false, isLast = false, children }) => {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="flex gap-4">
      <div className="flex flex-col items-center">
        <button
          onClick={() => setOpen(!open)}
          className={`w-10 h-10 rounded-full flex items-center justify-center shrink-0 shadow-md cursor-pointer transition-all hover:scale-110 hover:shadow-lg ${bgColor}`}
          aria-label={`展开/收起 ${title}`}
        >
          {icon}
        </button>
        {!isLast && <div className="w-0.5 flex-1 bg-slate-200 mt-1" />}
      </div>
      <div className="flex-1 pb-6 min-w-0">
        <button
          onClick={() => setOpen(!open)}
          className="flex items-center gap-2 w-full text-left group mt-2"
        >
          <h4 className="text-sm font-bold text-slate-800 group-hover:text-blue-600 transition-colors">{title}</h4>
          {open ? <ChevronUp size={14} className="text-slate-400" /> : <ChevronDown size={14} className="text-slate-400" />}
        </button>
        {open && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            className="mt-3 bg-white rounded-xl border border-slate-200 p-4 shadow-sm overflow-hidden"
          >
            {children}
          </motion.div>
        )}
      </div>
    </div>
  );
};

// --- Text Diff Block ---
const TextDiffBlock: React.FC<{ label: string; raw: string; cleaned: string }> = ({ label, raw, cleaned }) => {
  const hasChange = raw !== cleaned;
  const maxPreviewLength = 280;
  const shouldCollapse = Math.max(raw.length, cleaned.length) > maxPreviewLength;
  const [expanded, setExpanded] = useState(false);

  if (!raw && !cleaned) return null;

  const blockClass = expanded
    ? 'max-h-[32rem]'
    : 'max-h-48';

  const summarizeText = (value: string) => {
    const normalized = value.replace(/\s+/g, ' ').trim();
    if (!normalized) return '（无内容）';
    if (normalized.length <= 120) return normalized;
    return `${normalized.slice(0, 120)}...`;
  };

  const SummaryCard = ({
    title,
    value,
    tone,
  }: {
    title: string;
    value: string;
    tone: 'raw' | 'cleaned' | 'single';
  }) => {
    const toneClass = tone === 'raw'
      ? 'border-red-100 bg-red-50/70 text-slate-500'
      : tone === 'cleaned'
        ? 'border-emerald-100 bg-emerald-50/80 text-slate-700'
        : 'border-slate-200 bg-slate-50 text-slate-700';

    return (
      <div className={`rounded-lg border px-3 py-2.5 ${toneClass}`}>
        <div className="mb-1 text-[11px] font-medium uppercase tracking-[0.08em] text-slate-400">{title}</div>
        <div className="text-xs leading-6">{summarizeText(value)}</div>
      </div>
    );
  };

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs font-medium text-slate-500">{label}</div>
        {shouldCollapse ? (
          <button
            type="button"
            onClick={() => setExpanded((value) => !value)}
            className="shrink-0 rounded-full border border-slate-200 bg-white px-2.5 py-1 text-[11px] font-medium text-slate-500 transition-colors hover:border-slate-300 hover:text-slate-700"
          >
            {expanded ? '收起' : '展开全文'}
          </button>
        ) : null}
      </div>
      {shouldCollapse && !expanded ? (
        hasChange ? (
          <div className="space-y-2">
            <SummaryCard title="原始文本" value={raw} tone="raw" />
            <SummaryCard title="清洗后" value={cleaned} tone="cleaned" />
          </div>
        ) : (
          <SummaryCard title="内容摘要" value={raw} tone="single" />
        )
      ) : null}
      {hasChange ? (
        <div className={`space-y-2 ${shouldCollapse && !expanded ? 'hidden' : ''}`}>
          <pre className={`text-xs leading-6 whitespace-pre-wrap break-words rounded-lg p-3 bg-red-50 text-slate-400 line-through overflow-auto border border-red-100 ${blockClass}`}>{raw || '（无内容）'}</pre>
          <pre className={`text-xs leading-6 whitespace-pre-wrap break-words rounded-lg p-3 bg-emerald-50 text-slate-800 overflow-auto border border-emerald-100 ${blockClass}`}>{cleaned || '（无内容）'}</pre>
        </div>
      ) : (
        <pre className={`text-xs leading-6 whitespace-pre-wrap break-words rounded-lg p-3 bg-slate-50 text-slate-700 overflow-auto border border-slate-200 ${blockClass} ${shouldCollapse && !expanded ? 'hidden' : ''}`}>{raw || '（无内容）'}</pre>
      )}
    </div>
  );
};

// --- Detail View (Task 11.3) ---
const DetailView: React.FC<{
  runId: string;
  onBack: () => void;
  showToast: (msg: string, isError?: boolean) => void;
  pt?: string | null;
}> = ({
  runId,
  onBack,
  showToast,
  pt,
}) => {
  const [run, setRun] = useState<RunDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [annotationDialogOpen, setAnnotationDialogOpen] = useState(false);

  const fetchDetail = useCallback(() => {
    setLoading(true);
    setError(null);
    api.getRunDetail(runId, pt ?? undefined)
      .then(setRun)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [runId, pt]);

  useEffect(() => { fetchDetail(); }, [fetchDetail]);

  if (loading) {
    return (
      <div className="space-y-6 pb-20">
        <Skeleton className="h-16 w-full rounded-2xl" />
        <div className="flex gap-6">
          <Skeleton className="h-96 w-2/5 rounded-2xl" />
          <Skeleton className="h-96 w-3/5 rounded-2xl" />
        </div>
      </div>
    );
  }

  if (error || !run) {
    return <ErrorBox message={error || '无法加载运行详情'} onRetry={fetchDetail} />;
  }

  const wideRow = run.wideRow as Record<string, unknown>;
  const rawWideRow = (run.rawWideRow ?? run.wideRow) as Record<string, unknown>;
  const temporal = run.temporalSignalRecord;
  const risk = run.riskRecord;
  const decision = run.decisionRecord;
  const snippetRecall = run.snippetRecallRecord;
  const audit = run.audit as Record<string, unknown>;
  const decisionConfidence = temporal?.confidence ?? run.timeNormalizationRecord?.confidence ?? null;
  const latestAnnotation = getLatestAnnotation(run.annotations);
  const displayLabel = latestAnnotation?.annotatedLabel ?? decision?.validityType ?? null;

  // 辅助函数：从 wideRow 取字段（兼容 camelCase 和 snake_case）
  const getField = (row: Record<string, unknown>, camel: string, snake: string): string =>
    String(row[camel] ?? row[snake] ?? '');

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: 20 }} className="relative space-y-6 pb-20">
      {/* Header */}
      <div className="flex items-center justify-between bg-white p-4 rounded-2xl border border-slate-200 shadow-sm sticky top-0 z-10">
        <div className="flex items-center gap-4">
          <button onClick={onBack} className="flex items-center gap-2 text-blue-600 hover:text-blue-700 font-medium px-3 py-1.5 rounded-lg hover:bg-blue-50 transition-colors">
            <ArrowLeft size={18} /> 返回列表
          </button>
          <div className="w-px h-6 bg-slate-200" />
          <div className="flex items-center gap-3 min-w-0">
            <h2 className="text-lg font-bold text-slate-900">{run.entityKey}</h2>
            <DetailSummaryBadge
              route={run.route}
              validityType={displayLabel}
              confidence={decisionConfidence}
              estimatedExpiry={decision?.estimatedExpiry ?? null}
            />
          </div>
        </div>
        <div className="flex items-center gap-3">
          {run.errorType && (
            <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium border bg-red-50 text-red-600 border-red-200">
              错误: {run.errorType}
            </span>
          )}
          <button
            onClick={() => setAnnotationDialogOpen(true)}
            className="inline-flex items-center gap-2 rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-slate-800"
          >
            <Tag size={14} />
            标注
          </button>
        </div>
      </div>

      {/* Two-column layout */}
      <div className="flex gap-6 items-start">
        {/* Left column: 职位基本信息 (40%) */}
        <div className="w-2/5 shrink-0 space-y-4">
          <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-3 border-b border-slate-100 bg-slate-50">
              <h3 className="text-sm font-bold text-slate-700 flex items-center gap-2">
                <Database size={16} className="text-blue-500" />
                职位基本信息
              </h3>
            </div>
            <div className="p-5 space-y-3">
              <DetailField label="职位 ID (info_id)" value={run.entityKey} />
              <DetailField label="用户 ID (user_id)" value={getField(wideRow, 'userId', 'user_id') || '-'} />
              <DetailField label="工种 ID (occupation_id)" value={getField(wideRow, 'occupationId', 'occupation_id') || '-'} />
              <DetailField label="发布时间 (publish_time)" value={formatDateTime(getField(wideRow, 'publishTime', 'publish_time') || undefined)} />
              <div className="grid grid-cols-3 gap-3">
                <DetailField label="IM 消息数" value={getField(wideRow, 'imMsgCount', 'im_msg_count') || '-'} />
                <DetailField label="通话记录数" value={getField(wideRow, 'callRecordCount', 'call_record_count') || '-'} />
                <DetailField label="投诉时间" value={formatDateTime(getField(wideRow, 'firstComplaintTime', 'first_complaint_time') || undefined) || '-'} />
              </div>
              <div className="pt-2 border-t border-slate-100">
                <DetailField label="路由结果" value={run.route === 'formal' ? '正式输出 (formal)' : '待审核 (fallback)'} />
              </div>
              <div className="pt-2 border-t border-slate-100">
                <DetailField
                  label="当前标注"
                  value={latestAnnotation ? (VALIDITY_TYPE_LABELS[latestAnnotation.annotatedLabel] || latestAnnotation.annotatedLabel) : '未标注'}
                />
              </div>
              <div className="pt-2 border-t border-slate-100 space-y-2">
                <div className="text-xs text-slate-400 mb-1">版本信息</div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div><span className="text-slate-400">feature_schema: </span><span className="text-slate-700">{String(audit.feature_schema_version ?? '-')}</span></div>
                  <div><span className="text-slate-400">graph: </span><span className="text-slate-700">{String(audit.graph_version ?? '-')}</span></div>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
              <h3 className="text-sm font-bold text-slate-700 flex items-center gap-2">
                <Tag size={16} className="text-slate-500" />
                标注历史
              </h3>
              <span className="text-xs text-slate-400">{run.annotations.length}/3</span>
            </div>
            {run.annotations.length > 0 ? (
              <div className="p-5 space-y-3">
                {run.annotations.map((annotation, index) => (
                  <div key={`${annotation.createdAt}-${index}`} className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
                    <div className="flex items-start justify-between gap-3">
                      <div className="space-y-2">
                        <span className="inline-flex items-center rounded-full border border-blue-200 bg-blue-50 px-2.5 py-1 text-xs font-medium text-blue-700">
                          {VALIDITY_TYPE_LABELS[annotation.annotatedLabel] || annotation.annotatedLabel}
                        </span>
                        <div className="text-xs text-slate-500">
                          {annotation.reviewerName || '当前登录用户'} · {formatDateTime(annotation.createdAt)}
                        </div>
                      </div>
                      <span className="text-[11px] text-slate-400">第 {index + 1} 次</span>
                    </div>
                    {annotation.reviewerNotes ? (
                      <p className="mt-2 text-xs leading-5 text-slate-600">{annotation.reviewerNotes}</p>
                    ) : (
                      <p className="mt-2 text-xs text-slate-400">无备注</p>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <div className="p-6 text-center text-sm text-slate-400">暂无标注记录</div>
            )}
          </div>

          {/* 执行耗时 */}
          {run.timingMs && Object.keys(run.timingMs).length > 0 && (
            <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
              <div className="px-5 py-3 border-b border-slate-100 bg-slate-50">
                <h3 className="text-sm font-bold text-slate-700 flex items-center gap-2">
                  <Activity size={16} className="text-slate-500" />
                  执行耗时
                </h3>
              </div>
              <div className="p-5">
                <div className="flex flex-wrap gap-2">
                  {Object.entries(run.timingMs).map(([step, ms]) => (
                    <div key={step} className="bg-slate-50 rounded-lg px-3 py-1.5">
                      <div className="text-xs text-slate-400">{step}</div>
                      <div className="text-sm font-medium text-slate-900">{(ms as number).toFixed(0)}ms</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Right column: 推理流程 (60%) */}
        <div className="w-3/5 min-w-0">
          <div className="bg-slate-50 rounded-2xl border border-slate-200 p-5">
            <h3 className="text-sm font-bold text-slate-700 mb-5 flex items-center gap-2">
              <Activity size={16} className="text-indigo-500" />
              推理流程
            </h3>

            {/* Step 1: 数据加载 */}
            <TimelineStep icon={<Database size={18} className="text-white" />} title="数据加载" bgColor="bg-blue-500">
              <div className="space-y-2 text-xs">
                <DetailField label="数据来源" value="WideRow" />
                <DetailField label="运行 ID" value={run.runId} />
                <div className="grid grid-cols-2 gap-2">
                  <DetailField label="feature_schema_version" value={String(audit.feature_schema_version ?? '-')} />
                  <DetailField label="graph_version" value={String(audit.graph_version ?? '-')} />
                </div>
              </div>
            </TimelineStep>

            {/* Step 2: 文本清洗 */}
            <TimelineStep icon={<Eraser size={18} className="text-white" />} title="文本清洗" bgColor="bg-emerald-500">
              <div className="space-y-3">
                <TextDiffBlock
                  label="职位详情 (job_detail)"
                  raw={getField(rawWideRow, 'jobDetail', 'job_detail')}
                  cleaned={getField(wideRow, 'jobDetail', 'job_detail')}
                />
                <TextDiffBlock
                  label="ASR 转写 (asr_result)"
                  raw={getField(rawWideRow, 'asrResult', 'asr_result')}
                  cleaned={getField(wideRow, 'asrResult', 'asr_result')}
                />
                <TextDiffBlock
                  label="IM 消息 (im_text)"
                  raw={getField(rawWideRow, 'imText', 'im_text')}
                  cleaned={getField(wideRow, 'imText', 'im_text')}
                />
                <TextDiffBlock
                  label="投诉内容 (complaint_content)"
                  raw={getField(rawWideRow, 'complaintContent', 'complaint_content')}
                  cleaned={getField(wideRow, 'complaintContent', 'complaint_content')}
                />
              </div>
            </TimelineStep>

            {/* Step 3: 规则召回 */}
            <TimelineStep icon={<Search size={18} className="text-white" />} title="规则召回" bgColor="bg-violet-500">
              {snippetRecall ? (
                <div className="space-y-3">
                  <div className="grid grid-cols-3 gap-3">
                    <DetailField label="命中召回" value={snippetRecall.hasRecall ? '是' : '否'} />
                    <DetailField label="时效命中数" value={String(snippetRecall.temporalMatchCount)} />
                    <DetailField label="投诉命中数" value={String(snippetRecall.complaintMatchCount)} />
                  </div>
                  {snippetRecall.matchedSources && snippetRecall.matchedSources.length > 0 && (
                    <div>
                      <div className="text-xs font-medium text-slate-500 mb-1">命中来源</div>
                      <div className="flex gap-1.5 flex-wrap">
                        {snippetRecall.matchedSources.map((src, i) => (
                          <span key={i} className="px-2 py-0.5 rounded-full text-xs font-medium bg-violet-50 text-violet-700 border border-violet-200">{src}</span>
                        ))}
                      </div>
                    </div>
                  )}
                  {snippetRecall.matches.length > 0 && (
                    <div className="space-y-2">
                      <div className="text-xs font-medium text-slate-500">规则命中</div>
                      {snippetRecall.matches.map((m, i) => (
                        <div key={i} className={`rounded-lg p-3 border text-xs space-y-1 ${
                          m.matchedBucket === 'complaint'
                            ? 'bg-red-50 border-red-200'
                            : 'bg-blue-50 border-blue-200'
                        }`}>
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                              m.matchedBucket === 'complaint' ? 'bg-red-100 text-red-700' : 'bg-blue-100 text-blue-700'
                            }`}>{m.matchedBucket}</span>
                            <span className="text-slate-500">来源: {m.source}</span>
                          </div>
                          {m.matchedTerms.length > 0 && (
                            <div className="flex gap-1 flex-wrap">
                              {m.matchedTerms.map((t, j) => (
                                <span key={j} className="px-1.5 py-0.5 rounded bg-white/60 text-slate-600 text-xs border">{t}</span>
                              ))}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-xs text-slate-400 text-center py-4">无规则召回数据</div>
              )}
            </TimelineStep>

            {/* Step 4: 信号检测 */}
            <TimelineStep icon={<Sparkles size={18} className="text-white" />} title="信号检测" bgColor="bg-cyan-500">
              {temporal ? (
                <div className="space-y-3">
                  <div className="grid grid-cols-2 gap-3">
                    <DetailField label="有时效信号" value={temporal.hasTemporalSignal ? '是' : '否'} />
                    <DetailField label="时效状态" value={TEMPORAL_STATUS_LABELS[temporal.temporalStatus] || temporal.temporalStatus} />
                    <DetailField label="信号类型" value={SIGNAL_TYPE_LABELS[temporal.signalType] || temporal.signalType} />
                    <div>
                      <div className="text-xs text-slate-400 mb-1">置信度</div>
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${getConfidenceBadgeClass(temporal.confidence)}`}>
                        {(temporal.confidence * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>
                  {temporal.evidenceSummary && temporal.evidenceSummary.length > 0 && (
                    <div>
                      <div className="text-xs text-slate-400 mb-1">证据摘要</div>
                      <ul className="space-y-1">
                        {temporal.evidenceSummary.map((e, i) => (
                          <li key={i} className="text-xs text-slate-700 bg-slate-50 rounded-lg px-3 py-2">{e}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                  {temporal.cannotDetermineReason && (
                    <div>
                      <div className="text-xs text-slate-400 mb-1">无法判定原因</div>
                      <p className="text-xs text-amber-700 bg-amber-50 rounded-lg p-2.5">{temporal.cannotDetermineReason}</p>
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-xs text-slate-400 text-center py-4">无信号检测数据</div>
              )}
            </TimelineStep>

            {/* Step 5: 风险评估 */}
            <TimelineStep icon={<ShieldAlert size={18} className="text-white" />} title="风险评估" bgColor="bg-orange-500">
              {risk ? (
                <div className="space-y-3">
                  <div className="grid grid-cols-2 gap-3">
                    <DetailField
                      label="招满状态"
                      value={getFillStatusLabel(risk.fillStatus)}
                      highlight={risk.fillStatus === 'confirmed_filled' || risk.fillStatus === 'suspected_filled'}
                    />
                    <DetailField label="联系不上" value={risk.isUnreachable ? '是' : '否'} highlight={risk.isUnreachable} />
                    <DetailField label="过期风险" value={risk.staleRiskHint ? '是' : '否'} highlight={risk.staleRiskHint} />
                    <div>
                      <div className="text-xs text-slate-400 mb-1">风险分数</div>
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${
                        risk.riskScore >= 0.7 ? 'bg-red-50 text-red-600 border-red-200' :
                        risk.riskScore >= 0.4 ? 'bg-amber-50 text-amber-700 border-amber-200' :
                        'bg-emerald-50 text-emerald-700 border-emerald-200'
                      }`}>
                        {(risk.riskScore * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>
                  {risk.complaintRiskHint?.hasComplaintSignal && (
                    <div className="bg-red-50 border border-red-200 rounded-lg p-3 space-y-1 text-xs">
                      <div className="font-medium text-red-700">投诉详情</div>
                      <div className="text-red-600">类型: {risk.complaintRiskHint.complaintSignalType}</div>
                      {risk.complaintRiskHint.complaintSummary && (
                        <div className="text-red-600">摘要: {risk.complaintRiskHint.complaintSummary}</div>
                      )}
                    </div>
                  )}
                  {risk.complaintSummary && (
                    <div>
                      <div className="text-xs text-slate-400 mb-1">投诉摘要</div>
                      <p className="text-xs text-slate-700 bg-slate-50 rounded-lg px-3 py-2">{risk.complaintSummary}</p>
                    </div>
                  )}
                  {risk.estimatedFilledAt && (
                    <div>
                      <div className="text-xs text-slate-400 mb-1">推断招满时间</div>
                      <p className="text-xs text-slate-700 bg-slate-50 rounded-lg px-3 py-2">
                        {formatDateTime(risk.estimatedFilledAt)}
                      </p>
                      {risk.estimatedFilledReason ? (
                        <p className="mt-2 text-xs text-slate-500">{risk.estimatedFilledReason}</p>
                      ) : null}
                    </div>
                  )}
                  {risk.riskReasons.length > 0 && (
                    <div>
                      <div className="text-xs text-slate-400 mb-1">风险原因</div>
                      <ul className="space-y-1">
                        {risk.riskReasons.map((r, i) => (
                          <li key={i} className="text-xs text-slate-700 bg-slate-50 rounded-lg px-3 py-2">{r}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-xs text-slate-400 text-center py-4">无风险评估数据</div>
              )}
            </TimelineStep>

            {/* Step 6: 时间归一化 */}
            <TimelineStep icon={<Timer size={18} className="text-white" />} title="时间归一化" bgColor="bg-purple-500">
              {temporal ? (
                <div className="space-y-3">
                  <div className="grid grid-cols-2 gap-3">
                    <DetailField label="可归一化" value={temporal.normalizable ? '是' : '否'} />
                    <DetailField label="锚定类型" value={temporal.anchorType || '-'} />
                    <DetailField label="开工时间" value={formatDateTime(temporal.workStartAt ?? undefined)} />
                    <DetailField label="招聘有效期" value={formatDateTime(temporal.recruitmentValidUntil ?? undefined)} />
                    <DetailField label="工期 (小时)" value={temporal.durationHours != null ? String(temporal.durationHours) : '-'} />
                  </div>
                  {temporal.interpretation && (
                    <div>
                      <div className="text-xs text-slate-400 mb-1">解释</div>
                      <p className="text-xs text-slate-700 bg-slate-50 rounded-lg p-2.5">{temporal.interpretation}</p>
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-xs text-slate-400 text-center py-4">无时间归一化数据</div>
              )}
            </TimelineStep>

            {/* Step 7: 最终决策 (always expanded) */}
            <TimelineStep icon={<CheckCircle2 size={18} className="text-white" />} title="最终决策" bgColor="bg-emerald-600" defaultOpen isLast>
              {decision ? (
                <div className="space-y-3">
                  <div className="grid grid-cols-2 gap-3">
                    <DetailField label="有效期类型" value={VALIDITY_TYPE_LABELS[decision.validityType] || decision.validityType} />
                    <DetailField label="预估截止时间" value={formatDateTime(decision.estimatedExpiry ?? undefined)} />
                    <div>
                      <div className="text-xs text-slate-400 mb-1">置信度</div>
                      {decisionConfidence != null ? (
                        <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${getConfidenceBadgeClass(decisionConfidence)}`}>
                          {formatConfidence(decisionConfidence)}
                        </span>
                      ) : (
                        <div className="text-sm font-medium text-slate-900">-</div>
                      )}
                    </div>
                  </div>
                  {decision.reason && (
                    <div>
                      <div className="text-xs text-slate-400 mb-1">决策理由</div>
                      <p className="text-xs text-slate-700 bg-blue-50 rounded-lg p-2.5">{decision.reason}</p>
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-xs text-slate-400 text-center py-4">无最终决策数据</div>
              )}
            </TimelineStep>
          </div>
        </div>
      </div>

      <AnimatePresence>
        {annotationDialogOpen && (
          <AnnotationDialog
            open={annotationDialogOpen}
            runId={run.runId}
            currentLabel={decision?.validityType ?? null}
            defaultLabel={latestAnnotation?.annotatedLabel ?? decision?.validityType ?? null}
            pt={pt}
            showToast={showToast}
            onClose={(submitted) => {
              setAnnotationDialogOpen(false);
              if (submitted) {
                fetchDetail();
              }
            }}
          />
        )}
      </AnimatePresence>
    </motion.div>
  );
};

function DetailField({ label, value, highlight = false }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div>
      <div className="text-xs text-slate-400 mb-1">{label}</div>
      <div className={`text-sm font-medium ${highlight ? 'text-red-600' : 'text-slate-900'}`}>{value}</div>
    </div>
  );
}

// --- Main App ---
type Page = 'dashboard' | 'review' | 'fallbacks' | 'settings' | 'date-range' | 'online-query';

export default function App() {
  // Auth state
  const [authLoading, setAuthLoading] = useState(true);
  const [session, setSession] = useState<AuthSession | null>(null);

  // Navigation
  const [page, setPage] = useState<Page>('dashboard');
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  // Date state
  const [dates, setDates] = useState<DateEntry[]>([]);
  const [latestPt, setLatestPt] = useState<string | null>(null);
  const [selectedPt, setSelectedPt] = useState<string | null>(null);
  const [datesLoading, setDatesLoading] = useState(false);

  // Toast
  const [toast, setToast] = useState<{ msg: string; isError: boolean } | null>(null);
  const showToast = (msg: string, isError = false) => {
    setToast({ msg, isError });
    setTimeout(() => setToast(null), 3000);
  };

  // Batch dialog
  const [batchOpen, setBatchOpen] = useState(false);

  // Search bar
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);

  // --- Auth ---
  useEffect(() => {
    api.getAuthSession()
      .then(s => {
        setSession(s);
        setAuthLoading(false);
      })
      .catch(() => {
        setSession({ enabled: false, authenticated: true, user: null, loginUrl: null });
        setAuthLoading(false);
      });
  }, []);

  // --- Dates ---
  const fetchDates = useCallback(() => {
    setDatesLoading(true);
    api.getDates()
      .then(d => {
        setDates(d.dates);
        setLatestPt(d.latestPt);
        if (!selectedPt && d.latestPt) setSelectedPt(d.latestPt);
      })
      .catch((e: any) => {
        setDates([]);
        setLatestPt(null);
        showToast(e?.message || '无法加载业务日期，请确认后端服务已启动', true);
      })
      .finally(() => setDatesLoading(false));
  }, [selectedPt]);

  useEffect(() => { fetchDates(); }, []);

  // --- Auth gates ---
  if (authLoading) {
    return <FullScreenState title="加载中..." description="正在验证身份信息" />;
  }

  if (session?.enabled && !session.authenticated) {
    return (
      <FullScreenState
        title="职位新鲜度审核控制台"
        description="请使用飞书账号登录以访问系统"
        action={
          session.loginUrl ? (
            <a href={session.loginUrl} className="inline-flex items-center gap-2 px-6 py-3 bg-blue-600 text-white rounded-xl font-medium hover:bg-blue-700 transition-colors shadow-lg shadow-blue-500/25">
              飞书登录
            </a>
          ) : null
        }
      />
    );
  }

  if (session?.accessDenied) {
    return (
      <FullScreenState
        title="暂无访问权限"
        description={session.requestStatus === 'pending' ? '您的权限申请正在审批中，请耐心等待' : '请联系管理员获取访问权限'}
      />
    );
  }

  const userName = session?.user?.name || '';

  const handleLogout = async () => {
    try {
      await api.logout();
      window.location.reload();
    } catch {
      window.location.reload();
    }
  };

  const handleSearch = async () => {
    const q = searchQuery.trim();
    if (!q) return;
    setSearchLoading(true);
    try {
      const results = await api.search(q, selectedPt ?? undefined);
      setSearchResults(results);
      setSearchOpen(true);
      if (results.length === 1) {
        setSelectedRunId(results[0].runId);
        setSearchOpen(false);
        setSearchQuery('');
        setSearchResults([]);
      }
    } catch (e: any) {
      showToast(e.message || '搜索失败', true);
    } finally {
      setSearchLoading(false);
    }
  };

  return (
    <div className="flex h-screen bg-slate-50 overflow-hidden">
      {/* Sidebar */}
      <div className="w-64 bg-slate-900 flex flex-col shrink-0">
        <div className="p-6 border-b border-slate-800">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-blue-500/25">
              <Clock size={20} className="text-white" />
            </div>
            <div>
              <h1 className="text-white font-bold text-sm">新鲜度审核</h1>
              <p className="text-slate-500 text-xs">Freshness Pipeline</p>
            </div>
          </div>
        </div>

        <nav className="flex-1 p-4 space-y-1">
          <SidebarItem icon={LayoutDashboard} label="数据概览" active={page === 'dashboard'} onClick={() => { setPage('dashboard'); setSelectedRunId(null); }} />
          <SidebarItem icon={ClipboardList} label="审核列表" active={page === 'review'} onClick={() => { setPage('review'); setSelectedRunId(null); }} />
          <SidebarItem icon={Activity} label="日期范围" active={page === 'date-range'} onClick={() => { setPage('date-range'); setSelectedRunId(null); }} />
          <SidebarItem icon={Search} label="在线查询" active={page === 'online-query'} onClick={() => { setPage('online-query'); setSelectedRunId(null); }} />
          <SidebarItem icon={Settings} label="系统设置" active={page === 'settings'} onClick={() => { setPage('settings'); setSelectedRunId(null); }} />
        </nav>

        {/* User info */}
        {session?.user && (
          <div className="p-4 border-t border-slate-800">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-full bg-slate-700 flex items-center justify-center text-xs text-slate-300 font-bold">
                {userName.charAt(0) || '?'}
              </div>
              <div className="flex-1 min-w-0">
                <div className="text-sm text-slate-300 truncate">{userName}</div>
              </div>
              <button onClick={handleLogout} className="p-1.5 text-slate-500 hover:text-slate-300 transition-colors" title="退出登录">
                <LogOut size={16} />
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-y-auto">
        {/* Top Bar */}
        <div className="sticky top-0 z-20 bg-white/80 backdrop-blur-xl border-b border-slate-200 px-8 py-4 flex items-center justify-between">
          <DateSelector
            dates={dates}
            latestPt={latestPt}
            selectedPt={selectedPt}
            loading={datesLoading}
            onSelect={pt => { setSelectedPt(pt); setSelectedRunId(null); }}
          />
          <div className="flex items-center gap-3">
            {/* Search bar */}
            <div className="relative">
              <div className="flex items-center">
                <div className="relative">
                  <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={e => { setSearchQuery(e.target.value); if (!e.target.value.trim()) setSearchOpen(false); }}
                    onKeyDown={e => { if (e.key === 'Enter') handleSearch(); }}
                    onBlur={() => setTimeout(() => setSearchOpen(false), 200)}
                    placeholder="搜索 info_id..."
                    className="w-52 pl-9 pr-3 py-2 border border-slate-200 rounded-xl text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none bg-slate-50 hover:bg-white transition-colors"
                  />
                  {searchLoading && <Loader2 size={14} className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 animate-spin" />}
                </div>
              </div>
              {/* Search results dropdown */}
              {searchOpen && searchResults.length > 0 && (
                <div className="absolute top-full mt-1 right-0 w-80 bg-white rounded-xl border border-slate-200 shadow-lg z-50 max-h-64 overflow-y-auto">
                  {searchResults.map(r => (
                    <div
                      key={r.runId}
                      onMouseDown={() => {
                        setSelectedRunId(r.runId);
                        setSearchOpen(false);
                        setSearchQuery('');
                        setSearchResults([]);
                      }}
                      className="px-4 py-3 hover:bg-slate-50 cursor-pointer border-b border-slate-100 last:border-b-0 flex items-center justify-between"
                    >
                      <div>
                        <div className="text-sm font-medium text-slate-900">{r.entityKey}</div>
                        <div className="text-xs text-slate-400 mt-0.5">{r.validityType || '-'}</div>
                      </div>
                      <StatusBadge status={r.route} />
                    </div>
                  ))}
                </div>
              )}
              {searchOpen && searchResults.length === 0 && !searchLoading && (
                <div className="absolute top-full mt-1 right-0 w-80 bg-white rounded-xl border border-slate-200 shadow-lg z-50 p-4 text-center text-sm text-slate-400">
                  未找到匹配结果
                </div>
              )}
            </div>
            <button
              onClick={() => setBatchOpen(true)}
              className="px-4 py-2 bg-slate-900 text-white text-sm font-medium rounded-xl hover:bg-slate-800 transition-colors flex items-center gap-2"
            >
              <Send size={14} /> 触发批量
            </button>
          </div>
        </div>

        {/* Page Content */}
        <div className="p-8">
          <AnimatePresence mode="wait">
            {selectedRunId ? (
              <DetailView
                key={selectedRunId}
                runId={selectedRunId}
                onBack={() => setSelectedRunId(null)}
                showToast={showToast}
                pt={selectedPt}
              />
            ) : page === 'dashboard' ? (
              <DashboardPage key="dashboard" pt={selectedPt} />
            ) : page === 'review' ? (
              <ReviewListPage key="review" pt={selectedPt} onSelectRun={setSelectedRunId} />
            ) : page === 'date-range' ? (
              <DateRangeView key="date-range" dates={dates} onSelectDate={pt => { setSelectedPt(pt); setPage('dashboard'); }} />
            ) : page === 'online-query' ? (
              <OnlineQueryPage key="online-query" latestPt={latestPt} onSelectRun={setSelectedRunId} showToast={showToast} />
            ) : page === 'settings' ? (
              <SettingsPage key="settings" showToast={showToast} />
            ) : null}
          </AnimatePresence>
        </div>
      </div>

      {/* Toast */}
      <AnimatePresence>
        {toast && (
          <motion.div
            initial={{ opacity: 0, y: 50 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 50 }}
            className={`fixed bottom-6 right-6 z-50 px-5 py-3 rounded-xl shadow-lg text-sm font-medium ${
              toast.isError ? 'bg-red-600 text-white' : 'bg-slate-900 text-white'
            }`}
          >
            {toast.msg}
          </motion.div>
        )}
      </AnimatePresence>

      {/* Batch Dialog */}
      <AnimatePresence>
        <BatchConfirmDialog
          open={batchOpen}
          onClose={() => setBatchOpen(false)}
          onSuccess={fetchDates}
          showToast={showToast}
        />
      </AnimatePresence>
    </div>
  );
}

// --- Stats Dashboard (Task 11.1) ---

const TEMPORAL_STATUS_LABELS: Record<string, string> = {
  has_signal: '有信号',
  no_signal: '无信号',
  cannot_determine: '无法判定',
  conflict: '冲突',
};

const TEMPORAL_STATUS_COLORS: Record<string, string> = {
  has_signal: 'bg-emerald-500',
  no_signal: 'bg-slate-400',
  cannot_determine: 'bg-amber-500',
  conflict: 'bg-red-500',
};

const SIGNAL_TYPE_LABELS: Record<string, string> = {
  absolute_datetime: '绝对时间',
  date_range: '日期范围',
  relative_time: '相对时间',
  duration_only: '仅工期',
  holiday_window: '节假日窗口',
  vague_time: '模糊时间',
  no_signal: '无信号',
  conflict: '冲突',
};

const SIGNAL_TYPE_COLORS: Record<string, string> = {
  absolute_datetime: 'bg-blue-500',
  date_range: 'bg-indigo-500',
  relative_time: 'bg-cyan-500',
  duration_only: 'bg-teal-500',
  holiday_window: 'bg-purple-500',
  vague_time: 'bg-orange-500',
  no_signal: 'bg-slate-400',
  conflict: 'bg-red-500',
};

const VALIDITY_TYPE_LABELS: Record<string, string> = {
  exact_date: '明确日期',
  fuzzy_time: '模糊时间',
  no_validity: '无时效',
  exactDate: '明确日期',
  fuzzyTime: '模糊时间',
  noValidity: '无时效',
};

const VALIDITY_TYPE_COLORS: Record<string, string> = {
  exact_date: 'bg-emerald-500',
  fuzzy_time: 'bg-amber-500',
  no_validity: 'bg-slate-400',
  exactDate: 'bg-emerald-500',
  fuzzyTime: 'bg-amber-500',
  noValidity: 'bg-slate-400',
};

type QueryFlowStatus = 'pending' | 'running' | 'done' | 'error';

interface QueryFlowStageTemplate {
  id: string;
  name: string;
  icon: React.ComponentType<{ size?: number; className?: string }>;
}

interface QueryFlowStage extends QueryFlowStageTemplate {
  status: QueryFlowStatus;
  elapsedMs: number | null;
  message: string;
}

const ONLINE_QUERY_FLOW_TEMPLATES: QueryFlowStageTemplate[] = [
  { id: 'validate', name: '入参校验', icon: ClipboardList },
  { id: 'load', name: 'ODPS 宽表查询', icon: Database },
  { id: 'recall', name: '规则召回', icon: Search },
  { id: 'decision', name: '新鲜度判断', icon: Sparkles },
  { id: 'summary', name: '结果汇总', icon: CheckCircle2 },
];

const ONLINE_QUERY_FLOW_STATUS_STYLES: Record<QueryFlowStatus, string> = {
  pending: 'bg-slate-300',
  running: 'bg-blue-500 animate-pulse',
  done: 'bg-emerald-500',
  error: 'bg-red-500',
};

function buildOnlineQueryStages(overrides: Array<Partial<QueryFlowStage>> = []): QueryFlowStage[] {
  return ONLINE_QUERY_FLOW_TEMPLATES.map((stage, index) => ({
    ...stage,
    status: 'pending',
    elapsedMs: null,
    message: '',
    ...overrides[index],
  }));
}

const InlineProcessPills: React.FC<{ run: RunDetail }> = ({ run }) => {
  const pills = [
    { label: '规则召回', active: Boolean(run.snippetRecallRecord?.hasRecall || run.snippetRecallRecord) },
    { label: '信号检测', active: Boolean(run.signalDetectionRecord || run.temporalSignalRecord) },
    { label: '风险评估', active: Boolean(run.riskRecord) },
    { label: '时间归一化', active: Boolean(run.timeNormalizationRecord || run.temporalSignalRecord) },
    { label: '最终决策', active: Boolean(run.decisionRecord) },
  ];

  return (
    <div className="flex flex-wrap items-center gap-2">
      {pills.map((pill) => (
        <span
          key={pill.label}
          className={`inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium transition-colors ${
            pill.active
              ? 'border-blue-200 bg-blue-50 text-blue-700'
              : 'border-slate-200 bg-slate-50 text-slate-400'
          }`}
        >
          {pill.label}
        </span>
      ))}
    </div>
  );
};

const AnnotationDialog: React.FC<{
  open: boolean;
  runId: string;
  currentLabel: string | null;
  defaultLabel: string | null;
  pt?: string | null;
  showToast: (msg: string, isError?: boolean) => void;
  onClose: (submitted?: boolean) => void;
}> = ({
  open,
  runId,
  currentLabel,
  defaultLabel,
  pt,
  showToast,
  onClose,
}) => {
  const [selectedLabel, setSelectedLabel] = useState(defaultLabel || currentLabel || '');
  const [reviewerNotes, setReviewerNotes] = useState('');
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) return;
    setSelectedLabel(defaultLabel || currentLabel || '');
    setReviewerNotes('');
  }, [open, currentLabel, defaultLabel]);

  if (!open) return null;

  const handleSubmit = async () => {
    if (!selectedLabel) return;
    setSubmitting(true);
    try {
      await api.submitAnnotation(
        runId,
        {
          annotatedLabel: selectedLabel,
          reviewerNotes,
        },
        pt ?? undefined
      );
      showToast('标注提交成功');
      onClose(true);
    } catch (e: any) {
      showToast(e.message || '标注提交失败', true);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm" onClick={() => onClose()}>
      <motion.div
        initial={{ opacity: 0, scale: 0.96 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.96 }}
        className="w-full max-w-md rounded-2xl border border-slate-200 bg-white p-6 shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-bold text-slate-900">人工标注</h3>
            <p className="mt-1 text-xs text-slate-500">单条记录最多可追加 3 次标注，用于审核纠偏与留痕。</p>
          </div>
          <button onClick={() => onClose()} className="rounded-lg p-1 text-slate-400 transition-colors hover:text-slate-600">
            <X size={18} />
          </button>
        </div>

        <div className="mt-5 space-y-4">
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600">模型结果</label>
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
              {currentLabel ? (VALIDITY_TYPE_LABELS[currentLabel] || currentLabel) : '暂无结果'}
            </div>
          </div>

          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600">标注标签</label>
            <select
              value={selectedLabel}
              onChange={(e) => setSelectedLabel(e.target.value)}
              className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none transition-colors focus:border-blue-500 focus:ring-2 focus:ring-blue-500/15"
            >
              <option value="">请选择标注结果</option>
              {Object.entries(VALIDITY_TYPE_LABELS).map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600">备注</label>
            <textarea
              value={reviewerNotes}
              onChange={(e) => setReviewerNotes(e.target.value)}
              rows={4}
              placeholder="可选，记录纠偏原因或判断依据"
              className="w-full resize-none rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none transition-colors focus:border-blue-500 focus:ring-2 focus:ring-blue-500/15"
            />
          </div>
        </div>

        <div className="mt-6 flex gap-3">
          <button
            onClick={() => onClose()}
            className="flex-1 rounded-xl border border-slate-200 px-4 py-2.5 text-sm font-medium text-slate-700 transition-colors hover:bg-slate-50"
          >
            取消
          </button>
          <button
            onClick={handleSubmit}
            disabled={submitting || !selectedLabel}
            className="flex flex-1 items-center justify-center gap-2 rounded-xl bg-slate-900 px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-slate-800 disabled:bg-slate-400"
          >
            {submitting ? <><Loader2 size={14} className="animate-spin" /> 提交中...</> : <><Tag size={14} /> 提交标注</>}
          </button>
        </div>
      </motion.div>
    </div>
  );
};

const OnlineQueryFlowCard: React.FC<{
  stages: QueryFlowStage[];
  loading: boolean;
  response: OnlineQueryResponse | null;
  queryError: string | null;
  queryDurationMs: number | null;
  requestedCount: number;
}> = ({ stages, loading, response, queryError, queryDurationMs, requestedCount }) => {
  const foundCount = response?.results.length ?? 0;
  const notFoundCount = response?.notFound.length ?? 0;
  const formalCount = response?.results.filter((item) => item.route === 'formal').length ?? 0;
  const fallbackCount = response ? response.results.length - formalCount : 0;

  return (
    <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
      <div className="px-6 py-4 border-b border-slate-100 bg-slate-50/70">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h3 className="text-sm font-bold text-slate-800">查询流程</h3>
            <p className="mt-1 text-xs text-slate-500">参考批量流程设计，展示本次 ODPS 在线查询与结果覆盖写回的处理流转。</p>
          </div>
          <span
            className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-medium ${
              queryError
                ? 'border-red-200 bg-red-50 text-red-600'
                : loading
                  ? 'border-blue-200 bg-blue-50 text-blue-700'
                  : response
                    ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                    : 'border-slate-200 bg-slate-50 text-slate-500'
            }`}
          >
            {queryError ? '查询失败' : loading ? '查询中' : response ? '已完成' : '等待提交'}
          </span>
        </div>
      </div>

      <div className="p-6">
        <div className="relative">
          <div className="absolute left-5 top-6 bottom-6 w-0.5 bg-slate-200" />
          <div className="space-y-4">
            {stages.map((stage) => {
              const Icon = stage.icon;
              return (
                <div key={stage.id} className="relative z-10 flex items-center gap-4">
                  <div className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-full text-white shadow-md ring-4 ring-white transition-all ${ONLINE_QUERY_FLOW_STATUS_STYLES[stage.status]}`}>
                    {stage.status === 'running' ? <Loader2 size={18} className="animate-spin" /> : <Icon size={18} />}
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className={`text-sm font-bold ${stage.status === 'pending' ? 'text-slate-400' : 'text-slate-700'}`}>
                        {stage.name}
                      </span>
                      {stage.elapsedMs != null && (
                        <span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[10px] text-slate-400">
                          {Math.round(stage.elapsedMs)}ms
                        </span>
                      )}
                      {stage.status === 'done' && <CheckCircle2 size={14} className="text-emerald-500" />}
                      {stage.status === 'error' && <XCircle size={14} className="text-red-500" />}
                    </div>
                    {stage.message ? (
                      <div className="mt-1 text-xs text-slate-500">{stage.message}</div>
                    ) : stage.status === 'pending' ? (
                      <div className="mt-1 text-xs text-slate-300">等待推进</div>
                    ) : null}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div className="mt-6 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
            <div className="text-xs text-slate-400">提交数量</div>
            <div className="mt-1 text-lg font-semibold text-slate-900">{requestedCount || '-'}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
            <div className="text-xs text-slate-400">命中 / 未命中</div>
            <div className="mt-1 text-lg font-semibold text-slate-900">
              {foundCount} / {notFoundCount}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
            <div className="text-xs text-slate-400">formal / fallback</div>
            <div className="mt-1 text-lg font-semibold text-slate-900">
              {formalCount} / {fallbackCount}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
            <div className="text-xs text-slate-400">总耗时</div>
            <div className="mt-1 text-lg font-semibold text-slate-900">
              {queryDurationMs != null ? `${queryDurationMs}ms` : '-'}
            </div>
          </div>
        </div>

        {queryError && (
          <div className="mt-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-600">
            {queryError}
          </div>
        )}
      </div>
    </div>
  );
};

function DistributionBar({ distribution, labels, colors }: {
  distribution: Record<string, number>;
  labels: Record<string, string>;
  colors: Record<string, string>;
}) {
  const total = Object.values(distribution).reduce((a, b) => a + b, 0);
  if (total === 0) return <div className="text-sm text-slate-400 text-center py-4">暂无数据</div>;

  const entries = Object.entries(distribution).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]);
  const maxCount = Math.max(...entries.map(([, v]) => v));

  return (
    <div className="space-y-3">
      {entries.map(([key, count]) => {
        const pct = ((count / total) * 100).toFixed(1);
        const barPct = (count / maxCount) * 100;
        return (
          <div key={key} className="flex items-center gap-3">
            <span className="text-sm text-slate-600 w-20 shrink-0 text-right">{labels[key] || key}</span>
            <div className="flex-1 flex items-center gap-2">
              <div className="flex-1 h-7 bg-slate-100 rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full ${colors[key] || 'bg-blue-500'} transition-all flex items-center justify-end pr-2`}
                  style={{ width: `${barPct}%`, minWidth: count > 0 ? '32px' : '0' }}
                >
                  {barPct > 20 && <span className="text-xs font-bold text-white drop-shadow-sm">{count}</span>}
                </div>
              </div>
              {barPct <= 20 && <span className="text-xs font-bold text-slate-700 shrink-0">{count}</span>}
            </div>
            <span className="text-xs text-slate-400 w-12 shrink-0 text-right">{pct}%</span>
          </div>
        );
      })}
    </div>
  );
}

const DashboardPage: React.FC<{ pt: string | null }> = ({ pt }) => {
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    api.getStats(pt ?? undefined)
      .then(setStats)
      .catch((e: any) => {
        setStats(null);
        setError(e?.message || '无法加载统计数据，请确认后端服务已启动');
      })
      .finally(() => setLoading(false));
  }, [pt]);

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-6">
      <h2 className="text-xl font-bold text-slate-900">数据概览</h2>
      {loading ? (
        <div className="space-y-6">
          <div className="grid grid-cols-3 gap-6">
            {Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-32 rounded-2xl" />)}
          </div>
          <Skeleton className="h-48 rounded-2xl" />
        </div>
      ) : error ? (
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6">
          <ErrorBox message={error} onRetry={() => {
            setLoading(true);
            setError(null);
            api.getStats(pt ?? undefined)
              .then(setStats)
              .catch((e: any) => {
                setStats(null);
                setError(e?.message || '无法加载统计数据，请确认后端服务已启动');
              })
              .finally(() => setLoading(false));
          }} />
        </div>
      ) : stats ? (
        <div className="space-y-6">
          {/* Summary card */}
          <div className="bg-white p-6 rounded-2xl border border-slate-200 shadow-sm">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-xl bg-blue-100 flex items-center justify-center">
                <Database size={20} className="text-blue-600" />
              </div>
              <div>
                <h3 className="text-2xl font-bold text-slate-900">{stats.totalCount.toLocaleString()}</h3>
                <p className="text-sm text-slate-500">已处理总数</p>
              </div>
            </div>
          </div>

          {/* Validity type distribution */}
          <div className="bg-white p-6 rounded-2xl border border-slate-200 shadow-sm">
            <h3 className="text-sm font-bold text-slate-700 mb-4 flex items-center gap-2">
              <Clock size={16} className="text-blue-500" />
              有效期类型分布
            </h3>
            <DistributionBar
              distribution={stats.validityTypeDistribution}
              labels={VALIDITY_TYPE_LABELS}
              colors={VALIDITY_TYPE_COLORS}
            />
          </div>
        </div>
      ) : (
        <div className="text-center py-12 text-sm text-slate-400">暂无数据</div>
      )}
    </motion.div>
  );
}

// --- Review List (Task 11.2) ---

const ValidityTypeBadge = ({ type }: { type: string | null }) => {
  if (!type) return <span className="text-xs text-slate-400">-</span>;
  const colorMap: Record<string, string> = {
    exact_date: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    fuzzy_time: 'bg-amber-50 text-amber-700 border-amber-200',
    no_validity: 'bg-slate-50 text-slate-600 border-slate-200',
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${colorMap[type] || 'bg-slate-50 text-slate-600 border-slate-200'}`}>
      {VALIDITY_TYPE_LABELS[type] || type}
    </span>
  );
};

const AnnotationBadge = ({ type }: { type: string | null }) => {
  if (!type) {
    return (
      <span className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs font-medium text-slate-400">
        未标注
      </span>
    );
  }

  return (
    <span className="inline-flex items-center rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-700">
      {VALIDITY_TYPE_LABELS[type] || type}
    </span>
  );
};

const ReviewListPage: React.FC<{ pt: string | null; onSelectRun: (id: string) => void }> = ({ pt, onSelectRun }) => {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [annotationStatus, setAnnotationStatus] = useState<'all' | 'annotated' | 'unannotated'>('all');
  const limit = 20;

  useEffect(() => {
    setOffset(0);
  }, [pt, annotationStatus]);

  useEffect(() => {
    setLoading(true);
    setError(null);
    api.getRuns(
      offset,
      limit,
      pt ?? undefined,
      annotationStatus === 'all' ? undefined : annotationStatus
    )
      .then(r => { setRuns(r.items); setTotal(r.total); })
      .catch((e: any) => {
        setRuns([]);
        setTotal(0);
        setError(e?.message || '无法加载审核列表，请确认后端服务已启动');
      })
      .finally(() => setLoading(false));
  }, [pt, offset, annotationStatus]);

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-6">
      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="border-b border-slate-100 px-6 py-5">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h2 className="text-xl font-bold text-slate-900">全部审核记录</h2>
              <p className="mt-1 text-sm text-slate-500">点击任意记录查看详情链路并追加人工标注。</p>
            </div>
            <span className="text-sm text-slate-500">共 {total} 条</span>
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-2">
            {[
              { key: 'all', label: '全部' },
              { key: 'annotated', label: '已标注' },
              { key: 'unannotated', label: '待标注' },
            ].map((item) => (
              <button
                key={item.key}
                onClick={() => setAnnotationStatus(item.key as 'all' | 'annotated' | 'unannotated')}
                className={`rounded-xl px-4 py-2 text-sm font-medium transition-colors ${
                  annotationStatus === item.key
                    ? 'bg-blue-600 text-white shadow-sm shadow-blue-500/20'
                    : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                }`}
              >
                {item.label}
              </button>
            ))}
          </div>
        </div>

        {loading ? (
          <div className="p-6 space-y-3">
            {Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} className="h-12 w-full rounded-lg" />)}
          </div>
        ) : error ? (
          <div className="p-6">
            <ErrorBox
              message={error}
              onRetry={() => {
                setLoading(true);
                setError(null);
                api.getRuns(
                  offset,
                  limit,
                  pt ?? undefined,
                  annotationStatus === 'all' ? undefined : annotationStatus
                )
                  .then(r => { setRuns(r.items); setTotal(r.total); })
                  .catch((e: any) => {
                    setRuns([]);
                    setTotal(0);
                    setError(e?.message || '无法加载审核列表，请确认后端服务已启动');
                  })
                  .finally(() => setLoading(false));
              }}
            />
          </div>
        ) : runs.length > 0 ? (
          <>
            {/* Table header */}
            <div className="px-6 py-3 bg-slate-50 border-b border-slate-200 grid grid-cols-12 gap-4 text-xs font-medium text-slate-500">
              <div className="col-span-2">Info ID</div>
              <div className="col-span-2">有效期类型</div>
              <div className="col-span-2">标注</div>
              <div className="col-span-2">预估截止时间</div>
              <div className="col-span-2">风险提示</div>
              <div className="col-span-1">状态</div>
              <div className="col-span-1"></div>
            </div>
            {/* Table rows */}
            <div className="divide-y divide-slate-100">
              {runs.map(run => (
                <div
                  key={run.runId}
                  onClick={() => onSelectRun(run.runId)}
                  className="px-6 py-4 grid grid-cols-12 gap-4 items-center hover:bg-slate-50 cursor-pointer transition-colors"
                >
                  <div className="col-span-2 min-w-0">
                    <div className="text-sm font-medium text-slate-900 truncate" title={run.entityKey}>{run.entityKey}</div>
                    <div className="text-xs text-slate-400 mt-0.5">{formatDateTime(run.timestamp ?? undefined)}</div>
                  </div>
                  <div className="col-span-2">
                    <ValidityTypeBadge type={run.validityType} />
                  </div>
                  <div className="col-span-2">
                    <AnnotationBadge type={run.annotatedLabel} />
                    <div className="mt-1 text-xs text-slate-400">
                      {run.annotations.length > 0 ? `${run.annotations.length} 次标注` : '待处理'}
                    </div>
                  </div>
                  <div className="col-span-2">
                    <span className="text-sm text-slate-700">{formatDateTime(run.estimatedExpiry ?? undefined)}</span>
                  </div>
                  <div className="col-span-2 flex flex-wrap gap-1">
                    {run.staleRiskHint && (
                      <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs bg-orange-50 text-orange-600 border border-orange-200">
                        过期风险
                      </span>
                    )}
                    {run.complaintRiskHint?.hasComplaintSignal && (
                      <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs bg-red-50 text-red-600 border border-red-200">
                        投诉
                      </span>
                    )}
                    {!run.staleRiskHint && !run.complaintRiskHint?.hasComplaintSignal && (
                      <span className="text-xs text-slate-400">-</span>
                    )}
                  </div>
                  <div className="col-span-1">
                    {run.errorType ? (
                      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border bg-red-50 text-red-600 border-red-200">
                        {run.errorType}
                      </span>
                    ) : (
                      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border bg-emerald-50 text-emerald-700 border-emerald-200">
                        <CheckCircle2 size={12} className="mr-1" />成功
                      </span>
                    )}
                  </div>
                  <div className="col-span-1 flex justify-end">
                    <ChevronRight size={16} className="text-slate-300" />
                  </div>
                </div>
              ))}
            </div>
          </>
        ) : (
          <div className="p-8 text-center text-sm text-slate-400">暂无记录</div>
        )}
      </div>

      {/* Pagination */}
      {total > limit && (
        <div className="flex items-center justify-center gap-3">
          <button
            onClick={() => setOffset(Math.max(0, offset - limit))}
            disabled={offset === 0}
            className="px-4 py-2 text-sm border border-slate-200 rounded-lg disabled:opacity-50 hover:bg-slate-50 transition-colors"
          >
            上一页
          </button>
          <span className="text-sm text-slate-500">
            {offset + 1}-{Math.min(offset + limit, total)} / {total}
          </span>
          <button
            onClick={() => setOffset(offset + limit)}
            disabled={offset + limit >= total}
            className="px-4 py-2 text-sm border border-slate-200 rounded-lg disabled:opacity-50 hover:bg-slate-50 transition-colors"
          >
            下一页
          </button>
        </div>
      )}
    </motion.div>
  );
}

const FallbacksPage: React.FC<{ pt: string | null }> = ({ pt }) => {
  const [fallbacks, setFallbacks] = useState<FallbackRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(true);
  const limit = 20;

  useEffect(() => {
    setLoading(true);
    api.getFallbacks(offset, limit, pt ?? undefined)
      .then(r => { setFallbacks(r.items); setTotal(r.total); })
      .catch(() => { setFallbacks([]); setTotal(0); })
      .finally(() => setLoading(false));
  }, [pt, offset]);

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-slate-900">降级记录</h2>
        <span className="text-sm text-slate-500">共 {total} 条</span>
      </div>

      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
        {loading ? (
          <div className="p-6 space-y-3">
            {Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} className="h-12 w-full rounded-lg" />)}
          </div>
        ) : fallbacks.length > 0 ? (
          <>
            <div className="px-6 py-3 bg-slate-50 border-b border-slate-200 grid grid-cols-12 gap-4 text-xs font-medium text-slate-500">
              <div className="col-span-4">Entity Key</div>
              <div className="col-span-3">错误类型</div>
              <div className="col-span-5">审计信息</div>
            </div>
            <div className="divide-y divide-slate-100">
              {fallbacks.map((fb, idx) => (
                <div key={`${fb.entityKey}-${idx}`} className="px-6 py-4 grid grid-cols-12 gap-4 items-center">
                  <div className="col-span-4 text-sm font-medium text-slate-900 truncate">{fb.entityKey}</div>
                  <div className="col-span-3">
                    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border bg-red-50 text-red-600 border-red-200">
                      {fb.errorType || '未知'}
                    </span>
                  </div>
                  <div className="col-span-5 text-xs text-slate-500 truncate">
                    {fb.decisionRecord ? JSON.stringify(fb.decisionRecord).slice(0, 80) + '...' : '-'}
                  </div>
                </div>
              ))}
            </div>
          </>
        ) : (
          <div className="p-8 text-center text-sm text-slate-400">暂无降级记录</div>
        )}
      </div>

      {total > limit && (
        <div className="flex items-center justify-center gap-3">
          <button
            onClick={() => setOffset(Math.max(0, offset - limit))}
            disabled={offset === 0}
            className="px-4 py-2 text-sm border border-slate-200 rounded-lg disabled:opacity-50 hover:bg-slate-50 transition-colors"
          >
            上一页
          </button>
          <span className="text-sm text-slate-500">
            {offset + 1}-{Math.min(offset + limit, total)} / {total}
          </span>
          <button
            onClick={() => setOffset(offset + limit)}
            disabled={offset + limit >= total}
            className="px-4 py-2 text-sm border border-slate-200 rounded-lg disabled:opacity-50 hover:bg-slate-50 transition-colors"
          >
            下一页
          </button>
        </div>
      )}
    </motion.div>
  );
}

const SettingsPage: React.FC<{ showToast: (msg: string, isError?: boolean) => void }> = ({ showToast }) => {
  type SettingsFieldKey = keyof SettingsResponse;
  type SettingsFormState = {
    [K in SettingsFieldKey]: SettingsResponse[K] extends boolean ? boolean : string;
  };

  const settingsFields: Array<{
    key: SettingsFieldKey;
    label: string;
    desc: string;
    type: 'text' | 'number' | 'boolean';
    min?: number;
    max?: number;
  }> = [
    { key: 'llmModel', label: 'LLM 模型名称', desc: '用于新鲜度推理的大语言模型标识。', type: 'text' },
    { key: 'llmTimeoutSec', label: '请求超时时间 (秒)', desc: '单次 LLM 请求的最大等待时间。', type: 'number', min: 1 },
    { key: 'llmMaxRetry', label: '最大重试次数', desc: 'LLM 请求失败后的最大重试次数。', type: 'number', min: 0 },
    { key: 'workerCount', label: '并发工作线程数', desc: '批量任务运行时的并行 worker 数，建议 1-32。', type: 'number', min: 1, max: 32 },
    { key: 'providerRateLimitPerMinute', label: '每分钟限流次数', desc: 'Provider 每分钟允许的最大请求次数。', type: 'number', min: 1 },
    { key: 'maxInFlight', label: '最大并发任务数', desc: '同一时刻允许进行中的 LLM 请求数。', type: 'number', min: 1 },
    { key: 'batchMaxRows', label: '单次最大条数', desc: '批量处理单次允许的最大记录数。', type: 'number', min: 1, max: 100 },
    { key: 'fetchOnlyFilledComplaints', label: '仅拉取投诉命中已招满数据', desc: '开启后，`fetch` / `fetch-run` 拉数阶段只保留投诉文本命中“已招满”的记录。', type: 'boolean' },
  ];

  const promptTemplates = [
    {
      title: '信号检测 (Signal Detection)',
      version: 'v1',
      fileName: 'signal_detection_v1.yaml',
      path: 'backend/src/job_freshness/prompts/signal_detection_v1.yaml',
      summary: '负责识别职位文本中的时效相关信号。',
    },
    {
      title: '时间归一化 (Time Normalization)',
      version: 'v1',
      fileName: 'time_normalization_v1.yaml',
      path: 'backend/src/job_freshness/prompts/time_normalization_v1.yaml',
      summary: '将相对时间、工期、开工时间等表达归一化为结构化结果。',
    },
    {
      title: '最终裁决 (Final Decision)',
      version: 'v1',
      fileName: 'final_decision_v1.yaml',
      path: 'backend/src/job_freshness/prompts/final_decision_v1.yaml',
      summary: '综合信号、风险和归一化结果输出最终新鲜度判断。',
    },
  ];

  const toSettingsForm = (value: SettingsResponse): SettingsFormState => ({
    llmModel: value.llmModel,
    llmTimeoutSec: String(value.llmTimeoutSec),
    llmMaxRetry: String(value.llmMaxRetry),
    workerCount: String(value.workerCount),
    providerRateLimitPerMinute: String(value.providerRateLimitPerMinute),
    maxInFlight: String(value.maxInFlight),
    batchMaxRows: String(value.batchMaxRows),
    fetchOnlyFilledComplaints: value.fetchOnlyFilledComplaints,
  });

  const [settings, setSettings] = useState<SettingsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [settingsError, setSettingsError] = useState<string | null>(null);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [settingsForm, setSettingsForm] = useState<SettingsFormState | null>(null);
  const [settingsValidation, setSettingsValidation] = useState<Partial<Record<SettingsFieldKey, string>>>({});

  const fetchSettings = useCallback(() => {
    setLoading(true);
    setSettingsError(null);
    api.getSettings()
      .then((value) => {
        setSettings(value);
        setSettingsForm(toSettingsForm(value));
        setSettingsValidation({});
      })
      .catch((error: any) => {
        setSettings(null);
        setSettingsError(error?.message || '无法加载系统设置');
      })
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    fetchSettings();
  }, [fetchSettings]);

  const validateSettings = (): boolean => {
    if (!settingsForm) return false;

    const nextErrors: Partial<Record<SettingsFieldKey, string>> = {};

    settingsFields.forEach((field) => {
      if (field.type === 'boolean') {
        return;
      }

      const rawValue = String(settingsForm[field.key]).trim();
      if (!rawValue) {
        nextErrors[field.key] = '该字段不能为空';
        return;
      }

      if (field.type === 'number') {
        const numericValue = Number(rawValue);
        if (!Number.isFinite(numericValue) || !Number.isInteger(numericValue)) {
          nextErrors[field.key] = '请输入整数';
          return;
        }
        if (field.min != null && numericValue < field.min) {
          nextErrors[field.key] = `不能小于 ${field.min}`;
          return;
        }
        if (field.max != null && numericValue > field.max) {
          nextErrors[field.key] = `不能大于 ${field.max}`;
        }
      }
    });

    setSettingsValidation(nextErrors);
    return Object.keys(nextErrors).length === 0;
  };

  const handleSaveSettings = async () => {
    if (!settingsForm || !validateSettings()) return;

    setSettingsSaving(true);
    try {
      const updated = await api.updateSettings({
        llmModel: String(settingsForm.llmModel).trim(),
        llmTimeoutSec: Number(settingsForm.llmTimeoutSec),
        llmMaxRetry: Number(settingsForm.llmMaxRetry),
        workerCount: Number(settingsForm.workerCount),
        providerRateLimitPerMinute: Number(settingsForm.providerRateLimitPerMinute),
        maxInFlight: Number(settingsForm.maxInFlight),
        batchMaxRows: Number(settingsForm.batchMaxRows),
        fetchOnlyFilledComplaints: settingsForm.fetchOnlyFilledComplaints,
      });
      setSettings(updated);
      setSettingsForm(toSettingsForm(updated));
      setSettingsValidation({});
      showToast('系统设置已保存');
    } catch (error: any) {
      showToast(error?.message || '系统设置保存失败', true);
    } finally {
      setSettingsSaving(false);
    }
  };

  const dirtyCount = settings && settingsForm
    ? settingsFields.filter((field) => {
        if (field.type === 'boolean') {
          return settingsForm[field.key] !== settings[field.key];
        }
        return String(settingsForm[field.key]) !== String(settings[field.key]);
      }).length
    : 0;

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-6">
      <div>
        <h2 className="text-xl font-bold text-slate-900">系统设置</h2>
        <p className="mt-1 text-sm text-slate-500">保持单栏布局，只展示必要配置和 Prompt 模板信息。</p>
      </div>

      {loading ? (
        <>
          <Skeleton className="h-[640px] rounded-2xl" />
          <Skeleton className="h-[220px] rounded-2xl" />
        </>
      ) : settingsError ? (
        <div className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
          <ErrorBox message={settingsError} onRetry={fetchSettings} />
        </div>
      ) : settings && settingsForm ? (
        <>
          <div className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
            <div className="flex flex-col gap-3 border-b border-slate-100 px-6 py-5 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <h3 className="text-lg font-semibold text-slate-900">系统运行时配置</h3>
                <p className="mt-1 text-sm text-slate-500">修改流水线运行参数，保存后立即生效。</p>
              </div>
              <div className="flex items-center gap-3">
                <span className="text-xs text-slate-400">{dirtyCount > 0 ? `已修改 ${dirtyCount} 项` : '未修改'}</span>
                <button
                  onClick={handleSaveSettings}
                  disabled={loading || settingsSaving || dirtyCount === 0}
                  className="inline-flex items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-blue-700 disabled:bg-slate-300"
                >
                  {settingsSaving ? <><Loader2 size={14} className="animate-spin" /> 保存中...</> : <><Save size={14} /> 保存设置</>}
                </button>
              </div>
            </div>

            <div className="divide-y divide-slate-100">
              {settingsFields.map((field) => (
                <div key={field.key} className="px-6 py-5">
                  <div className="mb-3 flex items-center justify-between gap-4">
                    <label className="text-sm font-medium text-slate-900">{field.label}</label>
                    <span className="text-xs text-slate-400 shrink-0">
                      当前值: {field.type === 'boolean' ? (settings[field.key] ? '开启' : '关闭') : String(settings[field.key])}
                    </span>
                  </div>
                  {field.type === 'boolean' ? (
                    <label className="flex items-center justify-between rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
                      <div className="pr-4">
                        <div className="text-sm font-medium text-slate-800">
                          {settingsForm[field.key] ? '已开启' : '已关闭'}
                        </div>
                        <div className="mt-1 text-xs text-slate-400">该开关只影响拉数阶段，不影响在线查询。</div>
                      </div>
                      <button
                        type="button"
                        role="switch"
                        aria-checked={Boolean(settingsForm[field.key])}
                        onClick={() => {
                          setSettingsForm((prev) => prev ? { ...prev, [field.key]: !prev[field.key] } : prev);
                        }}
                        className={`relative inline-flex h-7 w-12 shrink-0 items-center rounded-full transition-colors ${
                          settingsForm[field.key] ? 'bg-blue-600' : 'bg-slate-300'
                        }`}
                      >
                        <span
                          className={`inline-block h-5 w-5 transform rounded-full bg-white transition-transform ${
                            settingsForm[field.key] ? 'translate-x-6' : 'translate-x-1'
                          }`}
                        />
                      </button>
                    </label>
                  ) : (
                    <input
                      type={field.type}
                      min={field.min}
                      max={field.max}
                      value={String(settingsForm[field.key])}
                      onChange={(event) => {
                        const value = event.target.value;
                        setSettingsForm((prev) => prev ? { ...prev, [field.key]: value } : prev);
                        setSettingsValidation((prev) => {
                          const next = { ...prev };
                          delete next[field.key];
                          return next;
                        });
                      }}
                      className={`w-full rounded-xl border px-4 py-3 text-sm outline-none transition-colors focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20 ${
                        settingsValidation[field.key]
                          ? 'border-red-300 bg-red-50/50'
                          : 'border-slate-200 bg-white'
                      }`}
                    />
                  )}
                  {settingsValidation[field.key] ? (
                    <p className="mt-2 text-xs text-red-500">{settingsValidation[field.key]}</p>
                  ) : null}
                  <p className="mt-2 text-xs text-slate-400">{field.desc}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
            <div className="border-b border-slate-100 px-6 py-5">
              <h3 className="text-lg font-semibold text-slate-900">Prompt 模板</h3>
              <p className="mt-1 text-sm text-slate-500">模板文件来自 `backend/src/job_freshness/prompts` 目录。</p>
            </div>
            <div className="divide-y divide-slate-100">
              {promptTemplates.map((item) => (
                <details key={item.fileName} className="group">
                  <summary className="flex cursor-pointer items-center justify-between px-6 py-5 hover:bg-slate-50/60">
                    <div className="flex items-center gap-3">
                      <span className="text-sm font-medium text-slate-900">{item.title}</span>
                      <span className="rounded bg-slate-100 px-2 py-0.5 text-[11px] text-slate-500">{item.version}</span>
                    </div>
                    <ChevronRight size={16} className="text-slate-400 transition-transform group-open:rotate-90" />
                  </summary>
                  <div className="space-y-2 px-6 pb-5 text-sm text-slate-600">
                    <p>{item.summary}</p>
                    <p className="text-xs text-slate-400">模板文件: {item.fileName}</p>
                    <p className="text-xs text-slate-400 break-all">路径: {item.path}</p>
                  </div>
                </details>
              ))}
            </div>
          </div>
        </>
      ) : (
        <div className="text-center py-12 text-sm text-slate-400">无法加载设置</div>
      )}
    </motion.div>
  );
}


// --- Online Query Page (在线查询) ---

const OnlineQueryPage: React.FC<{
  latestPt: string | null;
  onSelectRun: (id: string) => void;
  showToast: (msg: string, isError?: boolean) => void;
}> = ({ latestPt, onSelectRun, showToast }) => {
  const [infoIdsText, setInfoIdsText] = useState('');
  const [pt, setPt] = useState(latestPt ?? '');
  const [loading, setLoading] = useState(false);
  const [response, setResponse] = useState<OnlineQueryResponse | null>(null);
  const [queryStages, setQueryStages] = useState<QueryFlowStage[]>(() => buildOnlineQueryStages());
  const [requestedIds, setRequestedIds] = useState<string[]>([]);
  const [queryDurationMs, setQueryDurationMs] = useState<number | null>(null);
  const [queryError, setQueryError] = useState<string | null>(null);
  const queryTimersRef = useRef<Array<ReturnType<typeof setTimeout>>>([]);

  useEffect(() => {
    if (latestPt && !pt) setPt(latestPt);
  }, [latestPt, pt]);

  const clearQueryTimers = useCallback(() => {
    queryTimersRef.current.forEach((timer) => clearTimeout(timer));
    queryTimersRef.current = [];
  }, []);

  useEffect(() => () => clearQueryTimers(), [clearQueryTimers]);

  const handleQuery = async () => {
    const ids = infoIdsText
      .split(/[\n,]+/)
      .map(s => s.trim())
      .filter(Boolean);
    if (ids.length === 0) {
      showToast('请输入至少一个 info_id', true);
      return;
    }
    if (!pt) {
      showToast('请选择业务日期', true);
      return;
    }

    const startedAt = performance.now();
    clearQueryTimers();
    setLoading(true);
    setResponse(null);
    setRequestedIds(ids);
    setQueryDurationMs(null);
    setQueryError(null);
    setQueryStages(buildOnlineQueryStages([
      { status: 'done', elapsedMs: 0, message: `已校验 ${ids.length} 个 info_id` },
      { status: 'running', message: `按 ${pt} 查询 ODPS 宽表并准备执行流水线` },
    ]));

    queryTimersRef.current = [
      setTimeout(() => {
        setQueryStages(buildOnlineQueryStages([
          { status: 'done', elapsedMs: 40, message: `已校验 ${ids.length} 个 info_id` },
          { status: 'done', elapsedMs: 280, message: 'ODPS 宽表查询完成，开始匹配召回规则' },
          { status: 'running', message: '正在检查时效与投诉相关命中' },
        ]));
      }, 280),
      setTimeout(() => {
        setQueryStages(buildOnlineQueryStages([
          { status: 'done', elapsedMs: 40, message: `已校验 ${ids.length} 个 info_id` },
          { status: 'done', elapsedMs: 280, message: 'ODPS 宽表查询完成，开始匹配召回规则' },
          { status: 'done', elapsedMs: 620, message: '规则召回完成，进入新鲜度判断' },
          { status: 'running', message: '正在组合信号、风险与时间归一化结果' },
        ]));
      }, 620),
      setTimeout(() => {
        setQueryStages(buildOnlineQueryStages([
          { status: 'done', elapsedMs: 40, message: `已校验 ${ids.length} 个 info_id` },
          { status: 'done', elapsedMs: 280, message: 'ODPS 宽表查询完成，开始匹配召回规则' },
          { status: 'done', elapsedMs: 620, message: '规则召回完成，进入新鲜度判断' },
          { status: 'done', elapsedMs: 920, message: '新鲜度判断完成，开始汇总结果' },
          { status: 'running', message: '正在覆盖旧结果并整理命中/未命中列表' },
        ]));
      }, 920),
    ];

    try {
      const res = await api.onlineQuery({ infoIds: ids, pt });
      const totalMs = Math.round(performance.now() - startedAt);
      const formalCount = res.results.filter((item) => item.route === 'formal').length;
      const fallbackCount = res.results.length - formalCount;

      clearQueryTimers();
      setResponse(res);
      setQueryDurationMs(totalMs);
      setQueryStages(buildOnlineQueryStages([
        { status: 'done', elapsedMs: 40, message: `已校验 ${ids.length} 个 info_id` },
        { status: 'done', elapsedMs: Math.min(totalMs, 280), message: `ODPS 命中 ${res.results.length} 条待执行记录` },
        { status: 'done', elapsedMs: Math.min(totalMs, 620), message: `完成召回匹配，未命中 ${res.notFound.length} 个 info_id` },
        { status: 'done', elapsedMs: Math.min(totalMs, 920), message: `formal ${formalCount} 条，fallback ${fallbackCount} 条` },
        { status: 'done', elapsedMs: totalMs, message: '结果已覆盖写回，可继续查看详情链路' },
      ]));
    } catch (e: any) {
      const message = e.message || '查询失败';
      const totalMs = Math.round(performance.now() - startedAt);

      clearQueryTimers();
      setQueryDurationMs(totalMs);
      setQueryError(message);
      setQueryStages(buildOnlineQueryStages([
        { status: 'done', elapsedMs: 0, message: `已校验 ${ids.length} 个 info_id` },
        { status: 'error', elapsedMs: totalMs, message },
      ]));
      showToast(message, true);
    } finally {
      setLoading(false);
    }
  };

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-6">
      <h2 className="text-xl font-bold text-slate-900">在线查询</h2>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.1fr)_minmax(360px,0.9fr)]">
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6 space-y-5">
          <div className="flex items-start justify-between gap-4">
            <div>
              <h3 className="text-sm font-bold text-slate-800">查询参数</h3>
              <p className="mt-1 text-xs text-slate-500">支持多条 `info_id` 批量在线发起 ODPS 查询；若该 `info_id` 已跑过，会用最新结果覆盖旧记录。</p>
            </div>
            <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-500">
              当前 pt: {pt || '-'}
            </span>
          </div>

          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Info IDs</label>
            <textarea
              value={infoIdsText}
              onChange={e => setInfoIdsText(e.target.value)}
              placeholder="输入 info_id，每行一个"
              rows={7}
              className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none resize-y font-mono"
            />
            <p className="mt-2 text-xs text-slate-400">
              已识别 {infoIdsText.split(/[\n,]+/).map((item) => item.trim()).filter(Boolean).length} 个待查询 ID
            </p>
          </div>

          <div className="flex flex-col gap-4 sm:flex-row sm:items-end">
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">业务日期 (pt)</label>
              <input
                value={pt}
                onChange={e => setPt(e.target.value)}
                placeholder="yyyymmdd"
                className="w-full sm:w-40 px-3 py-2 border border-slate-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none"
              />
            </div>
            <button
              onClick={handleQuery}
              disabled={loading}
              className="px-5 py-2 bg-blue-600 text-white text-sm font-medium rounded-xl hover:bg-blue-700 disabled:bg-blue-400 transition-colors flex items-center justify-center gap-2"
            >
              {loading ? <><Loader2 size={14} className="animate-spin" /> 查询中...</> : <><Search size={14} /> 开始查询</>}
            </button>
          </div>
        </div>

        <OnlineQueryFlowCard
          stages={queryStages}
          loading={loading}
          response={response}
          queryError={queryError}
          queryDurationMs={queryDurationMs}
          requestedCount={requestedIds.length}
        />
      </div>

      {/* Results */}
      {response && (
        <div className="space-y-4">
          {/* Not found */}
          {response.notFound.length > 0 && (
            <div className="bg-amber-50 border border-amber-200 rounded-xl p-4">
              <div className="text-sm font-medium text-amber-700 mb-1">未找到以下 info_id ({response.notFound.length} 个)</div>
              <div className="text-xs text-amber-600 font-mono">{response.notFound.join(', ')}</div>
            </div>
          )}

          {/* Found results */}
          {response.results.length > 0 && (
            <div className="space-y-3">
              <div className="flex items-center justify-between gap-4">
                <div className="text-sm text-slate-500">找到 {response.results.length} 条结果</div>
                <div className="text-xs text-slate-400">点击卡片可进入完整推理流程</div>
              </div>
              {response.results.map(run => (
                <div
                  key={run.runId}
                  onClick={() => onSelectRun(run.runId)}
                  className="bg-white rounded-xl border border-slate-200 shadow-sm p-5 hover:border-blue-300 hover:shadow-md cursor-pointer transition-all"
                >
                  <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between mb-4">
                    <div className="space-y-2 min-w-0">
                      <div className="text-sm font-bold text-slate-900">{run.entityKey}</div>
                      <InlineProcessPills run={run} />
                    </div>
                    <div className="flex items-center gap-2 shrink-0">
                      <StatusBadge status={run.route} />
                      <ChevronRight size={16} className="text-slate-300" />
                    </div>
                  </div>
                  <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                    {run.temporalSignalRecord && (
                      <>
                        <div>
                          <div className="text-xs text-slate-400">时效状态</div>
                          <span className={`inline-flex items-center mt-1 px-2 py-0.5 rounded-full text-xs font-medium border ${
                            run.temporalSignalRecord.temporalStatus === 'has_signal'
                              ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
                              : run.temporalSignalRecord.temporalStatus === 'no_signal'
                                ? 'bg-slate-50 text-slate-600 border-slate-200'
                                : 'bg-amber-50 text-amber-700 border-amber-200'
                          }`}>
                            {TEMPORAL_STATUS_LABELS[run.temporalSignalRecord.temporalStatus] || run.temporalSignalRecord.temporalStatus}
                          </span>
                        </div>
                        <div>
                          <div className="text-xs text-slate-400">信号类型</div>
                          <span className="inline-flex items-center mt-1 px-2 py-0.5 rounded-full text-xs font-medium border bg-blue-50 text-blue-700 border-blue-200">
                            {SIGNAL_TYPE_LABELS[run.temporalSignalRecord.signalType] || run.temporalSignalRecord.signalType}
                          </span>
                        </div>
                        <div>
                          <div className="text-xs text-slate-400">置信度</div>
                          <span className={`inline-flex items-center mt-1 px-2 py-0.5 rounded-full text-xs font-medium border ${getConfidenceBadgeClass(run.temporalSignalRecord.confidence)}`}>
                            {(run.temporalSignalRecord.confidence * 100).toFixed(0)}%
                          </span>
                        </div>
                      </>
                    )}
                    {run.riskRecord && (
                      <div>
                        <div className="text-xs text-slate-400">风险</div>
                        <div className="flex gap-1 mt-1 flex-wrap">
                          {run.riskRecord.fillStatus !== 'not_filled' && (
                            <span className={`px-1.5 py-0.5 rounded text-xs border ${getFillStatusBadgeClass(run.riskRecord.fillStatus)}`}>
                              {getFillStatusLabel(run.riskRecord.fillStatus)}
                            </span>
                          )}
                          {run.riskRecord.isUnreachable && <span className="px-1.5 py-0.5 rounded text-xs bg-red-50 text-red-600 border border-red-200">联系不上</span>}
                          {run.riskRecord.fillStatus === 'not_filled' && !run.riskRecord.isUnreachable && <span className="text-xs text-slate-400">-</span>}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}

          {response.results.length === 0 && response.notFound.length === 0 && (
            <div className="text-center py-8 text-sm text-slate-400">无结果</div>
          )}
        </div>
      )}
    </motion.div>
  );
};
