import React, { useState, useEffect, useCallback } from 'react';
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
  Send,
  XCircle,
  Search,
  Sparkles,
  ShieldAlert,
  Timer,
  FileText,
  Eraser,
  LogOut,
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
} from './api/types';
import DateSelector from './components/DateSelector';
import DateRangeView from './components/DateRangeView';

// --- Utility ---
function formatDateTime(value?: string): string {
  if (!value) return '-';
  const trimmed = value.trim();
  const normalized = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(trimmed)
    ? `${trimmed.replace(' ', 'T')}Z`
    : trimmed;
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return value ?? '-';
  return parsed.toLocaleString('zh-CN', { hour12: false });
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
  if (!raw && !cleaned) return null;
  return (
    <div className="space-y-1">
      <div className="text-xs font-medium text-slate-500">{label}</div>
      {hasChange ? (
        <div className="space-y-1">
          <pre className="text-xs whitespace-pre-wrap break-words rounded-lg p-2.5 bg-red-50 text-slate-400 line-through max-h-32 overflow-y-auto border border-red-100">{raw || '（无内容）'}</pre>
          <pre className="text-xs whitespace-pre-wrap break-words rounded-lg p-2.5 bg-emerald-50 text-slate-800 max-h-32 overflow-y-auto border border-emerald-100">{cleaned || '（无内容）'}</pre>
        </div>
      ) : (
        <pre className="text-xs whitespace-pre-wrap break-words rounded-lg p-2.5 bg-slate-50 text-slate-700 max-h-32 overflow-y-auto">{raw || '（无内容）'}</pre>
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
          <h2 className="text-lg font-bold text-slate-900 flex items-center gap-3">
            {run.entityKey}
            <StatusBadge status={run.route} />
          </h2>
        </div>
        {run.errorType && (
          <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium border bg-red-50 text-red-600 border-red-200">
            错误: {run.errorType}
          </span>
        )}
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
                <DetailField label="投诉次数" value={getField(wideRow, 'complaintCount', 'complaint_count') || '-'} />
              </div>
              <div className="pt-2 border-t border-slate-100">
                <DetailField label="路由结果" value={run.route === 'formal' ? '正式输出 (formal)' : '待审核 (fallback)'} />
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
                    <div>
                      <div className="text-xs text-slate-400 mb-1">置信度</div>
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${getConfidenceBadgeClass(risk.confidence)}`}>
                        {(risk.confidence * 100).toFixed(0)}%
                      </span>
                    </div>
                    <DetailField label="投诉信号" value={risk.complaintRiskHint.hasComplaintSignal ? '有' : '无'} highlight={risk.complaintRiskHint.hasComplaintSignal} />
                  </div>
                  {risk.complaintRiskHint.hasComplaintSignal && (
                    <div className="bg-red-50 border border-red-200 rounded-lg p-3 space-y-1 text-xs">
                      <div className="font-medium text-red-700">投诉详情</div>
                      <div className="text-red-600">类型: {risk.complaintRiskHint.complaintSignalType}</div>
                      {risk.complaintRiskHint.complaintSummary && (
                        <div className="text-red-600">摘要: {risk.complaintRiskHint.complaintSummary}</div>
                      )}
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
                    <DetailField label="时效状态" value={TEMPORAL_STATUS_LABELS[decision.temporalStatus] || decision.temporalStatus} />
                    <DetailField label="信号类型" value={SIGNAL_TYPE_LABELS[decision.signalType] || decision.signalType} />
                    <div>
                      <div className="text-xs text-slate-400 mb-1">置信度</div>
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${getConfidenceBadgeClass(decision.confidence)}`}>
                        {(decision.confidence * 100).toFixed(0)}%
                      </span>
                    </div>
                    <DetailField label="低置信" value={decision.lowConfidence ? '是' : '否'} highlight={decision.lowConfidence} />
                    <DetailField label="过期风险" value={decision.staleRiskHint ? '是' : '否'} highlight={decision.staleRiskHint} />
                    <div>
                      <div className="text-xs text-slate-400 mb-1">风险分数</div>
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${
                        decision.riskScore >= 0.7 ? 'bg-red-50 text-red-600 border-red-200' :
                        decision.riskScore >= 0.4 ? 'bg-amber-50 text-amber-700 border-amber-200' :
                        'bg-emerald-50 text-emerald-700 border-emerald-200'
                      }`}>
                        {(decision.riskScore * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>
                  {decision.decisionReason && (
                    <div>
                      <div className="text-xs text-slate-400 mb-1">决策理由</div>
                      <p className="text-xs text-slate-700 bg-blue-50 rounded-lg p-2.5">{decision.decisionReason}</p>
                    </div>
                  )}
                  <div className="pt-2 border-t border-slate-100">
                    <span className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-bold border ${
                      run.route === 'formal'
                        ? 'bg-emerald-50 text-emerald-700 border-emerald-300'
                        : 'bg-amber-50 text-amber-700 border-amber-300'
                    }`}>
                      {run.route === 'formal' ? <CheckCircle2 size={14} /> : <AlertCircle size={14} />}
                      {run.route === 'formal' ? '正式输出 (formal)' : '待审核 (fallback)'}
                    </span>
                  </div>
                </div>
              ) : (
                <div className="text-xs text-slate-400 text-center py-4">无最终决策数据</div>
              )}
            </TimelineStep>
          </div>
        </div>
      </div>
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
type Page = 'dashboard' | 'review' | 'fallbacks' | 'settings' | 'date-range';

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
      .catch(() => {})
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
          <SidebarItem icon={AlertCircle} label="降级记录" active={page === 'fallbacks'} onClick={() => { setPage('fallbacks'); setSelectedRunId(null); }} />
          <SidebarItem icon={Activity} label="日期范围" active={page === 'date-range'} onClick={() => { setPage('date-range'); setSelectedRunId(null); }} />
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
            ) : page === 'fallbacks' ? (
              <FallbacksPage key="fallbacks" pt={selectedPt} />
            ) : page === 'date-range' ? (
              <DateRangeView key="date-range" dates={dates} onSelectDate={pt => { setSelectedPt(pt); setPage('dashboard'); }} />
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

function DistributionBar({ distribution, labels, colors }: {
  distribution: Record<string, number>;
  labels: Record<string, string>;
  colors: Record<string, string>;
}) {
  const total = Object.values(distribution).reduce((a, b) => a + b, 0);
  if (total === 0) return <div className="text-sm text-slate-400 text-center py-4">暂无数据</div>;

  const entries = Object.entries(distribution).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]);

  return (
    <div className="space-y-3">
      {/* Stacked bar */}
      <div className="flex h-4 rounded-full overflow-hidden bg-slate-100">
        {entries.map(([key, count]) => (
          <div
            key={key}
            className={`${colors[key] || 'bg-slate-300'} transition-all`}
            style={{ width: `${(count / total) * 100}%` }}
            title={`${labels[key] || key}: ${count} (${((count / total) * 100).toFixed(1)}%)`}
          />
        ))}
      </div>
      {/* Legend */}
      <div className="flex flex-wrap gap-x-4 gap-y-2">
        {entries.map(([key, count]) => (
          <div key={key} className="flex items-center gap-2">
            <div className={`w-3 h-3 rounded-sm ${colors[key] || 'bg-slate-300'}`} />
            <span className="text-xs text-slate-600">{labels[key] || key}</span>
            <span className="text-xs font-medium text-slate-900">{count}</span>
            <span className="text-xs text-slate-400">({((count / total) * 100).toFixed(1)}%)</span>
          </div>
        ))}
      </div>
    </div>
  );
}

const DashboardPage: React.FC<{ pt: string | null }> = ({ pt }) => {
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api.getStats(pt ?? undefined)
      .then(setStats)
      .catch(() => setStats(null))
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

          {/* Temporal status distribution */}
          <div className="bg-white p-6 rounded-2xl border border-slate-200 shadow-sm">
            <h3 className="text-sm font-bold text-slate-700 mb-4 flex items-center gap-2">
              <Clock size={16} className="text-blue-500" />
              时效状态分布
            </h3>
            <DistributionBar
              distribution={stats.temporalStatusDistribution}
              labels={TEMPORAL_STATUS_LABELS}
              colors={TEMPORAL_STATUS_COLORS}
            />
          </div>

          {/* Signal type distribution */}
          <div className="bg-white p-6 rounded-2xl border border-slate-200 shadow-sm">
            <h3 className="text-sm font-bold text-slate-700 mb-4 flex items-center gap-2">
              <Activity size={16} className="text-indigo-500" />
              信号类型分布
            </h3>
            <DistributionBar
              distribution={stats.signalTypeDistribution}
              labels={SIGNAL_TYPE_LABELS}
              colors={SIGNAL_TYPE_COLORS}
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

const TemporalStatusBadge = ({ status }: { status: string | null }) => {
  if (!status) return <span className="text-xs text-slate-400">-</span>;
  const colorMap: Record<string, string> = {
    has_signal: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    no_signal: 'bg-slate-50 text-slate-600 border-slate-200',
    cannot_determine: 'bg-amber-50 text-amber-700 border-amber-200',
    conflict: 'bg-red-50 text-red-600 border-red-200',
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${colorMap[status] || 'bg-slate-50 text-slate-600 border-slate-200'}`}>
      {TEMPORAL_STATUS_LABELS[status] || status}
    </span>
  );
};

const SignalTypeBadge = ({ type }: { type: string | null }) => {
  if (!type) return <span className="text-xs text-slate-400">-</span>;
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border bg-blue-50 text-blue-700 border-blue-200">
      {SIGNAL_TYPE_LABELS[type] || type}
    </span>
  );
};

const ReviewListPage: React.FC<{ pt: string | null; onSelectRun: (id: string) => void }> = ({ pt, onSelectRun }) => {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(true);
  const limit = 20;

  useEffect(() => {
    setLoading(true);
    api.getRuns(offset, limit, pt ?? undefined)
      .then(r => { setRuns(r.items); setTotal(r.total); })
      .catch(() => { setRuns([]); setTotal(0); })
      .finally(() => setLoading(false));
  }, [pt, offset]);

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-slate-900">审核列表</h2>
        <span className="text-sm text-slate-500">共 {total} 条</span>
      </div>

      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
        {loading ? (
          <div className="p-6 space-y-3">
            {Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} className="h-12 w-full rounded-lg" />)}
          </div>
        ) : runs.length > 0 ? (
          <>
            {/* Table header */}
            <div className="px-6 py-3 bg-slate-50 border-b border-slate-200 grid grid-cols-12 gap-4 text-xs font-medium text-slate-500">
              <div className="col-span-2">Info ID</div>
              <div className="col-span-2">时效状态</div>
              <div className="col-span-2">信号类型</div>
              <div className="col-span-1">置信度</div>
              <div className="col-span-2">风险提示</div>
              <div className="col-span-2">路由</div>
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
                    <TemporalStatusBadge status={run.temporalStatus} />
                  </div>
                  <div className="col-span-2">
                    <SignalTypeBadge type={run.signalType} />
                  </div>
                  <div className="col-span-1">
                    {run.confidence != null ? (
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${getConfidenceBadgeClass(run.confidence)}`}>
                        {(run.confidence * 100).toFixed(0)}%
                      </span>
                    ) : (
                      <span className="text-xs text-slate-400">-</span>
                    )}
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
                  <div className="col-span-2">
                    <StatusBadge status={run.route} />
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
  const [settings, setSettings] = useState<SettingsResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getSettings()
      .then(setSettings)
      .catch(() => setSettings(null))
      .finally(() => setLoading(false));
  }, []);

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="space-y-6">
      <h2 className="text-xl font-bold text-slate-900">系统设置</h2>
      {loading ? (
        <Skeleton className="h-64 rounded-2xl" />
      ) : settings ? (
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <div className="text-xs text-slate-400 mb-1">LLM 模型</div>
              <div className="text-sm font-medium text-slate-900">{settings.llmModel}</div>
            </div>
            <div>
              <div className="text-xs text-slate-400 mb-1">超时时间</div>
              <div className="text-sm font-medium text-slate-900">{settings.llmTimeoutSec}s</div>
            </div>
            <div>
              <div className="text-xs text-slate-400 mb-1">最大重试</div>
              <div className="text-sm font-medium text-slate-900">{settings.llmMaxRetry}</div>
            </div>
            <div>
              <div className="text-xs text-slate-400 mb-1">并发线程</div>
              <div className="text-sm font-medium text-slate-900">{settings.workerCount}</div>
            </div>
          </div>
        </div>
      ) : (
        <div className="text-center py-12 text-sm text-slate-400">无法加载设置</div>
      )}
    </motion.div>
  );
}