import React, { useState, useCallback } from 'react';
import { Calendar, Search, Database, CheckCircle2, AlertCircle, Loader2, ChevronRight } from 'lucide-react';
import { api } from '../api/client';
import type { DateEntry, StatsResponse, DailySummaryEntry } from '../api/types';

interface DateRangeViewProps {
  dates: DateEntry[];
  onSelectDate: (pt: string) => void;
}

/** Format yyyymmdd → yyyy-mm-dd */
function formatPt(pt: string): string {
  if (/^\d{8}$/.test(pt)) return `${pt.slice(0, 4)}-${pt.slice(4, 6)}-${pt.slice(6, 8)}`;
  return pt;
}

const DateRangeView: React.FC<DateRangeViewProps> = ({ dates, onSelectDate }) => {
  const validDates = dates.filter(d => d.pt !== '_legacy' && /^\d{8}$/.test(d.pt));
  const oldestPt = validDates.length > 0 ? validDates[validDates.length - 1].pt : '';
  const newestPt = validDates.length > 0 ? validDates[0].pt : '';

  const [startPt, setStartPt] = useState(oldestPt);
  const [endPt, setEndPt] = useState(newestPt);
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [summaries, setSummaries] = useState<DailySummaryEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [queried, setQueried] = useState(false);

  const handleQuery = useCallback(async () => {
    if (!startPt || !endPt) return;
    setLoading(true);
    setError(null);
    try {
      const [statsData, summaryData] = await Promise.all([
        api.getStatsRange(startPt, endPt),
        api.getDailySummary(startPt, endPt),
      ]);
      setStats(statsData);
      setSummaries(summaryData.summaries);
      setQueried(true);
    } catch (e: any) {
      setError(e.message || '查询失败');
    } finally {
      setLoading(false);
    }
  }, [startPt, endPt]);

  return (
    <div className="space-y-6">
      {/* Date Range Selector */}
      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6">
        <div className="flex items-center gap-2 text-slate-700 mb-4">
          <Calendar size={16} className="text-blue-500" />
          <span className="text-sm font-bold">日期范围查询</span>
        </div>
        <div className="flex items-end gap-4 flex-wrap">
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1">起始日期</label>
            <select
              value={startPt}
              onChange={e => setStartPt(e.target.value)}
              disabled={loading}
              className="px-3 py-2 border border-slate-200 rounded-lg text-sm bg-white focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none disabled:opacity-50"
            >
              {validDates.map(d => (
                <option key={d.pt} value={d.pt}>{formatPt(d.pt)}（{d.recordCount} 条）</option>
              ))}
            </select>
          </div>
          <div className="text-slate-400 pb-2">至</div>
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1">结束日期</label>
            <select
              value={endPt}
              onChange={e => setEndPt(e.target.value)}
              disabled={loading}
              className="px-3 py-2 border border-slate-200 rounded-lg text-sm bg-white focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none disabled:opacity-50"
            >
              {validDates.map(d => (
                <option key={d.pt} value={d.pt}>{formatPt(d.pt)}（{d.recordCount} 条）</option>
              ))}
            </select>
          </div>
          <button
            onClick={handleQuery}
            disabled={loading || !startPt || !endPt}
            className="flex items-center gap-2 px-5 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-colors"
          >
            {loading ? <Loader2 size={14} className="animate-spin" /> : <Search size={14} />}
            查询
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-sm text-red-600">
          {error}
        </div>
      )}

      {/* Aggregated Stats */}
      {queried && stats && !loading && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-white p-5 rounded-2xl border border-slate-200 shadow-sm">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-xl bg-blue-100 flex items-center justify-center">
                <Database size={20} className="text-blue-600" />
              </div>
            </div>
            <h3 className="text-2xl font-bold text-slate-900">{stats.totalCount.toLocaleString()}</h3>
            <p className="text-sm text-slate-500 mt-1">已处理总数</p>
          </div>
          <div className="bg-white p-5 rounded-2xl border border-slate-200 shadow-sm">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-xl bg-emerald-100 flex items-center justify-center">
                <CheckCircle2 size={20} className="text-emerald-600" />
              </div>
            </div>
            <h3 className="text-2xl font-bold text-slate-900">{stats.formalCount.toLocaleString()}</h3>
            <p className="text-sm text-slate-500 mt-1">正式输出</p>
          </div>
          <div className="bg-white p-5 rounded-2xl border border-slate-200 shadow-sm">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-xl bg-amber-100 flex items-center justify-center">
                <AlertCircle size={20} className="text-amber-600" />
              </div>
            </div>
            <h3 className="text-2xl font-bold text-slate-900">{stats.fallbackCount.toLocaleString()}</h3>
            <p className="text-sm text-slate-500 mt-1">待审核</p>
          </div>
        </div>
      )}

      {/* Daily Summary Cards */}
      {queried && summaries.length > 0 && !loading && (
        <div>
          <h3 className="text-sm font-bold text-slate-800 mb-4">每日摘要</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {summaries.map(s => (
              <button
                key={s.pt}
                onClick={() => onSelectDate(s.pt)}
                className="bg-white rounded-xl border border-slate-200 shadow-sm p-5 text-left hover:border-blue-300 hover:shadow-md transition-all group"
              >
                <div className="flex items-center justify-between mb-3">
                  <span className="text-sm font-bold text-slate-900">{formatPt(s.pt)}</span>
                  <ChevronRight size={16} className="text-slate-300 group-hover:text-blue-500 transition-colors" />
                </div>
                <div className="grid grid-cols-3 gap-2 text-center">
                  <div>
                    <div className="text-lg font-bold text-slate-900">{s.totalCount}</div>
                    <div className="text-[10px] text-slate-400">总数</div>
                  </div>
                  <div>
                    <div className="text-lg font-bold text-emerald-600">{s.formalCount}</div>
                    <div className="text-[10px] text-slate-400">正式</div>
                  </div>
                  <div>
                    <div className="text-lg font-bold text-amber-600">{s.fallbackCount}</div>
                    <div className="text-[10px] text-slate-400">降级</div>
                  </div>
                </div>
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Empty state */}
      {queried && !loading && !error && summaries.length === 0 && (
        <div className="text-center py-12 text-sm text-slate-400">
          该日期范围内无数据
        </div>
      )}

      {/* Loading state */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <Loader2 size={24} className="animate-spin text-blue-500" />
          <span className="ml-3 text-sm text-slate-500">查询中...</span>
        </div>
      )}
    </div>
  );
};

export default DateRangeView;
