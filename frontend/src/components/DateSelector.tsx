import React from 'react';
import { Calendar, Clock } from 'lucide-react';
import type { DateEntry } from '../api/types';

interface DateSelectorProps {
  dates: DateEntry[];
  latestPt: string | null;
  selectedPt: string | null;
  loading: boolean;
  onSelect: (pt: string) => void;
}

/** Format yyyymmdd → yyyy-mm-dd, show "历史数据" for _legacy */
function formatPt(pt: string): string {
  if (pt === '_legacy') return '历史数据';
  if (/^\d{8}$/.test(pt)) return `${pt.slice(0, 4)}-${pt.slice(4, 6)}-${pt.slice(6, 8)}`;
  // Range: 20260410_20260412 → 2026-04-10 ~ 2026-04-12
  const rangeMatch = pt.match(/^(\d{8})_(\d{8})$/);
  if (rangeMatch) {
    const [, s, e] = rangeMatch;
    return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)} ~ ${e.slice(0, 4)}-${e.slice(4, 6)}-${e.slice(6, 8)}`;
  }
  return pt;
}

export default function DateSelector({ dates, latestPt, selectedPt, loading, onSelect }: DateSelectorProps) {
  const isLegacy = selectedPt === '_legacy';
  const hasLegacy = dates.some(d => d.pt === '_legacy');
  const dateDates = dates.filter(d => d.pt !== '_legacy');

  return (
    <div className="flex items-center gap-3">
      <div className="flex items-center gap-2 text-slate-500">
        <Calendar size={16} />
        <span className="text-xs font-medium">数据日期</span>
      </div>

      <select
        value={isLegacy ? '__legacy_placeholder__' : (selectedPt ?? '')}
        onChange={e => onSelect(e.target.value)}
        disabled={loading}
        className={`px-3 py-1.5 border rounded-lg text-sm bg-white focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none disabled:opacity-50 disabled:cursor-not-allowed ${
          isLegacy ? 'border-amber-300 text-amber-600' : 'border-slate-200'
        }`}
      >
        {isLegacy && (
          <option value="__legacy_placeholder__" disabled>
            当前: 历史数据 — 选择日期切回
          </option>
        )}
        {dateDates.map(d => (
          <option key={d.pt} value={d.pt}>
            {formatPt(d.pt)}（{d.recordCount} 条）
          </option>
        ))}
      </select>

      {hasLegacy && (
        <button
          onClick={() => isLegacy ? onSelect(latestPt ?? dateDates[0]?.pt ?? '') : onSelect('_legacy')}
          disabled={loading}
          className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed ${
            isLegacy
              ? 'bg-amber-100 text-amber-700 border border-amber-300'
              : 'bg-slate-100 text-slate-600 hover:bg-slate-200 border border-slate-200'
          }`}
        >
          <Clock size={14} />
          {isLegacy ? '返回最新' : '历史数据'}
        </button>
      )}

      {loading && (
        <div className="w-4 h-4 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
      )}
    </div>
  );
}
