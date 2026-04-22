import type {
  AuthSession,
  StatsResponse,
  PaginatedResponse,
  RunSummary,
  RunDetail,
  SearchResult,
  BatchRequest,
  BatchAccepted,
  FallbackRecord,
  ReviewRequest,
  ReviewResponse,
  SettingsResponse,
  SettingsUpdate,
  DatesResponse,
  DailySummaryResponse,
} from './types';

const BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api';
const TIMEOUT_MS = 10_000;

// snake_case → camelCase deep converter
function toCamelCase(obj: unknown): unknown {
  if (Array.isArray(obj)) return obj.map(toCamelCase);
  if (obj !== null && typeof obj === 'object') {
    return Object.fromEntries(
      Object.entries(obj as Record<string, unknown>).map(([k, v]) => [
        k.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase()),
        toCamelCase(v),
      ])
    );
  }
  return obj;
}

// camelCase → snake_case deep converter (for request bodies)
function toSnakeCase(obj: unknown): unknown {
  if (Array.isArray(obj)) return obj.map(toSnakeCase);
  if (obj !== null && typeof obj === 'object') {
    return Object.fromEntries(
      Object.entries(obj as Record<string, unknown>).map(([k, v]) => [
        k.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`),
        toSnakeCase(v),
      ])
    );
  }
  return obj;
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = 'ApiError';
  }
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
    const resp = await fetch(`${BASE_URL}${path}`, {
      ...options,
      credentials: 'include',
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
    });

    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      throw new ApiError(resp.status, body.detail || resp.statusText);
    }

    const json = await resp.json();
    return toCamelCase(json) as T;
  } catch (err) {
    if (err instanceof ApiError) throw err;
    if ((err as Error).name === 'AbortError') {
      throw new ApiError(0, 'Request timed out');
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

export const api = {
  // --- Auth ---
  getAuthSession: () => request<AuthSession>('/auth/session'),

  getLoginUrl: (next = '/') => `${BASE_URL}/auth/login?next=${encodeURIComponent(next)}`,

  logout: () =>
    request<{ status: string }>('/auth/logout', {
      method: 'POST',
    }),

  // --- Dates ---
  getDates: () => request<DatesResponse>('/dates'),

  getDailySummary: (ptStart: string, ptEnd: string) =>
    request<DailySummaryResponse>(`/daily-summary?pt_start=${ptStart}&pt_end=${ptEnd}`),

  // --- Stats ---
  getStats: (pt?: string) => {
    const params = new URLSearchParams();
    if (pt) params.set('pt', pt);
    const qs = params.toString();
    return request<StatsResponse>(`/stats${qs ? `?${qs}` : ''}`);
  },

  getStatsRange: (ptStart: string, ptEnd: string) =>
    request<StatsResponse>(`/stats?pt_start=${ptStart}&pt_end=${ptEnd}`),

  // --- Runs ---
  getRuns: (offset = 0, limit = 20, pt?: string) => {
    const params = new URLSearchParams({ offset: String(offset), limit: String(limit) });
    if (pt) params.set('pt', pt);
    return request<PaginatedResponse<RunSummary>>(`/runs?${params}`);
  },

  getRunDetail: (runId: string, pt?: string) => {
    const params = new URLSearchParams();
    if (pt) params.set('pt', pt);
    const qs = params.toString();
    return request<RunDetail>(`/runs/${encodeURIComponent(runId)}${qs ? `?${qs}` : ''}`);
  },

  // --- Search ---
  search: (query: string, pt?: string) => {
    const params = new URLSearchParams({ query });
    if (pt) params.set('pt', pt);
    return request<SearchResult[]>(`/search?${params}`);
  },

  // --- Batch ---
  triggerBatch: (params: BatchRequest) =>
    request<BatchAccepted>('/batch', {
      method: 'POST',
      body: JSON.stringify(toSnakeCase(params)),
    }),

  // --- Fallbacks ---
  getFallbacks: (offset = 0, limit = 20, pt?: string) => {
    const params = new URLSearchParams({ offset: String(offset), limit: String(limit) });
    if (pt) params.set('pt', pt);
    return request<PaginatedResponse<FallbackRecord>>(`/fallbacks?${params}`);
  },

  submitReview: (entityKey: string, review: ReviewRequest, pt?: string) => {
    const params = new URLSearchParams();
    if (pt) params.set('pt', pt);
    const qs = params.toString();
    return request<ReviewResponse>(
      `/fallbacks/${encodeURIComponent(entityKey)}/review${qs ? `?${qs}` : ''}`,
      { method: 'PUT', body: JSON.stringify(toSnakeCase(review)) }
    );
  },

  // --- Settings ---
  getSettings: () => request<SettingsResponse>('/settings'),

  updateSettings: (settings: SettingsUpdate) =>
    request<SettingsResponse>('/settings', {
      method: 'PUT',
      body: JSON.stringify(toSnakeCase(settings)),
    }),
};
