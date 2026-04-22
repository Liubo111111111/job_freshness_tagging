/**
 * TypeScript type definitions corresponding to backend Pydantic models.
 * Field names use camelCase; the API client handles snake_case → camelCase conversion.
 */

// --- 新鲜度业务类型 ---

export interface TemporalSignalRecord {
  hasTemporalSignal: boolean;
  temporalStatus: string;
  signalType: string;
  normalizable: boolean;
  anchorType: string;
  workStartAt: string | null;
  recruitmentValidUntil: string | null;
  durationHours: number | null;
  evidenceSummary: string[];
  interpretation: string;
  confidence: number;
  cannotDetermineReason: string | null;
}

export interface ComplaintRiskHint {
  hasComplaintSignal: boolean;
  complaintSignalType: string;
  complaintSummary: string;
}

export interface RiskRecord {
  staleRiskHint: boolean;
  complaintRiskHint: ComplaintRiskHint;
  riskScore: number;
  riskReasons: string[];
  confidence: number;
}

export interface FreshnessDecisionRecord {
  temporalStatus: string;
  signalType: string;
  workStartAt: string | null;
  recruitmentValidUntil: string | null;
  durationHours: number | null;
  normalizable: boolean;
  confidence: number;
  staleRiskHint: boolean;
  complaintRiskHint: ComplaintRiskHint;
  riskScore: number;
  riskReasons: string[];
  evidenceSummary: string[];
  decisionReason: string;
  lowConfidence: boolean;
}

// --- 统计 ---

export interface StatsResponse {
  totalCount: number;
  formalCount: number;
  fallbackCount: number;
  temporalStatusDistribution: Record<string, number>;
  signalTypeDistribution: Record<string, number>;
}

// --- 日期分区 ---

export interface DateEntry {
  pt: string;
  recordCount: number;
}

export interface DatesResponse {
  dates: DateEntry[];
  latestPt: string | null;
}

export interface DailySummaryEntry {
  pt: string;
  totalCount: number;
  formalCount: number;
  fallbackCount: number;
}

export interface DailySummaryResponse {
  summaries: DailySummaryEntry[];
}

// --- 运行记录 ---

export interface RunSummary {
  runId: string;
  entityKey: string;
  temporalStatus: string | null;
  signalType: string | null;
  confidence: number | null;
  staleRiskHint: boolean | null;
  complaintRiskHint: ComplaintRiskHint | null;
  route: 'formal' | 'fallback';
  errorType: string | null;
  timestamp: string | null;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  offset: number;
  limit: number;
}

export interface RuleMatch {
  source: string;
  matchedTerms: string[];
  matchedBucket: string;
}

export interface SnippetRecallRecord {
  hasRecall: boolean;
  matches: RuleMatch[];
  temporalMatchCount: number;
  complaintMatchCount: number;
  matchedSources: string[];
}

export interface SignalDetectionRecord {
  hasTemporalSignal: boolean;
  temporalStatus: string;
  signalType: string;
  evidenceSummary: string[];
  confidence: number;
  cannotDetermineReason: string | null;
}

export interface TimeNormalizationRecord {
  normalizable: boolean;
  anchorType: string;
  workStartAt: string | null;
  recruitmentValidUntil: string | null;
  durationHours: number | null;
  interpretation: string;
  confidence: number;
}

export interface RunDetail {
  runId: string;
  entityKey: string;
  wideRow: Record<string, unknown>;
  rawWideRow: Record<string, unknown> | null;
  snippetRecallRecord: SnippetRecallRecord | null;
  signalDetectionRecord: SignalDetectionRecord | null;
  timeNormalizationRecord: TimeNormalizationRecord | null;
  temporalSignalRecord: TemporalSignalRecord | null;
  riskRecord: RiskRecord | null;
  decisionRecord: FreshnessDecisionRecord | null;
  route: string;
  errorType: string | null;
  audit: Record<string, unknown>;
  timingMs: Record<string, number> | null;
}

// --- 搜索 ---

export interface SearchResult {
  entityKey: string;
  route: string;
  runId: string;
}

// --- 批量任务 ---

export interface BatchRequest {
  pt: string;
  inputPath: string;
  workerCount?: number;
  providerRateLimitPerMinute?: number;
  maxInFlight?: number;
}

export interface BatchAccepted {
  taskId: string;
  message: string;
  status: string;
}

// --- Fallback / 审核 ---

export interface FallbackRecord {
  entityKey: string;
  errorType: string | null;
  decisionRecord: Record<string, unknown> | null;
  audit: Record<string, unknown>;
}

export interface ReviewRequest {
  reviewerNotes?: string;
}

export interface ReviewResponse {
  entityKey: string;
  status: string;
}

// --- Auth ---

export interface AuthUser {
  openId: string;
  name: string;
  enName?: string;
  avatarUrl?: string;
  email?: string;
  enterpriseEmail?: string;
  userId?: string;
  tenantKey?: string;
  isAdmin?: boolean;
}

export interface AuthSession {
  enabled: boolean;
  authenticated: boolean;
  accessDenied?: boolean;
  requestStatus?: string | null;
  user: AuthUser | null;
  loginUrl: string | null;
}

// --- 系统配置 ---

export interface SettingsResponse {
  llmModel: string;
  llmTimeoutSec: number;
  llmMaxRetry: number;
  workerCount: number;
  providerRateLimitPerMinute: number;
  maxInFlight: number;
  batchMaxRows: number;
}

export interface SettingsUpdate {
  llmModel?: string;
  llmTimeoutSec?: number;
  llmMaxRetry?: number;
  workerCount?: number;
  providerRateLimitPerMinute?: number;
  maxInFlight?: number;
  batchMaxRows?: number;
}
