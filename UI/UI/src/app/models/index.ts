// ============================================================
// models/index.ts
// Walmart CRP — TypeScript interfaces mirroring the PostgreSQL
// schema exactly. Every field name and casing matches the DB.
//
// HOW TO ADD NEW TABLES / FIELDS IN THE FUTURE:
//   1. Add a new interface below in the appropriate section.
//   2. Add a corresponding service in /services/ following the
//      same pattern as existing services (inject ApiService,
//      expose signals, expose Observable methods).
//   3. Register the route/page in app.routes.ts if needed.
// ============================================================


// ── Platform / Auth (app-level, no DB table) ─────────────────
// These are managed by the backend auth system, not in the seed schema.

// The platform only has two user types:
//   super_admin  — operator of the platform (admin console access)
//   client_user  — a tenant-side user (client portal access)
// Older 'admin' / 'viewer' rows are collapsed into 'client_user' by
// backend/db/migration_retire_admin_role.sql and migration_retire_viewer_role.sql.
export type UserRole = 'super_admin' | 'client_user';

export interface AuthUser {
  id: string;
  email: string;
  name: string;
  role: UserRole;
  clientAccess: string[];   // e.g. ['CLT-001'] or ['*'] for all
  token: string;
  refreshToken?: string;
}

export interface LoginRequest  { email: string; password: string; loginRole?: string; }
export interface LoginResponse { user: AuthUser; token: string; refreshToken: string; }

// Platform user management (admins / ops users — not retail customers)
export interface AppUser {
  id: string;
  name: string;
  email: string;
  role: UserRole;
  clientAccess: string[];
  lastLogin: string | null;
  status: 'active' | 'inactive' | 'locked';
  createdAt: string;
}

export interface CreateUserRequest {
  name: string;
  email: string;
  role: UserRole;
  clientAccess: string[];
  password: string;
}


// ── TABLE: client_config ─────────────────────────────────────
export interface ClientConfig {
  client_id: string;            // e.g. 'CLT-001'
  client_name: string;          // e.g. 'Walmart Inc.'
  client_code: string;          // e.g. 'WMT'
  currency: string;             // e.g. 'USD'
  timezone: string;             // e.g. 'America/Chicago'
  churn_window_days: number;    // e.g. 90
  high_ltv_threshold: number;   // e.g. 500.0
  mid_ltv_threshold: number;    // e.g. 250.0
  max_discount_pct: number;     // e.g. 30.0
}


// ── TABLE: customers ─────────────────────────────────────────
export interface Customer {
  client_id: string;
  customer_id: string;           // e.g. 'WMT-CUST-00001'
  customer_email: string;
  customer_name: string;
  customer_phone: string;
  account_created_date: string;  // ISO datetime string
  registration_channel: string;  // e.g. 'Organic Search'
  country_code: string;          // e.g. 'US'
  state: string;
  city: string;
  zip_code: number;
  shipping_address: string;
  preferred_device: string;      // e.g. 'Mobile App'
  email_opt_in: boolean | null;
  sms_opt_in: boolean | null;
}


// ── TABLE: orders ────────────────────────────────────────────
export interface Order {
  client_id: string;
  order_id: string;              // e.g. 'WMT-ORD-H-00001-000'
  customer_id: string;
  order_date: string;            // ISO datetime string
  order_status: string;          // 'Completed' | 'Returned' | 'Pending' | 'Cancelled'
  order_value_usd: number;
  discount_usd: number;
  coupon_code: string | null;
  payment_method: string;        // e.g. 'Debit Card'
  order_item_count: number;
}


// ── TABLE: line_items ────────────────────────────────────────
export interface LineItem {
  client_id: string;
  line_item_id: string;          // e.g. 'LI-00001'
  order_id: string;
  customer_id: string;
  product_id: number;
  quantity: number;
  unit_price_usd: number;
  final_line_total_usd: number;
  item_discount_usd: number;
  item_status: string;           // e.g. 'Returned'
}


// ── TABLE: products ──────────────────────────────────────────
export interface Product {
  product_id: number;
  sku: string;                   // e.g. 'SKU-BEEF-001'
  product_name: string;
  category_id: number;
  sub_category_id: number;
  sub_sub_category_id: number;
  brand_id: number;
  product_price_id: number;
  rating: number;                // e.g. 4.1
  active: 0 | 1;
  not_available: 0 | 1;
}


// ── TABLE: product_prices ─────────────────────────────────────
export interface ProductPrice {
  price_id: number;
  product_id: number;
  qty_range_label: string;       // e.g. '1 unit'
  qty_min: number;
  qty_max: number;
  unit_price_usd: number;
  cost_price_usd: number | null; // supplier cost — enables margin-safe discounts
}


// ── TABLE: product_vendor_mapping ─────────────────────────────
export interface ProductVendorMapping {
  pv_id: number;
  product_id: number;
  brand_id: number;
  vendor_id: number;
}


// ── TABLE: categories ────────────────────────────────────────
export interface Category {
  category_id: number;
  category_name: string;         // e.g. 'Electronics'
}


// ── TABLE: sub_categories ────────────────────────────────────
export interface SubCategory {
  sub_category_id: number;
  sub_category_name: string;
  category_id: number;
}


// ── TABLE: sub_sub_categories ────────────────────────────────
export interface SubSubCategory {
  sub_sub_category_id: number;
  sub_sub_category_name: string;
  sub_category_id: number;
  category_id: number;
}


// ── TABLE: brands ────────────────────────────────────────────
export interface Brand {
  brand_id: number;
  brand_name: string;
  brand_description: string;
  vendor_id: number;
  active: 0 | 1;
  not_available: 0 | 1;
  category_hint: string;         // e.g. 'Electronics'
}


// ── TABLE: vendors ───────────────────────────────────────────
export interface Vendor {
  vendor_id: number;
  vendor_name: string;
  vendor_description: string;
  vendor_contact_no: string;
  vendor_address: string;
  vendor_email: string;
}


// ── TABLE: value_tiers ───────────────────────────────────────
export type TierName = 'Platinum' | 'Gold' | 'Silver' | 'Bronze';

export interface ValueTier {
  tier_id: string;               // e.g. 'T-01'
  tier_name: TierName;
  threshold_type: string;        // e.g. 'quartile'
  threshold_value: number;       // e.g. 0.75
  description: string;
  benefits: string;              // pipe-separated list
}


// ── TABLE: business_segments ─────────────────────────────────
export interface BusinessSegment {
  segment_id: string;            // e.g. 'SEG-001'
  segment_name: string;          // e.g. 'Champions'
  description: string;
  criteria: string;              // e.g. 'rfm_total_score >= 12'
  recommended_focus: string;
}


// ── TABLE: value_propositions ─────────────────────────────────
export type DbRiskLevel  = 'At-Risk' | 'Returning' | 'Reactivated' | 'New';
export type DbChannel    = 'Email' | 'SMS' | 'Push' | 'Email + SMS' | 'Email + Push' | 'Push + SMS';
export type DbActionType = 'Personal Outreach' | 'Loyalty Reward' | 'Reactivation' | 'Welcome';

export interface ValueProposition {
  tier_name: TierName;
  risk_level: DbRiskLevel;
  action_type: DbActionType;
  message_template: string;      // raw template with {name} placeholders
  discount_pct: number;
  channel: DbChannel;
  priority: number;
}


// ── TABLE: customer_reviews ──────────────────────────────────
export type ReviewSentiment = 'positive' | 'negative' | 'neutral';

export interface CustomerReview {
  client_id: string;
  review_id: string;             // e.g. 'REV-00001'
  customer_id: string;
  product_id: number;
  order_id: string;
  rating: number;                // 1–5
  review_text: string;
  review_date: string;           // ISO date string
  sentiment: ReviewSentiment;
}


// ── TABLE: support_tickets ───────────────────────────────────
export type TicketPriority = 'low' | 'medium' | 'high' | 'critical';
export type TicketStatus   = 'open' | 'in_progress' | 'resolved' | 'closed';

export interface SupportTicket {
  client_id: string;
  ticket_id: string;             // e.g. 'TKT-00001'
  customer_id: string;
  ticket_type: string;           // e.g. 'Billing Query'
  priority: TicketPriority;
  status: TicketStatus;
  channel: string;               // e.g. 'Email'
  opened_date: string;           // ISO datetime string
  resolved_date: string | null;
  resolution_time_hrs: number | null;
}


// ── Upload ───────────────────────────────────────────────────
// MasterType keys map 1:1 to DB tables that accept file uploads.
export type MasterType =
  | 'customer'
  | 'order'
  | 'line_items'
  | 'product'
  | 'price'
  | 'vendor_map'
  | 'category'
  | 'sub_category'
  | 'sub_sub_category'
  | 'brand'
  | 'vendor'
  | 'customer_reviews'
  | 'support_tickets'
  | 'login_event';

export type UploadStatus = 'idle' | 'uploading' | 'success' | 'error';

export interface UploadedFile {
  masterType: MasterType;
  fileName: string;
  fileSize: number;
  rowCount: number;
  uploadedAt: string;
  status: UploadStatus;
  errorMessage?: string;
}

export interface SourceOption {
  key: string;
  label: string;
  customer_match: string[];
  custom: boolean;
}

export interface MatchReport {
  matched: number;
  skipped: number;
  skippedSample: string[];
}

export interface UploadResponse {
  masterType: MasterType;
  fileName: string;
  rowCount: number;
  columns: string[];
  uploadedAt: string;
  matchReport?: MatchReport | null;
}

/** First-N-rows preview of a staged master file (GET /uploads/preview). */
export interface UploadPreview {
  masterType: MasterType;
  fileName: string;
  columns: string[];
  rows: (string | number | boolean | null)[][];
  shownRows: number;
  totalRows: number;
}


// ── Upload Batch Lifecycle ───────────────────────────────────
// Multi-tenant staging flow: files land in staging_* tables tied to
// a pending batch, then user commits (moves to real tables) or discards.

export type BatchStatus = 'pending' | 'committed' | 'discarded';

export interface BatchFileSummary {
  masterType: MasterType;
  rowCount: number;
}

export interface PendingBatch {
  batchId: string;
  createdAt: string | null;
  status: BatchStatus;
  totalRows: number;
  files: BatchFileSummary[];
}

export interface BatchInfoResponse {
  pendingBatch: PendingBatch | null;
}

export interface CommitResponse {
  committed: true;
  batchId: string;
  rowsCommitted: Record<string, number>;
  mvRefreshed: boolean;
  mvRefreshWarning?: string;
}

export interface DiscardResponse {
  discarded: boolean;
  batchId?: string;
  rowsDeleted?: number;
  reason?: string;
}


// ── Pipeline / Run ───────────────────────────────────────────
export type PipelineStageStatus = 'pending' | 'running' | 'done' | 'error';

export interface PipelineStage {
  stage: number;
  label: string;
  status: PipelineStageStatus;
  message: string;
  timestamp: string | null;
}

export interface PipelineRunRequest {
  clientId: string;
  mode: 'churn' | 'retention' | 'segmentation' | 'full';
}

export interface PipelineRunResponse {
  jobId: string;
  status: 'queued' | 'running' | 'complete' | 'failed';
  progress: number;
  stages: PipelineStage[];
  startedAt: string;
  completedAt?: string;
  durationSeconds?: number;
  summary?: PipelineSummary;
}

export interface PipelineSummary {
  totalCustomers: number;
  totalOrders: number;
  totalLineItems: number;
  churned: number;
  churnRate: number;
  atRisk: number;
  highValue: number;
  repeatCustomers: number;
  mlFeatures: number;
  outputSheets: number;
}


// ── Dashboard (aggregated / computed — no direct DB table) ────
export interface DashboardKpis {
  totalCustomers: number;
  // Customers present in mv_customer_features — i.e. those with at least one
  // non-Cancelled order, so the ML pipeline has features / churn scores for
  // them. Always <= totalCustomers; the diff is unscoredCustomers.
  scoredCustomers?: number;
  unscoredCustomers?: number;
  totalOrders: number;
  repeatCustomers: number;
  highValue: number;
  churned: number;
  churnRate: number;
  // Current values from the client's Settings page — dashboard labels
  // interpolate these so "inactive ≥ X days" and "X+ orders" match what
  // the user actually configured.
  churnWindowDays?: number;
  minRepeatOrders?: number;
  // 2026-04-25: highValuePercentile removed. Backend no longer returns it
  // because the high_value_percentile column was dropped from client_config.
  // The High Value KPI tile now shows the Platinum-tier customer count.
  lastRunDate: string;
}

// Mirrors business_segments for UI display
export interface SegmentDistribution {
  label: string;    // = segment_name
  count: number;
  pct: number;
  color: string;    // UI-only, assigned by frontend
}

// Mirrors value_tiers for UI display
export interface TierDistribution {
  label: string;    // = tier_name (with emoji prefix added by frontend)
  count: number;
  pct: number;
  color: string;    // UI-only
}

// OrderRow type removed 2026-04-29 along with the /dashboard/orders
// endpoint and recentOrders payload field. Restore from git history
// if the dashboard's Detail Data Tabs ever return.

export interface DashboardData {
  kpis: DashboardKpis;
  segments: SegmentDistribution[];
  churnBreakdown: SegmentDistribution[];
  tiers: TierDistribution[];
  repeatVsOneTime: { repeat: number; oneTime: number; total: number };
}


// ── Analytics (aggregated / computed — no direct DB table) ────
// Pipeline-run fields (lastRun, avgDuration, pipelineRunsLast30,
// monthlyTrend) were removed because there is no pipeline_runs log
// table backing them. The backend either hardcoded them or counted
// `orders` and mislabeled them as "runs".
export interface ClientMetric {
  clientId: string;
  clientName: string;
  customers: number;
  orders: number;
  churnPct: number;
  highValue: number;
  color: string;
}

export interface AnalyticsData {
  platformKpis: {
    activeClients: number;
    totalClients: number;
    totalCustomers: number;
    totalOrders: number;
    avgChurnRate: number;
  };
  clientMetrics: ClientMetric[];
}


// 2026-04-25: Removed the entire Messages section.
// The Analyst Agent's Message Templates page was retired because outreach
// generation is the Retention Agent's responsibility. Removed from this file:
//   * type aliases:  TierKey, RiskLevel, Channel
//   * mapping consts: DB_TO_TIER_KEY, TIER_KEY_TO_DB,
//                     DB_TO_RISK_LEVEL, RISK_LEVEL_TO_DB,
//                     DB_TO_CHANNEL, CHANNEL_TO_DB
//   * interfaces:    MessageTemplate, SaveTemplatesRequest
// The DB-side enum types (TierName, DbRiskLevel, DbChannel) stay because
// segments.service.ts and value_propositions still consume them.
