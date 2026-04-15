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

export type UserRole = 'super_admin' | 'admin' | 'client_user' | 'viewer';

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
  | 'support_tickets';

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

export interface UploadResponse {
  masterType: MasterType;
  fileName: string;
  rowCount: number;
  columns: string[];
  uploadedAt: string;
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
  totalOrders: number;
  repeatCustomers: number;
  highValue: number;
  churned: number;
  churnRate: number;
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

// Derived from orders (joined with customers) for dashboard table
export interface OrderRow {
  orderId: string;               // = order_id
  customer: string;              // = customer_name (joined)
  date: string;                  // = order_date
  items: number;                 // = order_item_count
  gross: number;                 // = order_value_usd
  discount: number;              // = discount_usd
  net: number;                   // derived: order_value_usd - discount_usd
  status: string;                // = order_status (normalised to lower-case by API)
  couponCode?: string | null;    // = coupon_code
  paymentMethod?: string;        // = payment_method
}

export interface DashboardData {
  kpis: DashboardKpis;
  segments: SegmentDistribution[];
  churnBreakdown: SegmentDistribution[];
  tiers: TierDistribution[];
  repeatVsOneTime: { repeat: number; oneTime: number; total: number };
  recentOrders: OrderRow[];
  totalOrderPages: number;
}


// ── Analytics (aggregated / computed — no direct DB table) ────
export interface ClientMetric {
  clientId: string;
  clientName: string;
  customers: number;
  orders: number;
  churnPct: number;
  highValue: number;
  lastRun: string;
  avgDuration: number;
  color: string;
}

export interface MonthlyTrend {
  month: string;
  runsByClient: Record<string, number>;
  totalRuns: number;
  avgDurationSeconds: number;
}

export interface AnalyticsData {
  platformKpis: {
    activeClients: number;
    totalClients: number;
    totalCustomers: number;
    totalOrders: number;
    avgChurnRate: number;
    pipelineRunsLast30: number;
  };
  clientMetrics: ClientMetric[];
  monthlyTrend: MonthlyTrend[];
}


// ── Messages (richer version of value_propositions for the UI) ─
// DB-aligned keys use snake_case matching value_propositions columns.
// subject / body / active / updatedAt / id are managed by the
// messages API and do not exist in the seed schema.

// Frontend-normalised enums (mapped from DB values in the service layer)
export type TierKey   = 'platinum' | 'gold' | 'silver' | 'bronze';
export type RiskLevel = 'at_risk' | 'returning' | 'reactivated' | 'new';
export type Channel   = 'email' | 'sms' | 'push' | 'email_sms' | 'email_push' | 'push_sms';

// Mapping helpers — use in services to convert DB ↔ frontend values
export const DB_TO_TIER_KEY: Record<TierName, TierKey> = {
  Platinum: 'platinum', Gold: 'gold', Silver: 'silver', Bronze: 'bronze'
};
export const TIER_KEY_TO_DB: Record<TierKey, TierName> = {
  platinum: 'Platinum', gold: 'Gold', silver: 'Silver', bronze: 'Bronze'
};

export const DB_TO_RISK_LEVEL: Record<DbRiskLevel, RiskLevel> = {
  'At-Risk': 'at_risk', Returning: 'returning', Reactivated: 'reactivated', New: 'new'
};
export const RISK_LEVEL_TO_DB: Record<RiskLevel, DbRiskLevel> = {
  at_risk: 'At-Risk', returning: 'Returning', reactivated: 'Reactivated', new: 'New'
};

export const DB_TO_CHANNEL: Record<DbChannel, Channel> = {
  Email: 'email', SMS: 'sms', Push: 'push',
  'Email + SMS': 'email_sms', 'Email + Push': 'email_push', 'Push + SMS': 'push_sms'
};
export const CHANNEL_TO_DB: Record<Channel, DbChannel> = {
  email: 'Email', sms: 'SMS', push: 'Push',
  email_sms: 'Email + SMS', email_push: 'Email + Push', push_sms: 'Push + SMS'
};

export interface MessageTemplate {
  id: string;                    // frontend-generated / messages API
  // DB-aligned fields matching value_propositions columns (normalised)
  tier_name: TierKey;            // DB: tier_name  (Platinum → 'platinum')
  risk_level: RiskLevel;         // DB: risk_level (At-Risk  → 'at_risk')
  discount_pct: number;          // DB: discount_pct
  channel: Channel;              // DB: channel    (Email + SMS → 'email_sms')
  action_type: string;           // DB: action_type
  message_template: string;      // DB: message_template (raw text, {placeholders})
  priority: number;              // DB: priority
  // Extended fields (messages API only, not in seed schema)
  subject: string;
  body: string;
  active: boolean;
  updatedAt: string;
}

export interface SaveTemplatesRequest {
  clientId: string;
  templates: MessageTemplate[];
}
