import { HttpInterceptorFn, HttpResponse } from '@angular/common/http';
import { of, delay } from 'rxjs';
import { environment } from '../../environments/environment';

// ── Mock data store ────────────────────────────────────────────────────────────
const MOCK_TOKEN   = 'mock-jwt-token-dev-only';
const MOCK_REFRESH = 'mock-refresh-token-dev-only';

const MOCK_ADMIN_USER = {
  id: 'usr-001', email: 'admin@walmart.com', name: 'Admin User',
  role: 'super_admin', clientAccess: ['*'],
  token: MOCK_TOKEN, refreshToken: MOCK_REFRESH,
};

const MOCK_CLIENT_USER = {
  id: 'usr-006', email: 'ops@walmart.com', name: 'Walmart Ops',
  role: 'client_user', clientAccess: ['CLT-001'],
  token: MOCK_TOKEN, refreshToken: MOCK_REFRESH,
};

let mockUsers = [
  { id:'usr-001', name:'Raj Mehta',        email:'raj@analytics.com',     role:'super_admin', clientAccess:['*'],           lastLogin:'2026-03-17', status:'active',   createdAt:'2025-01-01' },
  { id:'usr-002', name:'Priya Sharma',     email:'priya@analytics.com',   role:'super_admin', clientAccess:['*'],           lastLogin:'2026-03-16', status:'active',   createdAt:'2025-01-01' },
  { id:'usr-003', name:'David Kim',        email:'d.kim@analytics.com',   role:'admin',       clientAccess:['CLT-001','CLT-002'], lastLogin:'2026-03-17', status:'active', createdAt:'2025-02-01' },
  { id:'usr-004', name:'Sara Lee',         email:'s.lee@analytics.com',   role:'admin',       clientAccess:['CLT-003','CLT-004'], lastLogin:'2026-03-15', status:'active', createdAt:'2025-03-01' },
  { id:'usr-005', name:'Tom Baker',        email:'t.baker@analytics.com', role:'admin',       clientAccess:['CLT-005'],     lastLogin:'2026-03-10', status:'inactive', createdAt:'2025-04-01' },
  { id:'usr-006', name:'Walmart Ops',      email:'ops@walmart.com',       role:'client_user', clientAccess:['CLT-001'],     lastLogin:'2026-03-17', status:'active',   createdAt:'2025-01-15' },
  { id:'usr-007', name:'Target Analytics', email:'bi@target.com',         role:'client_user', clientAccess:['CLT-002'],     lastLogin:'2026-03-14', status:'active',   createdAt:'2025-02-10' },
  { id:'usr-008', name:'Costco BI',        email:'bi@costco.com',         role:'client_user', clientAccess:['CLT-003'],     lastLogin:'2026-03-12', status:'locked',   createdAt:'2025-03-05' },
];

const MOCK_DASHBOARD = {
  kpis: { totalCustomers:200, totalOrders:1894, repeatCustomers:159, highValue:50, churned:88, churnRate:44.0, lastRunDate:'2026-03-17' },
  segments: [
    { label:'Champions',      count:55, pct:27.5 }, { label:'Hibernating',    count:41, pct:20.5 },
    { label:'At-Risk',        count:22, pct:11.0 }, { label:'Loyal',          count:21, pct:10.5 },
    { label:'Potential Loyal',count:20, pct:10.0 }, { label:'New',            count:8,  pct:4.0  },
    { label:'Lost',           count:1,  pct:0.5  },
  ],
  churnBreakdown: [
    { label:'Churned',    count:88, pct:44.0 }, { label:'At-Risk', count:22, pct:11.0 },
    { label:'Returning',  count:51, pct:25.5 }, { label:'Active / New', count:39, pct:19.5 },
  ],
  tiers: [
    { label:'💎 Platinum (Top 25%)', count:50, pct:25 }, { label:'🥇 Gold (25–50%)',   count:50, pct:25 },
    { label:'🥈 Silver (50–75%)',    count:50, pct:25 }, { label:'🥉 Bronze (Bottom)', count:50, pct:25 },
  ],
  repeatVsOneTime: { repeat:159, oneTime:41, total:200 },
  recentOrders: [
    { orderId:'ORD-18940', customer:'Sarah Johnson',   date:'2026-03-10', items:4, gross:142.80, discount:12.00, net:130.80, status:'completed' },
    { orderId:'ORD-18939', customer:'Olivia Martinez', date:'2026-03-12', items:2, gross:89.50,  discount:0,     net:89.50,  status:'completed' },
    { orderId:'ORD-18938', customer:'Michael Brown',   date:'2026-02-28', items:6, gross:230.10, discount:23.01, net:207.09, status:'completed' },
    { orderId:'ORD-18937', customer:'Emma Wilson',     date:'2025-12-15', items:1, gross:45.00,  discount:4.50,  net:40.50,  status:'returned'  },
    { orderId:'ORD-18936', customer:'William Taylor',  date:'2026-03-01', items:3, gross:112.40, discount:0,     net:112.40, status:'completed' },
  ],
  totalOrderPages: 379,
};

const MOCK_ANALYTICS = {
  platformKpis: { activeClients:3, totalClients:5, totalCustomers:697, totalOrders:6526, avgChurnRate:42.3, pipelineRunsLast30:142 },
  clientMetrics: [
    { clientId:'CLT-001', clientName:'Walmart Inc.',     customers:200, orders:1894, churnPct:44.0, highValue:50, lastRun:'2026-03-17 22:43', avgDuration:6.3, color:'' },
    { clientId:'CLT-002', clientName:'Target Corp.',     customers:185, orders:1742, churnPct:39.5, highValue:46, lastRun:'2026-03-16 14:12', avgDuration:5.8, color:'' },
    { clientId:'CLT-003', clientName:'Costco Wholesale', customers:312, orders:2890, churnPct:43.4, highValue:78, lastRun:'2026-03-15 09:30', avgDuration:7.1, color:'' },
  ],
  monthlyTrend: [
    { month:'Jan 2026', runsByClient:{'CLT-001':28,'CLT-002':25,'CLT-003':29}, totalRuns:82, avgDurationSeconds:6.0 },
    { month:'Feb 2026', runsByClient:{'CLT-001':26,'CLT-002':24,'CLT-003':27}, totalRuns:77, avgDurationSeconds:6.1 },
    { month:'Mar 2026', runsByClient:{'CLT-001':17,'CLT-002':16,'CLT-003':18}, totalRuns:51, avgDurationSeconds:6.1 },
  ],
};


const MOCK_CATEGORIES = [
  { category_id:1, category_name:'Clothing & Apparel' },
  { category_id:2, category_name:'Automotive' },
  { category_id:3, category_name:'Baby & Kids' },
  { category_id:4, category_name:'Electronics' },
  { category_id:5, category_name:'Grocery' },
  { category_id:6, category_name:'Health & Beauty' },
  { category_id:7, category_name:'Home & Garden' },
  { category_id:8, category_name:'Office Supplies' },
  { category_id:9, category_name:'Sports & Outdoors' },
  { category_id:10, category_name:'Toys & Games' },
];

const MOCK_BRANDS = [
  { brand_id:3,  brand_name:'Apple',      brand_description:'Premium consumer electronics', vendor_id:1,  active:1, not_available:0, category_hint:'Electronics' },
  { brand_id:29, brand_name:'Great Value',brand_description:'Walmart private-label grocery', vendor_id:21, active:1, not_available:0, category_hint:'Grocery' },
  { brand_id:56, brand_name:'Samsung',    brand_description:'Global electronics leader',    vendor_id:3,  active:1, not_available:0, category_hint:'Electronics' },
];

const MOCK_SEGMENTS = [
  { segment_id:'SEG-001', segment_name:'Champions',      description:'Highest RFM scores',         criteria:'rfm_total_score >= 12', recommended_focus:'Reward & retain.' },
  { segment_id:'SEG-002', segment_name:'Loyal',          description:'Regular high-frequency',     criteria:'frequency >= 4',        recommended_focus:'Upsell & cross-sell.' },
  { segment_id:'SEG-003', segment_name:'At-Risk',        description:'Was good, now slipping',     criteria:'recency > 45',          recommended_focus:'Reactivation campaign.' },
  { segment_id:'SEG-004', segment_name:'Hibernating',    description:'Low RFM, inactive',          criteria:'rfm_total_score <= 5',  recommended_focus:'Win-back discount.' },
  { segment_id:'SEG-005', segment_name:'Potential Loyal',description:'Recent, moderate frequency', criteria:'frequency >= 2',        recommended_focus:'Loyalty programme enrolment.' },
  { segment_id:'SEG-006', segment_name:'New',            description:'First purchase < 30d',       criteria:'recency <= 30 AND orders = 1', recommended_focus:'Onboarding flow.' },
  { segment_id:'SEG-007', segment_name:'Lost',           description:'No purchase in 180+ days',   criteria:'recency >= 180',        recommended_focus:'Sunset or deep discount.' },
];

const MOCK_TIERS = [
  { tier_id:'T-01', tier_name:'Platinum', threshold_type:'quartile', threshold_value:0.75, description:'VIP — highest lifetime revenue.',    benefits:'Free express shipping | Dedicated support | Early sale access | Exclusive coupons' },
  { tier_id:'T-02', tier_name:'Gold',     threshold_type:'quartile', threshold_value:0.50, description:'High-value consistent shoppers.',     benefits:'Free standard shipping | Priority support | Sale preview' },
  { tier_id:'T-03', tier_name:'Silver',   threshold_type:'quartile', threshold_value:0.25, description:'Mid-tier, growing customers.',        benefits:'Discounted shipping | Standard support' },
  { tier_id:'T-04', tier_name:'Bronze',   threshold_type:'quartile', threshold_value:0.00, description:'Entry-level, price-sensitive buyers.', benefits:'Standard shipping | Self-service support' },
];

// ── Route matcher ──────────────────────────────────────────────────────────────
function matchMock(method: string, url: string): HttpResponse<unknown> | null {
  const path = url.replace(/.*\/api\/v1/, '').split('?')[0];
  const query = url.includes('?') ? url.split('?')[1] : '';

  // Auth
  if (method === 'POST' && path === '/auth/login') return null; // handled inside interceptor
  if (method === 'GET'  && path === '/auth/me')    return null;
  if (method === 'POST' && path === '/auth/logout') return ok({});

  // Dashboard
  if (method === 'GET' && path === '/dashboard') return ok(MOCK_DASHBOARD);
  if (method === 'GET' && path === '/dashboard/orders') return ok({ orders: MOCK_DASHBOARD.recentOrders, total:1894, pages:379 });

  // Analytics
  if (method === 'GET' && path === '/analytics') return ok(MOCK_ANALYTICS);

  // Users
  if (method === 'GET' && path === '/users') return ok(mockUsers);
  if (method === 'POST' && path === '/users') return null; // handled specially
  if (method.match(/PUT|PATCH/) && path.startsWith('/users/')) {
    const id = path.split('/')[2];
    // return updated user
    const u = mockUsers.find(u => u.id === id);
    return u ? ok(u) : notFound();
  }
  if (method === 'DELETE' && path.startsWith('/users/')) return ok({});

  // Upload
  if (method === 'POST' && path.startsWith('/uploads/')) return ok({ masterType: path.split('/')[2], fileName:'uploaded.xlsx', rowCount: Math.floor(Math.random()*2000+50), columns:[], uploadedAt: new Date().toISOString() });
  if (method === 'GET'  && path === '/uploads')           return ok([]);
  if (method === 'DELETE' && path.startsWith('/uploads/')) return ok({});

  // Pipeline
  if (method === 'POST' && path === '/pipeline/run') return null; // handled specially
  if (method === 'GET'  && path.startsWith('/pipeline/status/')) return null; // handled specially
  if (method === 'GET'  && path === '/pipeline/last-run') return ok({
    jobId:'JOB-4818', status:'complete', progress:100, startedAt:'2026-03-17 22:43:41',
    completedAt:'2026-03-17 22:43:47', durationSeconds:6.3,
    stages: [
      { stage:1,  label:'Load masters',       status:'done', message:'Read all 15 masters · 7,912 total rows loaded', timestamp:'22:43:41' },
      { stage:2,  label:'Client config',      status:'done', message:'Client config wired · CLT-001 · Churn: 90d', timestamp:'22:43:42' },
      { stage:3,  label:'Validation',         status:'done', message:'Validation complete · 0 rows quarantined', timestamp:'22:43:42' },
      { stage:4,  label:'Normalisation',      status:'done', message:'Normalisation complete · ref date: 2026-03-17', timestamp:'22:43:42' },
      { stage:5,  label:'Identity resolve',   status:'done', message:'Identity resolved · 200 unique customers', timestamp:'22:43:42' },
      { stage:6,  label:'Retention labels',   status:'done', message:'Retention labelled · 88 churned / 200 · At-Risk: 22', timestamp:'22:43:42' },
      { stage:7,  label:'Tier assignment',    status:'done', message:'Tiers assigned · Platinum:50 Gold:50 Silver:50 Bronze:50', timestamp:'22:43:42' },
      { stage:8,  label:'RFM features',       status:'done', message:'RFM + vendor features · vendor_diversity median: 10', timestamp:'22:43:43' },
      { stage:9,  label:'ML feature vector',  status:'done', message:'ML feature vector built · 65 features', timestamp:'22:43:43' },
      { stage:10, label:'Write output',       status:'done', message:'12 output sheets written', timestamp:'22:43:47' },
    ],
    summary:{ totalCustomers:200, totalOrders:1894, totalLineItems:5740, churned:88, churnRate:44.0, atRisk:22, highValue:50, repeatCustomers:159, mlFeatures:65, outputSheets:12 }
  });

  // Messages
  if (method === 'GET'  && path === '/messages/templates') return ok([]);  // triggers fallback to defaults
  if (method === 'POST' && path === '/messages/templates') return null;    // handled specially

  // Validation
  if (method === 'GET' && path === '/validation') return ok({});

  return null; // no mock found — pass through
}

function ok(body: unknown, ms = 300): HttpResponse<unknown> {
  return new HttpResponse({ status: 200, body });
}
function notFound(): HttpResponse<unknown> {
  return new HttpResponse({ status: 404, body: { message: 'Not found' } });
}

// ── The interceptor ────────────────────────────────────────────────────────────
export const mockInterceptor: HttpInterceptorFn = (req, next) => {
  if (!environment.useMocks) return next(req);

  const method = req.method;
  const url    = req.url;
  const path   = url.replace(/.*\/api\/v1/, '').split('?')[0];

  // ── Auth login (needs to read body) ──
  if (method === 'POST' && path === '/auth/login') {
    const body = req.body as { email: string; password: string };
    if (!body?.email || !body?.password) {
      return of(new HttpResponse({ status: 400, body: { message: 'Email and password required' } })).pipe(delay(300));
    }
    const isAdmin = body.email.includes('admin') || body.email.includes('analytics');
    const user = isAdmin ? MOCK_ADMIN_USER : MOCK_CLIENT_USER;
    // Override with actual email
    const resolved = { ...user, email: body.email };
    const res = { user: resolved, token: MOCK_TOKEN, refreshToken: MOCK_REFRESH };
    return of(new HttpResponse({ status: 200, body: res })).pipe(delay(600));
  }

  // ── Auth /me ──
  if (method === 'GET' && path === '/auth/me') {
    const stored = localStorage.getItem('wap_user');
    if (stored) return of(new HttpResponse({ status: 200, body: JSON.parse(stored) })).pipe(delay(200));
    return of(new HttpResponse({ status: 401, body: { message: 'Unauthorized' } })).pipe(delay(200));
  }

  // ── Create user ──
  if (method === 'POST' && path === '/users') {
    const body = req.body as Record<string, unknown>;
    const newUser = {
      id: 'usr-' + Date.now(),
      name: body['name'] as string,
      email: body['email'] as string,
      role: body['role'] as string,
      clientAccess: (body['clientAccess'] as string[]) ?? [],
      lastLogin: null as unknown as string,
      status: 'active',
      createdAt: new Date().toISOString().split('T')[0],
    };
    mockUsers = [...mockUsers, newUser as typeof mockUsers[0]];
    return of(new HttpResponse({ status: 201, body: newUser })).pipe(delay(500));
  }

  // ── Update user ──
  if ((method === 'PUT' || method === 'PATCH') && path.startsWith('/users/')) {
    const id = path.split('/')[2];
    const changes = req.body as Partial<typeof mockUsers[0]>;
    const idx = mockUsers.findIndex(u => u.id === id);
    if (idx === -1) return of(new HttpResponse({ status: 404, body: { message: 'User not found' } })).pipe(delay(300));
    mockUsers[idx] = { ...mockUsers[idx], ...changes };
    return of(new HttpResponse({ status: 200, body: mockUsers[idx] })).pipe(delay(400));
  }

  // ── Delete user ──
  if (method === 'DELETE' && path.startsWith('/users/')) {
    const id = path.split('/')[2];
    mockUsers = mockUsers.filter(u => u.id !== id);
    return of(new HttpResponse({ status: 200, body: {} })).pipe(delay(400));
  }

  // ── Pipeline run ──
  if (method === 'POST' && path === '/pipeline/run') {
    const jobId = 'JOB-' + Date.now();
    return of(new HttpResponse({
      status: 200,
      body: {
        jobId, status: 'running', progress: 0, startedAt: new Date().toISOString(),
        stages: [
          { stage:1,  label:'Load masters',      status:'pending', message:'Waiting…', timestamp: null },
          { stage:2,  label:'Client config',     status:'pending', message:'Waiting…', timestamp: null },
          { stage:3,  label:'Validation',        status:'pending', message:'Waiting…', timestamp: null },
          { stage:4,  label:'Normalisation',     status:'pending', message:'Waiting…', timestamp: null },
          { stage:5,  label:'Identity resolve',  status:'pending', message:'Waiting…', timestamp: null },
          { stage:6,  label:'Retention labels',  status:'pending', message:'Waiting…', timestamp: null },
          { stage:7,  label:'Tier assignment',   status:'pending', message:'Waiting…', timestamp: null },
          { stage:8,  label:'RFM features',      status:'pending', message:'Waiting…', timestamp: null },
          { stage:9,  label:'ML feature vector', status:'pending', message:'Waiting…', timestamp: null },
          { stage:10, label:'Write output',      status:'pending', message:'Waiting…', timestamp: null },
        ]
      }
    })).pipe(delay(500));
  }

  // ── Pipeline status polling (simulates progressive completion) ──
  if (method === 'GET' && path.startsWith('/pipeline/status/')) {
    // Each poll call advances progress — stateless simulation using time
    const started = parseInt(path.split('/').pop()?.replace('JOB-','') ?? '0', 10);
    const elapsed = Date.now() - started;
    const totalMs = 8000; // 8 second simulated run
    const pct = Math.min(100, Math.round((elapsed / totalMs) * 100));
    const stagesComplete = Math.min(10, Math.floor(pct / 10));
    const stageMsgs = [
      'Read all 15 masters · 7,912 total rows loaded',
      'Client config wired · CLT-001 · Churn: 90d',
      'Validation complete · 0 rows quarantined',
      'Normalisation complete · ref date: 2026-03-17',
      'Identity resolved · 200 unique customers',
      'Retention labelled · 88 churned / 200 · At-Risk: 22',
      'Tiers assigned · Platinum:50 Gold:50 Silver:50 Bronze:50',
      'RFM + vendor features · vendor_diversity median: 10',
      'ML feature vector built · 65 features',
      '12 output sheets written',
    ];
    const now = new Date().toISOString().split('T')[1].split('.')[0];
    const stages = stageMsgs.map((msg, i) => ({
      stage: i + 1,
      label: msg.split('·')[0].trim(),
      status: i < stagesComplete ? 'done' : i === stagesComplete ? 'running' : 'pending',
      message: i < stagesComplete ? msg : i === stagesComplete ? msg + '…' : 'Waiting…',
      timestamp: i < stagesComplete ? now : null,
    }));
    const isComplete = pct >= 100;
    return of(new HttpResponse({
      status: 200,
      body: {
        jobId: path.split('/').pop(), status: isComplete ? 'complete' : 'running',
        progress: pct, startedAt: new Date(started).toISOString(),
        completedAt: isComplete ? new Date().toISOString() : undefined,
        durationSeconds: isComplete ? 8.3 : undefined,
        stages,
        summary: isComplete ? { totalCustomers:200, totalOrders:1894, totalLineItems:5740, churned:88, churnRate:44.0, atRisk:22, highValue:50, repeatCustomers:159, mlFeatures:65, outputSheets:12 } : undefined,
      }
    })).pipe(delay(200));
  }

  // ── Save message templates ──
  if (method === 'POST' && path === '/messages/templates') {
    return of(new HttpResponse({ status: 200, body: req.body })).pipe(delay(400));
  }

  // ── Catalogue master data ──
  if (method === 'GET' && path === '/catalogue/categories')         return of(ok(MOCK_CATEGORIES)).pipe(delay(300));
  if (method === 'GET' && path === '/catalogue/sub-categories')     return of(ok([])).pipe(delay(300));
  if (method === 'GET' && path === '/catalogue/sub-sub-categories') return of(ok([])).pipe(delay(300));
  if (method === 'GET' && path === '/catalogue/brands')             return of(ok(MOCK_BRANDS)).pipe(delay(300));
  if (method === 'GET' && path === '/catalogue/vendors')            return of(ok([])).pipe(delay(300));

  // ── Customers ──
  if (method === 'GET' && path === '/customers')          return of(ok({ data: [], total: 0, pages: 0 })).pipe(delay(300));
  if (method === 'GET' && path.startsWith('/customers/')) return of(ok(null)).pipe(delay(300));

  // ── Orders ──
  if (method === 'GET' && path === '/orders') return of(ok({ data: [], total: 0, pages: 0 })).pipe(delay(300));

  // ── Segments & tiers ──
  if (method === 'GET' && path === '/segments')            return of(ok(MOCK_SEGMENTS)).pipe(delay(300));
  if (method === 'GET' && path === '/tiers')               return of(ok(MOCK_TIERS)).pipe(delay(300));
  if (method === 'GET' && path === '/value-propositions')  return of(ok([])).pipe(delay(300));

  // ── Reviews & support tickets ──
  if (method === 'GET' && path === '/reviews')                      return of(ok({ data: [], total: 0, pages: 0 })).pipe(delay(300));
  if (method === 'GET' && path === '/support-tickets')              return of(ok({ data: [], total: 0, pages: 0 })).pipe(delay(300));
  if (method.match(/PUT|PATCH/) && path.startsWith('/support-tickets/')) return of(ok({})).pipe(delay(300));

  // ── Generic mock lookup ──
  const generic = matchMock(method, url);
  if (generic) return of(generic).pipe(delay(350));

  // No mock matched — let it pass through (will fail if no backend, which is fine)
  return next(req);
};
