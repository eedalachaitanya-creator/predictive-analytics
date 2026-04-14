import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
@Component({ selector:'app-sys-config', standalone:true, imports:[CommonModule,FormsModule], templateUrl:'./sys-config.html', styleUrls:['./sys-config.scss'] })
export class SysConfigComponent {
  globals = [
    { p:'churn_window_days',     val:'90',        type:'integer', override:true,  note:'Days before a customer is churned' },
    { p:'min_repeat_orders',     val:'2',         type:'integer', override:true,  note:'Min orders for repeat classification' },
    { p:'high_value_percentile', val:'75',        type:'integer', override:true,  note:'Top spend percentile = High Value' },
    { p:'tier_method',           val:'Quartile',  type:'enum',    override:true,  note:'Quartile or Custom thresholds' },
    { p:'reference_date_mode',   val:'auto',      type:'enum',    override:true,  note:'auto = today; fixed = manual date' },
    { p:'quarantine_on_missing', val:'True',      type:'bool',    override:true,  note:'Quarantine rows with missing key cols' },
    { p:'currency_default',      val:'USD',       type:'string',  override:true,  note:'ISO 4217 currency code' },
    { p:'ml_feature_version',    val:'v3',        type:'enum',    override:false, note:'Feature set version for ML export' },
    { p:'max_file_size_mb',      val:'50',        type:'integer', override:false, note:'Max upload size per file (MB)' },
  ];
  emails = [
    { event:'Pipeline run completed',     recip:'admin@analytics.com',     enabled:true,  last:'2026-03-17 22:43' },
    { event:'Pipeline run failed',        recip:'admin@analytics.com',     enabled:true,  last:'—' },
    { event:'New client added',           recip:'admin@analytics.com',     enabled:true,  last:'2026-01-15' },
    { event:'Validation warning > 5%',   recip:'admin + client ops',       enabled:true,  last:'2026-03-16' },
    { event:'User login from new device', recip:'security@analytics.com',  enabled:true,  last:'2026-03-14' },
    { event:'Data upload exceeds 40MB',  recip:'admin@analytics.com',     enabled:false, last:'—' },
  ];
  retention = [
    { type:'Raw uploaded files',  period:'30 days',  policy:'Auto-deleted after 30 days from upload' },
    { type:'Processed outputs',   period:'90 days',  policy:'Kept for 3 months then archived' },
    { type:'Audit logs',          period:'365 days', policy:'Full year retention for compliance' },
    { type:'User session data',   period:'7 days',   policy:'Browser sessions cleared after 7 days' },
    { type:'Quarantine records',  period:'90 days',  policy:'Matched to processed output retention' },
  ];
}
