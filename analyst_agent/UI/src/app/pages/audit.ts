import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
@Component({ selector:'app-audit', standalone:true, imports:[CommonModule,FormsModule], templateUrl:'./audit.html', styleUrls:['./audit.scss'] })
export class AuditComponent {
  events = [
    { n:247, ts:'2026-03-17 22:43:41', user:'ops@walmart.com',         client:'CLT-001', action:'Pipeline Run',  detail:'JOB-4821 started · Full Pipeline',         ip:'192.168.1.10', ok:true },
    { n:246, ts:'2026-03-17 22:43:00', user:'ops@walmart.com',         client:'CLT-001', action:'File Upload',   detail:'Line Items Master · 5,740 rows',            ip:'192.168.1.10', ok:true },
    { n:245, ts:'2026-03-17 22:41:10', user:'bi@target.com',           client:'CLT-002', action:'Pipeline Run',  detail:'JOB-4820 started · Full Pipeline',          ip:'10.0.0.22',    ok:true },
    { n:244, ts:'2026-03-17 22:40:55', user:'bi@target.com',           client:'CLT-002', action:'File Upload',   detail:'Order Master · 1,742 rows',                 ip:'10.0.0.22',    ok:true },
    { n:243, ts:'2026-03-17 21:15:00', user:'raj@analytics.com',       client:'SYSTEM',  action:'Config Change', detail:'Global churn_window_days → 90',             ip:'10.0.0.1',     ok:true },
    { n:242, ts:'2026-03-17 14:12:00', user:'bi@target.com',           client:'CLT-002', action:'Pipeline Run',  detail:'JOB-4817 completed · 5.8s',                 ip:'10.0.0.22',    ok:true },
    { n:241, ts:'2026-03-17 12:30:00', user:'raj@analytics.com',       client:'SYSTEM',  action:'User Added',    detail:'s.lee@analytics.com · Admin role',          ip:'10.0.0.1',     ok:true },
    { n:240, ts:'2026-03-17 09:30:00', user:'bi@costco.com',           client:'CLT-003', action:'Pipeline Run',  detail:'JOB-4816 completed · 7.1s',                 ip:'172.16.0.5',   ok:true },
    { n:239, ts:'2026-03-16 22:45:00', user:'ops@walmart.com',         client:'CLT-001', action:'Settings Saved',detail:'tier_method → Quartile',                   ip:'192.168.1.10', ok:true },
    { n:238, ts:'2026-03-16 14:10:00', user:'bi@target.com',           client:'CLT-002', action:'File Upload',   detail:'Customer Master · 185 rows',                ip:'10.0.0.22',    ok:true },
    { n:237, ts:'2026-03-15 11:20:00', user:'t.baker@analytics.com',   client:'CLT-004', action:'Pipeline Run',  detail:'JOB-4812 · 3 validation warnings',          ip:'172.16.1.8',   ok:false },
    { n:236, ts:'2026-03-14 09:10:00', user:'priya@analytics.com',     client:'SYSTEM',  action:'Login',         detail:'New device detected · Chrome / Mac',        ip:'203.0.113.5',  ok:false },
  ];
}
