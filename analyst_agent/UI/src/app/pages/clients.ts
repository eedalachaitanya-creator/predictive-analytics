import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
@Component({ selector:'app-clients', standalone:true, imports:[CommonModule], templateUrl:'./clients.html', styleUrls:['./clients.scss'] })
export class ClientsComponent {
  selected = signal('CLT-001');
  clients = [
    { id:'CLT-001', name:'Walmart Inc.',      industry:'Retail',        users:5, lastRun:'2026-03-17 22:43', time:'6.3s', status:'active' },
    { id:'CLT-002', name:'Target Corp.',      industry:'Retail',        users:3, lastRun:'2026-03-16 14:12', time:'5.8s', status:'active' },
    { id:'CLT-003', name:'Costco Wholesale',  industry:'Wholesale',     users:2, lastRun:'2026-03-15 09:30', time:'7.1s', status:'active' },
    { id:'CLT-004', name:'Best Buy Co.',      industry:'Electronics',   users:1, lastRun:'—',               time:'—',    status:'pending' },
    { id:'CLT-005', name:'Home Depot',        industry:'Home Improve.', users:0, lastRun:'—',               time:'—',    status:'setup' },
  ];
  detail = { id:'CLT-001', name:'Walmart Inc.', industry:'Retail — Grocery & General Merchandise', contact:'ops@walmart.com', start:'2025-01-15', sla:'Enterprise', retention:'90 days', customers:200, orders:1894, lastRun:'2026-03-17 22:43:41', duration:'6.3 seconds', sheets:12, features:65, churn:'44.0%' };
}
