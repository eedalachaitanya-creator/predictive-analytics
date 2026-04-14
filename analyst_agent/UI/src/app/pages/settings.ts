import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

@Component({ selector:'app-settings', standalone:true, imports:[CommonModule,FormsModule], templateUrl:'./settings.html', styleUrls:['./settings.scss'] })
export class SettingsComponent {
  churnWindow = signal(90);
  repeatThreshold = signal(2);
  highValuePct = signal(75);
  recentGapWindow = signal(3);
  predMode = signal('full');
  tierMethod = signal('quartile');
  platMin = signal(500); goldMin = signal(250); silverMin = signal(100); bronzeMin = signal(0);
  saved = signal(false);

  segments = [
    { name:'Champions',      cond:'R=5 · F≥4 · M≥4', focus:'Upsell premium / loyalty rewards',    active:true },
    { name:'Loyal Customers',cond:'F≥4',              focus:'Exclusive offers, early access',       active:true },
    { name:'Potential Loyal',cond:'R≥3 · F=2-3',      focus:'Onboarding series, nurture',           active:true },
    { name:'At-Risk',        cond:'R=2 · F≥3',        focus:'Win-back campaigns, discount',         active:true },
    { name:'Hibernating',    cond:'R≤2 · F≤3',        focus:'Re-engagement / low-cost nudge',       active:true },
    { name:'New Customers',  cond:'R=5 · F=1',        focus:'Welcome flow, product education',      active:true },
    { name:'Lost Customers', cond:'R=1 · F≤2',        focus:'Final win-back or suppress',           active:true },
    { name:'Casual Shoppers',cond:'RFM total 6–11',   focus:'Seasonal promotions, awareness',       active:true },
  ];
  vpRules = [
    { tier:'💎 Platinum', risk:'At-Risk',    action:'Personalised win-back', disc:'15%', ch:'Email+SMS', tpl:'We miss you, {name}…' },
    { tier:'💎 Platinum', risk:'Returning',  action:'Loyalty reward',        disc:'10%', ch:'Email',     tpl:'Welcome back, {name}!' },
    { tier:'🥇 Gold',     risk:'At-Risk',    action:'Discount offer',        disc:'10%', ch:'Email',     tpl:'Special offer for you…' },
    { tier:'🥇 Gold',     risk:'Returning',  action:'Cross-sell',            disc:'5%',  ch:'Push',      tpl:'Based on your last buy…' },
    { tier:'🥈 Silver',   risk:'At-Risk',    action:'Nudge campaign',        disc:'5%',  ch:'SMS',       tpl:'Don\'t miss out…' },
    { tier:'🥈 Silver',   risk:'New',        action:'Onboarding series',     disc:'0%',  ch:'Email',     tpl:'Getting started with…' },
    { tier:'🥉 Bronze',   risk:'Reactivated',action:'Re-engagement',         disc:'8%',  ch:'Email+Push',tpl:'Come back & save…' },
    { tier:'🥉 Bronze',   risk:'New',        action:'Welcome offer',         disc:'5%',  ch:'Email',     tpl:'Welcome! Here\'s 5% off…' },
  ];
  vendorCfg = [
    { param:'client_name',       val:'Walmart Inc.',  type:'string',  desc:'Full legal name of the retail client' },
    { param:'client_id',         val:'CLT-001',       type:'string',  desc:'Unique client identifier used as composite key' },
    { param:'churn_window_days', val:'90',            type:'integer', desc:'Days since last order before flagging as churned' },
    { param:'reference_date',    val:'2026-03-17',    type:'date',    desc:'Pipeline reference date (ISO 8601)' },
    { param:'min_repeat_orders', val:'2',             type:'integer', desc:'Min completed orders to qualify as repeat customer' },
    { param:'high_value_pct',    val:'75',            type:'integer', desc:'Percentile cutoff for High Value flag (quartile mode)' },
    { param:'currency',          val:'USD',           type:'string',  desc:'Transaction currency for all monetary columns' },
  ];

  save() { this.saved.set(true); setTimeout(() => this.saved.set(false), 2500); }
}
