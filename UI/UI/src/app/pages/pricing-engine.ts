import { Component } from '@angular/core';
import { StrategistRecommendTab } from './strategist/tabs/recommend';
import { StrategistStatsTab } from './strategist/tabs/stats';

@Component({
  selector: 'app-pricing-engine',
  standalone: true,
//   imports: [StrategistRecommendTab],
  imports: [StrategistRecommendTab, StrategistStatsTab],
  templateUrl: './pricing-engine.html',
  styleUrls: ['./pricing-engine.scss']
})
export class PricingEngineComponent {}