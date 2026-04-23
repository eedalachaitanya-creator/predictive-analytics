import { Component } from '@angular/core';
import { StrategistTrendTab } from './strategist/tabs/trend';

@Component({
  selector: 'app-market-trends',
  standalone: true,
  imports: [StrategistTrendTab],
  templateUrl: './market-trends.html',
  styleUrls: ['./market-trends.scss']
})
export class MarketTrendsComponent {}