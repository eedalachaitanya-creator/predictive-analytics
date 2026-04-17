import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StrategistRecommendTab } from './tabs/recommend';
import { StrategistTrendTab }     from './tabs/trend';
import { StrategistStatsTab }     from './tabs/stats';

type StrategistTab = 'recommend' | 'trend' | 'stats';
interface TabDef { id: StrategistTab; label: string; icon: string; }

@Component({
  selector: 'app-strategist',
  standalone: true,
  imports: [CommonModule, StrategistRecommendTab, StrategistTrendTab, StrategistStatsTab],
  templateUrl: './strategist.html',
  styleUrls: ['./strategist.scss']
})
export class StrategistComponent {
  activeTab = signal<StrategistTab>('recommend');

  tabs: TabDef[] = [
    { id: 'recommend', label: 'Pricing Engine',   icon: '🧠' },
    { id: 'trend',     label: 'Market Trends',    icon: '📈' },
    { id: 'stats',     label: 'Pipeline Monitor', icon: '📊' },
  ];

  setTab(tab: StrategistTab) { this.activeTab.set(tab); }
}