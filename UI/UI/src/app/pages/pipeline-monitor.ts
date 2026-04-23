import { Component } from '@angular/core';
import { StrategistStatsTab } from './strategist/tabs/stats';

@Component({
  selector: 'app-pipeline-monitor',
  standalone: true,
  imports: [StrategistStatsTab],
  templateUrl: './pipeline-monitor.html',
  styleUrls: ['./pipeline-monitor.scss']
})
export class PipelineMonitorComponent {}