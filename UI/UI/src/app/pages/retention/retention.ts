import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RetentionRunTab }           from './tabs/run';
import { RetentionEscalationsTab }   from './tabs/escalations';
import { RetentionSummaryTab }       from './tabs/summary';
import { RetentionInterventionsTab } from './tabs/interventions';

type RetentionTab = 'run' | 'interventions' | 'escalations' | 'summary';
interface TabDef { id: RetentionTab; label: string; icon: string; }

@Component({
  selector: 'app-retention',
  standalone: true,
  imports: [CommonModule, RetentionRunTab, RetentionEscalationsTab, RetentionSummaryTab, RetentionInterventionsTab],
  templateUrl: './retention.html',
  styleUrls: ['./retention.scss']
})
export class RetentionComponent {
  activeTab = signal<RetentionTab>('run');

  tabs: TabDef[] = [
    { id: 'run',           label: 'Run Pipeline',   icon: '🚀' },
    { id: 'interventions', label: 'Interventions',  icon: '📋' },
    { id: 'escalations',   label: 'Escalations',    icon: '🔔' },
    { id: 'summary',       label: 'Summary',        icon: '📊' },
  ];

  setTab(tab: RetentionTab) { this.activeTab.set(tab); }
}