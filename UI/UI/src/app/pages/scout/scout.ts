import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ScoutChatTab } from './tabs/chat';
import { ScoutMonitorTab } from './tabs/monitor';
import { ScoutSearchTab } from './tabs/search';
import { ScoutCompareTab } from './tabs/compare';
import { ScoutPlatformsTab } from './tabs/platforms';

type ScoutTab = 'chat' | 'monitor' | 'search' | 'compare' | 'platforms';

interface TabDef {
  id: ScoutTab;
  label: string;
  icon: string;
}

@Component({
  selector: 'app-scout',
  standalone: true,
  imports: [CommonModule, ScoutChatTab, ScoutMonitorTab, ScoutSearchTab, ScoutCompareTab, ScoutPlatformsTab],
  templateUrl: './scout.html',
  styleUrls: ['./scout.scss']
})
export class ScoutComponent {
  // Chat is the default landing tab — primary interaction is "ask Scout"
  activeTab = signal<ScoutTab>('chat');

  tabs: TabDef[] = [
    { id: 'chat',      label: 'Chat',          icon: '💬' },
    { id: 'monitor',   label: 'Price Monitor', icon: '📈' },
    { id: 'search',    label: 'Search',        icon: '🔍' },
    { id: 'compare',   label: 'Compare',       icon: '⚖️' },
    { id: 'platforms', label: 'Platforms',     icon: '🌐' },
  ];

  setTab(tab: ScoutTab) {
    this.activeTab.set(tab);
  }
}