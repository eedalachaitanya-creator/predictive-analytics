import { Component } from '@angular/core';
import { RetentionSummaryTab } from './retention/tabs/summary';

@Component({
  selector: 'app-retention-summary',
  standalone: true,
  imports: [RetentionSummaryTab],
  templateUrl: './retention-summary.html',
  styleUrls: ['./retention-summary.scss']
})
export class RetentionSummaryComponent {}