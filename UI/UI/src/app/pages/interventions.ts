import { Component } from '@angular/core';
import { RetentionInterventionsTab } from './retention/tabs/interventions';

@Component({
  selector: 'app-interventions',
  standalone: true,
  imports: [RetentionInterventionsTab],
  templateUrl: './interventions.html',
  styleUrls: ['./interventions.scss']
})
export class InterventionsComponent {}