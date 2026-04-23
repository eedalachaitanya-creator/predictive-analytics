import { Component } from '@angular/core';
import { RetentionEscalationsTab } from './retention/tabs/escalations';

@Component({
  selector: 'app-escalations',
  standalone: true,
  imports: [RetentionEscalationsTab],
  templateUrl: './escalations.html',
  styleUrls: ['./escalations.scss']
})
export class EscalationsComponent {}