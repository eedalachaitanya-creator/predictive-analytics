import { Component } from '@angular/core';
import { RetentionRunTab } from './retention/tabs/run';

@Component({
  selector: 'app-run-pipeline',
  standalone: true,
  imports: [RetentionRunTab],
  templateUrl: './run-pipeline.html',
  styleUrls: ['./run-pipeline.scss']
})
export class RunPipelineComponent {}