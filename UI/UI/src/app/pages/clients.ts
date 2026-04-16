import { Component, OnInit, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../services/api.service';

interface ClientRow {
  client_id: string;
  client_name: string;
  client_code: string;
  created_at: string | null;
}

@Component({
  selector: 'app-clients',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './clients.html',
  styleUrls: ['./clients.scss']
})
export class ClientsComponent implements OnInit {
  private api = inject(ApiService);

  clients = signal<ClientRow[]>([]);
  loading = signal(true);
  selected = signal<ClientRow | null>(null);

  ngOnInit() {
    this.loadClients();
  }

  loadClients() {
    this.loading.set(true);
    this.api.get<ClientRow[]>('/clients').subscribe({
      next: (data) => {
        this.clients.set(data);
        this.loading.set(false);
        // Auto-select first client for detail view
        if (data.length > 0) {
          this.selected.set(data[0]);
        }
      },
      error: () => {
        this.loading.set(false);
      }
    });
  }

  selectClient(c: ClientRow) {
    this.selected.set(c);
  }

  formatDate(iso: string | null): string {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
  }
}
