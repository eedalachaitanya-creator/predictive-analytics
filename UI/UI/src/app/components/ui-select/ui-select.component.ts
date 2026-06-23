import {
  Component, ElementRef, EventEmitter, HostListener, Input, Output,
  signal, inject, ChangeDetectionStrategy, OnDestroy,
} from '@angular/core';
import { CommonModule } from '@angular/common';

export interface UiSelectOption { value: string; label: string; }

/**
 * Accessible, dependency-free custom dropdown (`<app-ui-select>`).
 *
 * Why this exists: a native `<select>`'s option popup is rendered by the
 * browser/OS, so CSS cannot cap its height, give it a scrollbar, or stop it
 * spilling off the page. This renders its own panel so we control all three.
 *
 * The panel is `position: fixed` and positioned from the trigger's
 * getBoundingClientRect(): that escapes any ancestor `overflow:hidden`
 * (e.g. `.card`) and lets us flip it above the trigger + clamp its height to
 * the viewport, so it can never run off-screen. Capped at 280px with an
 * internal scrollbar.
 *
 * A11y: combobox + listbox with `aria-activedescendant` (focus stays on the
 * trigger). Keyboard: ↑/↓ move, Enter/Space select, Esc closes, Home/End jump.
 */
@Component({
  selector: 'app-ui-select',
  standalone: true,
  imports: [CommonModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <button type="button" class="uis-trigger" #trigger
            [style.width]="width || null"
            [disabled]="disabled"
            role="combobox"
            aria-haspopup="listbox"
            [attr.aria-expanded]="open()"
            [attr.aria-label]="ariaLabel || null"
            [attr.aria-activedescendant]="open() && activeIndex() >= 0 ? id + '-opt-' + activeIndex() : null"
            (click)="toggle()"
            (keydown)="onKeydown($event)">
      <span class="uis-value" [class.uis-placeholder]="!hasValue()">{{ selectedLabel() }}</span>
      <span class="uis-caret" [class.open]="open()" aria-hidden="true">▾</span>
    </button>

    @if (open()) {
      <ul class="uis-panel" role="listbox" [attr.aria-label]="ariaLabel || null"
          [style.left.px]="panel().left"
          [style.top.px]="panel().top"
          [style.bottom.px]="panel().bottom"
          [style.width.px]="panel().width"
          [style.maxHeight.px]="panel().maxHeight">
        @for (o of options; track o.value; let i = $index) {
          <li class="uis-option"
              [id]="id + '-opt-' + i"
              role="option"
              [attr.aria-selected]="o.value === value"
              [class.selected]="o.value === value"
              [class.active]="i === activeIndex()"
              (mouseenter)="activeIndex.set(i)"
              (click)="choose(o)">
            <span class="uis-check" aria-hidden="true">{{ o.value === value ? '✓' : '' }}</span>
            <span class="uis-label">{{ o.label }}</span>
          </li>
        }
        @if (!options.length) {
          <li class="uis-empty">No options</li>
        }
      </ul>
    }
  `,
  styles: [`
    :host { display: inline-block; position: relative; }

    .uis-trigger {
      display: flex; align-items: center; justify-content: space-between; gap: 8px;
      padding: 10px 13px;
      background: var(--bg-surface); color: var(--text-primary);
      border: 1px solid var(--border); border-radius: var(--radius-sm);
      font-family: 'Sora', sans-serif; font-size: 13px;
      cursor: pointer; text-align: left; min-width: 120px; width: 100%;
      transition: border-color .2s, box-shadow .2s;
    }
    .uis-trigger:hover:not(:disabled) { border-color: var(--accent-blue); }
    .uis-trigger:focus-visible { outline: none; border-color: var(--accent-blue); box-shadow: 0 0 0 3px rgba(26,111,212,.10); }
    .uis-trigger:disabled { opacity: .55; cursor: default; }

    .uis-value { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .uis-placeholder { color: var(--text-muted); }
    .uis-caret { flex-shrink: 0; font-size: 11px; color: var(--text-muted); transition: transform .15s ease; }
    .uis-caret.open { transform: rotate(180deg); }

    .uis-panel {
      position: fixed; z-index: 1100;
      margin: 0; padding: 4px; list-style: none;
      background: var(--bg-card, #fff); border: 1px solid var(--border);
      border-radius: var(--radius-sm); box-shadow: 0 12px 28px rgba(15, 23, 42, .16);
      overflow-y: auto; overflow-x: hidden;
      scrollbar-width: thin; scrollbar-color: var(--border-bright, #cbd5e1) transparent;
      animation: uisIn .12s ease;
    }
    .uis-panel::-webkit-scrollbar { width: 8px; }
    .uis-panel::-webkit-scrollbar-track { background: transparent; }
    .uis-panel::-webkit-scrollbar-thumb { background: var(--border-bright, #cbd5e1); border-radius: 4px; }
    .uis-panel::-webkit-scrollbar-thumb:hover { background: #94a3b8; }

    @keyframes uisIn { from { opacity: 0; transform: translateY(-2px); } to { opacity: 1; transform: translateY(0); } }

    .uis-option {
      display: flex; align-items: center; gap: 8px;
      padding: 8px 10px; border-radius: 6px; cursor: pointer;
      font-size: 13px; color: var(--text-primary); white-space: nowrap;
    }
    .uis-option .uis-label { overflow: hidden; text-overflow: ellipsis; }
    .uis-option .uis-check { width: 14px; flex-shrink: 0; color: var(--accent-blue); font-size: 12px; }
    .uis-option.active { background: rgba(26, 111, 212, .10); }
    .uis-option.selected { font-weight: 600; }
    .uis-empty { padding: 10px; text-align: center; color: var(--text-muted); font-size: 12px; }
  `],
})
export class UiSelectComponent implements OnDestroy {
  @Input() options: UiSelectOption[] = [];
  @Input() value = '';
  @Input() placeholder = 'Select…';
  @Input() ariaLabel = '';
  @Input() width = '';
  @Input() disabled = false;
  @Output() valueChange = new EventEmitter<string>();

  private host = inject(ElementRef<HTMLElement>);
  private static seq = 0;
  readonly id = `uis-${UiSelectComponent.seq++}`;

  // Capture-phase so it fires for scrolls in ANY ancestor scroll container,
  // not just the window (the fixed panel would otherwise detach from the
  // trigger). Bound once; attached only while the panel is open.
  // CRUCIAL: ignore scrolls that originate INSIDE this component — otherwise
  // scrolling the panel's own option list would close it (you'd never reach
  // the options at the bottom). Only an ancestor/page scroll closes it.
  private readonly onAnyScroll = (e: Event) => {
    if (this.host.nativeElement.contains(e.target as Node)) return;
    this.close();
  };

  ngOnDestroy() { document.removeEventListener('scroll', this.onAnyScroll, true); }

  open        = signal(false);
  activeIndex = signal(-1);
  panel       = signal<{ left: number; top: number | null; bottom: number | null; width: number; maxHeight: number }>(
    { left: 0, top: 0, bottom: null, width: 0, maxHeight: 280 });

  // Plain methods (not computed): `value`/`options` are non-signal @Inputs, so
  // a computed() would never recompute when they change. Template calls these
  // every CD pass, and OnPush runs CD whenever an @Input reference changes.
  hasValue(): boolean { return this.options.some(o => o.value === this.value); }
  selectedLabel(): string { return this.options.find(o => o.value === this.value)?.label ?? this.placeholder; }

  toggle() { this.open() ? this.close() : this.openPanel(); }

  /** Measure the trigger and place the panel below it — or above, if there
   *  isn't enough room — clamped so it never spills past the viewport edge. */
  private openPanel() {
    if (this.disabled) return;
    const trigger = this.host.nativeElement.querySelector('.uis-trigger') as HTMLElement;
    const r = trigger.getBoundingClientRect();
    const vh = window.innerHeight;
    const gap = 4, margin = 8;
    const spaceBelow = vh - r.bottom - margin;
    const spaceAbove = r.top - margin;
    const desired = Math.min(280, this.options.length * 38 + 8);

    let coords;
    if (spaceBelow >= Math.min(desired, 160) || spaceBelow >= spaceAbove) {
      coords = { left: r.left, top: r.bottom + gap, bottom: null,
                 width: r.width, maxHeight: Math.max(120, Math.min(desired, spaceBelow)) };
    } else {
      coords = { left: r.left, top: null, bottom: vh - r.top + gap,
                 width: r.width, maxHeight: Math.max(120, Math.min(desired, spaceAbove)) };
    }
    this.panel.set(coords);
    const sel = this.options.findIndex(o => o.value === this.value);
    this.activeIndex.set(sel >= 0 ? sel : 0);
    this.open.set(true);
    document.addEventListener('scroll', this.onAnyScroll, true);
  }

  close() {
    if (this.open()) {
      this.open.set(false);
      this.activeIndex.set(-1);
      document.removeEventListener('scroll', this.onAnyScroll, true);
    }
  }

  choose(o: UiSelectOption) {
    if (o.value !== this.value) this.valueChange.emit(o.value);
    this.close();
  }

  private moveActive(delta: number) {
    const n = this.options.length;
    if (!n) return;
    const cur = this.activeIndex();
    this.activeIndex.set(((cur < 0 ? 0 : cur) + delta + n) % n);
    this.scrollActiveIntoView();
  }

  private scrollActiveIntoView() {
    queueMicrotask(() => {
      const el = this.host.nativeElement.querySelector(`#${this.id}-opt-${this.activeIndex()}`) as HTMLElement | null;
      el?.scrollIntoView({ block: 'nearest' });
    });
  }

  onKeydown(e: KeyboardEvent) {
    if (this.disabled) return;
    switch (e.key) {
      case 'ArrowDown': e.preventDefault(); this.open() ? this.moveActive(1) : this.openPanel(); break;
      case 'ArrowUp':   e.preventDefault(); this.open() ? this.moveActive(-1) : this.openPanel(); break;
      case 'Home':      if (this.open()) { e.preventDefault(); this.activeIndex.set(0); this.scrollActiveIntoView(); } break;
      case 'End':       if (this.open()) { e.preventDefault(); this.activeIndex.set(this.options.length - 1); this.scrollActiveIntoView(); } break;
      case 'Enter':
      case ' ':
        e.preventDefault();
        if (!this.open()) { this.openPanel(); }
        else { const o = this.options[this.activeIndex()]; if (o) this.choose(o); }
        break;
      case 'Escape': if (this.open()) { e.preventDefault(); this.close(); } break;
      case 'Tab':    this.close(); break;
    }
  }

  // The panel is fixed (decoupled from page flow), so close it on any outside
  // interaction rather than trying to keep it glued to the moving trigger.
  @HostListener('document:click', ['$event'])
  onDocClick(e: MouseEvent) {
    if (this.open() && !this.host.nativeElement.contains(e.target as Node)) this.close();
  }
  @HostListener('window:resize') onResize() { this.close(); }
}
