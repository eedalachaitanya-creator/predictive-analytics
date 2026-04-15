import { Component, OnInit, inject, ViewChild, ElementRef, AfterViewChecked } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ChatService } from '../services/chat.service';

@Component({
  selector: 'app-chat',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './chat.html',
  styleUrls: ['./chat.scss'],
})
export class ChatComponent implements OnInit, AfterViewChecked {
  chat = inject(ChatService);

  userInput = '';
  private shouldScroll = false;

  @ViewChild('messagesContainer') messagesContainer!: ElementRef;

  ngOnInit() {
    this.chat.loadSuggestions();
  }

  ngAfterViewChecked() {
    if (this.shouldScroll) {
      this.scrollToBottom();
      this.shouldScroll = false;
    }
  }

  send() {
    const q = this.userInput.trim();
    if (!q || this.chat.loading()) return;
    this.chat.ask(q);
    this.userInput = '';
    this.shouldScroll = true;
  }

  useSuggestion(text: string) {
    this.userInput = text;
    this.send();
  }

  newChat() {
    this.chat.newConversation();
    this.userInput = '';
  }

  clearChat() {
    this.chat.clearConversation();
    this.userInput = '';
  }

  onKeyDown(event: KeyboardEvent) {
    // Send on Enter (without Shift)
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      this.send();
    }
  }

  private scrollToBottom() {
    try {
      const el = this.messagesContainer?.nativeElement;
      if (el) el.scrollTop = el.scrollHeight;
    } catch (_) {}
  }

  /** Format markdown-like content for display */
  formatContent(content: string): string {
    if (!content) return '';

    // Convert markdown tables to HTML tables
    content = this.convertTables(content);

    // Convert **bold** to <strong>
    content = content.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');

    // Convert `code` to <code>
    content = content.replace(/`([^`]+)`/g, '<code>$1</code>');

    // Convert ```code blocks``` to <pre><code>
    content = content.replace(/```(\w*)\n?([\s\S]*?)```/g,
      '<pre><code>$2</code></pre>');

    // Convert newlines to <br> (outside of pre blocks)
    content = content.replace(/\n/g, '<br>');

    // Fix double <br> inside <pre>
    content = content.replace(/<pre><code>([\s\S]*?)<\/code><\/pre>/g, (match, code) => {
      return '<pre><code>' + code.replace(/<br>/g, '\n') + '</code></pre>';
    });

    return content;
  }

  private convertTables(content: string): string {
    const lines = content.split('\n');
    let inTable = false;
    let tableHtml = '';
    let result: string[] = [];

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();

      if (line.startsWith('|') && line.endsWith('|')) {
        if (!inTable) {
          inTable = true;
          tableHtml = '<table class="chat-table">';
        }

        // Skip separator rows like |---|---|
        if (/^\|[\s\-:]+\|$/.test(line.replace(/\|/g, '|').replace(/[\s\-:]/g, ''))) {
          continue;
        }

        const cells = line.split('|').filter(c => c.trim() !== '');
        const isHeader = !inTable || (i > 0 && lines[i-1]?.trim().startsWith('|') === false);
        const tag = tableHtml === '<table class="chat-table">' ? 'th' : 'td';
        tableHtml += '<tr>' + cells.map(c => `<${tag}>${c.trim()}</${tag}>`).join('') + '</tr>';
      } else {
        if (inTable) {
          tableHtml += '</table>';
          result.push(tableHtml);
          tableHtml = '';
          inTable = false;
        }
        result.push(lines[i]);
      }
    }

    if (inTable) {
      tableHtml += '</table>';
      result.push(tableHtml);
    }

    return result.join('\n');
  }
}
