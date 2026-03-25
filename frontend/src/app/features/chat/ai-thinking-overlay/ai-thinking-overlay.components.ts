import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AgentEvent } from '@app/core/models';

/**
 * Decision item for display (transformed from backend AgentEvent)
 */
interface DecisionItem {
  id: string;
  type: 'orchestrator' | 'agent' | 'thinking' | 'progress' | 'reasoning';
  sourceName: string;
  text: string;
  reasoning?: string;
}

/**
 * Friendly names for agent sources
 */
const FRIENDLY_NAMES: Record<string, string> = {
  'main_supervisor': 'Main Supervisor',
  'supervisor': 'Main Supervisor',
  'ba': 'BA Orchestrator',
  'ba_orchestrator': 'BA Orchestrator',
  'business_analyst': 'Business Analyst',
  'dev': 'Dev Orchestrator',
  'principal_dev': 'Principal Dev',
  'qa': 'QA Orchestrator',
  'qa_orchestrator': 'QA Orchestrator',
  'test_planner': 'Test Planner',
  'frontend_agent': 'Frontend Agent',
  'backend_agent': 'Backend Agent',
  'database_agent': 'Database Agent',
};

/**
 * AI Thinking Overlay Component
 * Shows real-time agent decision making process
 */
@Component({
  selector: 'app-ai-thinking-overlay',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="bg-white border-t border-gray-200 px-6 py-4">
      <div class="flex items-center gap-3 mb-3">
        <div class="w-8 h-8 rounded-full bg-bmo-blue flex items-center justify-center animate-pulse">
          <svg class="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                  d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"/>
          </svg>
        </div>
        <div>
          <h3 class="font-medium text-gray-800">AI is thinking...</h3>
          <p class="text-sm text-gray-500">Processing your request</p>
        </div>
      </div>

      <!-- Decisions List -->
      @if (decisions.length > 0) {
        <div class="space-y-2 ml-11">
          @for (decision of decisions; track decision.id) {
            <div 
              class="flex items-start gap-2 text-sm animate-fade-in"
              [class]="getDecisionClass(decision.type)"
            >
              <!-- Icon based on type -->
              <span class="flex-shrink-0 mt-0.5">
                @switch (decision.type) {
                  @case ('orchestrator') {
                    <svg class="w-4 h-4 text-purple-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                            d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z"/>
                    </svg>
                  }
                  @case ('agent') {
                    <svg class="w-4 h-4 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                            d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>
                    </svg>
                  }
                  @case ('thinking') {
                    <svg class="w-4 h-4 text-yellow-500 animate-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                            d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
                    </svg>
                  }
                  @default {
                    <svg class="w-4 h-4 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                            d="M5 13l4 4L19 7"/>
                    </svg>
                  }
                }
              </span>
              
              <div class="flex-1">
                <span class="font-medium text-gray-700">{{ decision.sourceName }}:</span>
                <span class="text-gray-600 ml-1">{{ decision.text }}</span>
                @if (decision.reasoning) {
                  <p class="text-xs text-gray-400 mt-0.5">{{ decision.reasoning }}</p>
                }
              </div>
            </div>
          }
        </div>
      }

      <!-- Loading dots -->
      <div class="flex items-center gap-1 ml-11 mt-3">
        <span class="w-2 h-2 bg-primary-400 rounded-full animate-bounce" style="animation-delay: 0ms;"></span>
        <span class="w-2 h-2 bg-primary-400 rounded-full animate-bounce" style="animation-delay: 150ms;"></span>
        <span class="w-2 h-2 bg-primary-400 rounded-full animate-bounce" style="animation-delay: 300ms;"></span>
      </div>
    </div>
  `,
  styles: [`
    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(-5px); }
      to { opacity: 1; transform: translateY(0); }
    }
    .animate-fade-in {
      animation: fadeIn 0.3s ease-out;
    }
    .bg-bmo-blue {
      background-color: #0079C1;
    }
  `]
})
export class AiThinkingOverlayComponent {
  @Input() events: AgentEvent[] = [];

  get decisions(): DecisionItem[] {
    const decisionEvents: DecisionItem[] = [];
    const seenIds = new Set<string>();

    for (const event of this.events) {
      const uniqueId = `${event.event_id}_${event.event_type}`;
      if (seenIds.has(uniqueId)) continue;
      seenIds.add(uniqueId);

      const sourceName = this.getFriendlyName(event.source);
      const data = event.data as Record<string, unknown>;

      switch (event.event_type) {
        case 'orchestrator_decision':
        case 'orchestrator_routing':
          decisionEvents.push({
            id: uniqueId,
            type: 'orchestrator',
            sourceName,
            text: String(data?.['decision'] || data?.['routing_to'] || 'Making decision...'),
            reasoning: String(data?.['reasoning'] || data?.['reason'] || '')
          });
          break;

        case 'agent_decision':
          decisionEvents.push({
            id: uniqueId,
            type: 'agent',
            sourceName,
            text: String(data?.['decision'] || data?.['action'] || 'Processing...'),
            reasoning: String(data?.['reasoning'] || '')
          });
          break;

        case 'agent_thinking':
          decisionEvents.push({
            id: uniqueId,
            type: 'thinking',
            sourceName,
            text: String(data?.['thought'] || 'Thinking...')
          });
          break;

        case 'agent_started':
          decisionEvents.push({
            id: uniqueId,
            type: 'agent',
            sourceName,
            text: String(data?.['task'] || data?.['description'] || 'Started')
          });
          break;

        case 'team_started':
        case 'orchestrator_started':
          decisionEvents.push({
            id: uniqueId,
            type: 'orchestrator',
            sourceName,
            text: String(data?.['description'] || data?.['input_summary'] || 'Starting...')
          });
          break;

        case 'agent_completed':
        case 'team_completed':
          decisionEvents.push({
            id: uniqueId,
            type: 'progress',
            sourceName,
            text: String(data?.['output_summary'] || data?.['summary'] || 'Completed')
          });
          break;
      }
    }

    return decisionEvents.slice(-5).reverse();
  }

  getFriendlyName(source: string): string {
    if (!source) return 'System';
    const key = source.toLowerCase().replace(/\s+/g, '_');
    return FRIENDLY_NAMES[key] || source.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
  }

  getDecisionClass(type: string): string {
    const classes: Record<string, string> = {
      orchestrator: 'bg-purple-50 border-l-2 border-purple-400 pl-2 py-1 rounded-r',
      agent: 'bg-blue-50 border-l-2 border-blue-400 pl-2 py-1 rounded-r',
      thinking: 'bg-yellow-50 border-l-2 border-yellow-400 pl-2 py-1 rounded-r',
      progress: 'bg-green-50 border-l-2 border-green-400 pl-2 py-1 rounded-r',
      reasoning: 'bg-gray-50 border-l-2 border-gray-400 pl-2 py-1 rounded-r'
    };
    return classes[type] || '';
  }
}