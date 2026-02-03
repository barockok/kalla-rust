'use client';

import { useState, useRef, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Loader2, Send, RotateCcw } from 'lucide-react';
import { ChatMessage } from '@/components/chat/ChatMessage';
import { RecipeCard } from '@/components/chat/RecipeCard';
import type { ChatMessage as ChatMessageType, CardResponse } from '@/lib/chat-types';

export default function ReconcilePage() {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChatMessageType[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [phase, setPhase] = useState('greeting');
  const [recipeDraft, setRecipeDraft] = useState<Record<string, unknown> | null>(null);
  const [started, setStarted] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  useEffect(() => {
    if (!loading) inputRef.current?.focus();
  }, [loading]);

  const sendMessage = async (text: string, cardResponse?: CardResponse) => {
    if (!text.trim() && !cardResponse) return;
    setLoading(true);

    if (!cardResponse) {
      const userMsg: ChatMessageType = {
        role: 'user',
        segments: [{ type: 'text', content: text }],
        timestamp: new Date().toISOString(),
      };
      setMessages(prev => [...prev, userMsg]);
      setInput('');
    }

    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: sessionId,
          message: cardResponse ? undefined : text,
          card_response: cardResponse,
        }),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(errData.error || 'Request failed');
      }

      const data = await res.json();
      setSessionId(data.session_id);
      setPhase(data.phase);
      if (data.recipe_draft) setRecipeDraft(data.recipe_draft);
      if (data.message) setMessages(prev => [...prev, data.message]);
    } catch (err) {
      const errMsg: ChatMessageType = {
        role: 'agent',
        segments: [{ type: 'text', content: `Error: ${err instanceof Error ? err.message : 'Something went wrong'}` }],
        timestamp: new Date().toISOString(),
      };
      setMessages(prev => [...prev, errMsg]);
    } finally {
      setLoading(false);
    }
  };

  const handleCardAction = (cardId: string, action: string, value?: unknown) => {
    sendMessage('', { card_id: cardId, action, value });
  };

  const handleStart = () => {
    setStarted(true);
    sendMessage('Hello, I want to reconcile some data.');
  };

  const handleReset = () => {
    setSessionId(null);
    setMessages([]);
    setInput('');
    setPhase('greeting');
    setRecipeDraft(null);
    setStarted(false);
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (input.trim() && !loading) sendMessage(input);
  };

  if (!started) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] space-y-6">
        <div className="text-center space-y-2">
          <h1 className="text-3xl font-bold tracking-tight">Recipe Builder</h1>
          <p className="text-muted-foreground max-w-md">
            Build reconciliation recipes by demonstrating matches with examples.
            The AI agent will guide you through the process.
          </p>
        </div>
        <Button size="lg" onClick={handleStart}>
          Start Conversation
        </Button>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-[calc(100vh-8rem)]">
      <div className="flex items-center justify-between border-b px-4 py-2">
        <div className="flex items-center gap-2">
          <h1 className="text-lg font-semibold">Recipe Builder</h1>
          <span className="text-xs text-muted-foreground capitalize bg-muted px-2 py-0.5 rounded">
            {phase}
          </span>
        </div>
        <Button variant="ghost" size="sm" onClick={handleReset}>
          <RotateCcw className="h-4 w-4 mr-1" />
          Reset
        </Button>
      </div>
      <div className="flex-1 overflow-y-auto pb-32">
        {messages.map((msg, i) => (
          <ChatMessage key={i} message={msg} onCardAction={handleCardAction} />
        ))}
        {loading && (
          <div className="flex gap-3 px-4 py-3">
            <div className="h-8 w-8 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm font-medium">
              K
            </div>
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Thinking...
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
      <div className="border-t px-4 py-3 bg-background">
        <form onSubmit={handleSubmit} className="flex gap-2 max-w-3xl mx-auto">
          <Input
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={loading ? 'Waiting for response...' : 'Type your message...'}
            disabled={loading}
            className="flex-1"
          />
          <Button type="submit" disabled={loading || !input.trim()} aria-label="Send">
            <Send className="h-4 w-4" />
          </Button>
        </form>
      </div>
      <RecipeCard recipe={recipeDraft} />
    </div>
  );
}
