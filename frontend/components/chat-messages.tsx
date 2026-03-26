'use client'

import { useEffect, useRef } from 'react'
import { useChat } from '@/lib/chat-context'
import type { ChatMessage } from '@/lib/types'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible'
import * as ScrollAreaPrimitive from '@radix-ui/react-scroll-area'
import {
  UserIcon,
  BotIcon,
  CodeIcon, 
  SparklesIcon,
  ZapIcon,
  ChevronDownIcon
} from 'lucide-react'

import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

// ── Constants ──────────────────────────────────────────────────────────────────
const PAGE_SIZE = 10
       
// ── LoadingBubble ──────────────────────────────────────────────────────────────
function LoadingBubble() {
  return (
    <div className="flex gap-3 animate-fade-in">
      <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-linear-to-br from-primary/20 to-primary/10 ring-2 ring-primary/20">
        <BotIcon className="h-4 w-4 text-primary animate-pulse-subtle" />
      </div>
      <div className="flex-1 space-y-3 py-1">
        <div className="flex items-center gap-2">
          <div className="h-4 w-4 rounded-full bg-primary/20 animate-bounce-subtle" />
          <div className="h-4 w-4 rounded-full bg-primary/30 animate-bounce-subtle animation-delay-100" />
          <div className="h-4 w-4 rounded-full bg-primary/40 animate-bounce-subtle animation-delay-200" />
        </div>
        <Skeleton className="h-4 w-3/4 animate-shimmer" />
        <Skeleton className="h-4 w-1/2 animate-shimmer animation-delay-100" />
      </div>
    </div>
  )
}

// ── MessageBubble ──────────────────────────────────────────────────────────────
function MessageBubble({ message, index }: { message: ChatMessage; index: number }) {
  const isUser = message.role === 'user'

  if (message.isLoading) {
    return <LoadingBubble />
  }

  return (
    <div
      className={cn(
        'flex gap-3 animate-fade-in-up',
        isUser && 'flex-row-reverse'
      )}
      style={{ animationDelay: `${index * 50}ms` }}
    >
      <div
        className={cn(
          'flex h-9 w-9 shrink-0 items-center justify-center rounded-xl transition-transform duration-300 hover:scale-105',
          isUser
            ? 'bg-primary shadow-md shadow-primary/25'
            : 'bg-linear-to-br from-primary/20 to-primary/10 ring-2 ring-primary/20'
        )}
      >
        {isUser ? (
          <UserIcon className="h-4 w-4 text-primary-foreground" />
        ) : (
          <BotIcon className="h-4 w-4 text-primary" />
        )}
      </div>

      <div
        className={cn(
          'flex max-w-[80%] flex-col gap-2',
          isUser && 'items-end'
        )}
      >
        <div
  className={cn(
    'rounded-2xl px-4 py-3 shadow-sm transition-all duration-300',
    isUser
      ? 'bg-primary text-primary-foreground shadow-primary/20'
      : 'bg-card border border-border text-foreground'
  )}
>
  {isUser ? (
    <p className="whitespace-pre-wrap text-sm leading-relaxed">
      {message.content}
    </p>
  ) : (
    <div className="prose prose-sm dark:prose-invert max-w-none
      prose-table:w-full prose-table:text-xs
      prose-th:px-3 prose-th:py-2 prose-th:text-left prose-th:font-semibold prose-th:border prose-th:border-border
      prose-td:px-3 prose-td:py-2 prose-td:border prose-td:border-border prose-td:text-muted-foreground
      prose-thead:bg-muted/70 prose-tr:transition-colors hover:prose-tr:bg-muted/30
      prose-p:leading-relaxed prose-p:text-sm prose-p:my-1
      prose-strong:text-foreground prose-code:text-xs prose-code:bg-muted prose-code:px-1 prose-code:rounded">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>
        {message.content}
      </ReactMarkdown>
    </div>
  )}
</div>

        {/* SQL Preview — unchanged */}
        {message.sql && (
          <Collapsible className="w-full animate-scale-in">
            <CollapsibleTrigger className="flex items-center gap-2 rounded-lg px-3 py-1.5 text-xs text-muted-foreground transition-all duration-200 hover:bg-muted hover:text-foreground">
              <CodeIcon className="h-3.5 w-3.5" />
              <span>View Query</span>
              <ChevronDownIcon className="h-3.5 w-3.5 transition-transform duration-200 group-data-[state=open]:rotate-180" />
            </CollapsibleTrigger>
            <CollapsibleContent className="mt-2 animate-fade-in">
              <pre className="overflow-x-auto rounded-xl border border-border bg-muted/50 p-4 text-xs font-mono">
                <code className="text-foreground">{message.sql}</code>
              </pre>
            </CollapsibleContent>
          </Collapsible>
        )}

        {/* Strategy Badge — unchanged */}
        {message.strategy_used && (
          <Badge
            variant="secondary"
            className="text-xs gap-1 animate-scale-in animation-delay-200"
          >
            <ZapIcon className="h-3 w-3" />
            {message.strategy_used}
          </Badge>
        )}

        <span className="text-xs text-muted-foreground/70">
          {message.timestamp.toLocaleTimeString([], {
            hour: '2-digit',
            minute: '2-digit',
          })}
        </span>
      </div>
    </div>
  )
}

// ── ChatMessages ───────────────────────────────────────────────────────────────
export function   ChatMessages({ sidebarOpen }: { sidebarOpen: boolean }) {
  const { currentChat, isLoading, sendMessage } = useChat()
  // FIX: Direct ref to the Radix ScrollArea Viewport so we can imperatively
  // set scrollTop — avoids the scrollIntoView-on-wrong-ancestor bug.
  const viewportRef = useRef<HTMLDivElement>(null)

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    if (!viewportRef.current) return
    viewportRef.current.scrollTop = viewportRef.current.scrollHeight
  }, [currentChat?.messages])

  const SUGGESTION_CHIPS = [
    'Show all customers',
    'Total sales last month',
    'Top 10 products',
  ]

  if (isLoading) {
    return (
      <div className="flex flex-1 items-center justify-center">
        <div className="space-y-3 text-center animate-fade-in">
          <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-2xl bg-primary/10 animate-pulse-subtle">
            <SparklesIcon className="h-6 w-6 text-primary" />
          </div>
          <p className="text-sm text-muted-foreground">Loading chats...</p>
        </div>
      </div>
    )
  }

  if (!currentChat || currentChat.messages.length === 0) {
    return (
      <div className="flex flex-1 items-center justify-center p-8">
        <div className="max-w-md text-center animate-fade-in-up">
          <div className="mx-auto mb-6 flex h-20 w-20 items-center justify-center rounded-3xl bg-linear-to-br from-primary/20 to-primary/5 ring-4 ring-primary/10 shadow-lg shadow-primary/10">
            <BotIcon className="h-10 w-10 text-primary" />
          </div>
          <h2 className="mb-3 text-2xl font-semibold text-foreground">
            Welcome to Ez-Insights
          </h2>
          <p className="text-muted-foreground leading-relaxed">
            Ask questions about your database in plain English. I'll translate
            them into SQL queries and show you the results instantly.
          </p>
          <div className="mt-8 flex flex-wrap justify-center gap-2">
            {SUGGESTION_CHIPS.map((text, i) => (
              <Badge
                key={text}
                variant="outline"
                className="cursor-pointer px-4 py-2 text-sm transition-all duration-300 hover:bg-primary hover:text-primary-foreground hover:border-primary hover:scale-105 animate-fade-in"
                style={{ animationDelay: `${(i + 1) * 100}ms` }}
                onClick={() => sendMessage(text)}
              >
                {text}
              </Badge>
            ))}
          </div>
        </div>
      </div>
    )
  }

  // Has messages — logo + name stay at top, description + chips are gone
  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      {/* Sticky header — logo + name only */}
      {!sidebarOpen && (
        <div className="flex items-center gap-3 border-b border-border px-6 py-3 bg-background/80 backdrop-blur-sm animate-fade-in">
          <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-linear-to-br from-primary/20 to-primary/5 ring-2 ring-primary/10">
            <BotIcon className="h-4 w-4 text-primary" />
          </div>
          <span className="font-semibold text-foreground">Ez-Insights</span>
        </div>
      )}

      {/* Messages */}
      <ScrollAreaPrimitive.Root className="flex-1 overflow-hidden theme-transition">
        <ScrollAreaPrimitive.Viewport ref={viewportRef} className="h-full w-full">
          <div className="mx-auto max-w-3xl space-y-6 p-6 pb-8">
            {currentChat.messages.map((message, index) => (
              <MessageBubble key={message.id} message={message} index={index} />
            ))}
          </div>
        </ScrollAreaPrimitive.Viewport>
        <ScrollAreaPrimitive.Scrollbar orientation="vertical" className="flex touch-none select-none transition-colors h-full w-2.5 border-l border-l-transparent p-px">
          <ScrollAreaPrimitive.Thumb className="relative flex-1 rounded-full bg-border" />
        </ScrollAreaPrimitive.Scrollbar>
      </ScrollAreaPrimitive.Root>
    </div>
  )
}