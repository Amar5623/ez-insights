'use client'

import { useEffect, useRef, useState } from 'react'
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
  TableIcon,
  ChevronDownIcon,
  SparklesIcon,
  ZapIcon,
  ChevronDownIcon as ChevronIcon,
} from 'lucide-react'

// ── Constants ──────────────────────────────────────────────────────────────────
const PAGE_SIZE = 10

// ── ResultTable ────────────────────────────────────────────────────────────────
// Replaces the old static slice(0,10) + "Showing X of Y" text.
// Keeps all fetched rows in memory — no re-fetch on "Show more".
function ResultTable({
  results,
  rowCount,
}: {
  results: Record<string, unknown>[]
  rowCount: number
}) {
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE)

  const displayed  = results.slice(0, visibleCount)
  const remaining  = results.length - visibleCount
  const columns    = Object.keys(results[0])

  return (
    <div className="overflow-hidden rounded-xl border border-border shadow-sm">
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border bg-muted/70">
              {columns.map((col) => (
                <th
                  key={col}
                  className="whitespace-nowrap px-4 py-3 text-left font-semibold text-foreground"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {displayed.map((row, i) => (
              <tr
                key={i}
                className="border-b border-border/50 last:border-0 transition-colors hover:bg-muted/30"
              >
                {Object.values(row).map((val, j) => (
                  <td
                    key={j}
                    className="whitespace-nowrap px-4 py-2.5 text-muted-foreground"
                  >
                    {String(val ?? '')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Footer: row count summary + Show more button */}
      <div className="flex items-center justify-between border-t border-border bg-muted/30 px-4 py-2">
        <span className="text-xs text-muted-foreground">
          Showing {Math.min(visibleCount, results.length)} of {rowCount} rows
        </span>

        {remaining > 0 && (
          <button
            onClick={() => setVisibleCount((v) => v + PAGE_SIZE)}
            className="flex items-center gap-1 rounded-md px-2 py-1 text-xs text-primary transition-colors hover:bg-primary/10"
          >
            <ChevronIcon className="h-3 w-3" />
            Show {Math.min(PAGE_SIZE, remaining)} more
          </button>
        )}
      </div>
    </div>
  )
}

// ── LoadingBubble ──────────────────────────────────────────────────────────────
function LoadingBubble() {
  return (
    <div className="flex gap-3 animate-fade-in">
      <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-primary/20 to-primary/10 ring-2 ring-primary/20">
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
            : 'bg-gradient-to-br from-primary/20 to-primary/10 ring-2 ring-primary/20'
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
          <p className="whitespace-pre-wrap text-sm leading-relaxed">
            {message.content}
          </p>
        </div>

        {/* SQL Preview — unchanged */}
        {message.sql && (
          <Collapsible className="w-full animate-scale-in">
            <CollapsibleTrigger className="flex items-center gap-2 rounded-lg px-3 py-1.5 text-xs text-muted-foreground transition-all duration-200 hover:bg-muted hover:text-foreground">
              <CodeIcon className="h-3.5 w-3.5" />
              <span>View SQL Query</span>
              <ChevronDownIcon className="h-3.5 w-3.5 transition-transform duration-200 group-data-[state=open]:rotate-180" />
            </CollapsibleTrigger>
            <CollapsibleContent className="mt-2 animate-fade-in">
              <pre className="overflow-x-auto rounded-xl border border-border bg-muted/50 p-4 text-xs font-mono">
                <code className="text-foreground">{message.sql}</code>
              </pre>
            </CollapsibleContent>
          </Collapsible>
        )}

        {/* Results Table — now uses ResultTable with pagination */}
        {message.results && message.results.length > 0 && (
          <Collapsible className="w-full animate-scale-in animation-delay-100">
            <CollapsibleTrigger className="flex items-center gap-2 rounded-lg px-3 py-1.5 text-xs text-muted-foreground transition-all duration-200 hover:bg-muted hover:text-foreground">
              <TableIcon className="h-3.5 w-3.5" />
              <span>View Results ({message.row_count} rows)</span>
              <ChevronDownIcon className="h-3.5 w-3.5 transition-transform duration-200 group-data-[state=open]:rotate-180" />
            </CollapsibleTrigger>
            <CollapsibleContent className="mt-2 animate-fade-in">
              <ResultTable
                results={message.results}
                rowCount={message.row_count ?? message.results.length}
              />
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
export function ChatMessages({ sidebarOpen }: { sidebarOpen: boolean }) {
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
          <div className="mx-auto mb-6 flex h-20 w-20 items-center justify-center rounded-3xl bg-gradient-to-br from-primary/20 to-primary/5 ring-4 ring-primary/10 shadow-lg shadow-primary/10">
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
          <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-gradient-to-br from-primary/20 to-primary/5 ring-2 ring-primary/10">
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
        <ScrollAreaPrimitive.Scrollbar orientation="vertical" className="flex touch-none select-none transition-colors h-full w-2.5 border-l border-l-transparent p-[1px]">
          <ScrollAreaPrimitive.Thumb className="relative flex-1 rounded-full bg-border" />
        </ScrollAreaPrimitive.Scrollbar>
      </ScrollAreaPrimitive.Root>
    </div>
  )
}