'use client'

import { useEffect, useRef } from 'react'
import { useChat } from '@/lib/chat-context'
import type { ChatMessage } from '@/lib/types'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
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
  ChevronDownIcon,
  SparklesIcon,
  ZapIcon,
} from 'lucide-react'

// ── Markdown renderer ──────────────────────────────────────────────────────────
// Parses the LLM answer into segments: markdown tables → <table>, rest → <p>
// No external library needed.

function parseMarkdownTable(
  block: string,
): { headers: string[]; rows: string[][] } | null {
  const lines = block.trim().split('\n').filter(l => l.trim())
  if (lines.length < 2) return null
  const isSeparator = (l: string) => /^\|?[\s\-|:]+\|?$/.test(l.trim())
  if (!isSeparator(lines[1])) return null

  const parseRow = (l: string) =>
    l
      .trim()
      .replace(/^\||\|$/g, '')
      .split('|')
      .map(c => c.trim())

  const headers = parseRow(lines[0])
  const rows = lines.slice(2).map(parseRow)
  return { headers, rows }
}

function MarkdownTable({ block }: { block: string }) {
  const parsed = parseMarkdownTable(block)
  if (!parsed)
    return (
      <p className="text-sm leading-relaxed whitespace-pre-wrap">{block}</p>
    )

  const { headers, rows } = parsed

  return (
    <div className="my-2 overflow-hidden rounded-xl border border-border shadow-sm">
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border bg-muted/70">
              {headers.map((h, i) => (
                <th
                  key={i}
                  className="whitespace-nowrap px-4 py-3 text-left font-semibold text-foreground"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr
                key={i}
                className="border-b border-border/50 last:border-0 transition-colors hover:bg-muted/30"
              >
                {row.map((cell, j) => (
                  <td
                    key={j}
                    className="whitespace-nowrap px-4 py-2.5 text-muted-foreground"
                  >
                    {cell}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// Splits answer text into table blocks and text blocks, renders each correctly
function MarkdownAnswer({ content }: { content: string }) {
  const segments = content.split(/((?:^\|.+\n?)+)/m)

  return (
    <div className="space-y-1">
      {segments.map((segment, i) => {
        if (!segment.trim()) return null
        const lines = segment.trim().split('\n')
        const looksLikeTable =
          lines.length >= 2 && lines[0].trim().startsWith('|')

        if (looksLikeTable) {
          return <MarkdownTable key={i} block={segment} />
        }

        return (
          <div key={i} className="text-sm leading-relaxed">
            {segment.split('\n').map((line, j) => {
              if (!line.trim()) return <br key={j} />

              const parts = line.split(/(\*\*[^*]+\*\*)/g)
              return (
                <p key={j} className="my-0.5">
                  {parts.map((part, k) =>
                    part.startsWith('**') && part.endsWith('**') ? (
                      <strong key={k} className="font-semibold text-foreground">
                        {part.slice(2, -2)}
                      </strong>
                    ) : (
                      part
                    ),
                  )}
                </p>
              )
            })}
          </div>
        )
      })}
    </div>
  )
}

// ── LoadingBubble ──────────────────────────────────────────────────────────────
function ThinkingBubble() {
  return (
    <div className="flex gap-3 animate-fade-in">
      <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-primary/20 to-primary/10 ring-2 ring-primary/20">
        <BotIcon className="h-4 w-4 text-primary animate-pulse-subtle" />
      </div>
      <div className="flex items-center gap-1.5 py-3">
        <div className="h-2 w-2 rounded-full bg-primary/40 animate-bounce-subtle" />
        <div className="h-2 w-2 rounded-full bg-primary/60 animate-bounce-subtle animation-delay-100" />
        <div className="h-2 w-2 rounded-full bg-primary/80 animate-bounce-subtle animation-delay-200" />
      </div>
    </div>
  )
}
//── MessageBubble ──────────────────────────────────────────────────────────────
function MessageBubble({
  message,
  index,
}: {
  message: ChatMessage
  index: number
}) {
  const isUser = message.role === 'user'
  const isStreaming = !isUser && message.isLoading && !!message.content

  // Only show thinking dots when waiting for the FIRST token
  if (!isUser && message.isLoading && !message.content) {
    return <ThinkingBubble />
  }

  return (
    <div
      className={cn('flex gap-3 animate-fade-in-up', isUser && 'flex-row-reverse')}
      style={{ animationDelay: `${index * 50}ms` }}
    >
      <div
        className={cn(
          'flex h-9 w-9 shrink-0 items-center justify-center rounded-xl transition-transform duration-300 hover:scale-105',
          isUser
            ? 'bg-primary shadow-md shadow-primary/25'
            : 'bg-gradient-to-br from-primary/20 to-primary/10 ring-2 ring-primary/20',
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
          isUser && 'items-end',
        )}
      >
        {/*
         * Main bubble.
         * The LLM streams the full formatted response as text:
         *   1. Intro line ("Here are the top 10 orders:")
         *   2. Markdown table (rendered by MarkdownAnswer → MarkdownTable)
         *   3. Footer ("_Showing 10 of 47 total rows. Say **show more**..._")
         * No separate results widget needed.
         */}
        <div
          className={cn(
            'rounded-2xl px-4 py-3 shadow-sm transition-all duration-300',
            isUser
              ? 'bg-primary text-primary-foreground shadow-primary/20'
              : 'bg-card border border-border text-foreground',
          )}
        >
          {isUser ? (
            <p className="whitespace-pre-wrap text-sm leading-relaxed">
              {message.content}
            </p>
          ) : (
            <div className="relative">
              <MarkdownAnswer content={message.content} />
              {isStreaming && (
                <span
                  className="inline-block w-[2px] h-[1em] bg-primary align-middle ml-0.5 animate-pulse"
                  aria-hidden="true"
                />
              )}
            </div>
          )}
        </div>

        {/* SQL Preview — only after streaming is done */}
        {!isStreaming && message.sql && (
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

        {/* Strategy Badge — only after streaming is done */}
        {!isStreaming && message.strategy_used && (
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
  const viewportRef = useRef<HTMLDivElement>(null)

  // Auto-scroll to bottom whenever messages change
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
            Ask questions about your database in plain English. I&apos;ll
            translate them into SQL queries and show you the results instantly.
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

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      {/* Compact header shown when sidebar is collapsed */}
      {!sidebarOpen && (
        <div className="flex items-center gap-3 border-b border-border px-6 py-3 bg-background/80 backdrop-blur-sm animate-fade-in">
          <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-gradient-to-br from-primary/20 to-primary/5 ring-2 ring-primary/10">
            <BotIcon className="h-4 w-4 text-primary" />
          </div>
          <span className="font-semibold text-foreground">Ez-Insights</span>
        </div>
      )}

      <ScrollAreaPrimitive.Root className="flex-1 overflow-hidden theme-transition">
        <ScrollAreaPrimitive.Viewport ref={viewportRef} className="h-full w-full">
          <div className="mx-auto max-w-3xl space-y-6 p-6 pb-8">
            {currentChat.messages.map((message, index) => (
              <MessageBubble key={message.id} message={message} index={index} />
            ))}
          </div>
        </ScrollAreaPrimitive.Viewport>
        <ScrollAreaPrimitive.Scrollbar
          orientation="vertical"
          className="flex touch-none select-none transition-colors h-full w-2.5 border-l border-l-transparent p-px"
        >
          <ScrollAreaPrimitive.Thumb className="relative flex-1 rounded-full bg-border" />
        </ScrollAreaPrimitive.Scrollbar>
      </ScrollAreaPrimitive.Root>
    </div>
  )
}