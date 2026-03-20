'use client'

import { useState, useRef, useEffect, type KeyboardEvent } from 'react'
import { useChat } from '@/lib/chat-context'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { SendIcon, SparklesIcon } from 'lucide-react'
import { cn } from '@/lib/utils'

export function ChatInput() {
  const { sendMessage, isSending } = useChat()
  const [value, setValue] = useState('')
  const [isFocused, setIsFocused] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  // Auto-resize textarea
  useEffect(() => {
    const textarea = textareaRef.current
    if (textarea) {
      textarea.style.height = 'auto'
      textarea.style.height = `${Math.min(textarea.scrollHeight, 150)}px`
    }
  }, [value])

  function handleSubmit() {
    if (value.trim() && !isSending) {
      sendMessage(value.trim())
      setValue('')
      // Reset textarea height
      if (textareaRef.current) {
        textareaRef.current.style.height = 'auto'
      }
    }
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit()
    }
  }

  return (
    <div className="border-t border-border bg-background p-4 theme-transition animate-fade-in">
      <div className="mx-auto max-w-3xl">
        <div 
          className={cn(
            "flex items-end gap-3 rounded-2xl border bg-card p-2 shadow-lg transition-all duration-300",
            isFocused 
              ? "border-primary/50 ring-4 ring-primary/10 shadow-primary/5" 
              : "border-input shadow-sm hover:border-primary/30"
          )}
        >
          <Textarea
            ref={textareaRef}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={handleKeyDown}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setIsFocused(false)}
            placeholder="Ask anything about your database..."
            disabled={isSending}
            rows={1}
            className="min-h-[44px] max-h-[150px] flex-1 resize-none border-0 bg-transparent px-3 py-3 text-sm focus-visible:ring-0 focus-visible:ring-offset-0 placeholder:text-muted-foreground/60"
          />
          <Button
            onClick={handleSubmit}
            disabled={isSending || !value.trim()}
            size="icon"
            className={cn(
              "h-10 w-10 shrink-0 rounded-xl transition-all duration-300",
              value.trim() && !isSending
                ? "bg-primary shadow-md shadow-primary/25 hover:shadow-lg hover:shadow-primary/30 hover:scale-105 active:scale-95"
                : "bg-muted text-muted-foreground"
            )}
          >
            {isSending ? (
              <SparklesIcon className="h-4 w-4 animate-pulse-subtle" />
            ) : (
              <SendIcon className="h-4 w-4" />
            )}
            <span className="sr-only">Send message</span>
          </Button>
        </div>
        <p className="mt-3 text-center text-xs text-muted-foreground/70">
          Press <kbd className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium">Enter</kbd> to send, <kbd className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium">Shift + Enter</kbd> for new line
        </p>
      </div>
    </div>
  )
}
