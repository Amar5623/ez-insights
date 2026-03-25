'use client'

import { useRef, useEffect } from 'react'
import { useAuth } from '@/lib/auth-context'
import { useChat } from '@/lib/chat-context'
import { Button } from '@/components/ui/button'
import { ThemeToggle } from '@/components/theme-toggle'
import { cn } from '@/lib/utils'
import * as ScrollAreaPrimitive from '@radix-ui/react-scroll-area'
import {
  DatabaseIcon,
  PlusIcon,
  MessageSquareIcon,
  TrashIcon,
  LogOutIcon,
  UserIcon,
  SparklesIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
} from 'lucide-react'

interface ChatSidebarProps {
  isOpen: boolean
  onToggle: () => void
}

export function ChatSidebar({ isOpen, onToggle }: ChatSidebarProps) {
  const { user, logout } = useAuth()
  const { chats, currentChat, createNewChat, selectChat, deleteChat } = useChat()

  const viewportRef = useRef<HTMLDivElement>(null)
  const prevChatsLength = useRef(chats.length)

  useEffect(() => {
    if (chats.length > prevChatsLength.current && viewportRef.current) {
      viewportRef.current.scrollTop = 0
    }
    prevChatsLength.current = chats.length
  }, [chats.length])

  return (
    <>
      {/* Sidebar panel */}
      <aside
        className={cn(
          'relative flex h-full flex-col border-r border-sidebar-border bg-sidebar theme-transition overflow-hidden',
          'transition-[width] duration-300 ease-in-out',
          isOpen ? 'w-64' : 'w-0 border-r-0'
        )}
      >
        {/* Inner wrapper — fixed width so content never wraps during animation */}
        <div className="flex h-full w-64 flex-col">

          {/* Header */}
          <div className="flex items-center justify-between border-b border-sidebar-border p-4 animate-fade-in">
            <div className="flex items-center gap-2">
              <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-primary shadow-lg shadow-primary/25 transition-transform duration-300 hover:scale-105">
                <DatabaseIcon className="h-4 w-4 text-primary-foreground" />
              </div>
              <span className="font-semibold text-sidebar-foreground whitespace-nowrap">Ez-Insights</span>
            </div>
            <div className="flex items-center gap-1">
              <ThemeToggle />
              <button
                onClick={onToggle}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-muted-foreground transition-all duration-200 hover:bg-sidebar-accent hover:text-sidebar-foreground active:scale-90"
                title="Collapse sidebar"
              >
                <ChevronLeftIcon className="h-4 w-4" />
              </button>
            </div>
          </div>

          {/* New Chat Button */}
          <div className="p-3 animate-fade-in animation-delay-100">
            <Button
              onClick={createNewChat}
              className="w-full justify-start gap-2 bg-primary text-primary-foreground shadow-md shadow-primary/20 transition-all duration-300 hover:shadow-lg hover:shadow-primary/30 hover:scale-[1.02] active:scale-[0.98]"
            >
              <PlusIcon className="h-4 w-4" />
              New Chat
              <SparklesIcon className="ml-auto h-3.5 w-3.5 opacity-70" />
            </Button>
          </div>

          {/* Recent Chats */}
          <div className="flex-1 overflow-hidden animate-fade-in animation-delay-200">
            <div className="px-3 py-2">
              <span className="text-xs font-medium uppercase tracking-wider text-muted-foreground whitespace-nowrap">
                Recent Chats
              </span>
            </div>

            <ScrollAreaPrimitive.Root className="h-full overflow-hidden px-2">
              <ScrollAreaPrimitive.Viewport ref={viewportRef} className="h-full w-full">
                {chats.length === 0 ? (
                  <div className="px-3 py-8 text-center">
                    <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-muted animate-pulse-subtle">
                      <MessageSquareIcon className="h-5 w-5 text-muted-foreground" />
                    </div>
                    <p className="text-sm text-muted-foreground">No chats yet</p>
                    <p className="mt-1 text-xs text-muted-foreground/70">Start a new conversation!</p>
                  </div>
                ) : (
                  <div className="flex flex-col gap-1 pb-4">
                    {chats.map((chat, index) => (
                      <div
                        key={chat.id}
                        className={cn(
                          'group relative flex items-center rounded-lg transition-all duration-200 animate-slide-in-left',
                          currentChat?.id === chat.id
                            ? 'bg-sidebar-accent shadow-sm'
                            : 'hover:bg-sidebar-accent/50'
                        )}
                        style={{ animationDelay: `${Math.min(index, 10) * 50}ms` }}
                      >
                        <button
                          onClick={() => selectChat(chat.id)}
                          className="flex flex-1 items-center gap-2 p-2.5 text-left"
                        >
                          <div className={cn(
                            'flex h-7 w-7 shrink-0 items-center justify-center rounded-lg transition-colors duration-200',
                            currentChat?.id === chat.id
                              ? 'bg-primary/15 text-primary'
                              : 'bg-muted text-muted-foreground'
                          )}>
                            <MessageSquareIcon className="h-3.5 w-3.5" />
                          </div>
                          <span className={cn(
                            'truncate text-sm transition-colors duration-200 max-w-[140px]',
                            currentChat?.id === chat.id
                              ? 'text-sidebar-foreground font-medium'
                              : 'text-sidebar-foreground/80'
                          )}>
                            {chat.title}
                          </span>
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation()
                            deleteChat(chat.id)
                          }}
                          className="mr-2 rounded-md p-1.5 opacity-0 transition-all duration-200 hover:bg-destructive/10 group-hover:opacity-100 active:scale-90"
                          title="Delete chat"
                        >
                          <TrashIcon className="h-3.5 w-3.5 text-muted-foreground transition-colors hover:text-destructive" />
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </ScrollAreaPrimitive.Viewport>
              <ScrollAreaPrimitive.Scrollbar
                orientation="vertical"
                className="flex touch-none select-none transition-colors h-full w-2.5 border-l border-l-transparent p-[1px]"
              >
                <ScrollAreaPrimitive.Thumb className="relative flex-1 rounded-full bg-border" />
              </ScrollAreaPrimitive.Scrollbar>
            </ScrollAreaPrimitive.Root>
          </div>

          {/* User Section */}
          <div className="border-t border-sidebar-border p-3 animate-fade-in animation-delay-300">
            <div className="flex items-center gap-3 rounded-xl bg-sidebar-accent/50 p-2.5 transition-colors duration-300 hover:bg-sidebar-accent">
              <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-gradient-to-br from-primary/20 to-primary/10 ring-2 ring-primary/20 transition-transform duration-300 hover:scale-105">
                <UserIcon className="h-4 w-4 text-primary" />
              </div>
              <div className="flex-1 overflow-hidden">
                <p className="truncate text-sm font-medium text-sidebar-foreground">
                  {user?.name || 'Guest'}
                </p>
                <p className="truncate text-xs text-muted-foreground">
                  {user?.email || ''}
                </p>
              </div>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => logout()}
                className="h-8 w-8 shrink-0 rounded-lg transition-all duration-200 hover:bg-destructive/10 hover:text-destructive active:scale-90"
                title="Sign out"
              >
                <LogOutIcon className="h-4 w-4" />
              </Button>
            </div>
          </div>

        </div>
      </aside>

      {/* Expand button — floats on the edge when sidebar is collapsed */}
      {!isOpen && (
        <div className="flex flex-col items-center justify-start pt-4 animate-fade-in">
          <button
            onClick={onToggle}
            className={cn(
              'group relative flex h-8 w-5 items-center justify-center',
              'bg-sidebar border-y border-r border-sidebar-border shadow-md',
              'rounded-r-md',
              'text-muted-foreground transition-all duration-200',
              'hover:bg-primary hover:text-primary-foreground hover:border-primary',
              'hover:shadow-lg hover:shadow-primary/25 hover:w-6 active:scale-95'
            )}
            title="Open sidebar"
          >
            <ChevronRightIcon className="h-3.5 w-3.5 transition-transform duration-200 group-hover:translate-x-0.5" />
          </button>
        </div>
      )}
    </>
  )
}