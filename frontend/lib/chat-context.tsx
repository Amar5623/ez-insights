'use client'

import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  useRef,
  type ReactNode,
} from 'react'
import type { Chat, ChatMessage, QueryResponse } from './types'
import {
  sendQuery,
  fetchChats,
  createChat,
  deleteChatAPI,
  fetchMessages,
  updateChatTitle,
  setActiveChatContext,
} from './api'
import { useAuth } from './auth-context'

// ── Context type ──────────────────────────────────────────────────────────────

interface ChatContextType {
  chats: Chat[]
  currentChat: Chat | null
  isLoading: boolean
  isSending: boolean
  createNewChat: () => Promise<Chat | undefined>
  selectChat: (chatId: string) => Promise<void>
  deleteChat: (chatId: string) => Promise<void>
  sendMessage: (content: string) => Promise<void>
}

const ChatContext = createContext<ChatContextType | null>(null)

const CONTEXT_WINDOW_SIZE = 5

// ── Helpers ───────────────────────────────────────────────────────────────────

function buildContextWindow(
  messages: ChatMessage[],
): Array<{ question: string; sql: string; answer: string }> {
  const completed = messages.filter(m => !m.isLoading)
  const turns: Array<{ question: string; sql: string; answer: string }> = []

  for (let i = completed.length - 1; i >= 1 && turns.length < CONTEXT_WINDOW_SIZE; i--) {
    const msg = completed[i]
    const prev = completed[i - 1]
    if (msg.role === 'assistant' && prev.role === 'user') {
      turns.unshift({
        question: prev.content,
        sql: msg.sql || '',
        answer: msg.content,
      })
      i-- // skip the user message we just consumed
    }
  }

  return turns
}

function generateChatTitle(firstMessage: string): string {
  return firstMessage.slice(0, 40) + (firstMessage.length > 40 ? '...' : '')
}

// ── Provider ──────────────────────────────────────────────────────────────────

export function ChatProvider({ children }: { children: ReactNode }) {
  const { user } = useAuth()
  const [chats, setChats] = useState<Chat[]>([])
  const [currentChatId, setCurrentChatId] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isSending, setIsSending] = useState(false)

  // Stable ref for chats so callbacks don't go stale
  const chatsRef = useRef<Chat[]>(chats)
  useEffect(() => {
    chatsRef.current = chats
  }, [chats])

  const currentChat = chats.find(c => c.id === currentChatId) ?? null

  // Keep api.ts chat context in sync
  useEffect(() => {
    setActiveChatContext(currentChatId, user?.id ?? null)
  }, [currentChatId, user?.id])

  // ── Load messages helper ─────────────────────────────────────────────────────
  // Returns the updated chat list so callers can chain off it without stale state.

  const loadMessagesIntoChat = useCallback(
    async (chatId: string, userId: string, chatList: Chat[]): Promise<Chat[]> => {
      try {
        const remote = await fetchMessages(chatId, userId)
        const messages: ChatMessage[] = remote.map(m => ({
          id: m.id,
          role: m.role as 'user' | 'assistant',
          content: m.role === 'assistant' ? (m.answer ?? '') : m.question,
          sql: m.sql ?? undefined,
          results: undefined,
          row_count: m.row_count ?? undefined,
          strategy_used: m.strategy_used ?? undefined,
          timestamp: new Date(m.created_at),
        }))

        const updated = chatList.map(c => (c.id === chatId ? { ...c, messages } : c))
        setChats(updated)
        return updated
      } catch (err) {
        console.error('[chat] Failed to load messages:', err)
        return chatList
      }
    },
    [],
  )

  // ── Load chats on mount / user change ────────────────────────────────────────

  useEffect(() => {
    if (!user) {
      setChats([])
      setCurrentChatId(null)
      setIsLoading(false)
      return
    }

    let cancelled = false

    const loadChats = async () => {
      setIsLoading(true)
      try {
        const remote = await fetchChats(user.id)
        if (cancelled) return

        const restored: Chat[] = remote.map(c => ({
          id: c.id,
          title: c.title,
          messages: [],
          created_at: new Date(c.created_at),
          updated_at: new Date(c.updated_at),
        }))

        setChats(restored)

        if (restored.length > 0) {
          const withMessages = await loadMessagesIntoChat(restored[0].id, user.id, restored)
          if (cancelled) return
          setChats(withMessages)
          setCurrentChatId(restored[0].id)
        }
      } catch (err) {
        if (!cancelled) {
          console.error('[chat] Failed to load chats:', err)
        }
      } finally {
        if (!cancelled) setIsLoading(false)
      }
    }

    loadChats()

    return () => {
      cancelled = true
    }
  }, [user, loadMessagesIntoChat])

  // ── Actions ──────────────────────────────────────────────────────────────────

  const createNewChat = useCallback(async (): Promise<Chat | undefined> => {
    if (!user) return undefined
    try {
      const remote = await createChat(user.id, 'New Chat')
      const newChat: Chat = {
        id: remote.id,
        title: remote.title,
        messages: [],
        created_at: new Date(remote.created_at),
        updated_at: new Date(remote.updated_at),
      }
      setChats(prev => [newChat, ...prev])
      setCurrentChatId(newChat.id)
      return newChat
    } catch (err) {
      console.error('[chat] Failed to create chat:', err)
      return undefined
    }
  }, [user])

  const selectChat = useCallback(
    async (chatId: string) => {
      if (!user) return
      setCurrentChatId(chatId)

      const existing = chatsRef.current.find(c => c.id === chatId)
      if (existing && existing.messages.length === 0) {
        await loadMessagesIntoChat(chatId, user.id, chatsRef.current)
      }
    },
    [user, loadMessagesIntoChat],
  )

  const deleteChat = useCallback(
    async (chatId: string) => {
      if (!user) return
      try {
        await deleteChatAPI(chatId, user.id)
        setChats(prev => {
          const next = prev.filter(c => c.id !== chatId)
          if (currentChatId === chatId) {
            setCurrentChatId(next.length > 0 ? next[0].id : null)
          }
          return next
        })
      } catch (err) {
        console.error('[chat] Failed to delete chat:', err)
      }
    },
    [user, currentChatId],
  )

  const sendMessage = useCallback(
    async (content: string) => {
      if (!user || isSending) return

      // ── Ensure a chat exists ─────────────────────────────────────────────
      let activeChatId = currentChatId
      if (!activeChatId) {
        const newChat = await createNewChat()
        if (!newChat) return
        activeChatId = newChat.id
      }

      // ── Optimistically set chat title on first message ───────────────────
      const currentChats = chatsRef.current
      const chat = currentChats.find(c => c.id === activeChatId) ?? {
        messages: [] as ChatMessage[],
      }
      const isFirstMessage = chat.messages.length === 0
      if (isFirstMessage) {
        const title = generateChatTitle(content)
        setChats(prev => prev.map(c => (c.id === activeChatId ? { ...c, title } : c)))
        updateChatTitle(activeChatId, user.id, title).catch(() => {})
      }

      // ── Add user + loading placeholder messages ──────────────────────────
      const userMessage: ChatMessage = {
        id: crypto.randomUUID(),
        role: 'user',
        content,
        timestamp: new Date(),
      }
      const loadingMessageId = crypto.randomUUID()
      const loadingMessage: ChatMessage = {
        id: loadingMessageId,
        role: 'assistant',
        content: '',
        timestamp: new Date(),
        isLoading: true,
      }

      setChats(prev =>
        prev.map(c =>
          c.id === activeChatId
            ? {
                ...c,
                messages: [...c.messages, userMessage, loadingMessage],
                updated_at: new Date(),
              }
            : c,
        ),
      )

      setIsSending(true)

      try {
        const context = buildContextWindow(chat.messages)
        let streamedAnswer = ''

        await sendQuery(
          { question: content, context },

          // onChunk — append each streamed word into the loading placeholder
          (chunk: string) => {
            streamedAnswer += chunk
            setChats(prev =>
              prev.map(c =>
                c.id === activeChatId
                  ? {
                      ...c,
                      messages: c.messages.map(m =>
                        m.id === loadingMessageId
                          ? { ...m, content: streamedAnswer, isLoading: false }
                          : m,
                      ),
                    }
                  : c,
              ),
            )
          },

          // onDone — attach sql / strategy metadata when stream ends
          (meta: Partial<QueryResponse>) => {
            // meta.answer is the full answer; prefer it over the streamed
            // accumulation in case any chunk was missed
            if (meta.answer) {
              streamedAnswer = meta.answer
            }
            setChats(prev =>
              prev.map(c =>
                c.id === activeChatId
                  ? {
                      ...c,
                      messages: c.messages.map(m =>
                        m.id === loadingMessageId
                          ? {
                              ...m,
                              content: streamedAnswer,
                              sql: meta.sql,
                              row_count: meta.row_count,
                              strategy_used: meta.strategy_used,
                              isLoading: false,
                              timestamp: new Date(),
                            }
                          : m,
                      ),
                      updated_at: new Date(),
                    }
                  : c,
              ),
            )
          },

          // onError — replace loading placeholder with error text
          (err: string) => {
            setChats(prev =>
              prev.map(c =>
                c.id === activeChatId
                  ? {
                      ...c,
                      messages: c.messages.map(m =>
                        m.id === loadingMessageId
                          ? {
                              ...m,
                              content: `⚠️ ${err}`,
                              isLoading: false,
                              timestamp: new Date(),
                            }
                          : m,
                      ),
                    }
                  : c,
              ),
            )
          },
        )
      } catch (error) {
        // Catches network-level failures that happen before the stream starts
        setChats(prev =>
          prev.map(c =>
            c.id === activeChatId
              ? {
                  ...c,
                  messages: c.messages.map(m =>
                    m.id === loadingMessageId
                      ? {
                          ...m,
                          content:
                            error instanceof Error
                              ? `⚠️ ${error.message}`
                              : '⚠️ An error occurred while processing your query.',
                          isLoading: false,
                          timestamp: new Date(),
                        }
                      : m,
                  ),
                }
              : c,
          ),
        )
      } finally {
        setIsSending(false)
      }
    },
    [user, currentChatId, isSending, createNewChat],
  )

  // ── Render ────────────────────────────────────────────────────────────────

  return (
    <ChatContext.Provider
      value={{
        chats,
        currentChat,
        isLoading,
        isSending,
        createNewChat,
        selectChat,
        deleteChat,
        sendMessage,
      }}
    >
      {children}
    </ChatContext.Provider>
  )
}

export function useChat() {
  const context = useContext(ChatContext)
  if (!context) throw new Error('useChat must be used within a ChatProvider')
  return context
}