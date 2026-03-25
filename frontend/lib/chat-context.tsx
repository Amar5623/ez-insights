'use client'

import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from 'react'
import type { Chat, ChatMessage } from './types'
import { sendQuery, fetchChats, createChat, deleteChatAPI, fetchMessages, updateChatTitle, setActiveChatContext } from './api'
import { useAuth } from './auth-context'

// ── Types ─────────────────────────────────────────────────────────────────────

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

function buildContextWindow(messages: ChatMessage[]): Array<{ question: string; sql: string; answer: string }> {
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
      i--
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

  const currentChat = chats.find(c => c.id === currentChatId) ?? null

  useEffect(() => {
    setActiveChatContext(currentChatId, user?.id ?? null)
  }, [currentChatId, user?.id])

  // ── Load chats on mount ──────────────────────────────────────────────────────

  useEffect(() => {
    if (!user) {
      setChats([])
      setCurrentChatId(null)
      setIsLoading(false)
      return
    }

    const loadChats = async () => {
      setIsLoading(true)
      try {
        const remote = await fetchChats(user.id)
        const restored: Chat[] = remote.map(c => ({
          id: c.id,
          title: c.title,
          messages: [],
          created_at: new Date(c.created_at),
          updated_at: new Date(c.updated_at),
        }))
        setChats(restored)
        if (restored.length > 0) {
          await loadMessagesIntoChat(restored[0].id, user.id, restored)
          setCurrentChatId(restored[0].id)
        }
      } catch (err) {
        console.error('[chat] Failed to load chats:', err)
      } finally {
        setIsLoading(false)
      }
    }

    loadChats()
  }, [user])

  // ── Load messages for a chat ─────────────────────────────────────────────────

  const loadMessagesIntoChat = async (
    chatId: string,
    userId: string,
    chatList: Chat[],
  ): Promise<Chat[]> => {
    try {
      const remote = await fetchMessages(chatId, userId)
      const messages: ChatMessage[] = remote.map(m => ({
        id: m.id,
        role: m.role,
        content: m.role === 'assistant' ? (m.answer ?? '') : m.question,
        sql: m.sql ?? undefined,
        results: undefined,
        row_count: m.row_count ?? undefined,
        strategy_used: m.strategy_used ?? undefined,
        timestamp: new Date(m.created_at),
      }))

      const updated = chatList.map(c =>
        c.id === chatId ? { ...c, messages } : c,
      )
      setChats(updated)
      return updated
    } catch (err) {
      console.error('[chat] Failed to load messages:', err)
      return chatList
    }
  }

  // ── Actions ──────────────────────────────────────────────────────────────────

  const createNewChat = useCallback(async () => {
    if (!user) return
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
    }
  }, [user])

  const selectChat = useCallback(async (chatId: string) => {
    if (!user) return
    setCurrentChatId(chatId)

    const existing = chats.find(c => c.id === chatId)
    if (existing && existing.messages.length === 0) {
      await loadMessagesIntoChat(chatId, user.id, chats)
    }
  }, [user, chats])

  const deleteChat = useCallback(async (chatId: string) => {
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
  }, [user, currentChatId])

  const sendMessage = useCallback(async (content: string) => {
    if (!user || isSending) return

    let activeChatId = currentChatId
    if (!activeChatId) {
      const newChat = await createNewChat()
      if (!newChat) return
      activeChatId = newChat.id
    }

    const chat = chats.find(c => c.id === activeChatId) ?? { messages: [] as ChatMessage[] }
    const isFirstMessage = chat.messages.length === 0
    if (isFirstMessage) {
      const title = generateChatTitle(content)
      setChats(prev => prev.map(c =>
        c.id === activeChatId ? { ...c, title } : c,
      ))
      updateChatTitle(activeChatId, user.id, title).catch(() => {})
    }

    const userMessage: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'user',
      content,
      timestamp: new Date(),
    }
    const loadingMessage: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'assistant',
      content: '',
      timestamp: new Date(),
      isLoading: true,
    }

    setChats(prev => prev.map(c =>
      c.id === activeChatId
        ? { ...c, messages: [...c.messages, userMessage, loadingMessage], updated_at: new Date() }
        : c,
    ))

    setIsSending(true)

    try {
      const context = buildContextWindow(chat.messages)
      const response = await sendQuery({ question: content, context })

      const assistantMessage: ChatMessage = {
        id: loadingMessage.id,
        role: 'assistant',
        content: response.answer,
        sql: response.sql,
        results: response.results,
        row_count: response.row_count,
        strategy_used: response.strategy_used,
        timestamp: new Date(),
      }

      setChats(prev => prev.map(c =>
        c.id === activeChatId
          ? {
              ...c,
              messages: c.messages.map(m =>
                m.id === loadingMessage.id ? assistantMessage : m,
              ),
              updated_at: new Date(),
            }
          : c,
      ))
    } catch (error) {
      const errorMessage: ChatMessage = {
        id: loadingMessage.id,
        role: 'assistant',
        content: error instanceof Error ? error.message : 'An error occurred while processing your query.',
        timestamp: new Date(),
      }

      setChats(prev => prev.map(c =>
        c.id === activeChatId
          ? {
              ...c,
              messages: c.messages.map(m =>
                m.id === loadingMessage.id ? errorMessage : m,
              ),
            }
          : c,
      ))
    } finally {
      setIsSending(false)
    }
  }, [user, currentChatId, isSending, chats, createNewChat])

  return (
    <ChatContext.Provider value={{
      chats,
      currentChat,
      isLoading,
      isSending,
      createNewChat,
      selectChat,
      deleteChat,
      sendMessage,
    }}>
      {children}
    </ChatContext.Provider>
  )
}

export function useChat() {
  const context = useContext(ChatContext)
  if (!context) throw new Error('useChat must be used within a ChatProvider')
  return context
}