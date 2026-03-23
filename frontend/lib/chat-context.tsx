'use client'

import { createContext, useContext, useState, useEffect, useCallback, type ReactNode } from 'react'
import type { Chat, ChatMessage } from './types'
import { sendQuery } from './api'

interface ChatContextType {
  chats: Chat[]
  currentChat: Chat | null
  isLoading: boolean
  isSending: boolean
  createNewChat: () => void
  selectChat: (chatId: string) => void
  deleteChat: (chatId: string) => void
  sendMessage: (content: string) => Promise<void>
}

const ChatContext = createContext<ChatContextType | null>(null)

const STORAGE_KEY = 'ez_insights_chats'
const MAX_RECENT_CHATS = 5

export function ChatProvider({ children }: { children: ReactNode }) {
  const [chats, setChats] = useState<Chat[]>([])
  const [currentChatId, setCurrentChatId] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isSending, setIsSending] = useState(false)

  useEffect(() => {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored) {
      try {
        const parsed = JSON.parse(stored) as Chat[]
        const restored = parsed.map(chat => ({
          ...chat,
          created_at: new Date(chat.created_at),
          updated_at: new Date(chat.updated_at),
          messages: chat.messages.map(msg => ({
            ...msg,
            timestamp: new Date(msg.timestamp),
          })),
        }))
        setChats(restored)
        if (restored.length > 0) {
          setCurrentChatId(restored[0].id)
        }
      } catch {
        localStorage.removeItem(STORAGE_KEY)
      }
    }
    setIsLoading(false)
  }, [])

  useEffect(() => {
    if (!isLoading) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(chats))
    }
  }, [chats, isLoading])

  const currentChat = chats.find(c => c.id === currentChatId) || null

  const createNewChat = useCallback(() => {
    const newChat: Chat = {
      id: crypto.randomUUID(),
      title: 'New Chat',
      messages: [],
      created_at: new Date(),
      updated_at: new Date(),
    }
    setChats(prev => [newChat, ...prev].slice(0, MAX_RECENT_CHATS))
    setCurrentChatId(newChat.id)
  }, [])

  const selectChat = useCallback((chatId: string) => {
    setCurrentChatId(chatId)
  }, [])

  const deleteChat = useCallback((chatId: string) => {
    setChats(prev => {
      const filtered = prev.filter(c => c.id !== chatId)
      if (currentChatId === chatId) {
        setCurrentChatId(filtered.length > 0 ? filtered[0].id : null)
      }
      return filtered
    })
  }, [currentChatId])

  const sendMessage = useCallback(async (content: string) => {
    if (!content.trim() || isSending) return

    let chatId = currentChatId

    if (!chatId) {
      const newChat: Chat = {
        id: crypto.randomUUID(),
        title: content.slice(0, 30) + (content.length > 30 ? '...' : ''),
        messages: [],
        created_at: new Date(),
        updated_at: new Date(),
      }
      setChats(prev => [newChat, ...prev].slice(0, MAX_RECENT_CHATS))
      chatId = newChat.id
      setCurrentChatId(chatId)
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

    setChats(prev => prev.map(chat => {
      if (chat.id === chatId) {
        const isFirstMessage = chat.messages.length === 0
        return {
          ...chat,
          title: isFirstMessage
            ? content.slice(0, 30) + (content.length > 30 ? '...' : '')
            : chat.title,
          messages: [...chat.messages, userMessage, loadingMessage],
          updated_at: new Date(),
        }
      }
      return chat
    }))

    setIsSending(true)

    try {
      const currentMessages = chats.find(c => c.id === chatId)?.messages ?? []

      const context = currentMessages
        .filter(m => !m.isLoading && m.role === 'assistant' && m.sql)
        .slice(-5)
        .map(m => ({
          question: m.content,
          sql: m.sql ?? '',
          answer: m.content,
        }))

      const response = await sendQuery({
        question: content,
        context,
      })

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

      setChats(prev => prev.map(chat => {
        if (chat.id === chatId) {
          return {
            ...chat,
            messages: chat.messages.map(msg =>
              msg.id === loadingMessage.id ? assistantMessage : msg
            ),
            updated_at: new Date(),
          }
        }
        return chat
      }))
    } catch (error) {
      const errorMessage: ChatMessage = {
        id: loadingMessage.id,
        role: 'assistant',
        content: error instanceof Error
          ? error.message
          : 'An error occurred while processing your query.',
        timestamp: new Date(),
      }

      setChats(prev => prev.map(chat => {
        if (chat.id === chatId) {
          return {
            ...chat,
            messages: chat.messages.map(msg =>
              msg.id === loadingMessage.id ? errorMessage : msg
            ),
            updated_at: new Date(),
          }
        }
        return chat
      }))
    } finally {
      setIsSending(false)
    }
  }, [currentChatId, isSending, chats])

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
  if (!context) {
    throw new Error('useChat must be used within a ChatProvider')
  }
  return context
}