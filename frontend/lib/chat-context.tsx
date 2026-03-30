'use client'

/**
 * lib/chat-context.tsx
 *
 * Central state for all chat operations.
 *
 * PAGINATION CHANGES
 * ──────────────────
 * The backend now returns three extra fields on every SSE "done" event:
 *   - all_results  : all rows the DB returned (up to MAX_RESULT_ROWS)
 *   - total_rows   : true total in the DB (may exceed all_results.length)
 *   - page_size    : backend page size (from settings.PAGE_SIZE)
 *
 * chat-context stores these on each assistant ChatMessage so that
 * chat-messages.tsx can render a results table and offer paging controls.
 *
 * showMore(messageId) implements two tiers of "show more":
 *   1. Client-side expand — if all_results has rows beyond what's currently
 *      shown in `results`, slice the next page_size rows and update the
 *      message locally. Zero network cost.
 *   2. Server-side load — if the user has exhausted all_results but
 *      total_rows > all_results.length, send a new SSE query with
 *      displayed_count = all_results.length so the backend generates
 *      SQL with the correct OFFSET and returns the next batch.
 */

import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  useRef,
  type ReactNode,
} from 'react'
import type { Chat, ChatMessage } from './types'
import {
  sendQuery,
  fetchChats,
  createChat as createChatAPI,
  deleteChatAPI,
  fetchMessages,
  setActiveChatContext,
  type QueryResponse,
} from './api'
import { useAuth } from './auth-context'

// ── Context shape ──────────────────────────────────────────────────────────────

interface ChatContextType {
  chats: Chat[]
  currentChat: Chat | null
  isLoading: boolean    // true while fetching chats / messages from remote
  isSending: boolean    // true while an SSE query is in flight
  sendMessage: (text: string) => void
  createNewChat: () => void
  selectChat: (id: string) => void
  deleteChat: (id: string) => void
}

const ChatContext = createContext<ChatContextType | null>(null)

// ── Helpers ────────────────────────────────────────────────────────────────────

function makeId(): string {
  return Math.random().toString(36).slice(2, 11)
}

/** Build the context array the backend expects from prior assistant messages. */
function buildContext(
  messages: ChatMessage[],
): Array<{ question: string; sql: string; answer: string }> {
  const context: Array<{ question: string; sql: string; answer: string }> = []
  for (const msg of messages) {
    if (msg.role === 'assistant' && msg.content && !msg.isLoading) {
      // Walk backwards to find the paired user message (the question)
      const idx = messages.indexOf(msg)
      const prev = messages[idx - 1]
      if (prev?.role === 'user') {
        context.push({
          question: prev.content,
          sql: msg.sql ?? '',
          answer: msg.content,
        })
      }
    }
  }
  // Backend only needs recent context — keep last 6 pairs
  return context.slice(-6)
}

// ── Provider ───────────────────────────────────────────────────────────────────

export function ChatProvider({ children }: { children: ReactNode }) {
  const { user } = useAuth()

  const [chats, setChats] = useState<Chat[]>([])
  const [currentChatId, setCurrentChatId] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [isSending, setIsSending] = useState(false)

  // Ref so SSE callbacks always see fresh chats without stale closure issues
  const chatsRef = useRef(chats)
  useEffect(() => { chatsRef.current = chats }, [chats])

  const currentChat = chats.find(c => c.id === currentChatId) ?? null

  // ── Load chats from remote on login ───────────────────────────────────────
  useEffect(() => {
    if (!user) {
      setChats([])
      setCurrentChatId(null)
      return
    }

    setIsLoading(true)
    fetchChats(user.id)
      .then(async (remoteChats) => {
        if (remoteChats.length === 0) {
          setIsLoading(false)
          return
        }

        // Map remote chats → local Chat (messages loaded lazily on selectChat)
        const localChats: Chat[] = remoteChats.map(rc => ({
          id: rc.id,
          title: rc.title,
          messages: [],
          created_at: rc.created_at,
          updated_at: rc.updated_at,
        }))
        setChats(localChats)

        // Auto-select the most recent chat and load its messages
        const latest = remoteChats[0]
        setCurrentChatId(latest.id)
        setActiveChatContext(latest.id, user.id)

        try {
          const msgs = await fetchMessages(latest.id, user.id)
          const local: ChatMessage[] = msgs.map(m => ({
            id: m.id,
            role: m.role,
            content: m.role === 'user' ? m.question : (m.answer ?? ''),
            sql: m.sql,
            strategy_used: m.strategy_used,
            row_count: m.row_count,
            timestamp: new Date(m.created_at),
          }))
          setChats(prev =>
            prev.map(c => (c.id === latest.id ? { ...c, messages: local } : c)),
          )
        } catch {
          // Non-fatal — chat opens empty
        }
      })
      .catch(() => {
        // Network error — start with empty chats
      })
      .finally(() => setIsLoading(false))
  }, [user])

  // ── Create new chat ────────────────────────────────────────────────────────
  const createNewChat = useCallback(async () => {
    if (!user) return

    try {
      const remote = await createChatAPI(user.id, 'New Chat')
      const newChat: Chat = {
        id: remote.id,
        title: remote.title,
        messages: [],
        created_at: remote.created_at,
        updated_at: remote.updated_at,
      }
      setChats(prev => [newChat, ...prev])
      setCurrentChatId(remote.id)
      setActiveChatContext(remote.id, user.id)
    } catch {
      // Fallback: create a local-only chat (won't persist across reload)
      const id = makeId()
      const newChat: Chat = { id, title: 'New Chat', messages: [], created_at: new Date().toISOString(), updated_at: new Date().toISOString() }
      setChats(prev => [newChat, ...prev])
      setCurrentChatId(id)
      setActiveChatContext(id, user?.id ?? null)
    }
  }, [user])

  // ── Select an existing chat ────────────────────────────────────────────────
  const selectChat = useCallback(async (id: string) => {
    setCurrentChatId(id)
    setActiveChatContext(id, user?.id ?? null)

    const chat = chatsRef.current.find(c => c.id === id)
    if (!chat || chat.messages.length > 0 || !user) return

    // Lazy-load messages for this chat
    try {
      const msgs = await fetchMessages(id, user.id)
      const local: ChatMessage[] = msgs.map(m => ({
        id: m.id,
        role: m.role,
        content: m.role === 'user' ? m.question : (m.answer ?? ''),
        sql: m.sql,
        strategy_used: m.strategy_used,
        row_count: m.row_count,
        timestamp: new Date(m.created_at),
      }))
      setChats(prev => prev.map(c => (c.id === id ? { ...c, messages: local } : c)))
    } catch {
      // Non-fatal
    }
  }, [user])

  // ── Delete a chat ──────────────────────────────────────────────────────────
  const deleteChat = useCallback(async (id: string) => {
    setChats(prev => prev.filter(c => c.id !== id))
    if (currentChatId === id) {
      const remaining = chatsRef.current.filter(c => c.id !== id)
      const next = remaining[0] ?? null
      setCurrentChatId(next?.id ?? null)
      setActiveChatContext(next?.id ?? null, user?.id ?? null)
    }

    if (user) {
      deleteChatAPI(id, user.id).catch(() => {
        // Restore on failure
        const deleted = chatsRef.current.find(c => c.id === id)
        if (deleted) setChats(prev => [deleted, ...prev])
      })
    }
  }, [currentChatId, user])

  // ── Patch a single message in state ───────────────────────────────────────
  const patchMessage = useCallback(
    (chatId: string, messageId: string, patch: Partial<ChatMessage>) => {
      setChats(prev =>
        prev.map(c =>
          c.id !== chatId
            ? c
            : {
                ...c,
                messages: c.messages.map(m =>
                  m.id === messageId ? { ...m, ...patch } : m,
                ),
              },
        ),
      )
    },
    [],
  )

  // ── Send a message (SSE streaming) ────────────────────────────────────────
  const sendMessage = useCallback(
    (text: string) => {
      if (isSending || !text.trim()) return

      // Auto-create a chat if none is active
      const runWithChat = async (chatId: string) => {
        const currentMessages =
          chatsRef.current.find(c => c.id === chatId)?.messages ?? []

        // 1. Add user message
        const userMsg: ChatMessage = {
          id: makeId(),
          role: 'user',
          content: text,
          timestamp: new Date(),
        }

        // 2. Add loading assistant placeholder
        const assistantId = makeId()
        const assistantPlaceholder: ChatMessage = {
          id: assistantId,
          role: 'assistant',
          content: '',
          isLoading: true,
          timestamp: new Date(),
        }

        setChats(prev =>
          prev.map(c =>
            c.id !== chatId
              ? c
              : { ...c, messages: [...c.messages, userMsg, assistantPlaceholder] },
          ),
        )

        setIsSending(true)
        let accumulated = ''

        await sendQuery(
          {
            question: text,
            context: buildContext(currentMessages),
            displayed_count: 0, // Fresh query always starts at offset 0
          },
          // onChunk — stream words into the assistant bubble
          (chunk) => {
            accumulated += chunk
            patchMessage(chatId, assistantId, {
              content: accumulated,
              isLoading: false,
            })
          },
          // onDone — store metadata; the LLM already streamed the formatted
          // answer text (intro + table + footer) via onChunk above.
          (meta) => {
            patchMessage(chatId, assistantId, {
              content: accumulated || meta.answer || '',
              isLoading: false,
              sql: meta.sql,
              strategy_used: meta.strategy_used,
              row_count: meta.row_count,
            })

            setIsSending(false)
          },
          // onError
          (err) => {
            patchMessage(chatId, assistantId, {
              content: `Error: ${err}`,
              isLoading: false,
            })
            setIsSending(false)
          },
        )
      }

      if (currentChatId) {
        runWithChat(currentChatId)
      } else {
        // No active chat — create one first, then send
        ;(async () => {
          if (!user) return
          try {
            const remote = await createChatAPI(user.id, text.slice(0, 40))
            const newChat: Chat = {
              id: remote.id,
              title: remote.title,
              messages: [],
              created_at: remote.created_at,
              updated_at: remote.updated_at,
            }
            setChats(prev => [newChat, ...prev])
            setCurrentChatId(remote.id)
            setActiveChatContext(remote.id, user.id)
            runWithChat(remote.id)
          } catch {
            const id = makeId()
            const newChat: Chat = { id, title: text.slice(0, 40), messages: [], created_at: new Date().toISOString(), updated_at: new Date().toISOString() }
            setChats(prev => [newChat, ...prev])
            setCurrentChatId(id)
            setActiveChatContext(id, user?.id ?? null)
            runWithChat(id)
          }
        })()
      }
    },
    [currentChatId, isSending, patchMessage, user],
  )

  return (
    <ChatContext.Provider
      value={{
        chats,
        currentChat,
        isLoading,
        isSending,
        sendMessage,
        createNewChat,
        selectChat,
        deleteChat
      }}
    >
      {children}
    </ChatContext.Provider>
  )
}

export function useChat(): ChatContextType {
  const ctx = useContext(ChatContext)
  if (!ctx) throw new Error('useChat must be used inside <ChatProvider>')
  return ctx
}