'use client'

/**
 * lib/chat-context.tsx
 *
 * Central state for all chat operations.
 *
 * PAGINATION CHANGES
 * ──────────────────
 * The backend returns three extra fields on every SSE "done" event:
 *   - total_rows : true total in the DB for this query
 *   - page_size  : backend page size (from settings.PAGE_SIZE)
 *
 * chat-context stores total_rows on each assistant ChatMessage so it can be
 * sent back to the backend on every subsequent pagination request.
 *
 * FIX Bug 1 — accurate footer:
 *   When the user says "show more", we read the previous assistant message's
 *   total_rows and include it in the sendQuery body as `total_rows`. The backend
 *   uses it in the answer prompt so the footer always says "X of 28" instead of
 *   "X of 10" (the batch count).
 *
 * FIX Bug 2 — show all remaining:
 *   When the user's message matches a "show all" phrase, we set show_all=true
 *   in the sendQuery body. The backend removes the PAGE_SIZE cap and returns all
 *   remaining rows at once. The displayed_count is still sent so the SQL gets the
 *   right OFFSET (rows 11–28, not rows 1–28 again).
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
  isLoading: boolean
  isSending: boolean
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
  return context.slice(-6)
}

/**
 * FIX Bug 2: detect "show all remaining" phrasing.
 * Keep this list in sync with SHOW_ALL_PATTERNS in backend intent_classifier.py.
 */
const SHOW_ALL_PHRASES = [
  /^\s*show\s+all\s*$/i,
  /^\s*show\s+all\s+remaining\s*$/i,
  /^\s*show\s+all\s+results?\s*$/i,
  /^\s*show\s+all\s+rows?\s*$/i,
  /^\s*show\s+everything\s*$/i,
  /^\s*show\s+the\s+rest\s*$/i,
  /^\s*show\s+remaining\s*$/i,
  /^\s*show\s+rest\s*$/i,
  /^\s*load\s+all\s*$/i,
  /^\s*load\s+all\s+remaining\s*$/i,
  /^\s*load\s+everything\s*$/i,
  /^\s*get\s+all\s+remaining\s*$/i,
  /^\s*get\s+everything\s*$/i,
  /^\s*see\s+all\s*$/i,
  /^\s*see\s+all\s+remaining\s*$/i,
  /^\s*see\s+everything\s*$/i,
  /^\s*view\s+all\s*$/i,
  /^\s*view\s+all\s+remaining\s*$/i,
  /^\s*view\s+everything\s*$/i,
  /^\s*give\s+me\s+all\s*$/i,
  /^\s*give\s+me\s+all\s+remaining\s*$/i,
  /^\s*give\s+me\s+everything\s*$/i,
  /^\s*show\s+all\s+of\s+them\s*$/i,
  /^\s*show\s+me\s+all\s*$/i,
  /^\s*show\s+me\s+everything\s*$/i,
  /^\s*show\s+me\s+the\s+rest\s*$/i,
  /^\s*show\s+me\s+all\s+remaining\s*$/i,
  /^\s*all\s+remaining\s*$/i,
  /^\s*all\s+results?\s*$/i,
  /^\s*all\s+rows?\s*$/i,
  /^\s*everything\s*$/i,
  /^\s*rest\s+of\s+(them|it|the\s+results?)\s*$/i,
]

/**
 * Standard "show more" patterns (next page only, no "all" signal).
 * Used to decide whether to pass displayed_count to the backend.
 */
const SHOW_MORE_PHRASES = [
  /^\s*more\s*$/i,
  /^\s*next\s*$/i,
  /^\s*continue\s*$/i,
  /^\s*show\s+more\s*$/i,
  /^\s*load\s+more\s*$/i,
  /^\s*next\s+page\s*$/i,
  /^\s*show\s+next\s+page\s*$/i,
  /^\s*more\s+results?\s*$/i,
  /^\s*show\s+\d+\s+more\s*$/i,
  /^\s*next\s+\d+\s*$/i,
  /^\s*give\s+me\s+more\s*$/i,
  /^\s*load\s+next\s*$/i,
  /^\s*show\s+more\s+results?\s*$/i,
  /^\s*get\s+more\s*$/i,
  /^\s*see\s+more\s*$/i,
  /^\s*view\s+more\s*$/i,
]

function isShowAll(text: string): boolean {
  return SHOW_ALL_PHRASES.some(p => p.test(text.trim()))
}

function isShowMore(text: string): boolean {
  return SHOW_MORE_PHRASES.some(p => p.test(text.trim()))
}

/**
 * Walk backwards through messages to find the last assistant message that has
 * a row_count > 0. Returns { displayedCount, totalRows } or null if not found.
 *
 * FIX Bug 1: We read total_rows (the true total from the original query) so
 * we can send it back to the backend on pagination calls.
 */
function getPaginationState(messages: ChatMessage[]): {
  displayedCount: number
  totalRows: number
} | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i]
    if (msg.role === 'assistant' && !msg.isLoading && (msg.row_count ?? 0) > 0) {
      return {
        displayedCount: msg.row_count ?? 0,
        totalRows: msg.total_rows ?? 0,
      }
    }
  }
  return null
}

// ── Provider ───────────────────────────────────────────────────────────────────

export function ChatProvider({ children }: { children: ReactNode }) {
  const { user } = useAuth()

  const [chats, setChats] = useState<Chat[]>([])
  const [currentChatId, setCurrentChatId] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [isSending, setIsSending] = useState(false)

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

        const localChats: Chat[] = remoteChats.map(rc => ({
          id: rc.id,
          title: rc.title,
          messages: [],
          created_at: rc.created_at,
          updated_at: rc.updated_at,
        }))
        setChats(localChats)

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
          // Non-fatal
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

        // ── Determine pagination parameters ────────────────────────────────
        const showAll = isShowAll(text)
        const showMore = isShowMore(text)
        const isPagination = showAll || showMore

        // FIX Bug 1 & Bug 2: find how many rows have been displayed and what
        // the true total is, so we can send both to the backend.
        const paginationState = isPagination
          ? getPaginationState(currentMessages)
          : null

        const displayed_count = paginationState?.displayedCount ?? 0

        // FIX Bug 1: send back the true total so the backend footer is accurate.
        const total_rows = paginationState?.totalRows ?? 0

        // FIX Bug 2: tell the backend to drop the PAGE_SIZE cap.
        const show_all = showAll

        await sendQuery(
          {
            question: text,
            context: buildContext(currentMessages),
            displayed_count,   // OFFSET for the next SQL page
            total_rows,        // FIX Bug 1: true total from original query
            show_all,          // FIX Bug 2: remove page cap
          },
          // onChunk — stream words into the assistant bubble
          (chunk) => {
            accumulated += chunk
            patchMessage(chatId, assistantId, {
              content: accumulated,
              isLoading: true,
            })
          },
          // onDone — store metadata including total_rows for future pagination calls
          (meta) => {
            patchMessage(chatId, assistantId, {
              content: accumulated || meta.answer || '',
              isLoading: false,
              sql: meta.sql,
              strategy_used: meta.strategy_used,
              // FIX Bug 1: store the running displayed count (offset + this batch)
              // so the next pagination call can send the right displayed_count.
              row_count: (displayed_count) + (meta.row_count ?? 0),
              // FIX Bug 1: store the true total so we can send it back next time.
              total_rows: meta.total_rows ?? 0,
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