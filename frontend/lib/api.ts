/**
 * frontend/lib/api.ts
 *
 * All backend communication goes through /api/proxy/* routes.
 * The proxy routes run on the Next.js server and add X-API-Key
 * server-side — the API key never reaches the browser bundle.
 *
 * REMOVED: NEXT_PUBLIC_API_KEY, NEXT_PUBLIC_API_URL
 * These were exposed in the JS bundle. Delete them from .env.local.
 *
 * ADDED to .env.local (server-only):
 *   BACKEND_URL=http://localhost:8000
 *   BACKEND_API_KEY=your-secret-key
 */

import type { QueryRequest, QueryResponse, HistoryItem, HealthResponse } from './types'

// JWT token — set by page.tsx when auth state changes
let _jwtToken: string | null = null
export function setJwtToken(token: string | null) {
  _jwtToken = token
}

// Active chat context — set by ChatProvider when active chat changes
let _activeChatId: string | null = null
let _activeUserId: string | null = null
export function setActiveChatContext(chatId: string | null, userId: string | null) {
  _activeChatId = chatId
  _activeUserId = userId
}

// ── Core fetch wrapper (for non-streaming REST calls) ──────────────────────────

async function proxyFetch(path: string, options: RequestInit = {}) {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string>),
  }
  if (_jwtToken) headers['Authorization'] = `Bearer ${_jwtToken}`

  // /api/proxy/... routes are handled by Next.js server which adds X-API-Key
  const response = await fetch(`/api/proxy${path}`, {
    ...options,
    headers,
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Request failed' }))
    throw new Error(error.detail || `HTTP ${response.status}`)
  }

  return response.json()
}

// ── Streaming query ────────────────────────────────────────────────────────────

export interface StreamCallbacks {
  onChunk: (chunk: string) => void
  onDone: (meta: Partial<QueryResponse>) => void
  onError: (err: string) => void
}

/**
 * Send a query and consume the SSE stream.
 *
 * Events received:
 *   {status: "thinking", done: false}     → pipeline is running, show spinner
 *   {chunk: " word",     done: false}     → answer text token, append to message
 *   {done: true, results, all_results, sql, ...} → final event with all data
 */
export async function sendQuery(
  request: QueryRequest,
  callbacks: StreamCallbacks,
): Promise<void> {
  const { onChunk, onDone, onError } = callbacks

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  }
  if (_jwtToken) headers['Authorization'] = `Bearer ${_jwtToken}`
  if (_activeChatId) headers['X-Chat-Id'] = _activeChatId
  if (_activeUserId) headers['X-User-Id'] = _activeUserId

  let response: Response
  try {
    response = await fetch('/api/proxy/query', {
      method: 'POST',
      headers,
      body: JSON.stringify(request),
    })
  } catch (err) {
    onError('Network error — is the server running?')
    return
  }

  if (!response.ok || !response.body) {
    const err = await response.json().catch(() => ({ detail: 'Request failed' }))
    onError(err.detail || `HTTP ${response.status}`)
    return
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  while (true) {
    const { done, value } = await reader.read()
    if (done) break

    buffer += decoder.decode(value, { stream: true })

    // SSE lines are separated by \n\n — split and process complete events
    const parts = buffer.split('\n\n')
    buffer = parts.pop() ?? ''   // last part may be incomplete

    for (const part of parts) {
      for (const line of part.split('\n')) {
        if (!line.startsWith('data: ')) continue
        const raw = line.slice(6).trim()
        if (!raw) continue

        let event: Record<string, unknown>
        try {
          event = JSON.parse(raw)
        } catch {
          continue  // skip malformed events
        }

        // "thinking" indicator — just a status ping, no data yet
        if (event.status === 'thinking') continue

        // Text token — append to streaming answer
        if (event.chunk !== undefined) {
          onChunk(event.chunk as string)
          continue
        }

        // Final done event — contains all data
        if (event.done === true) {
          if (event.error) {
            onError(event.error as string)
          } else {
            onDone(event as Partial<QueryResponse>)
          }
        }
      }
    }
  }
}

// ── History API ────────────────────────────────────────────────────────────────

export async function getHistory(limit: number = 20): Promise<HistoryItem[]> {
  return proxyFetch(`/history?limit=${limit}`)
}

export async function deleteHistoryItem(id: string): Promise<{ deleted: boolean }> {
  return proxyFetch(`/history/${id}`, { method: 'DELETE' })
}

// ── Health API ─────────────────────────────────────────────────────────────────

export async function checkHealth(): Promise<HealthResponse> {
  return proxyFetch('/health')
}

// ── Chat API ───────────────────────────────────────────────────────────────────

export interface ChatRecordAPI {
  id: string
  user_id: string
  title: string
  created_at: string
  updated_at: string
}

export interface MessageRecordAPI {
  id: string
  chat_id: string
  role: 'user' | 'assistant'
  question: string
  sql: string | null
  answer: string | null
  strategy_used: string | null
  row_count: number | null
  created_at: string
}

export async function fetchChats(userId: string): Promise<ChatRecordAPI[]> {
  return proxyFetch(`/chats?user_id=${encodeURIComponent(userId)}`)
}

export async function createChat(
  userId: string,
  title: string = 'New Chat',
): Promise<ChatRecordAPI> {
  return proxyFetch('/chats', {
    method: 'POST',
    body: JSON.stringify({ user_id: userId, title }),
  })
}

export async function updateChatTitle(
  chatId: string,
  userId: string,
  title: string,
): Promise<void> {
  await proxyFetch(
    `/chats/${chatId}/title?user_id=${encodeURIComponent(userId)}&title=${encodeURIComponent(title)}`,
    { method: 'PATCH' },
  )
}

export async function deleteChatAPI(chatId: string, userId: string): Promise<void> {
  await proxyFetch(
    `/chats/${chatId}?user_id=${encodeURIComponent(userId)}`,
    { method: 'DELETE' },
  )
}

export async function fetchMessages(
  chatId: string,
  userId: string,
): Promise<MessageRecordAPI[]> {
  return proxyFetch(`/chats/${chatId}/messages?user_id=${encodeURIComponent(userId)}`)
}