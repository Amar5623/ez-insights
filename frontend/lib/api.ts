import type { QueryRequest, QueryResponse, HistoryItem, HealthResponse } from './types'

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
const API_KEY = process.env.NEXT_PUBLIC_API_KEY || ''

// JWT token — set by page.tsx on auth change
let _jwtToken: string | null = null
export function setJwtToken(token: string | null) {
  _jwtToken = token
}

// Active chat context — set by chat-context.tsx on chat change
let _activeChatId: string | null = null
let _activeUserId: string | null = null
export function setActiveChatContext(chatId: string | null, userId: string | null) {
  _activeChatId = chatId
  _activeUserId = userId
}

// ── Core fetch wrapper ────────────────────────────────────────────────────────

async function fetchWithAuth(endpoint: string, options: RequestInit = {}) {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY,
    ...(options.headers as Record<string, string>),
  }

  if (_jwtToken) {
    headers['Authorization'] = `Bearer ${_jwtToken}`
  }

  const response = await fetch(`${API_URL}${endpoint}`, {
    ...options,
    headers,
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Request failed' }))
    throw new Error(error.detail || `HTTP ${response.status}`)
  }

  return response.json()
}

// ── Query API ─────────────────────────────────────────────────────────────────

export async function sendQuery(
  request: QueryRequest,
  onChunk: (chunk: string) => void,
  onDone: (meta: Partial<QueryResponse>) => void,
  onError: (err: string) => void,
): Promise<void> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY,
  }
  if (_jwtToken) headers['Authorization'] = `Bearer ${_jwtToken}`
  if (_activeChatId) headers['X-Chat-Id'] = _activeChatId
  if (_activeUserId) headers['X-User-Id'] = _activeUserId

  const response = await fetch(`${API_URL}/api/query`, {
    method: 'POST',
    headers,
    body: JSON.stringify(request),
  })

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

    const lines = buffer.split('\n')
    buffer = lines.pop() ?? ''

    for (const line of lines) {
      if (!line.startsWith('data: ')) continue
      const raw = line.slice(6).trim()
      if (!raw) continue
      try {
        const event = JSON.parse(raw)
        if (event.status === 'thinking') continue
        if (event.chunk !== undefined) {
          onChunk(event.chunk)
        } else if (event.done && !event.chunk) {
          onDone(event)
        }
      } catch {
        // ignore malformed
      }
    }
  }
}

// ── History API (legacy) ──────────────────────────────────────────────────────

export async function getHistory(limit: number = 20): Promise<HistoryItem[]> {
  return fetchWithAuth(`/api/history?limit=${limit}`)
}

export async function deleteHistoryItem(id: string): Promise<{ deleted: boolean }> {
  return fetchWithAuth(`/api/history/${id}`, { method: 'DELETE' })
}

// ── Health API ────────────────────────────────────────────────────────────────

export async function checkHealth(): Promise<HealthResponse> {
  return fetchWithAuth('/api/health')
}

// ── Chat API ──────────────────────────────────────────────────────────────────

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
  return fetchWithAuth(`/api/chats?user_id=${encodeURIComponent(userId)}`)
}

export async function createChat(userId: string, title: string = 'New Chat'): Promise<ChatRecordAPI> {
  return fetchWithAuth('/api/chats', {
    method: 'POST',
    body: JSON.stringify({ user_id: userId, title }),
  })
}

export async function updateChatTitle(
  chatId: string,
  userId: string,
  title: string,
): Promise<void> {
  await fetchWithAuth(
    `/api/chats/${chatId}/title?user_id=${encodeURIComponent(userId)}&title=${encodeURIComponent(title)}`,
    { method: 'PATCH' },
  )
}

export async function deleteChatAPI(chatId: string, userId: string): Promise<void> {
  await fetchWithAuth(
    `/api/chats/${chatId}?user_id=${encodeURIComponent(userId)}`,
    { method: 'DELETE' },
  )
}

export async function fetchMessages(chatId: string, userId: string): Promise<MessageRecordAPI[]> {
  return fetchWithAuth(
    `/api/chats/${chatId}/messages?user_id=${encodeURIComponent(userId)}`,
  )
}