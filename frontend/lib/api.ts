/**
 * lib/api.ts
 *
 * All fetch calls in one place, typed.
 *
 * TOKEN STRATEGY
 * ──────────────
 * page.tsx calls setJwtToken(token) in a useEffect whenever auth state changes.
 * BUT: ChatProvider's useEffect that calls loadChats fires in the SAME render
 * cycle — before setJwtToken has run. So we fall back to reading the token
 * directly from localStorage. By the time `user` is truthy in auth-context,
 * the token is guaranteed to already be in localStorage (set during login/signup
 * and confirmed by the /api/auth/me check). This eliminates the "Not
 * authenticated" 401 on the very first fetchChats call.
 */

// ── Module-level state ─────────────────────────────────────────────────────────

const TOKEN_KEY = 'ez_insights_token'

let _jwtToken: string | null = null
let _activeChatId: string | null = null
let _activeUserId: string | null = null

/** Called by page.tsx whenever auth token changes. */
export function setJwtToken(token: string | null): void {
  _jwtToken = token
}

/** Called by chat-context whenever active chat / user changes. */
export function setActiveChatContext(chatId: string | null, userId: string | null): void {
  _activeChatId = chatId
  _activeUserId = userId
}

/**
 * Returns the JWT — prefers the in-memory value set by setJwtToken(),
 * falls back to localStorage so the very first API call on mount works even
 * before page.tsx's useEffect has fired.
 */
function getToken(): string | null {
  if (_jwtToken) return _jwtToken
  if (typeof window !== 'undefined') {
    const stored = localStorage.getItem(TOKEN_KEY)
    if (stored && stored !== 'undefined') return stored
  }
  return null
}

// ── Remote types (matching backend Pydantic models) ────────────────────────────

export interface RemoteChat {
  id: string
  title: string
  user_id: string
  created_at: string
  updated_at: string
}

export interface RemoteMessage {
  id: string
  chat_id: string
  user_id: string
  role: 'user' | 'assistant'
  question: string
  answer?: string
  sql?: string
  row_count?: number
  strategy_used?: string
  created_at: string
}

export interface QueryResponse {
  question: string
  sql: string
  results: Record<string, unknown>[]
  all_results: Record<string, unknown>[]
  row_count: number
  total_rows: number
  page_size: number
  strategy_used: string
  answer: string
  error: string | null
  done: boolean
  chunk?: string
  status?: string
}

// ── Generic authenticated proxy fetch ─────────────────────────────────────────

/**
 * Wraps fetch() for all /api/proxy/* calls:
 *  - Injects Authorization: Bearer <token>
 *  - Translates 401 → "Not authenticated"
 *  - Translates 502 / network failure → "Backend unreachable"
 */
export async function proxyFetch(path: string, options: RequestInit = {}): Promise<Response> {
  const token = getToken()
  if (!token) throw new Error('Not authenticated')

  const existingHeaders = (options.headers as Record<string, string>) ?? {}
  const headers: Record<string, string> = {
    ...existingHeaders,
    Authorization: `Bearer ${token}`,
  }

  let res: Response
  try {
    res = await fetch(path, { ...options, headers })
  } catch (err) {
    throw new Error('Backend unreachable')
  }

  if (res.status === 401) throw new Error('Not authenticated')
  if (res.status === 502) throw new Error('Backend unreachable')
  if (!res.ok) {
    const text = await res.text().catch(() => `HTTP ${res.status}`)
    let detail = text
    try {
      detail = JSON.parse(text)?.detail ?? text
    } catch {
      // keep raw text
    }
    throw new Error(detail)
  }

  return res
}

// ── Chat CRUD ─────────────────────────────────────────────────────────────────

export async function fetchChats(userId: string): Promise<RemoteChat[]> {
  const res = await proxyFetch(`/api/proxy/chats?user_id=${encodeURIComponent(userId)}`)
  return res.json()
}

export async function createChat(userId: string, title: string): Promise<RemoteChat> {
  const res = await proxyFetch('/api/proxy/chats', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user_id: userId, title }),
  })
  return res.json()
}

export async function deleteChatAPI(chatId: string, userId: string): Promise<void> {
  await proxyFetch(
    `/api/proxy/chats/${encodeURIComponent(chatId)}?user_id=${encodeURIComponent(userId)}`,
    { method: 'DELETE' },
  )
}

export async function fetchMessages(chatId: string, userId: string): Promise<RemoteMessage[]> {
  const res = await proxyFetch(
    `/api/proxy/chats/${encodeURIComponent(chatId)}/messages?user_id=${encodeURIComponent(userId)}`,
  )
  return res.json()
}

export async function updateChatTitle(
  chatId: string,
  userId: string,
  title: string,
): Promise<void> {
  await proxyFetch(
    `/api/proxy/chats/${encodeURIComponent(chatId)}/title?user_id=${encodeURIComponent(userId)}&title=${encodeURIComponent(title)}`,
    { method: 'PATCH' },
  )
}

// ── SSE streaming query ────────────────────────────────────────────────────────

/**
 * Streams a NL→SQL query via the backend SSE endpoint.
 *
 * CHANGE: body now accepts `displayed_count` — the number of rows the frontend
 * has already shown to the user. The backend uses this as the OFFSET when
 * generating the paginated SQL (LIMIT page_size OFFSET displayed_count).
 * On a fresh query this is 0 (default). On "Load more from DB" it equals
 * the number of rows currently displayed, so the backend fetches the next page.
 *
 * @param body     - { question, context?, displayed_count? }
 * @param onChunk  - called for each word chunk as the answer streams in
 * @param onDone   - called once with the final metadata payload (done: true)
 * @param onError  - called if the stream yields an error event or throws
 */
export async function sendQuery(
  body: {
    question: string
    context?: Array<{ question: string; sql: string; answer: string }>
    /**
     * Rows the user has already seen. Drives LIMIT/OFFSET on the backend.
     * Omit (or pass 0) for a fresh query; pass the current displayed row
     * count when the user clicks "Load more from database".
     */
    displayed_count?: number
  },
  onChunk: (chunk: string) => void,
  onDone: (meta: Partial<QueryResponse>) => void,
  onError: (err: string) => void,
): Promise<void> {
  const token = getToken()

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  }
  if (token) headers['Authorization'] = `Bearer ${token}`
  // Forward active chat context so the backend can persist messages
  if (_activeChatId) headers['X-Chat-Id'] = _activeChatId
  if (_activeUserId) headers['X-User-Id'] = _activeUserId

  let res: Response
  try {
    res = await fetch('/api/proxy/query', {
      method: 'POST',
      headers,
      // displayed_count is part of the body object — serialised automatically.
      // Backend QueryRequest: { question, context, displayed_count }
      body: JSON.stringify(body),
    })
  } catch {
    onError('Network error: could not reach the server.')
    return
  }

  if (!res.ok || !res.body) {
    const text = await res.text().catch(() => `HTTP ${res.status}`)
    onError(text)
    return
  }

  // ── Parse the SSE stream ─────────────────────────────────────────────────
  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })

      // SSE lines are separated by \n\n; process complete messages only
      const parts = buffer.split('\n\n')
      buffer = parts.pop() ?? ''

      for (const part of parts) {
        for (const line of part.split('\n')) {
          if (!line.startsWith('data: ')) continue
          const json = line.slice(6).trim()
          if (!json) continue

          let event: Record<string, unknown>
          try {
            event = JSON.parse(json)
          } catch {
            continue
          }

          if (event.done === true) {
            onDone(event as Partial<QueryResponse>)
          } else if (typeof event.chunk === 'string') {
            onChunk(event.chunk)
          } else if (event.error && typeof event.error === 'string') {
            onError(event.error)
          }
          // status: "thinking" events are intentionally ignored here —
          // chat-context already shows the loading bubble while isSending=true
        }
      }
    }
  } finally {
    reader.releaseLock()
  }
}