// frontend/lib/types.ts
// API Types matching FastAPI backend schemas

export interface QueryRequest {
  question: string
  db_type?: string | null
  context?: Array<{ question: string; sql: string; answer: string }>
}

export interface QueryResponse {
  question: string
  sql: string
  // first page of results (PAGE_SIZE rows)
  results: Record<string, unknown>[]
  // all rows fetched from DB (up to MAX_DB_FETCH_ROWS=100)
  all_results: Record<string, unknown>[]
  // count of results (first page)
  row_count: number
  // total rows fetched from DB
  total_rows: number
  // configured page size
  page_size: number
  strategy_used: string
  answer: string
  error: string | null
}

export interface HistoryItem {
  id: string
  question: string
  sql: string
  strategy_used: string
  row_count: number
  answer: string
  created_at: string
}

export interface HealthResponse {
  status: string
  db_type: string
  db_connected: boolean
  llm_provider: string
  strategy: string
}

// ── Chat UI types ──────────────────────────────────────────────────────────────

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  // The text content — streams in word by word for assistant messages
  content: string
  // First page of results (shown immediately)
  results?: Record<string, unknown>[]
  // All rows — used for client-side "show more" pagination
  all_results?: Record<string, unknown>[]
  // How many rows to show right now (incremented by "show more" clicks)
  displayed_rows?: number
  // Total rows the DB returned
  total_rows?: number
  // Page size setting from backend
  page_size?: number
  sql?: string
  row_count?: number
  strategy_used?: string
  timestamp: Date
  isLoading?: boolean
  isStreaming?: boolean
}

export interface Chat {
  id: string
  title: string
  messages: ChatMessage[]
  created_at: Date
  updated_at: Date
}

export interface User {
  id: string
  email: string
  name: string
}

export type LoadingState = 'idle' | 'loading' | 'error'