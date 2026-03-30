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

// lib/types.ts

export interface User {
  id: string
  email: string
  name?: string
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  isLoading?: boolean
  sql?: string
  strategy_used?: string
  timestamp: Date
  row_count?: number
}

export interface Chat {
  id: string
  title: string
  messages: ChatMessage[]
  created_at?: string
  updated_at?: string
}

export type LoadingState = 'idle' | 'loading' | 'error'