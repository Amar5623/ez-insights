// API Types matching FastAPI backend schemas

export interface QueryRequest {
  question: string
  db_type?: string | null
  context?: Array<{ question: string; sql: string; answer: string }>
}

export interface QueryResponse {
  question: string
  sql: string
  results: Record<string, unknown>[]
  row_count: number
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

// Chat UI types
export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  sql?: string
  results?: Record<string, unknown>[]
  row_count?: number
  strategy_used?: string
  timestamp: Date
  isLoading?: boolean
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
