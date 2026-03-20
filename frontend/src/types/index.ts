// Matches backend api/schemas.py — keep in sync

export interface QueryRequest {
  question: string
  db_type?: string
}

export interface QueryResponse {
  question: string
  sql: string
  results: Record<string, unknown>[]
  row_count: number
  strategy_used: StrategyType
  answer: string
  error?: string
}

export interface HistoryItem {
  id: string
  question: string
  sql: string
  strategy_used: StrategyType
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

export type StrategyType = 'sql_filter' | 'fuzzy' | 'vector' | 'combined' | 'auto'

export type LoadingState = 'idle' | 'loading' | 'success' | 'error'
