/**
 * Dev 3 owns this file.
 * All HTTP calls live here — components never call fetch directly.
 */
import type { QueryRequest, QueryResponse, HistoryItem, HealthResponse } from '../types'

const BASE_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'
const API_KEY = import.meta.env.VITE_API_KEY ?? ''

async function request<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY,
      ...options.headers,
    },
  })

  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body?.detail ?? `Request failed: ${res.status}`)
  }

  return res.json() as Promise<T>
}

export const api = {
  /** Send a natural language question, get back results + answer */
  query(payload: QueryRequest): Promise<QueryResponse> {
    return request<QueryResponse>('/api/query', {
      method: 'POST',
      body: JSON.stringify(payload),
    })
  },

  /** Fetch recent query history */
  getHistory(limit = 20): Promise<HistoryItem[]> {
    return request<HistoryItem[]>(`/api/history?limit=${limit}`)
  },

  /** Delete a history entry by id */
  deleteHistory(id: string): Promise<{ deleted: boolean }> {
    return request<{ deleted: boolean }>(`/api/history/${id}`, {
      method: 'DELETE',
    })
  },

  /** Check backend health + provider config */
  health(): Promise<HealthResponse> {
    return request<HealthResponse>('/api/health')
  },
}
