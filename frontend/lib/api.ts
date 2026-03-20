import type { QueryRequest, QueryResponse, HistoryItem, HealthResponse } from './types'

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
const API_KEY = process.env.NEXT_PUBLIC_API_KEY || ''

async function fetchWithAuth(endpoint: string, options: RequestInit = {}) {
  const headers = {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY,
    ...options.headers,
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

// Query API
export async function sendQuery(request: QueryRequest): Promise<QueryResponse> {
  return fetchWithAuth('/api/query', {
    method: 'POST',
    body: JSON.stringify(request),
  })
}

// History API
export async function getHistory(limit: number = 20): Promise<HistoryItem[]> {
  return fetchWithAuth(`/api/history?limit=${limit}`)
}

export async function deleteHistoryItem(id: string): Promise<{ deleted: boolean }> {
  return fetchWithAuth(`/api/history/${id}`, {
    method: 'DELETE',
  })
}

// Health API
export async function checkHealth(): Promise<HealthResponse> {
  return fetchWithAuth('/api/health')
}
