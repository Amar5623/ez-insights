/**
 * Dev 3 owns this file.
 * Hook for fetching and managing query history.
 */
import { useState, useEffect, useCallback } from 'react'
import { api } from '../services/api'
import type { HistoryItem } from '../types'

export function useHistory() {
  const [history, setHistory] = useState<HistoryItem[]>([])
  const [loading, setLoading] = useState(false)

  const fetch = useCallback(async () => {
    setLoading(true)
    try {
      const items = await api.getHistory()
      setHistory(items)
    } catch {
      // silently fail — history is non-critical
    } finally {
      setLoading(false)
    }
  }, [])

  const remove = useCallback(async (id: string) => {
    await api.deleteHistory(id)
    setHistory(prev => prev.filter(item => item.id !== id))
  }, [])

  const prepend = useCallback((item: HistoryItem) => {
    setHistory(prev => [item, ...prev])
  }, [])

  useEffect(() => { fetch() }, [fetch])

  return { history, loading, refresh: fetch, remove, prepend }
}
