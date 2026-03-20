/**
 * Dev 3 owns this file.
 * Hook for sending a question and managing loading/error state.
 */
import { useState, useCallback } from 'react'
import { api } from '../services/api'
import type { QueryResponse, LoadingState } from '../types'

export function useQuery() {
  const [result, setResult] = useState<QueryResponse | null>(null)
  const [state, setState] = useState<LoadingState>('idle')
  const [error, setError] = useState<string | null>(null)

  const submit = useCallback(async (question: string) => {
    if (!question.trim()) return

    setState('loading')
    setError(null)
    setResult(null)

    try {
      const res = await api.query({ question })
      if (res.error) {
        setError(res.error)
        setState('error')
      } else {
        setResult(res)
        setState('success')
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
      setState('error')
    }
  }, [])

  const reset = useCallback(() => {
    setResult(null)
    setState('idle')
    setError(null)
  }, [])

  return { result, state, error, submit, reset }
}
