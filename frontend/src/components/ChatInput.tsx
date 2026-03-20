/**
 * Dev 3 owns this file.
 * Text input area + submit button for sending questions.
 */
import { useState, type KeyboardEvent } from 'react'
import type { LoadingState } from '../types'

interface Props {
  onSubmit: (question: string) => void
  state: LoadingState
}

export function ChatInput({ onSubmit, state }: Props) {
  const [value, setValue] = useState('')
  const loading = state === 'loading'

  function handleSubmit() {
    if (value.trim() && !loading) {
      onSubmit(value.trim())
      setValue('')
    }
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit()
    }
  }

  return (
    <div style={{ display: 'flex', gap: '8px', alignItems: 'flex-end' }}>
      <textarea
        value={value}
        onChange={e => setValue(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Ask anything about your database... (Enter to send)"
        disabled={loading}
        rows={3}
        style={{
          flex: 1,
          padding: '10px 12px',
          borderRadius: '8px',
          border: '1px solid #d1d5db',
          fontSize: '14px',
          resize: 'none',
          fontFamily: 'inherit',
          outline: 'none',
        }}
      />
      <button
        onClick={handleSubmit}
        disabled={loading || !value.trim()}
        style={{
          padding: '10px 20px',
          borderRadius: '8px',
          border: 'none',
          background: loading ? '#9ca3af' : '#6366f1',
          color: '#fff',
          fontSize: '14px',
          fontWeight: 500,
          cursor: loading ? 'not-allowed' : 'pointer',
          whiteSpace: 'nowrap',
          height: '42px',
        }}
      >
        {loading ? 'Thinking...' : 'Ask'}
      </button>
    </div>
  )
}
