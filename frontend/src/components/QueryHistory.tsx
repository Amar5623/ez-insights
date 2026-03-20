/**
 * Dev 3 owns this file.
 * Sidebar list of past queries — click to re-run.
 */
import type { HistoryItem } from '../types'
import { StrategyBadge } from './StrategyBadge'

interface Props {
  history: HistoryItem[]
  onSelect: (question: string) => void
  onDelete: (id: string) => void
  loading: boolean
}

export function QueryHistory({ history, onSelect, onDelete, loading }: Props) {
  if (loading) {
    return <p style={{ fontSize: '13px', color: '#9ca3af', padding: '12px' }}>Loading history...</p>
  }

  if (!history.length) {
    return <p style={{ fontSize: '13px', color: '#9ca3af', padding: '12px' }}>No queries yet.</p>
  }

  return (
    <ul style={{ listStyle: 'none', margin: 0, padding: 0 }}>
      {history.map(item => (
        <li
          key={item.id}
          style={{
            padding: '10px 12px',
            borderBottom: '1px solid #f3f4f6',
            display: 'flex',
            flexDirection: 'column',
            gap: '4px',
          }}
        >
          <button
            onClick={() => onSelect(item.question)}
            style={{
              background: 'none',
              border: 'none',
              padding: 0,
              cursor: 'pointer',
              textAlign: 'left',
              fontSize: '13px',
              fontWeight: 500,
              color: '#111827',
            }}
          >
            {item.question}
          </button>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <StrategyBadge strategy={item.strategy_used} />
            <span style={{ fontSize: '11px', color: '#9ca3af' }}>
              {item.row_count} rows
            </span>
            <button
              onClick={() => onDelete(item.id)}
              style={{
                marginLeft: 'auto',
                background: 'none',
                border: 'none',
                cursor: 'pointer',
                color: '#d1d5db',
                fontSize: '13px',
                padding: '0 4px',
              }}
              title="Delete"
            >
              ✕
            </button>
          </div>
        </li>
      ))}
    </ul>
  )
}
