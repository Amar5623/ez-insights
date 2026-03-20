/**
 * Dev 3 owns this file.
 * Collapsible raw SQL / Mongo query preview.
 */
import { useState } from 'react'

interface Props {
  sql: string
  strategyName: string
}

export function SqlPreview({ sql, strategyName }: Props) {
  const [open, setOpen] = useState(false)

  if (!sql) return null

  const label = strategyName === 'vector' ? 'Vector query' : 'Generated SQL'

  return (
    <div style={{ marginTop: '8px' }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          background: 'none',
          border: 'none',
          color: '#6366f1',
          fontSize: '13px',
          cursor: 'pointer',
          padding: 0,
          fontWeight: 500,
        }}
      >
        {open ? '▾' : '▸'} {label}
      </button>
      {open && (
        <pre
          style={{
            marginTop: '6px',
            padding: '10px 14px',
            background: '#1e1e2e',
            color: '#cdd6f4',
            borderRadius: '6px',
            fontSize: '12px',
            overflowX: 'auto',
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-all',
          }}
        >
          {sql}
        </pre>
      )}
    </div>
  )
}
