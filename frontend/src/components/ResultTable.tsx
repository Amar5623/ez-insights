/**
 * Dev 3 owns this file.
 * Renders query results as a responsive table.
 */
interface Props {
  rows: Record<string, unknown>[]
}

export function ResultTable({ rows }: Props) {
  if (!rows.length) {
    return (
      <p style={{ color: '#6b7280', fontSize: '14px', margin: '8px 0' }}>
        No rows returned.
      </p>
    )
  }

  const columns = Object.keys(rows[0])

  return (
    <div style={{ overflowX: 'auto', borderRadius: '8px', border: '1px solid #e5e7eb' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
        <thead>
          <tr style={{ background: '#f9fafb' }}>
            {columns.map(col => (
              <th
                key={col}
                style={{
                  padding: '8px 12px',
                  textAlign: 'left',
                  fontWeight: 500,
                  color: '#374151',
                  borderBottom: '1px solid #e5e7eb',
                  whiteSpace: 'nowrap',
                }}
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={i}
              style={{ background: i % 2 === 0 ? '#fff' : '#f9fafb' }}
            >
              {columns.map(col => (
                <td
                  key={col}
                  style={{
                    padding: '7px 12px',
                    color: '#111827',
                    borderBottom: '1px solid #f3f4f6',
                    maxWidth: '280px',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                  title={String(row[col] ?? '')}
                >
                  {String(row[col] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
