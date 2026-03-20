/**
 * Dev 3 owns this file.
 * Main page — wires ChatInput, ResultTable, SqlPreview, QueryHistory together.
 */
import { useQuery } from '../hooks/useQuery'
import { useHistory } from '../hooks/useHistory'
import { ChatInput } from '../components/ChatInput'
import { ResultTable } from '../components/ResultTable'
import { SqlPreview } from '../components/SqlPreview'
import { QueryHistory } from '../components/QueryHistory'
import { StrategyBadge } from '../components/StrategyBadge'

export function Home() {
  const { result, state, error, submit } = useQuery()
  const { history, loading: historyLoading, remove } = useHistory()

  async function handleSubmit(question: string) {
    await submit(question)
  }

  return (
    <div style={{ display: 'flex', height: '100vh', fontFamily: 'system-ui, sans-serif' }}>

      {/* Sidebar */}
      <aside style={{
        width: '260px',
        borderRight: '1px solid #e5e7eb',
        display: 'flex',
        flexDirection: 'column',
        flexShrink: 0,
      }}>
        <div style={{ padding: '16px 12px', borderBottom: '1px solid #e5e7eb' }}>
          <span style={{ fontWeight: 600, fontSize: '14px', color: '#111827' }}>
            Query history
          </span>
        </div>
        <div style={{ overflowY: 'auto', flex: 1 }}>
          <QueryHistory
            history={history}
            onSelect={handleSubmit}
            onDelete={remove}
            loading={historyLoading}
          />
        </div>
      </aside>

      {/* Main */}
      <main style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>

        {/* Header */}
        <header style={{
          padding: '16px 24px',
          borderBottom: '1px solid #e5e7eb',
          fontWeight: 600,
          fontSize: '16px',
          color: '#111827',
        }}>
          NL-SQL — Ask your database anything
        </header>

        {/* Results area */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '24px' }}>

          {state === 'idle' && (
            <p style={{ color: '#9ca3af', fontSize: '14px' }}>
              Type a question below to query your database in plain English.
            </p>
          )}

          {state === 'loading' && (
            <p style={{ color: '#6366f1', fontSize: '14px' }}>Generating query...</p>
          )}

          {state === 'error' && (
            <div style={{
              background: '#fef2f2',
              border: '1px solid #fecaca',
              borderRadius: '8px',
              padding: '12px 16px',
              color: '#b91c1c',
              fontSize: '14px',
            }}>
              {error}
            </div>
          )}

          {state === 'success' && result && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>

              {/* Answer */}
              <div style={{
                background: '#f0fdf4',
                border: '1px solid #bbf7d0',
                borderRadius: '8px',
                padding: '14px 16px',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
                  <span style={{ fontWeight: 500, fontSize: '13px', color: '#166534' }}>Answer</span>
                  <StrategyBadge strategy={result.strategy_used} />
                  <span style={{ fontSize: '12px', color: '#6b7280', marginLeft: 'auto' }}>
                    {result.row_count} row{result.row_count !== 1 ? 's' : ''}
                  </span>
                </div>
                <p style={{ margin: 0, fontSize: '14px', color: '#111827', lineHeight: 1.6 }}>
                  {result.answer}
                </p>
              </div>

              {/* SQL preview */}
              <SqlPreview sql={result.sql} strategyName={result.strategy_used} />

              {/* Results table */}
              {result.results.length > 0 && (
                <div>
                  <p style={{ fontSize: '12px', color: '#6b7280', marginBottom: '6px' }}>
                    Showing {result.results.length} of {result.row_count} rows
                  </p>
                  <ResultTable rows={result.results} />
                </div>
              )}
            </div>
          )}
        </div>

        {/* Input bar */}
        <div style={{ padding: '16px 24px', borderTop: '1px solid #e5e7eb' }}>
          <ChatInput onSubmit={handleSubmit} state={state} />
        </div>
      </main>
    </div>
  )
}
