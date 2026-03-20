/**
 * Dev 3 owns this file.
 * Pill badge showing which query strategy was used.
 */
import type { StrategyType } from '../types'

const LABELS: Record<StrategyType, string> = {
  sql_filter: 'SQL filter',
  fuzzy: 'Fuzzy match',
  vector: 'Vector search',
  combined: 'Combined',
  auto: 'Auto',
}

const COLORS: Record<StrategyType, string> = {
  sql_filter: '#e0f2fe',   // light blue
  fuzzy: '#fef9c3',        // light yellow
  vector: '#ede9fe',       // light purple
  combined: '#dcfce7',     // light green
  auto: '#f3f4f6',         // light gray
}

const TEXT_COLORS: Record<StrategyType, string> = {
  sql_filter: '#0369a1',
  fuzzy: '#854d0e',
  vector: '#5b21b6',
  combined: '#166534',
  auto: '#374151',
}

interface Props {
  strategy: StrategyType
}

export function StrategyBadge({ strategy }: Props) {
  return (
    <span
      style={{
        display: 'inline-block',
        padding: '2px 10px',
        borderRadius: '999px',
        fontSize: '12px',
        fontWeight: 500,
        background: COLORS[strategy] ?? COLORS.auto,
        color: TEXT_COLORS[strategy] ?? TEXT_COLORS.auto,
      }}
    >
      {LABELS[strategy] ?? strategy}
    </span>
  )
}
