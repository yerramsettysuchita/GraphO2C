import { useState } from 'react'
import { MAIN_NODE_TYPES } from '../constants'

export default function TopBar({
  stats, currentType, onTypeChange,
  onSearch, onFit, highlightCount, onClearHighlights,
}) {
  const [searchVal, setSearchVal] = useState('')

  function handleSearch(val) {
    setSearchVal(val)
    onSearch(val)
  }

  return (
    <div style={{
      height: 52, background: '#FFFFFF',
      borderBottom: '1px solid #E4E6EB',
      display: 'flex', alignItems: 'center',
      padding: '0 16px', gap: 10, flexShrink: 0,
    }}>
      {/* Logo */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 7, flexShrink: 0 }}>
        <span style={{ fontSize: 22, color: '#2563EB', lineHeight: 1 }}>⬡</span>
        <span style={{ fontWeight: 500, fontSize: 15, color: '#111827' }}>GraphO2C</span>
      </div>

      <div style={{ width: 1, height: 24, background: '#E4E6EB', flexShrink: 0 }} />

      {/* Stats */}
      <div style={{
        fontFamily: '"DM Mono", monospace', fontSize: 12, color: '#6B7280',
        background: '#F0F2F5', padding: '4px 10px', borderRadius: 6, flexShrink: 0,
      }}>
        {stats.nodes.toLocaleString()} nodes · {stats.edges.toLocaleString()} edges
      </div>

      {/* Node type filter */}
      <select
        value={currentType}
        onChange={e => onTypeChange(e.target.value)}
        style={{
          padding: '5px 8px', border: '1px solid #E4E6EB',
          borderRadius: 6, fontSize: 12, color: '#374151',
          background: '#FFFFFF', cursor: 'pointer',
          fontFamily: '"DM Sans", sans-serif', flexShrink: 0,
        }}
      >
        {MAIN_NODE_TYPES.map(t => (
          <option key={t} value={t}>{t}</option>
        ))}
      </select>

      <div style={{ flex: 1 }} />

      {/* Search */}
      <input
        type="text"
        placeholder="Search nodes…"
        value={searchVal}
        onChange={e => handleSearch(e.target.value)}
        style={{
          width: 180, padding: '6px 12px',
          border: '1px solid #E4E6EB', borderRadius: 6,
          fontSize: 13, fontFamily: '"DM Sans", sans-serif',
          outline: 'none', background: '#F7F8FA', color: '#111827',
        }}
      />

      {/* Highlight badge */}
      {highlightCount > 0 && (
        <button
          onClick={onClearHighlights}
          style={{
            padding: '5px 10px', background: '#FEF3C7',
            border: '1px solid #F59E0B', borderRadius: 6,
            fontSize: 11, color: '#92400E', cursor: 'pointer',
            fontFamily: '"DM Mono", monospace', flexShrink: 0,
          }}
        >
          {highlightCount} highlighted · clear
        </button>
      )}

      {/* Fit button */}
      <button
        onClick={onFit}
        style={{
          padding: '6px 12px', background: '#F7F8FA',
          border: '1px solid #E4E6EB', borderRadius: 6,
          fontSize: 12, color: '#374151', cursor: 'pointer',
          flexShrink: 0,
        }}
      >
        Fit
      </button>
    </div>
  )
}
