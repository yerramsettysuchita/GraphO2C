import { NODE_COLORS, MAIN_NODE_TYPES } from '../constants'

export default function GraphCanvas({ containerRef, currentType, onTypeClick }) {
  return (
    <div style={{ flex: 1, position: 'relative', background: '#F7F8FA', overflow: 'hidden' }}>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      <Legend currentType={currentType} onTypeClick={onTypeClick} />
    </div>
  )
}

function Legend({ currentType, onTypeClick }) {
  return (
    <div style={{
      position: 'absolute', bottom: 12, left: 0, right: 0,
      display: 'flex', justifyContent: 'center',
      flexWrap: 'wrap', gap: 5, padding: '0 16px',
      pointerEvents: 'none',
    }}>
      {MAIN_NODE_TYPES.map(type => {
        const active = currentType === type
        const color = NODE_COLORS[type]
        return (
          <button
            key={type}
            onClick={() => onTypeClick(type)}
            style={{
              display: 'flex', alignItems: 'center', gap: 5,
              padding: '3px 9px', borderRadius: 12,
              border: `1.5px solid ${color}`,
              background: active ? color : '#FFFFFF',
              color: active ? '#FFFFFF' : color,
              fontSize: 11, fontFamily: '"DM Mono", monospace',
              cursor: 'pointer', pointerEvents: 'all',
              boxShadow: '0 1px 3px rgba(0,0,0,0.08)',
            }}
          >
            <span style={{
              width: 7, height: 7, borderRadius: '50%',
              background: active ? '#FFFFFF' : color,
              display: 'inline-block', flexShrink: 0,
            }} />
            {type}
          </button>
        )
      })}
    </div>
  )
}
