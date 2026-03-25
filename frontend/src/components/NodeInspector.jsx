import { NODE_COLORS } from '../constants'

export default function NodeInspector({ node, onClose, onViewFlow }) {
  if (!node) return null

  const color = NODE_COLORS[node.node_type] || '#6B7280'
  const shortId = node.node_id?.split('_').slice(1).join('_') || node.node_id
  const props = Object.entries(node.properties || {})
  const neighbors = node.neighbors || []

  return (
    <>
      <style>{`
        @keyframes slideUp {
          from { transform: translateY(100%); opacity: 0; }
          to   { transform: translateY(0);    opacity: 1; }
        }
      `}</style>
      <div style={{
        position: 'fixed', bottom: 0, left: 0, right: 0,
        background: '#FFFFFF',
        borderTop: `2px solid ${color}`,
        boxShadow: '0 -4px 24px rgba(0,0,0,0.10)',
        maxHeight: '42vh', overflowY: 'auto',
        padding: '16px 24px',
        zIndex: 100,
        animation: 'slideUp 0.18s ease',
      }}>
        {/* Header row */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 14 }}>
          <div>
            <span style={{
              display: 'inline-block', padding: '2px 8px',
              borderRadius: 4, background: color,
              color: '#FFFFFF', fontSize: 11,
              fontFamily: '"DM Mono", monospace', marginBottom: 5,
            }}>
              {node.node_type}
            </span>
            <div style={{ fontFamily: '"DM Mono", monospace', fontSize: 13, color: '#111827' }}>
              {shortId}
            </div>
          </div>

          <div style={{ display: 'flex', gap: 8 }}>
            <button
              onClick={() => onViewFlow(node.node_id)}
              style={{
                padding: '6px 14px', background: '#2563EB',
                color: '#FFFFFF', border: 'none', borderRadius: 6,
                fontSize: 12, fontWeight: 500, cursor: 'pointer',
              }}
            >
              View Full Flow
            </button>
            <button
              onClick={onClose}
              style={{
                padding: '6px 12px', background: '#F7F8FA',
                border: '1px solid #E4E6EB', borderRadius: 6,
                fontSize: 14, cursor: 'pointer', color: '#374151',
                lineHeight: 1,
              }}
            >
              ×
            </button>
          </div>
        </div>

        {/* Two-column body */}
        <div style={{ display: 'flex', gap: 32 }}>
          {/* Properties */}
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 11, fontWeight: 500, color: '#6B7280', marginBottom: 8, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              Properties
            </div>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <tbody>
                {props.slice(0, 8).map(([k, v]) => (
                  <tr key={k}>
                    <td style={{ padding: '3px 10px 3px 0', fontSize: 11, color: '#6B7280', fontFamily: '"DM Mono", monospace', whiteSpace: 'nowrap', verticalAlign: 'top' }}>
                      {k}
                    </td>
                    <td style={{ padding: '3px 0', fontSize: 12, color: '#111827', wordBreak: 'break-word' }}>
                      {String(v)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Connections */}
          {neighbors.length > 0 && (
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 11, fontWeight: 500, color: '#6B7280', marginBottom: 8, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                Connections ({neighbors.length})
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 5, maxHeight: 130, overflowY: 'auto' }}>
                {neighbors.slice(0, 12).map((n, i) => (
                  <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 12 }}>
                    <span style={{
                      width: 8, height: 8, borderRadius: '50%',
                      background: NODE_COLORS[n.node_type] || '#6B7280',
                      flexShrink: 0,
                    }} />
                    <span style={{ color: '#6B7280', fontFamily: '"DM Mono", monospace', fontSize: 10, flexShrink: 0 }}>
                      {n.edge_type}
                    </span>
                    <span style={{ color: '#111827', fontSize: 11, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {n.node_id.split('_').slice(1).join('_')}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  )
}
