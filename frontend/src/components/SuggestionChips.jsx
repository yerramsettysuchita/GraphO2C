import { SUGGESTIONS } from '../constants'

export default function SuggestionChips({ onSelect }) {
  return (
    <div style={{
      padding: '8px 16px 10px',
      borderTop: '1px solid #F0F2F5',
      display: 'flex',
      flexWrap: 'wrap',
      gap: 6,
    }}>
      {SUGGESTIONS.map((s, i) => (
        <button
          key={i}
          onClick={() => onSelect(s)}
          style={{
            padding: '5px 10px',
            background: '#F0F2F5',
            border: '1px solid #E4E6EB',
            borderRadius: 16,
            fontSize: 11,
            color: '#374151',
            cursor: 'pointer',
            fontFamily: '"DM Sans", sans-serif',
            lineHeight: 1.4,
            textAlign: 'left',
          }}
        >
          {s}
        </button>
      ))}
    </div>
  )
}
