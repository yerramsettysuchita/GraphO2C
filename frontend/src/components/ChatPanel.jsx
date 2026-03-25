import { useState, useRef, useEffect } from 'react'
import SuggestionChips from './SuggestionChips'

export default function ChatPanel({ messages, isLoading, isSlowQuery, onSend }) {
  const [input, setInput] = useState('')
  const bottomRef = useRef(null)
  const showChips = messages.length === 1 && !isLoading

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, isLoading])

  function handleSend() {
    if (!input.trim() || isLoading) return
    onSend(input.trim())
    setInput('')
  }

  return (
    <div style={{
      width: 360, flexShrink: 0, background: '#FFFFFF',
      borderLeft: '1px solid #E4E6EB',
      display: 'flex', flexDirection: 'column',
    }}>
      {/* Header */}
      <div style={{
        padding: '13px 16px', borderBottom: '1px solid #E4E6EB',
        fontWeight: 500, fontSize: 13, color: '#374151', flexShrink: 0,
      }}>
        Ask about your data
      </div>

      {/* Message list */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 10 }}>
        {messages.map((msg, i) => (
          <MessageBubble key={i} msg={msg} />
        ))}
        {isLoading && <LoadingBubble isSlowQuery={isSlowQuery} />}
        <div ref={bottomRef} />
      </div>

      {/* Suggestion chips (only on first load) */}
      {showChips && <SuggestionChips onSelect={q => onSend(q)} />}

      {/* Input row */}
      <div style={{
        padding: '12px 16px', borderTop: '1px solid #E4E6EB',
        display: 'flex', gap: 8, flexShrink: 0,
      }}>
        <input
          type="text"
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && handleSend()}
          placeholder="Ask a question…"
          disabled={isLoading}
          style={{
            flex: 1, padding: '8px 12px',
            border: '1px solid #E4E6EB', borderRadius: 8,
            fontSize: 13, fontFamily: '"DM Sans", sans-serif',
            outline: 'none',
            background: isLoading ? '#F7F8FA' : '#FFFFFF',
            color: '#111827',
          }}
        />
        <button
          onClick={handleSend}
          disabled={isLoading || !input.trim()}
          style={{
            padding: '8px 16px',
            background: isLoading || !input.trim() ? '#E4E6EB' : '#2563EB',
            color: '#FFFFFF', border: 'none', borderRadius: 8,
            fontSize: 13, fontWeight: 500,
            cursor: isLoading || !input.trim() ? 'not-allowed' : 'pointer',
            flexShrink: 0,
          }}
        >
          Send
        </button>
      </div>
    </div>
  )
}

function MessageBubble({ msg }) {
  const [showSql, setShowSql] = useState(false)
  const isUser = msg.role === 'user'

  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: isUser ? 'flex-end' : 'flex-start' }}>
      <div style={{
        maxWidth: '88%', padding: '8px 12px',
        borderRadius: isUser ? '12px 12px 2px 12px' : '12px 12px 12px 2px',
        background: isUser ? '#2563EB' : '#F7F8FA',
        color: isUser ? '#FFFFFF' : '#111827',
        fontSize: 13, lineHeight: 1.55, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
      }}>
        {msg.content}
      </div>

      {msg.sql && (
        <button
          onClick={() => setShowSql(!showSql)}
          style={{
            marginTop: 4, fontSize: 11, color: '#6B7280',
            background: 'none', border: 'none', cursor: 'pointer',
            padding: '2px 4px', fontFamily: '"DM Mono", monospace',
          }}
        >
          {showSql ? '▲ hide SQL' : '▼ show SQL'}
          {msg.rowCount !== undefined && ` · ${msg.rowCount} rows`}
        </button>
      )}

      {showSql && (
        <pre style={{
          marginTop: 4, padding: '8px 10px', background: '#F0F2F5',
          borderRadius: 6, fontSize: 11, fontFamily: '"DM Mono", monospace',
          color: '#374151', overflowX: 'auto', maxWidth: '100%',
          whiteSpace: 'pre-wrap', wordBreak: 'break-all',
        }}>
          {msg.sql}
        </pre>
      )}

      <span style={{ fontSize: 10, color: '#9CA3AF', marginTop: 3 }}>
        {formatTime(msg.timestamp)}
      </span>
    </div>
  )
}

function LoadingBubble({ isSlowQuery }) {
  return (
    <div style={{ display: 'flex', alignItems: 'flex-start' }}>
      <div style={{
        padding: '8px 12px',
        borderRadius: '12px 12px 12px 2px',
        background: '#F7F8FA', fontSize: 13, color: '#6B7280',
      }}>
        {isSlowQuery ? 'Still working — complex query…' : 'Thinking…'}
      </div>
    </div>
  )
}

function formatTime(ts) {
  if (!ts) return ''
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}
