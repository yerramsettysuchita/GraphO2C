export default function LoadingOverlay({ status, progress }) {
  return (
    <div style={{
      position: 'fixed', inset: 0, background: '#FFFFFF',
      display: 'flex', flexDirection: 'column',
      alignItems: 'center', justifyContent: 'center', zIndex: 9999,
    }}>
      <style>{`
        @keyframes logoPulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%       { opacity: 0.5; transform: scale(0.92); }
        }
      `}</style>

      <div style={{
        fontSize: 52, color: '#2563EB', marginBottom: 20,
        animation: 'logoPulse 1.6s ease-in-out infinite',
        lineHeight: 1,
      }}>
        ⬡
      </div>

      <div style={{ fontWeight: 500, fontSize: 18, color: '#111827', marginBottom: 6 }}>
        GraphO2C
      </div>

      <div style={{ fontSize: 13, color: '#6B7280', marginBottom: 28 }}>
        {status === 'error'
          ? 'Server unavailable — try refreshing.'
          : 'Starting up server…'}
      </div>

      <div style={{
        width: 220, height: 4, background: '#F0F2F5',
        borderRadius: 2, overflow: 'hidden',
      }}>
        <div style={{
          height: '100%',
          width: `${progress}%`,
          background: '#2563EB',
          borderRadius: 2,
          transition: 'width 0.4s ease',
        }} />
      </div>

      {status !== 'error' && (
        <div style={{ fontSize: 11, color: '#9CA3AF', marginTop: 10, fontFamily: '"DM Mono", monospace' }}>
          {Math.round(progress)}%
        </div>
      )}
    </div>
  )
}
