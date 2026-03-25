import { useState, useEffect, useCallback } from 'react'
import { api } from '../api'

export function useServer() {
  const [status, setStatus] = useState('connecting') // connecting | ready | error
  const [stats, setStats] = useState({ nodes: 0, edges: 0 })
  const [progress, setProgress] = useState(0)

  const waitForServer = useCallback(async () => {
    const maxWait = 90000
    const start = Date.now()

    while (Date.now() - start < maxWait) {
      try {
        const data = await api.health()
        setStats({ nodes: data.nodes, edges: data.edges })
        setProgress(100)
        setStatus('ready')
        return true
      } catch {
        const elapsed = (Date.now() - start) / 1000
        setProgress(Math.min(85, elapsed))
      }
      await new Promise(r => setTimeout(r, 2000))
    }
    setStatus('error')
    return false
  }, [])

  // Keep-alive ping every 10 minutes to prevent Render spin-down
  useEffect(() => {
    if (status !== 'ready') return
    const id = setInterval(() => {
      api.health().catch(() => {})
    }, 600000)
    return () => clearInterval(id)
  }, [status])

  return { status, stats, progress, waitForServer }
}
