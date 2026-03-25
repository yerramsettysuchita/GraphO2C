import { useState, useCallback, useRef } from 'react'
import { api } from '../api'

export function useChat(onNodesReferenced) {
  const [messages, setMessages] = useState([{
    role: 'assistant',
    content: 'Hi! I can help you analyze the Order to Cash process. Try asking about sales orders, billing documents, or deliveries.',
    timestamp: new Date(),
  }])
  const [isLoading, setIsLoading] = useState(false)
  const [isSlowQuery, setIsSlowQuery] = useState(false)
  const abortRef = useRef(null)

  const sendMessage = useCallback(async (question) => {
    if (!question.trim() || isLoading) return
    if (question.length > 500) return

    setMessages(prev => [...prev, {
      role: 'user',
      content: question,
      timestamp: new Date(),
    }])
    setIsLoading(true)
    setIsSlowQuery(false)

    const slowTimer = setTimeout(() => setIsSlowQuery(true), 10000)

    const controller = new AbortController()
    abortRef.current = controller
    const timeoutId = setTimeout(() => controller.abort(), 45000)

    try {
      const data = await api.query(question, controller.signal)
      clearTimeout(timeoutId)
      clearTimeout(slowTimer)

      if (data.nodes_referenced?.length > 0) {
        onNodesReferenced(data.nodes_referenced)
      }

      setMessages(prev => [...prev, {
        role: 'assistant',
        content: data.answer || 'No answer returned.',
        sql: data.sql_executed,
        rowCount: data.row_count,
        nodeCount: data.nodes_referenced?.length || 0,
        queryType: data.query_type,
        timestamp: new Date(),
      }])
    } catch (e) {
      clearTimeout(timeoutId)
      clearTimeout(slowTimer)
      const msg = e.name === 'AbortError'
        ? 'Request timed out after 45 seconds. Please try again.'
        : 'Connection error. Please check the server.'
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: msg,
        queryType: 'error',
        timestamp: new Date(),
      }])
    } finally {
      setIsLoading(false)
      setIsSlowQuery(false)
    }
  }, [isLoading, onNodesReferenced])

  return { messages, isLoading, isSlowQuery, sendMessage }
}
