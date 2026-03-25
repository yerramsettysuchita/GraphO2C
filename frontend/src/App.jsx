import { useRef, useState, useEffect } from 'react'
import { useServer } from './hooks/useServer'
import { useGraph } from './hooks/useGraph'
import { useChat } from './hooks/useChat'
import TopBar from './components/TopBar'
import GraphCanvas from './components/GraphCanvas'
import ChatPanel from './components/ChatPanel'
import NodeInspector from './components/NodeInspector'
import LoadingOverlay from './components/LoadingOverlay'

export default function App() {
  const { status, stats, progress, waitForServer } = useServer()
  const graphContainerRef = useRef(null)
  const {
    initCy, selectedNode, setSelectedNode,
    loadNodeType, highlightNodes, clearHighlights,
    fitGraph, searchNodes, highlightCount,
  } = useGraph(graphContainerRef)

  const { messages, isLoading, isSlowQuery, sendMessage } = useChat(highlightNodes)
  const [currentType, setCurrentType] = useState('SalesOrder')

  // Step 1: start polling for the server
  useEffect(() => {
    waitForServer()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Step 2: once server is ready and GraphCanvas is mounted, init Cytoscape
  useEffect(() => {
    if (status === 'ready') {
      initCy()
      loadNodeType('SalesOrder')
    }
  }, [status]) // eslint-disable-line react-hooks/exhaustive-deps

  function handleTypeChange(type) {
    setCurrentType(type)
    loadNodeType(type)
  }

  function handleViewFlow(nodeId) {
    const parts = nodeId.split('_')
    const type = parts[0].replace(/([A-Z])/g, ' $1').trim().toLowerCase()
    const id = parts.slice(1).join('_').replace('ABCD_2025_', '')
    sendMessage(`Trace the full flow of ${type} ${id}`)
  }

  if (status !== 'ready') {
    return <LoadingOverlay status={status} progress={progress} />
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', background: '#F7F8FA' }}>
      <TopBar
        stats={stats}
        currentType={currentType}
        onTypeChange={handleTypeChange}
        onSearch={searchNodes}
        onFit={fitGraph}
        highlightCount={highlightCount}
        onClearHighlights={clearHighlights}
      />

      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        <GraphCanvas
          containerRef={graphContainerRef}
          currentType={currentType}
          onTypeClick={handleTypeChange}
        />
        <ChatPanel
          messages={messages}
          isLoading={isLoading}
          isSlowQuery={isSlowQuery}
          onSend={sendMessage}
        />
      </div>

      {selectedNode && (
        <NodeInspector
          node={selectedNode}
          onClose={() => setSelectedNode(null)}
          onViewFlow={handleViewFlow}
        />
      )}
    </div>
  )
}
