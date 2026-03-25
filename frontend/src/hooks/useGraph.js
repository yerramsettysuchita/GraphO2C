import { useRef, useState, useCallback } from 'react'
import cytoscape from 'cytoscape'
import { api } from '../api'
import { NODE_COLORS, SMALL_NODE_TYPES } from '../constants'

function makeNodeLabel(type, props, id) {
  switch (type) {
    case 'SalesOrder':
      return `SO-${props.salesOrder || id.split('_')[1]}\n${
        props.totalNetAmount
          ? parseFloat(props.totalNetAmount).toLocaleString('en-IN', { maximumFractionDigits: 0 }) + ' INR'
          : ''
      }`
    case 'Customer':
      return props.businessPartnerFullName || id
    case 'BillingDocument':
      return `BD-${props.billingDocument || id.split('_')[1]}${
        props.billingDocumentIsCancelled === true || props.billingDocumentIsCancelled === 'true'
          ? '\n[REV]'
          : ''
      }`
    case 'Payment':
      return `Pay-${id.split('_').pop()}\n${
        props.amountInTransactionCurrency
          ? parseFloat(props.amountInTransactionCurrency).toLocaleString('en-IN', { maximumFractionDigits: 0 }) + ' INR'
          : ''
      }`
    case 'Product':
      return props.productDescription || props.product || id
    case 'Plant':
      return props.plantName || id
    case 'OutboundDelivery':
      return `DL-${props.deliveryDocument || id.split('_')[1]}`
    case 'JournalEntry':
      return `JE-${props.accountingDocument || id.split('_')[3]}`
    default:
      return id.split('_').slice(1).join('_') || id
  }
}

function getCytoscapeStyle() {
  return [
    {
      selector: 'node',
      style: {
        'background-color': '#FFFFFF',
        'border-width': 2,
        'border-color': 'data(typeColor)',
        width: 36, height: 36,
        label: 'data(label)',
        'font-size': 10,
        'font-family': '"DM Mono", monospace',
        color: '#374151',
        'text-valign': 'bottom',
        'text-halign': 'center',
        'text-margin-y': 4,
        'text-wrap': 'wrap',
        'text-max-width': 100,
      },
    },
    {
      selector: 'node:selected',
      style: {
        'background-color': 'data(typeColor)',
        color: '#FFFFFF',
        'border-width': 3,
        'border-color': '#FFFFFF',
      },
    },
    {
      selector: 'node.highlighted',
      style: {
        'border-color': '#F59E0B',
        'border-width': 3,
        'background-color': '#FFFBEB',
      },
    },
    { selector: 'node.dimmed', style: { opacity: 0.15 } },
    {
      selector: 'node.search-match',
      style: { 'border-color': '#F59E0B', 'border-width': 3, opacity: 1 },
    },
    {
      selector: 'node[typeSize = "small"]',
      style: { width: 24, height: 24, 'font-size': 9 },
    },
    {
      selector: 'edge',
      style: {
        width: 1,
        'line-color': '#D1D5DB',
        'target-arrow-color': '#D1D5DB',
        'target-arrow-shape': 'triangle',
        'curve-style': 'bezier',
        'arrow-scale': 0.8,
      },
    },
    {
      selector: 'edge.highlighted',
      style: {
        'line-color': '#2563EB',
        'target-arrow-color': '#2563EB',
        width: 2,
      },
    },
  ]
}

export function useGraph(containerRef) {
  const cyRef = useRef(null)
  const [selectedNode, setSelectedNode] = useState(null)
  const [highlightCount, setHighlightCount] = useState(0)

  const expandNeighbors = useCallback((nodeId, neighbors) => {
    const cy = cyRef.current
    if (!cy) return

    const existing = new Set(cy.nodes().map(n => n.id()))
    const toAdd = []

    neighbors.forEach(n => {
      if (!existing.has(n.node_id)) {
        const parts = n.node_id.split('_')
        const type = parts[0]
        const props = n.properties || {}
        toAdd.push({
          data: {
            id: n.node_id,
            node_type: type,
            typeColor: NODE_COLORS[type] || '#6B7280',
            typeSize: SMALL_NODE_TYPES.has(type) ? 'small' : 'normal',
            label: makeNodeLabel(type, props, n.node_id),
            properties: props,
          },
        })
        toAdd.push({
          data: {
            id: `${nodeId}--${n.node_id}`,
            source: nodeId,
            target: n.node_id,
            label: n.edge_type || '',
          },
        })
      }
    })

    if (toAdd.length > 0) {
      cy.add(toAdd)
      const clickedNode = cy.getElementById(nodeId)
      const neighborhood = clickedNode.neighborhood().add(clickedNode)
      cy.layout({
        name: 'concentric',
        fit: true,
        padding: 40,
        eles: neighborhood,
        concentric: n => (n.id() === nodeId ? 2 : 1),
        levelWidth: () => 1,
      }).run()
    }

    cy.nodes().removeClass('highlighted')
    cy.getElementById(nodeId).addClass('selected-focus')
    cy.edges().removeClass('highlighted')
    cy.getElementById(nodeId).connectedEdges().addClass('highlighted')
  }, [])

  const initCy = useCallback(() => {
    if (!containerRef.current || cyRef.current) return

    cyRef.current = cytoscape({
      container: containerRef.current,
      style: getCytoscapeStyle(),
      layout: { name: 'grid' },
      wheelSensitivity: 0.3,
    })

    cyRef.current.on('tap', 'node', async evt => {
      const node = evt.target
      const nodeId = node.id()
      try {
        const data = await api.nodeDetail(nodeId)
        setSelectedNode(data)
        expandNeighbors(nodeId, data.neighbors || [])
      } catch (e) {
        console.error('Node detail failed:', e)
      }
    })

    cyRef.current.on('tap', evt => {
      if (evt.target === cyRef.current) {
        setSelectedNode(null)
      }
    })
  }, [containerRef, expandNeighbors])

  const loadNodeType = useCallback(async (type, clear = true) => {
    const cy = cyRef.current
    if (!cy) return

    if (clear) {
      cy.elements().remove()
      setSelectedNode(null)
    }

    try {
      const data = await api.nodesByType(type, 100)
      const elements = (data.nodes || []).map(n => ({
        data: {
          id: n.node_id || n.id,
          node_type: type,
          typeColor: NODE_COLORS[type] || '#6B7280',
          typeSize: SMALL_NODE_TYPES.has(type) ? 'small' : 'normal',
          label: makeNodeLabel(type, n.properties || {}, n.node_id || n.id),
          properties: n.properties || {},
        },
      }))

      cy.add(elements)
      const layout = elements.length > 80 ? 'grid' : 'cose'
      cy.layout({ name: layout, animate: true, animationDuration: 300, padding: 40 }).run()

      if (clear && cy.nodes().length > 0) {
        setTimeout(() => cy.nodes()[0].trigger('tap'), 500)
      }
    } catch (e) {
      console.error('Load nodes failed:', e)
    }
  }, [])

  const highlightNodes = useCallback(async (nodeIds) => {
    const cy = cyRef.current
    if (!cy || !nodeIds?.length) return

    const neededTypes = new Set(nodeIds.map(id => id.split('_')[0]))
    const currentIds = new Set(cy.nodes().map(n => n.id()))
    const missingTypes = [...neededTypes].filter(t =>
      !nodeIds.some(id => id.startsWith(t + '_') && currentIds.has(id))
    )

    for (const type of missingTypes) {
      await loadNodeType(type, false)
    }

    cy.nodes().removeClass('highlighted')
    let count = 0
    nodeIds.forEach(id => {
      const node = cy.getElementById(id)
      if (node.length) { node.addClass('highlighted'); count++ }
    })
    setHighlightCount(count)

    const highlighted = cy.nodes('.highlighted')
    if (highlighted.length > 0) cy.fit(highlighted, 80)
  }, [loadNodeType])

  const clearHighlights = useCallback(() => {
    cyRef.current?.nodes().removeClass('highlighted')
    setHighlightCount(0)
  }, [])

  const fitGraph = useCallback(() => {
    cyRef.current?.fit(undefined, 40)
  }, [])

  const searchNodes = useCallback((term) => {
    const cy = cyRef.current
    if (!cy) return
    if (!term || term.length < 2) {
      cy.nodes().removeClass('dimmed search-match')
      return
    }
    cy.nodes().forEach(node => {
      const d = node.data()
      const props = d.properties || {}
      const searchable = [
        d.id,
        props.salesOrder, props.billingDocument,
        props.deliveryDocument, props.businessPartnerFullName,
        props.product, props.productDescription,
        props.accountingDocument,
      ].filter(Boolean).join(' ').toLowerCase()

      if (searchable.includes(term.toLowerCase())) {
        node.removeClass('dimmed').addClass('search-match')
      } else {
        node.addClass('dimmed').removeClass('search-match')
      }
    })
    const matches = cy.nodes('.search-match')
    if (matches.length > 0 && matches.length < 20) cy.fit(matches, 60)
  }, [])

  return {
    initCy, cyRef, selectedNode, setSelectedNode,
    loadNodeType, highlightNodes, clearHighlights,
    fitGraph, searchNodes, highlightCount,
  }
}
