const API_BASE =
  window.location.hostname === 'localhost' ||
  window.location.hostname === '127.0.0.1'
    ? 'http://localhost:8000'
    : window.location.origin

// Injected at build time via VITE_API_KEY env var.
// Empty string = no auth (local dev without key configured).
const GRAPHO2C_API_KEY = import.meta.env.VITE_API_KEY || ''

function authHeaders() {
  const h = { 'Content-Type': 'application/json' }
  if (GRAPHO2C_API_KEY) h['X-API-Key'] = GRAPHO2C_API_KEY
  return h
}

export const api = {
  health: () =>
    fetch(`${API_BASE}/health`).then(r => r.json()),

  summary: () =>
    fetch(`${API_BASE}/graph/summary`).then(r => r.json()),

  nodesByType: (type, limit = 100) =>
    fetch(`${API_BASE}/graph/nodes?type=${type}&limit=${limit}`).then(r => r.json()),

  nodeDetail: (nodeId) =>
    fetch(`${API_BASE}/graph/node/${encodeURIComponent(nodeId)}`).then(r => r.json()),

  path: (from, to) =>
    fetch(`${API_BASE}/graph/path?from=${from}&to=${to}`).then(r => r.json()),

  query: async (question, signal) => {
    const r = await fetch(`${API_BASE}/query`, {
      method: 'POST',
      headers: authHeaders(),
      body: JSON.stringify({ question }),
      signal,
    })
    return r.json()
  },
}
