import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../frontend-dist',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        // Split vendor libraries into separate cacheable chunks.
        // Cytoscape (~490KB) and React (~130KB) are versioned independently
        // from app logic, so browsers can cache them across deploys.
        manualChunks: {
          'react-vendor': ['react', 'react-dom'],
          'cytoscape-vendor': ['cytoscape'],
        },
      },
    },
  },
})
