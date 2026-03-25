export const NODE_COLORS = {
  Customer:         '#2563EB',
  SalesOrder:       '#7C3AED',
  OutboundDelivery: '#0891B2',
  BillingDocument:  '#D97706',
  JournalEntry:     '#059669',
  Payment:          '#DC2626',
  Product:          '#9333EA',
  Plant:            '#6B7280',
  SalesOrderItem:   '#A855F7',
  DeliveryItem:     '#06B6D4',
  BillingDocItem:   '#F59E0B',
}

export const SMALL_NODE_TYPES = new Set([
  'SalesOrderItem', 'DeliveryItem', 'BillingDocItem',
])

export const NODE_TYPES = Object.keys(NODE_COLORS)

export const MAIN_NODE_TYPES = [
  'Customer', 'SalesOrder', 'OutboundDelivery',
  'BillingDocument', 'JournalEntry', 'Payment', 'Product', 'Plant',
]

export const SUGGESTIONS = [
  'Which products have the most billing documents?',
  'Trace the full flow of sales order 740509',
  'How many orders were delivered but not billed?',
  'Which customers have the highest order value?',
]

export const SAP_STATUS = {
  A: 'Not started',
  B: 'In progress',
  C: 'Completed',
}
