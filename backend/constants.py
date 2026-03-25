"""
Shared constants for GraphO2C backend.
Node types and colors mirror frontend/src/constants.js — keep in sync.
"""

NODE_COLORS: dict[str, str] = {
    'Customer':         '#2563EB',
    'SalesOrder':       '#7C3AED',
    'OutboundDelivery': '#0891B2',
    'BillingDocument':  '#D97706',
    'JournalEntry':     '#059669',
    'Payment':          '#DC2626',
    'Product':          '#9333EA',
    'Plant':            '#6B7280',
    'SalesOrderItem':   '#A855F7',
    'DeliveryItem':     '#06B6D4',
    'BillingDocItem':   '#F59E0B',
}

NODE_TYPES: list[str] = list(NODE_COLORS.keys())
