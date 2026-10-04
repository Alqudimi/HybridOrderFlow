# MongoDB Index Strategy & ExecutionStats Benchmark Report

This report documents actual MongoDB execution metrics comparing performance before and after index creation.

| Query Name | Index Name | Index Type | Docs Examined (Before -> After) | Keys Examined (Before -> After) | Plan Stage (Before -> After) | IXSCAN Used? | In-Memory Sort Eliminated? |
|---|---|---|---|---|---|---|---|
| `orders_by_customer` | `idx_customer_id` | single | 943 -> **3** | 0 -> **3** | SORT (COLLSCAN) -> **SORT (IXSCAN)** | ✅ Yes | No |
| `orders_by_city_and_status` | `idx_city_status_order_date` | compound (ESR pattern) | 943 -> **12** | 0 -> **12** | SORT (COLLSCAN) -> **FETCH (IXSCAN)** | ✅ Yes | ✅ Yes |
| `orders_containing_item_sku` | `idx_items_sku` | multikey | 943 -> **269** | 0 -> **269** | SORT (COLLSCAN) -> **SORT (IXSCAN)** | ✅ Yes | No |

## Deep Analysis & Architectural Rationales

### Query: `orders_by_customer`
- **Index Created**: `idx_customer_id` (single)
- **Filter Specification**: `{'customer_id': 'عميل-1'}`
- **Sort Specification**: `{'order_date': -1}`
- **Total Docs Examined**: Before = `943`, After = `3` (Optimization: **314.33x** reduction)
- **Total Keys Examined**: Before = `0`, After = `3`
- **Execution Time (ms)**: Before = `0 ms`, After = `1 ms`
- **Stage Progression**: Before had `COLLSCAN=True`, `SORT=True` -> After has `IXSCAN=True`, `SORT=True`
- **Engineering Assessment**: Single-field index eliminates COLLSCAN, scanning only matching customer keys.

### Query: `orders_by_city_and_status`
- **Index Created**: `idx_city_status_order_date` (compound (ESR pattern))
- **Filter Specification**: `{'city': 'صنعاء', 'status': 'قيد الانتظار'}`
- **Sort Specification**: `{'order_date': -1}`
- **Total Docs Examined**: Before = `943`, After = `12` (Optimization: **78.58x** reduction)
- **Total Keys Examined**: Before = `0`, After = `12`
- **Execution Time (ms)**: Before = `0 ms`, After = `1 ms`
- **Stage Progression**: Before had `COLLSCAN=True`, `SORT=True` -> After has `IXSCAN=True`, `SORT=False`
- **Engineering Assessment**: Compound ESR index eliminates both COLLSCAN and in-memory SORT stage.

### Query: `orders_containing_item_sku`
- **Index Created**: `idx_items_sku` (multikey)
- **Filter Specification**: `{'items_json.sku': 'SKU-1010'}`
- **Sort Specification**: `{'order_date': -1}`
- **Total Docs Examined**: Before = `943`, After = `269` (Optimization: **3.51x** reduction)
- **Total Keys Examined**: Before = `0`, After = `269`
- **Execution Time (ms)**: Before = `1 ms`, After = `2 ms`
- **Stage Progression**: Before had `COLLSCAN=True`, `SORT=True` -> After has `IXSCAN=True`, `SORT=True`
- **Engineering Assessment**: Multikey B-Tree index indexes nested array elements, avoiding full array traversals.
