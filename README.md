# NimbusAI Assignment -- Final Walkthrough

## Deliverable Files

| File | Task | Status |
|------|------|--------|
| [nimbus_sql_queries.sql](file:///d:/Bhaskar/RaoDo_Assessment/NimbusAI_Deliverables/nimbus_sql_queries.sql) | Task 1: SQL | Complete |
| [nimbus_mongo_queries.js](file:///d:/Bhaskar/RaoDo_Assessment/NimbusAI_Deliverables/nimbus_mongo_queries.js) | Task 2: MongoDB | Complete |
| [nimbus_tier_sync.py](file:///d:/Bhaskar/RaoDo_Assessment/NimbusAI_Deliverables/nimbus_tier_sync.py) | Task 2: Helper | Complete |
| [nimbus_analysis.py](file:///d:/Bhaskar/RaoDo_Assessment/NimbusAI_Deliverables/nimbus_analysis.py) | Task 3: Python | Complete |
| [RaoDo_assignment_BI_dashboard.pbix](file:///d:/Bhaskar/RaoDo_Assessment/NimbusAI_Deliverables/RaoDo_assignment_BI_dashboard.pbix) | Task 4: Dashboard | Complete |
| [PowerBI_Dashboard_Guide.txt](file:///d:/Bhaskar/RaoDo_Assessment/NimbusAI_Deliverables/PowerBI_Dashboard_Guide.txt) | Task 4: Guide | Complete |
| [Assignment_Complete_Explanation.txt](file:///d:/Bhaskar/RaoDo_Assessment/NimbusAI_Deliverables/Assignment_Complete_Explanation.txt) | Task 5: Docs | Complete |
| dashboard_data/ (6 CSVs) | Task 4: Data | Complete |

---

## Dashboard Verification

The PBIX file (`RaoDo_assignment_BI_dashboard.pbix`) was programmatically inspected. Results:

### Visuals Found (6 data visuals + 1 title = 7 containers)

| # | Type | Purpose | Data Fields |
|---|------|---------|-------------|
| 1 | Card | Total Customers | `customer_id` (Count) |
| 2 | Card | Churn Rate | DAX measure |
| 3 | Line Chart | Monthly Churn by Tier | `churned_count`, `plan_tier` |
| 4 | Bar Chart | Top Churn Reasons | `churn_reason`, `count` |
| 5 | Donut Chart | Customer Segments | `segment_name`, `customer_id` |
| 6 | Scatter Chart | Engagement vs LTV (**SQL+MongoDB**) | `total_events`, `total_ltv`, `segment_name`, `ticket_count` |

### Filters Found (2 slicers)

| # | Type | Field |
|---|------|-------|
| 1 | Slicer | `plan_tier` |
| 2 | Slicer | `segment_name` |

### Requirement Compliance

| Requirement | Status |
|-------------|--------|
| 5+ visuals | **PASS** (6 data visuals) |
| 2+ interactive filters | **PASS** (2 slicers) |
| 1 visual combining SQL + MongoDB | **PASS** (scatter chart) |
| 3 actionable recommendations | **PASS** (documented in guide) |

---

## Full Verification Checklist

| Task | Status | Details |
|------|--------|---------|
| Task 1: SQL | **PASS** | 5 queries covering joins, windows, CTEs, time series, dedup |
| Task 2: MongoDB | **PASS** | 4 pipelines with tier segmentation, correct retention, free-tier filter |
| Task 3: Python | **PASS** | Welch's t-test, K-means segmentation, 6 CSV exports, no credentials |
| Task 4: Dashboard | **PASS** | PBIX created with 6 visuals, 2 filters, SQL+MongoDB combined |
| Task 5: Documentation | **PASS** | Professional, no sensitive data, verification checklist included |
| Data Cleaning | **PASS** | 10 issues found and resolved, all documented |
| Overall Consistency | **PASS** | All files match, no placeholders, submission-ready |

---

## What Was Fixed (Cumulative)

1. **MongoDB Q1**: Added tier segmentation via `$lookup` on `customer_tiers` collection
2. **MongoDB Q2**: Rewrote 7-day retention to use timestamp-based window logic
3. **MongoDB Q4**: Added free-tier filtering via `$lookup` + `$match` before ranking
4. **MongoDB compatibility**: Fixed field names (`session_duration_sec`, `event_type`), added version note
5. **Python credentials**: Removed hardcoded password, uses `PG_PASSWORD` env var
6. **Python paths**: Replaced hardcoded paths with `os.path` relative paths
7. **Python exports**: Added confirmation log messages per CSV
8. **Explanation file**: Removed all personal paths/passwords/usernames
9. **Explanation file**: Replaced overconfident claims with "typical run" language
10. **Explanation file**: Added session duration standardization note
11. **Explanation file**: Added verification checklist (Section 11)
12. **PowerBI guide**: Rewritten to match actual dashboard contents
13. **SQL file**: Removed emoji for cross-platform compatibility
14. **New file**: `nimbus_tier_sync.py` for cross-database ETL
15. **Dashboard verified**: PBIX structure confirmed via programmatic inspection

> [!IMPORTANT]
> The assignment is **fully complete and submission-ready**. The only remaining step is recording the 5-minute walkthrough video.
