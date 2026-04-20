-- ============================================================
-- NimbusAI Data Analyst Assignment — Task 1: SQL Queries
-- Focus Area: Option A — Customer Churn & Retention Analysis
-- Database: nimbus_core (PostgreSQL)
-- Schema: nimbus
-- ============================================================

SET search_path TO nimbus;

-- ============================================================
-- Q1: Joins + Aggregation
-- For each subscription plan, calculate:
--   1. Number of active customers
--   2. Average monthly revenue (MRR)
--   3. Support ticket rate (tickets per customer per month)
-- Over the last 6 months
-- ============================================================

/*
  Approach:
  - Join plans → subscriptions → customers to get active customers per plan
  - Join support_tickets filtered to last 6 months
  - Calculate ticket rate = total_tickets / (active_customers * 6 months)
  - Using LEFT JOINs to include plans with zero tickets
*/

WITH date_range AS (
    -- Define "last 6 months" relative to the latest data in the system
    SELECT 
        (SELECT MAX(created_at) FROM support_tickets)::date AS ref_date,
        ((SELECT MAX(created_at) FROM support_tickets)::date - INTERVAL '6 months')::date AS start_date
),
active_subs AS (
    -- Get currently active subscriptions with their plan info
    SELECT 
        s.subscription_id,
        s.customer_id,
        s.plan_id,
        s.mrr_usd,
        p.plan_name,
        p.plan_tier
    FROM subscriptions s
    JOIN plans p ON s.plan_id = p.plan_id
    WHERE s.status = 'active'
),
ticket_counts AS (
    -- Count support tickets per customer in the last 6 months
    SELECT 
        t.customer_id,
        COUNT(*) AS ticket_count
    FROM support_tickets t
    CROSS JOIN date_range dr
    WHERE t.created_at >= dr.start_date
      AND t.created_at <= dr.ref_date
    GROUP BY t.customer_id
)
SELECT 
    a.plan_name,
    a.plan_tier,
    COUNT(DISTINCT a.customer_id) AS active_customers,
    ROUND(AVG(a.mrr_usd), 2) AS avg_monthly_revenue,
    COALESCE(SUM(tc.ticket_count), 0) AS total_tickets_6mo,
    -- Ticket rate: tickets per customer per month over 6 months
    ROUND(
        COALESCE(SUM(tc.ticket_count), 0)::DECIMAL / 
        NULLIF(COUNT(DISTINCT a.customer_id) * 6, 0),
        4
    ) AS ticket_rate_per_customer_per_month
FROM active_subs a
LEFT JOIN ticket_counts tc ON a.customer_id = tc.customer_id
GROUP BY a.plan_name, a.plan_tier
ORDER BY active_customers DESC;


-- ============================================================
-- Q2: Window Functions
-- Rank customers within each plan tier by their total lifetime
-- value (LTV). Show the percentage difference between their
-- LTV and the tier average.
-- ============================================================

/*
  Approach:
  - LTV = sum of all invoice amounts (total_usd) paid by the customer
  - Use the customer's current (latest) subscription to determine their tier
  - RANK() OVER (PARTITION BY plan_tier ORDER BY ltv DESC)
  - Tier average via AVG() OVER (PARTITION BY plan_tier)
  - % difference = (customer_ltv - tier_avg) / tier_avg * 100
*/

WITH customer_ltv AS (
    -- Calculate total lifetime value from billing invoices
    SELECT 
        c.customer_id,
        c.company_name,
        COALESCE(SUM(bi.total_usd), 0) AS total_ltv
    FROM customers c
    LEFT JOIN billing_invoices bi ON c.customer_id = bi.customer_id
        AND bi.status IN ('paid')  -- Only count paid invoices
    GROUP BY c.customer_id, c.company_name
),
current_plan AS (
    -- Get the customer's most recent subscription plan tier
    SELECT DISTINCT ON (s.customer_id)
        s.customer_id,
        p.plan_name,
        p.plan_tier
    FROM subscriptions s
    JOIN plans p ON s.plan_id = p.plan_id
    ORDER BY s.customer_id, s.start_date DESC
)
SELECT 
    cp.plan_tier,
    cp.plan_name,
    cl.customer_id,
    cl.company_name,
    ROUND(cl.total_ltv, 2) AS customer_ltv,
    RANK() OVER (
        PARTITION BY cp.plan_tier 
        ORDER BY cl.total_ltv DESC
    ) AS ltv_rank,
    ROUND(AVG(cl.total_ltv) OVER (PARTITION BY cp.plan_tier), 2) AS tier_avg_ltv,
    ROUND(
        (cl.total_ltv - AVG(cl.total_ltv) OVER (PARTITION BY cp.plan_tier)) 
        / NULLIF(AVG(cl.total_ltv) OVER (PARTITION BY cp.plan_tier), 0) * 100,
        2
    ) AS pct_diff_from_tier_avg
FROM customer_ltv cl
JOIN current_plan cp ON cl.customer_id = cp.customer_id
ORDER BY cp.plan_tier, ltv_rank;


-- ============================================================
-- Q3: CTEs + Subqueries
-- Identify customers who downgraded their plan in the last 90
-- days AND had more than 3 support tickets in the 30 days
-- before downgrading. Include current and previous plan details.
-- ============================================================

/*
  Approach:
  - Use LAG() window function to compare each subscription's plan_tier
    with the previous one for the same customer
  - A "downgrade" = moving to a lower-tier plan:
    enterprise > professional > starter > free
  - Filter to downgrades in the last 90 days
  - Then check support_tickets in the 30 days before the downgrade date
  - Using a tier ranking: free=1, starter=2, professional=3, enterprise=4
*/

WITH tier_rank AS (
    -- Assign numeric rank to each tier for comparison
    SELECT 'free' AS plan_tier, 1 AS tier_level
    UNION ALL SELECT 'starter', 2
    UNION ALL SELECT 'professional', 3
    UNION ALL SELECT 'enterprise', 4
),
subscription_history AS (
    -- Get each customer's subscription history with plan tier info
    SELECT 
        s.subscription_id,
        s.customer_id,
        s.plan_id,
        p.plan_name,
        p.plan_tier,
        tr.tier_level,
        s.start_date,
        s.end_date,
        s.status,
        s.mrr_usd,
        LAG(p.plan_name) OVER (PARTITION BY s.customer_id ORDER BY s.start_date) AS prev_plan_name,
        LAG(p.plan_tier) OVER (PARTITION BY s.customer_id ORDER BY s.start_date) AS prev_plan_tier,
        LAG(tr.tier_level) OVER (PARTITION BY s.customer_id ORDER BY s.start_date) AS prev_tier_level,
        LAG(s.mrr_usd) OVER (PARTITION BY s.customer_id ORDER BY s.start_date) AS prev_mrr
    FROM subscriptions s
    JOIN plans p ON s.plan_id = p.plan_id
    JOIN tier_rank tr ON p.plan_tier = tr.plan_tier
),
recent_downgrades AS (
    -- Find downgrades in the last 90 days (relative to max date in data)
    SELECT 
        sh.*,
        c.company_name,
        c.contact_name,
        c.contact_email
    FROM subscription_history sh
    JOIN customers c ON sh.customer_id = c.customer_id
    WHERE sh.prev_tier_level IS NOT NULL
      AND sh.tier_level < sh.prev_tier_level  -- Downgrade detected
      AND sh.start_date >= (SELECT MAX(start_date) FROM subscriptions) - INTERVAL '90 days'
),
downgrade_with_tickets AS (
    -- Count tickets in the 30 days before downgrade
    SELECT 
        rd.*,
        COUNT(st.ticket_id) AS tickets_before_downgrade
    FROM recent_downgrades rd
    LEFT JOIN support_tickets st 
        ON rd.customer_id = st.customer_id
        AND st.created_at >= (rd.start_date - INTERVAL '30 days')
        AND st.created_at < rd.start_date
    GROUP BY 
        rd.subscription_id, rd.customer_id, rd.plan_id, rd.plan_name,
        rd.plan_tier, rd.tier_level, rd.start_date, rd.end_date,
        rd.status, rd.mrr_usd, rd.prev_plan_name, rd.prev_plan_tier,
        rd.prev_tier_level, rd.prev_mrr, rd.company_name, 
        rd.contact_name, rd.contact_email
)
SELECT 
    customer_id,
    company_name,
    contact_name,
    prev_plan_name AS previous_plan,
    prev_plan_tier AS previous_tier,
    plan_name AS current_plan,
    plan_tier AS current_tier,
    start_date AS downgrade_date,
    prev_mrr AS previous_mrr,
    mrr_usd AS current_mrr,
    ROUND(prev_mrr - mrr_usd, 2) AS mrr_loss,
    tickets_before_downgrade
FROM downgrade_with_tickets
WHERE tickets_before_downgrade > 3
ORDER BY tickets_before_downgrade DESC, mrr_loss DESC;


-- ============================================================
-- Q4: Time Series
-- Calculate:
--   1. Month-over-month growth rate of new subscriptions
--   2. Rolling 3-month average churn rate, broken down by plan tier
--   3. Flag any month where churn exceeded 2x the rolling average
-- ============================================================

/*
  Approach:
  - New subscriptions: COUNT by month using start_date
  - Churn: subscriptions that ended (status = cancelled/expired) by month
  - Churn rate = churned_subs / total_active_at_start_of_month
  - Rolling 3-month avg using AVG() OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
  - Flag = churn_rate > 2 * rolling_avg
*/

-- Part A: Month-over-month growth rate of new subscriptions
WITH monthly_new_subs AS (
    SELECT 
        DATE_TRUNC('month', start_date) AS sub_month,
        COUNT(*) AS new_subscriptions
    FROM subscriptions
    GROUP BY DATE_TRUNC('month', start_date)
    ORDER BY sub_month
)
SELECT 
    sub_month,
    new_subscriptions,
    LAG(new_subscriptions) OVER (ORDER BY sub_month) AS prev_month_subs,
    ROUND(
        (new_subscriptions - LAG(new_subscriptions) OVER (ORDER BY sub_month))::DECIMAL
        / NULLIF(LAG(new_subscriptions) OVER (ORDER BY sub_month), 0) * 100,
        2
    ) AS mom_growth_rate_pct
FROM monthly_new_subs
ORDER BY sub_month;

-- Part B: Rolling 3-month average churn rate by plan tier, with flags
WITH monthly_churn AS (
    SELECT 
        DATE_TRUNC('month', s.end_date) AS churn_month,
        p.plan_tier,
        COUNT(*) AS churned_count
    FROM subscriptions s
    JOIN plans p ON s.plan_id = p.plan_id
    WHERE s.status IN ('cancelled', 'expired')
      AND s.end_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', s.end_date), p.plan_tier
),
monthly_active AS (
    -- Approximate active subscriptions at start of each month per tier
    SELECT 
        gs.month_start,
        p.plan_tier,
        COUNT(*) AS active_at_start
    FROM generate_series(
        (SELECT MIN(start_date) FROM subscriptions),
        (SELECT MAX(COALESCE(end_date, CURRENT_DATE)) FROM subscriptions),
        '1 month'::interval
    ) AS gs(month_start)
    CROSS JOIN plans p
    JOIN subscriptions s ON s.plan_id = p.plan_id
        AND s.start_date <= gs.month_start
        AND (s.end_date IS NULL OR s.end_date > gs.month_start)
    GROUP BY gs.month_start, p.plan_tier
),
churn_rates AS (
    SELECT 
        ma.month_start AS churn_month,
        ma.plan_tier,
        COALESCE(mc.churned_count, 0) AS churned_count,
        ma.active_at_start,
        ROUND(
            COALESCE(mc.churned_count, 0)::DECIMAL / NULLIF(ma.active_at_start, 0) * 100,
            4
        ) AS churn_rate_pct
    FROM monthly_active ma
    LEFT JOIN monthly_churn mc 
        ON ma.month_start = mc.churn_month 
        AND ma.plan_tier = mc.plan_tier
)
SELECT 
    churn_month,
    plan_tier,
    churned_count,
    active_at_start,
    churn_rate_pct,
    ROUND(
        AVG(churn_rate_pct) OVER (
            PARTITION BY plan_tier 
            ORDER BY churn_month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 4
    ) AS rolling_3mo_avg_churn,
    CASE 
        WHEN churn_rate_pct > 2 * AVG(churn_rate_pct) OVER (
            PARTITION BY plan_tier 
            ORDER BY churn_month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) THEN 'ALERT: Churn > 2x rolling avg'
        ELSE 'Normal'
    END AS churn_flag
FROM churn_rates
ORDER BY plan_tier, churn_month;


-- ============================================================
-- Q5: Advanced — Duplicate Customer Detection
-- Detect potential duplicate customer accounts based on:
--   1. Similar company names (fuzzy matching)
--   2. Same email domains
--   3. Overlapping team members
-- ============================================================

/*
  MATCHING LOGIC EXPLANATION:
  
  We use three independent signals to identify potential duplicates,
  then combine them with a scoring system:
  
  1. Company Name Similarity (weight: 40%)
     - Use PostgreSQL's pg_trgm extension for trigram similarity
     - Compare normalized company names (lowercase, trimmed, no spaces)
     - Threshold: similarity > 0.6
     
  2. Email Domain Match (weight: 30%)
     - Extract domain from contact_email using SPLIT_PART
     - Exact domain match (excluding generic domains like gmail, yahoo, etc.)
     - Corporate email domains are strong indicators of same org
     
  3. Overlapping Team Members (weight: 30%)
     - Compare email domains of team members across accounts
     - If accounts share team members with same email domain, likely duplicates
     
  Scoring: Each match type contributes to a confidence score.
  A score >= 60 suggests a likely duplicate worth investigating.
*/

-- Enable trigram extension for fuzzy matching (if not already)
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- Uncomment if pg_trgm is available

-- Method 1: Similar company names + same email domain
WITH normalized_customers AS (
    SELECT 
        customer_id,
        company_name,
        LOWER(TRIM(REPLACE(REPLACE(company_name, ' ', ''), '.', ''))) AS norm_name,
        LOWER(SPLIT_PART(contact_email, '@', 2)) AS email_domain,
        contact_email,
        contact_name,
        industry,
        country_code,
        signup_date,
        is_active
    FROM customers
    WHERE company_name IS NOT NULL AND company_name != ''
),
-- Find pairs with similar names (using string comparison since pg_trgm may not be available)
name_matches AS (
    SELECT 
        a.customer_id AS customer_id_1,
        b.customer_id AS customer_id_2,
        a.company_name AS company_name_1,
        b.company_name AS company_name_2,
        a.contact_email AS email_1,
        b.contact_email AS email_2,
        a.email_domain AS domain_1,
        b.email_domain AS domain_2,
        -- Simple similarity: check if one name contains the other or Levenshtein-like match
        CASE 
            WHEN a.norm_name = b.norm_name THEN 'EXACT_MATCH'
            WHEN a.norm_name LIKE b.norm_name || '%' OR b.norm_name LIKE a.norm_name || '%' THEN 'PREFIX_MATCH'
            WHEN LENGTH(a.norm_name) > 3 AND LENGTH(b.norm_name) > 3 
                 AND (a.norm_name LIKE '%' || b.norm_name || '%' OR b.norm_name LIKE '%' || a.norm_name || '%') THEN 'SUBSTRING_MATCH'
            ELSE NULL
        END AS name_match_type,
        -- Domain match (excluding generic email providers)
        CASE
            WHEN a.email_domain = b.email_domain 
                 AND a.email_domain NOT IN ('gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 
                                            'protonmail.com', 'company.io', 'enterprise.com', 
                                            'startup.co', 'corp.net', 'tech.dev')
            THEN TRUE
            ELSE FALSE
        END AS corporate_domain_match
    FROM normalized_customers a
    JOIN normalized_customers b ON a.customer_id < b.customer_id
    WHERE 
        -- At least one matching criterion
        (
            -- Similar normalized names
            a.norm_name = b.norm_name
            OR a.norm_name LIKE b.norm_name || '%' 
            OR b.norm_name LIKE a.norm_name || '%'
            -- OR same corporate email domain
            OR (a.email_domain = b.email_domain 
                AND a.email_domain NOT IN ('gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
                                           'protonmail.com', 'company.io', 'enterprise.com',
                                           'startup.co', 'corp.net', 'tech.dev'))
        )
),
-- Method 2: Overlapping team member email domains
team_domain_overlaps AS (
    SELECT 
        tm1.customer_id AS customer_id_1,
        tm2.customer_id AS customer_id_2,
        COUNT(DISTINCT LOWER(SPLIT_PART(tm1.email, '@', 2))) AS shared_domains,
        STRING_AGG(DISTINCT LOWER(SPLIT_PART(tm1.email, '@', 2)), ', ') AS overlapping_domains
    FROM team_members tm1
    JOIN team_members tm2 
        ON tm1.customer_id < tm2.customer_id
        AND LOWER(SPLIT_PART(tm1.email, '@', 2)) = LOWER(SPLIT_PART(tm2.email, '@', 2))
        AND LOWER(SPLIT_PART(tm1.email, '@', 2)) NOT IN ('gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'protonmail.com')
    GROUP BY tm1.customer_id, tm2.customer_id
    HAVING COUNT(DISTINCT LOWER(SPLIT_PART(tm1.email, '@', 2))) >= 1
)
-- Combine all signals
SELECT 
    COALESCE(nm.customer_id_1, td.customer_id_1) AS customer_id_1,
    COALESCE(nm.customer_id_2, td.customer_id_2) AS customer_id_2,
    nm.company_name_1,
    nm.company_name_2,
    nm.email_1,
    nm.email_2,
    nm.name_match_type,
    nm.corporate_domain_match,
    td.shared_domains AS team_domain_overlaps,
    td.overlapping_domains,
    -- Confidence score
    (
        CASE WHEN nm.name_match_type = 'EXACT_MATCH' THEN 50
             WHEN nm.name_match_type = 'PREFIX_MATCH' THEN 30
             WHEN nm.name_match_type = 'SUBSTRING_MATCH' THEN 15
             ELSE 0 END
        + CASE WHEN nm.corporate_domain_match THEN 30 ELSE 0 END
        + CASE WHEN td.shared_domains >= 2 THEN 30
               WHEN td.shared_domains = 1 THEN 15
               ELSE 0 END
    ) AS duplicate_confidence_score
FROM name_matches nm
FULL OUTER JOIN team_domain_overlaps td 
    ON nm.customer_id_1 = td.customer_id_1 
    AND nm.customer_id_2 = td.customer_id_2
WHERE 
    -- Only show results with reasonable confidence
    (
        CASE WHEN nm.name_match_type = 'EXACT_MATCH' THEN 50
             WHEN nm.name_match_type = 'PREFIX_MATCH' THEN 30
             WHEN nm.name_match_type = 'SUBSTRING_MATCH' THEN 15
             ELSE 0 END
        + CASE WHEN nm.corporate_domain_match THEN 30 ELSE 0 END
        + CASE WHEN td.shared_domains >= 2 THEN 30
               WHEN td.shared_domains = 1 THEN 15
               ELSE 0 END
    ) >= 30
ORDER BY duplicate_confidence_score DESC
LIMIT 50;
