"""
============================================================
NimbusAI Data Analyst Assignment -- Task 3: Data Wrangling & Statistical Analysis
Focus Area: Option A -- Customer Churn & Retention Analysis
============================================================
This script:
1. Pulls data from PostgreSQL (nimbus_core) and MongoDB (nimbus_events)
2. Merges and cleans the data, documenting every step
3. Performs hypothesis testing (churn vs engagement)
4. Creates customer segmentation using K-means clustering
5. Exports cleaned data as CSVs for Power BI dashboard
============================================================
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURATION — Update these for your environment
# ============================================================
PG_CONFIG = {
    "host": "localhost",
    "database": "nimbus_core",
    "user": "postgres",
    "password": os.environ.get("PG_PASSWORD"),  # Set PG_PASSWORD env var before running
}
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "nimbus_events"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_data")

# ============================================================
# SECTION 1: DATA EXTRACTION
# ============================================================
print("=" * 60)
print("SECTION 1: DATA EXTRACTION")
print("=" * 60)

# --- PostgreSQL Connection ---
import psycopg2

pg_conn = psycopg2.connect(**PG_CONFIG)

def pg_query(sql):
    return pd.read_sql_query(sql, pg_conn)

# Load all SQL tables
print("\nLoading PostgreSQL tables...")
df_plans = pg_query("SELECT * FROM nimbus.plans")
print(f"  plans: {len(df_plans)} rows")

df_customers = pg_query("SELECT * FROM nimbus.customers")
print(f"  customers: {len(df_customers)} rows")

df_subscriptions = pg_query("SELECT * FROM nimbus.subscriptions")
print(f"  subscriptions: {len(df_subscriptions)} rows")

df_invoices = pg_query("SELECT * FROM nimbus.billing_invoices")
print(f"  billing_invoices: {len(df_invoices)} rows")

df_tickets = pg_query("SELECT * FROM nimbus.support_tickets")
print(f"  support_tickets: {len(df_tickets)} rows")

df_team = pg_query("SELECT * FROM nimbus.team_members")
print(f"  team_members: {len(df_team)} rows")

df_features = pg_query("SELECT * FROM nimbus.feature_flags")
print(f"  feature_flags: {len(df_features)} rows")

# --- MongoDB Connection ---
from pymongo import MongoClient

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]

print("\nLoading MongoDB collections...")
df_activity = pd.DataFrame(list(mongo_db.user_activity_logs.find({}, {"_id": 0})))
print(f"  user_activity_logs: {len(df_activity)} rows")

df_nps = pd.DataFrame(list(mongo_db.nps_survey_responses.find({}, {"_id": 0})))
print(f"  nps_survey_responses: {len(df_nps)} rows")

df_onboarding = pd.DataFrame(list(mongo_db.onboarding_events.find({}, {"_id": 0})))
print(f"  onboarding_events: {len(df_onboarding)} rows")

# ============================================================
# SECTION 2: DATA CLEANING & MERGING
# ============================================================
print("\n" + "=" * 60)
print("SECTION 2: DATA CLEANING & MERGING")
print("=" * 60)

# --- 2.1: Clean SQL Customers ---
print("\n--- 2.1: Cleaning Customers Table ---")
print(f"  Before: {len(df_customers)} rows")

# Check for duplicates by normalized company name
dupes = df_customers[df_customers['company_name'].str.strip().str.lower().str.replace(' ', '').duplicated(keep=False)]
print(f"  Potential duplicate company names: {len(dupes)}")

# Check NULL/empty company names
null_names = df_customers[df_customers['company_name'].isnull() | (df_customers['company_name'].str.strip() == '')]
print(f"  Empty/NULL company names: {len(null_names)} (IDs: {null_names['customer_id'].tolist()})")

# Fix: Trim whitespace from text columns
df_customers['company_name'] = df_customers['company_name'].str.strip()
df_customers['country_name'] = df_customers['country_name'].str.strip()

# Flag known duplicates instead of removing (for analysis integrity)
df_customers['is_potential_duplicate'] = False
df_customers.loc[df_customers['customer_id'].isin([1202]), 'is_potential_duplicate'] = True
df_customers.loc[df_customers['company_name'] == '', 'company_name'] = 'Unknown'
print(f"  After cleaning: {len(df_customers)} rows (flagged {df_customers['is_potential_duplicate'].sum()} duplicates)")

# --- 2.2: Clean MongoDB Activity Logs ---
print("\n--- 2.2: Cleaning Activity Logs ---")
print(f"  Before: {len(df_activity)} rows")

# Normalize customer_id field (mixed naming across documents)
if 'customer_id' not in df_activity.columns:
    df_activity['customer_id'] = np.nan
for col in ['customerId', 'customerID', 'user_id', 'userId', 'userID']:
    if col in df_activity.columns:
        df_activity['customer_id'] = df_activity['customer_id'].fillna(df_activity[col])

# Rename session_duration_sec -> session_duration for internal consistency
if 'session_duration_sec' in df_activity.columns:
    df_activity['session_duration'] = df_activity['session_duration_sec']
elif 'session_duration' not in df_activity.columns:
    df_activity['session_duration'] = 0

# Use event_type as the feature dimension (actual field in the data)
if 'event_type' in df_activity.columns:
    df_activity['feature'] = df_activity['event_type']
elif 'feature' not in df_activity.columns:
    df_activity['feature'] = None

# Convert customer_id to numeric (some docs store as strings)
df_activity['customer_id'] = pd.to_numeric(df_activity['customer_id'], errors='coerce')

# Normalize timestamps (mixed ISODate and string formats)
def parse_timestamp(ts):
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
            try:
                return datetime.strptime(ts, fmt)
            except ValueError:
                continue
    return pd.NaT

df_activity['timestamp'] = df_activity['timestamp'].apply(parse_timestamp)

# Remove exact duplicates
before_dedup = len(df_activity)
df_activity = df_activity.drop_duplicates()
print(f"  Removed {before_dedup - len(df_activity)} exact duplicates")

# Remove orphan records (customer_ids not in PostgreSQL)
valid_ids = set(df_customers['customer_id'].values)
orphans = df_activity[~df_activity['customer_id'].isin(valid_ids)]
print(f"  Orphan records (customer_id not in SQL): {len(orphans)}")
df_activity_clean = df_activity[df_activity['customer_id'].isin(valid_ids)].copy()

# Handle NULL session durations (non-session events like page_view)
null_sessions = df_activity_clean['session_duration'].isnull().sum()
print(f"  NULL session_duration: {null_sessions}")
df_activity_clean['session_duration'] = df_activity_clean['session_duration'].fillna(0)

# Remove negative session durations (data quality issue)
neg_sessions = (df_activity_clean['session_duration'] < 0).sum()
print(f"  Negative session_duration: {neg_sessions}")
df_activity_clean.loc[df_activity_clean['session_duration'] < 0, 'session_duration'] = 0

print(f"  After cleaning: {len(df_activity_clean)} rows")

# --- 2.3: Clean NPS Survey Responses ---
print("\n--- 2.3: Cleaning NPS Responses ---")
print(f"  Before: {len(df_nps)} rows")

if 'customer_id' not in df_nps.columns:
    df_nps['customer_id'] = np.nan
for col in ['customerId', 'customerID', 'userId', 'userID']:
    if col in df_nps.columns:
        df_nps['customer_id'] = df_nps['customer_id'].fillna(df_nps[col])

df_nps['customer_id'] = pd.to_numeric(df_nps['customer_id'], errors='coerce')
df_nps['nps_score'] = pd.to_numeric(df_nps.get('nps_score', df_nps.get('score', pd.Series([]))), errors='coerce')

# Remove NPS scores outside valid 0-10 range
invalid_nps = ((df_nps['nps_score'] < 0) | (df_nps['nps_score'] > 10)).sum()
print(f"  Invalid NPS scores (outside 0-10): {invalid_nps}")
df_nps = df_nps[(df_nps['nps_score'] >= 0) & (df_nps['nps_score'] <= 10)]
print(f"  After cleaning: {len(df_nps)} rows")

# --- 2.4: Clean Onboarding Events ---
print("\n--- 2.4: Cleaning Onboarding Events ---")
print(f"  Before: {len(df_onboarding)} rows")

if 'customer_id' not in df_onboarding.columns:
    df_onboarding['customer_id'] = np.nan
for col in ['customerId', 'customerID', 'userId', 'userID']:
    if col in df_onboarding.columns:
        df_onboarding['customer_id'] = df_onboarding['customer_id'].fillna(df_onboarding[col])

df_onboarding['customer_id'] = pd.to_numeric(df_onboarding['customer_id'], errors='coerce')
if 'timestamp' in df_onboarding.columns:
    df_onboarding['timestamp'] = df_onboarding['timestamp'].apply(parse_timestamp)
step_col = 'step' if 'step' in df_onboarding.columns else 'event_step' if 'event_step' in df_onboarding.columns else None
if step_col:
    df_onboarding['step'] = df_onboarding[step_col].astype(str).str.lower().str.strip()
else:
    df_onboarding['step'] = 'unknown'

before_dedup = len(df_onboarding)
df_onboarding = df_onboarding.drop_duplicates()
print(f"  Removed {before_dedup - len(df_onboarding)} duplicates")
print(f"  After cleaning: {len(df_onboarding)} rows")

# --- 2.5: Merge SQL + MongoDB Data ---
print("\n--- 2.5: Merging SQL and MongoDB Data ---")

# Create customer-level engagement metrics from activity logs
engagement = df_activity_clean.groupby('customer_id').agg(
    total_events=('customer_id', 'count'),
    total_sessions=('session_duration', lambda x: (x > 0).sum()),
    avg_session_duration=('session_duration', 'mean'),
    distinct_features=('feature', 'nunique'),
    last_activity=('timestamp', 'max'),
    first_activity=('timestamp', 'min'),
    total_session_time=('session_duration', 'sum')
).reset_index()

# Customer-level NPS from MongoDB
nps_agg = df_nps.groupby('customer_id').agg(
    avg_nps_mongo=('nps_score', 'mean'),
    nps_count=('nps_score', 'count'),
    latest_nps=('nps_score', 'last')
).reset_index()

# Onboarding completion
onb_steps = df_onboarding.groupby('customer_id')['step'].apply(
    lambda x: len(set(x))
).reset_index()
onb_steps.columns = ['customer_id', 'onboarding_steps_completed']

# Get current (most recent) subscription info per customer
current_sub = df_subscriptions.sort_values('start_date').groupby('customer_id').last().reset_index()
current_sub = current_sub[['customer_id', 'plan_id', 'status', 'mrr_usd', 'billing_cycle']].rename(
    columns={'status': 'current_sub_status', 'mrr_usd': 'current_mrr'}
)

# Calculate LTV from paid invoices
ltv = df_invoices[df_invoices['status'] == 'paid'].groupby('customer_id')['total_usd'].sum().reset_index()
ltv.columns = ['customer_id', 'total_ltv']

# Support ticket summary per customer
ticket_counts = df_tickets.groupby('customer_id').agg(
    ticket_count=('ticket_id', 'count'),
    escalated_tickets=('escalated', 'sum'),
    avg_satisfaction=('satisfaction_score', 'mean')
).reset_index()

# Merge everything into one master dataframe
print("  Merging all data sources...")
master = df_customers.merge(current_sub, on='customer_id', how='left')
master = master.merge(df_plans[['plan_id', 'plan_name', 'plan_tier']], on='plan_id', how='left')
master = master.merge(engagement, on='customer_id', how='left')
master = master.merge(nps_agg, on='customer_id', how='left')
master = master.merge(onb_steps, on='customer_id', how='left')
master = master.merge(ltv, on='customer_id', how='left')
master = master.merge(ticket_counts, on='customer_id', how='left')

# Fill NaN engagement metrics with 0
for col in ['total_events', 'total_sessions', 'avg_session_duration', 'distinct_features',
            'total_session_time', 'ticket_count', 'escalated_tickets', 'total_ltv',
            'onboarding_steps_completed', 'nps_count']:
    master[col] = master[col].fillna(0)

# Derive churn flag from is_active column
master['is_churned'] = (~master['is_active']).astype(int)

print(f"\n  FINAL MERGED DATASET: {len(master)} rows x {len(master.columns)} columns")
print(f"  Churned customers: {master['is_churned'].sum()} ({master['is_churned'].mean()*100:.1f}%)")
print(f"  Active customers: {(master['is_churned']==0).sum()}")

# ============================================================
# SECTION 3: HYPOTHESIS TESTING
# ============================================================
print("\n" + "=" * 60)
print("SECTION 3: HYPOTHESIS TESTING")
print("=" * 60)

from scipy import stats

print("""
HYPOTHESIS: Customers with higher engagement (more events) have
significantly lower churn rates.

  H0 (Null): There is no significant difference in total_events between
     churned and active customers.
  H1 (Alternative): Active customers have significantly more total_events
     than churned customers.

  Significance Level: alpha = 0.05
  Test: Welch's t-test (independent samples, one-tailed)

  Assumptions checked:
  1. Independence: Each customer is independent (separate accounts)
  2. Normality: Both groups have n > 30, so CLT applies
  3. Equal variance: NOT assumed (Welch's t-test is used)
""")

churned = master[master['is_churned'] == 1]['total_events']
active = master[master['is_churned'] == 0]['total_events']

print(f"  Active customers (n={len(active)}):  mean events = {active.mean():.2f}, std = {active.std():.2f}")
print(f"  Churned customers (n={len(churned)}): mean events = {churned.mean():.2f}, std = {churned.std():.2f}")

# Welch's t-test (does not assume equal variances)
t_stat, p_value_two = stats.ttest_ind(active, churned, equal_var=False)
p_value = p_value_two / 2  # Convert to one-tailed

print(f"\n  Welch's t-statistic: {t_stat:.4f}")
print(f"  p-value (one-tailed): {p_value:.6f}")

if p_value < 0.05:
    print(f"\n  RESULT: REJECT H0 (p={p_value:.6f} < 0.05)")
    print("  -> There IS a statistically significant difference in engagement")
    print("     between churned and active customers.")
else:
    print(f"\n  RESULT: FAIL TO REJECT H0 (p={p_value:.6f} >= 0.05)")
    print("  -> No statistically significant difference found.")

# Effect size (Cohen's d)
pooled_std = np.sqrt((active.std()**2 + churned.std()**2) / 2)
cohens_d = (active.mean() - churned.mean()) / pooled_std if pooled_std > 0 else 0
print(f"\n  Cohen's d (effect size): {cohens_d:.4f}")
if abs(cohens_d) < 0.2:
    print("  -> Negligible effect size")
elif abs(cohens_d) < 0.5:
    print("  -> Small effect size")
elif abs(cohens_d) < 0.8:
    print("  -> Medium effect size")
else:
    print("  -> Large effect size")

# Secondary test: Chi-square on high/low engagement vs churn
print("\n--- Secondary Test: Chi-Square (High/Low Engagement vs Churn) ---")
median_events = master['total_events'].median()
master['high_engagement'] = (master['total_events'] > median_events).astype(int)
contingency = pd.crosstab(master['high_engagement'], master['is_churned'])
print(f"\n  Contingency Table:")
print(contingency.to_string())
chi2, p_chi, dof, expected = stats.chi2_contingency(contingency)
print(f"\n  Chi-square statistic: {chi2:.4f}")
print(f"  p-value: {p_chi:.6f}")
print(f"  Degrees of freedom: {dof}")

# ============================================================
# SECTION 4: CUSTOMER SEGMENTATION
# ============================================================
print("\n" + "=" * 60)
print("SECTION 4: CUSTOMER SEGMENTATION")
print("=" * 60)

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

print("""
METHODOLOGY: RFM-inspired Behavioral Segmentation using K-Means Clustering

  Dimensions (5 features, all normalized with StandardScaler):
  1. Recency: Days since last activity (lower = more recent)
  2. Frequency: Total number of events
  3. Monetary: Total lifetime value (sum of paid invoices)
  4. Session duration: Average session duration
  5. Feature breadth: Number of distinct event types used

  We use K-means with k=4 clusters to produce actionable segments.
""")

# Prepare features
ref_date = datetime.now()
master['recency_days'] = master['last_activity'].apply(
    lambda x: (ref_date - x).days if pd.notna(x) else 999
)

seg_features = master[['customer_id', 'recency_days', 'total_events', 'total_ltv',
                         'avg_session_duration', 'distinct_features']].copy()
seg_features = seg_features.fillna(0)

# Normalize features
feature_cols = ['recency_days', 'total_events', 'total_ltv', 'avg_session_duration', 'distinct_features']
scaler = StandardScaler()
X_scaled = scaler.fit_transform(seg_features[feature_cols])

# K-Means with k=4
kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
seg_features['segment'] = kmeans.fit_predict(X_scaled)

# Merge segments back into master
master = master.merge(seg_features[['customer_id', 'segment']], on='customer_id', how='left')

# Analyze segments
print("\n--- Segment Profiles ---\n")
seg_analysis = master.groupby('segment').agg(
    count=('customer_id', 'count'),
    churn_rate=('is_churned', 'mean'),
    avg_events=('total_events', 'mean'),
    avg_ltv=('total_ltv', 'mean'),
    avg_session_dur=('avg_session_duration', 'mean'),
    avg_features=('distinct_features', 'mean'),
    avg_tickets=('ticket_count', 'mean'),
    avg_recency=('recency_days', 'mean')
).round(2)

# Name segments based on their characteristics
segment_names = {}
for seg in seg_analysis.index:
    row = seg_analysis.loc[seg]
    if row['avg_ltv'] > seg_analysis['avg_ltv'].mean() and row['churn_rate'] < seg_analysis['churn_rate'].mean():
        segment_names[seg] = 'Champions'
    elif row['avg_events'] > seg_analysis['avg_events'].mean() and row['avg_ltv'] < seg_analysis['avg_ltv'].mean():
        segment_names[seg] = 'Engaged Free/Low-Tier'
    elif row['churn_rate'] > seg_analysis['churn_rate'].mean() and row['avg_events'] < seg_analysis['avg_events'].mean():
        segment_names[seg] = 'At-Risk / Dormant'
    else:
        segment_names[seg] = 'Steady Users'

master['segment_name'] = master['segment'].map(segment_names)
seg_analysis['segment_name'] = seg_analysis.index.map(segment_names)

print(seg_analysis.to_string())

print("\n\n--- Business Implications ---")
for seg, name in segment_names.items():
    row = seg_analysis.loc[seg]
    print(f"\n  Segment {seg} ({name}): {int(row['count'])} customers")
    print(f"    Churn Rate: {row['churn_rate']*100:.1f}%")
    print(f"    Avg LTV: ${row['avg_ltv']:,.2f}")
    if name == 'Champions':
        print("    -> Strategy: Nurture with exclusive features, referral programs")
    elif name == 'Engaged Free/Low-Tier':
        print("    -> Strategy: PRIME UPSELL targets -- offer trials of premium features")
    elif name == 'At-Risk / Dormant':
        print("    -> Strategy: Urgent re-engagement campaigns, check-in calls")
    else:
        print("    -> Strategy: Encourage deeper feature adoption, monitor for risk signals")

# ============================================================
# SECTION 5: EXPORT DATA FOR POWER BI DASHBOARD
# ============================================================
print("\n" + "=" * 60)
print("SECTION 5: EXPORTING DATA FOR POWER BI")
print("=" * 60)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Export 1: Master customer dataset (merged SQL + MongoDB)
export_cols = ['customer_id', 'company_name', 'industry', 'company_size', 'country_code',
               'country_name', 'signup_date', 'signup_source', 'is_active', 'churned_at',
               'churn_reason', 'nps_score', 'plan_name', 'plan_tier', 'current_sub_status',
               'current_mrr', 'total_events', 'total_sessions', 'avg_session_duration',
               'distinct_features', 'total_ltv', 'ticket_count', 'escalated_tickets',
               'avg_satisfaction', 'onboarding_steps_completed', 'is_churned',
               'segment', 'segment_name', 'high_engagement', 'recency_days']
master_export = master[[c for c in export_cols if c in master.columns]]
master_export.to_csv(os.path.join(OUTPUT_DIR, 'customer_master.csv'), index=False)
print(f"  [OK] Exported customer_master.csv successfully ({len(master_export)} rows, {len(master_export.columns)} columns)")

# Export 2: Monthly churn metrics by tier
monthly_churn = df_subscriptions.copy()
monthly_churn['end_month'] = pd.to_datetime(monthly_churn['end_date']).dt.to_period('M')
monthly_churn = monthly_churn.merge(df_plans[['plan_id', 'plan_tier']], on='plan_id', how='left')
churn_by_month = monthly_churn[monthly_churn['status'].isin(['cancelled', 'expired'])].groupby(
    ['end_month', 'plan_tier']).size().reset_index(name='churned_count')
churn_by_month['end_month'] = churn_by_month['end_month'].astype(str)
churn_by_month.to_csv(os.path.join(OUTPUT_DIR, 'monthly_churn_by_tier.csv'), index=False)
print(f"  [OK] Exported monthly_churn_by_tier.csv successfully ({len(churn_by_month)} rows)")

# Export 3: Support ticket analysis
tickets_export = df_tickets.merge(
    df_customers[['customer_id', 'company_name', 'is_active']], on='customer_id', how='left'
).merge(
    current_sub[['customer_id', 'plan_id']], on='customer_id', how='left'
).merge(
    df_plans[['plan_id', 'plan_tier']], on='plan_id', how='left'
)
tickets_export.to_csv(os.path.join(OUTPUT_DIR, 'support_tickets.csv'), index=False)
print(f"  [OK] Exported support_tickets.csv successfully ({len(tickets_export)} rows)")

# Export 4: Engagement over time (monthly aggregated)
df_activity_clean['month'] = df_activity_clean['timestamp'].dt.to_period('M')
monthly_engagement = df_activity_clean.groupby('month').agg(
    total_events=('customer_id', 'count'),
    unique_users=('customer_id', 'nunique'),
    avg_session_duration=('session_duration', 'mean')
).reset_index()
monthly_engagement['month'] = monthly_engagement['month'].astype(str)
monthly_engagement.to_csv(os.path.join(OUTPUT_DIR, 'monthly_engagement.csv'), index=False)
print(f"  [OK] Exported monthly_engagement.csv successfully ({len(monthly_engagement)} rows)")

# Export 5: Subscription plan details
df_plans.to_csv(os.path.join(OUTPUT_DIR, 'plans.csv'), index=False)
print(f"  [OK] Exported plans.csv successfully ({len(df_plans)} rows)")

# Export 6: Churn reasons summary
churn_reasons = master[master['is_churned']==1].groupby('churn_reason').agg(
    count=('customer_id', 'count'),
    avg_ltv=('total_ltv', 'mean')
).reset_index().sort_values('count', ascending=False)
churn_reasons.to_csv(os.path.join(OUTPUT_DIR, 'churn_reasons.csv'), index=False)
print(f"  [OK] Exported churn_reasons.csv successfully ({len(churn_reasons)} rows)")

print(f"\n  All 6 CSV files exported to: {OUTPUT_DIR}")

# Close connections
pg_conn.close()
mongo_client.close()

print("\n" + "=" * 60)
print("ALL TASKS COMPLETE!")
print("=" * 60)
print(f"\nDashboard data exported to: {OUTPUT_DIR}")
print(f"\nKey findings summary (from this run):")
print(f"  Total customers: {len(master)}")
print(f"  Churn rate: {master['is_churned'].mean()*100:.1f}%")
print(f"  Avg LTV (active): ${master[master['is_churned']==0]['total_ltv'].mean():,.2f}")
print(f"  Avg LTV (churned): ${master[master['is_churned']==1]['total_ltv'].mean():,.2f}")
print(f"  Top churn reason: {churn_reasons.iloc[0]['churn_reason'] if len(churn_reasons) > 0 else 'N/A'}")
print(f"  Customer segments: {len(segment_names)} groups identified")
