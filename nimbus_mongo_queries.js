// ============================================================
// NimbusAI Data Analyst Assignment — Task 2: MongoDB Queries
// Focus Area: Option A — Customer Churn & Retention Analysis
// Database: nimbus_events (MongoDB 8.x)
// Collections: user_activity_logs, nps_survey_responses, onboarding_events
//
// PREREQUISITE: Before running Q1 and Q4, you must first create a
// helper collection that maps customer_id to subscription tier.
// This is done by running the SETUP section below, which uses
// a Python helper script (nimbus_tier_sync.py) to export tier
// data from PostgreSQL into a MongoDB lookup collection.
//
// NOTE: This solution uses $percentile and $median operators,
// which require MongoDB 7.0 or later. If using older versions,
// alternative implementations are required.
// ============================================================

use nimbus_events;

// ============================================================
// SETUP: Create tier lookup collection for cross-database queries
// ============================================================
//
// MongoDB cannot natively query PostgreSQL. To segment activity
// data by subscription tier (required for Q1 and Q4), we first
// export customer-to-tier mappings from PostgreSQL into a small
// MongoDB collection called "customer_tiers".
//
// Run this command ONCE before executing the queries below:
//   python nimbus_tier_sync.py
//
// That script creates the collection:
//   customer_tiers: { customer_id: <int>, plan_tier: <string> }
//
// This is a standard ETL pattern in multi-database environments.
// ============================================================


// ============================================================
// Q1: Aggregation Pipeline
// Calculate the average number of sessions per user per week,
// segmented by the user's subscription tier.
// Include 25th, 50th, and 75th percentile session durations.
// ============================================================

/*
  Approach:
  - Use $lookup to join activity logs with the customer_tiers
    collection (populated from PostgreSQL via nimbus_tier_sync.py)
  - Group by customer_id + year-week to count sessions per week
  - Group by customer_id to get avg sessions/week per user
  - Group by plan_tier to get tier-level metrics and percentiles
  - Uses $percentile (MongoDB 7.0+) for p25, p50, p75
*/

print("=== Q1: Average Sessions Per User Per Week, Segmented by Tier ===");

db.user_activity_logs.aggregate([
    // Stage 1: Normalize fields
    {
        $addFields: {
            parsed_timestamp: {
                $cond: {
                    if: { $eq: [{ $type: "$timestamp" }, "date"] },
                    then: "$timestamp",
                    else: { $toDate: { $ifNull: ["$timestamp", new Date("2024-01-01")] } }
                }
            },
            norm_customer_id: {
                $toInt: {
                    $ifNull: [
                        "$customer_id",
                        { $ifNull: ["$customerId", { $ifNull: ["$customerID", 0] }] }
                    ]
                }
            },
            session_dur: { $ifNull: ["$session_duration_sec", 0] }
        }
    },
    // Stage 2: Filter to events with a valid session duration > 0
    {
        $match: { session_dur: { $gt: 0 } }
    },
    // Stage 3: Extract year-week for grouping
    {
        $addFields: {
            year_week: {
                $dateToString: { format: "%Y-W%V", date: "$parsed_timestamp" }
            }
        }
    },
    // Stage 4: Lookup tier from customer_tiers collection
    {
        $lookup: {
            from: "customer_tiers",
            localField: "norm_customer_id",
            foreignField: "customer_id",
            as: "tier_info"
        }
    },
    {
        $addFields: {
            plan_tier: {
                $ifNull: [
                    { $arrayElemAt: ["$tier_info.plan_tier", 0] },
                    "unknown"
                ]
            }
        }
    },
    // Stage 5: Group by customer + week to count sessions per week
    {
        $group: {
            _id: {
                customer_id: "$norm_customer_id",
                year_week: "$year_week",
                plan_tier: "$plan_tier"
            },
            sessions_in_week: { $sum: 1 },
            week_durations: { $push: "$session_dur" }
        }
    },
    // Stage 6: Group by customer to get avg sessions/week and collect durations
    {
        $group: {
            _id: {
                customer_id: "$_id.customer_id",
                plan_tier: "$_id.plan_tier"
            },
            avg_sessions_per_week: { $avg: "$sessions_in_week" },
            total_weeks_active: { $sum: 1 },
            all_durations: { $push: "$week_durations" }
        }
    },
    // Stage 7: Flatten durations for percentile calculation
    {
        $addFields: {
            flat_durations: {
                $reduce: {
                    input: "$all_durations",
                    initialValue: [],
                    in: { $concatArrays: ["$$value", "$$this"] }
                }
            }
        }
    },
    // Stage 8: Group by plan_tier to produce tier-segmented results
    {
        $group: {
            _id: "$_id.plan_tier",
            user_count: { $sum: 1 },
            avg_sessions_per_user_per_week: { $avg: "$avg_sessions_per_week" },
            min_sessions_per_week: { $min: "$avg_sessions_per_week" },
            max_sessions_per_week: { $max: "$avg_sessions_per_week" },
            all_user_durations: { $push: "$flat_durations" }
        }
    },
    // Stage 9: Flatten all durations across users for tier-level percentiles
    {
        $addFields: {
            tier_durations: {
                $reduce: {
                    input: "$all_user_durations",
                    initialValue: [],
                    in: { $concatArrays: ["$$value", "$$this"] }
                }
            }
        }
    },
    {
        $addFields: {
            p25_session_duration: { $percentile: { input: "$tier_durations", p: [0.25], method: "approximate" } },
            p50_session_duration: { $percentile: { input: "$tier_durations", p: [0.50], method: "approximate" } },
            p75_session_duration: { $percentile: { input: "$tier_durations", p: [0.75], method: "approximate" } }
        }
    },
    // Stage 10: Clean output
    {
        $project: {
            _id: 0,
            plan_tier: "$_id",
            user_count: 1,
            avg_sessions_per_user_per_week: { $round: ["$avg_sessions_per_user_per_week", 2] },
            min_sessions_per_week: { $round: ["$min_sessions_per_week", 2] },
            max_sessions_per_week: { $round: ["$max_sessions_per_week", 2] },
            p25_session_duration_sec: { $arrayElemAt: ["$p25_session_duration", 0] },
            p50_session_duration_sec: { $arrayElemAt: ["$p50_session_duration", 0] },
            p75_session_duration_sec: { $arrayElemAt: ["$p75_session_duration", 0] }
        }
    },
    { $sort: { plan_tier: 1 } }
]).forEach(printjson);


// ============================================================
// Q2: Event Analysis
// For each product feature (event_type), compute:
//   1. Daily Active Users (DAU)
//   2. 7-day retention rate: percentage of users who used the
//      same event_type again within 7 days of their first use
// ============================================================

/*
  Approach:
  - DAU: Group by event_type + day, count distinct users, then average
  - 7-day retention:
    a) For each user+event_type, find the first use timestamp
    b) Collect all usage timestamps into an array
    c) Use $filter to find any timestamp that falls between
       first_use and first_use + 7 days (exclusive of first_use day)
    d) If any such timestamp exists, the user is retained
    e) Retention rate = retained_users / total_first_users * 100

  Note: We use event_type as the "feature" dimension since the
  activity logs track features via event_type (e.g., dashboard_view,
  report_generate, file_upload, export, etc.).
*/

print("\n=== Q2: DAU and 7-Day Retention Rate Per Feature (event_type) ===");

// Part A: Average DAU per event_type
print("\n--- Average Daily Active Users (DAU) Per Feature ---");
db.user_activity_logs.aggregate([
    {
        $addFields: {
            parsed_timestamp: {
                $cond: {
                    if: { $eq: [{ $type: "$timestamp" }, "date"] },
                    then: "$timestamp",
                    else: { $toDate: { $ifNull: ["$timestamp", new Date("2024-01-01")] } }
                }
            },
            norm_customer_id: {
                $ifNull: ["$customer_id", { $ifNull: ["$customerId", "$customerID"] }]
            }
        }
    },
    // Filter to events with a valid event_type
    { $match: { event_type: { $exists: true, $ne: null } } },
    {
        $addFields: {
            event_day: { $dateToString: { format: "%Y-%m-%d", date: "$parsed_timestamp" } }
        }
    },
    // Count distinct users per event_type per day
    {
        $group: {
            _id: { feature: "$event_type", day: "$event_day" },
            unique_users: { $addToSet: "$norm_customer_id" }
        }
    },
    {
        $addFields: { dau: { $size: "$unique_users" } }
    },
    // Average DAU per event_type
    {
        $group: {
            _id: "$_id.feature",
            avg_dau: { $avg: "$dau" },
            max_dau: { $max: "$dau" },
            min_dau: { $min: "$dau" },
            total_days_with_activity: { $sum: 1 }
        }
    },
    { $sort: { avg_dau: -1 } }
]).forEach(printjson);


// Part B: Corrected 7-day retention per event_type
print("\n--- 7-Day Feature Retention Rate (Corrected Logic) ---");
db.user_activity_logs.aggregate([
    {
        $addFields: {
            parsed_timestamp: {
                $cond: {
                    if: { $eq: [{ $type: "$timestamp" }, "date"] },
                    then: "$timestamp",
                    else: { $toDate: { $ifNull: ["$timestamp", new Date("2024-01-01")] } }
                }
            },
            norm_customer_id: {
                $ifNull: ["$customer_id", { $ifNull: ["$customerId", "$customerID"] }]
            }
        }
    },
    { $match: { event_type: { $exists: true, $ne: null } } },
    // Collect all usage timestamps per user per event_type
    {
        $group: {
            _id: {
                feature: "$event_type",
                customer_id: "$norm_customer_id"
            },
            first_use: { $min: "$parsed_timestamp" },
            all_timestamps: { $push: "$parsed_timestamp" }
        }
    },
    // Calculate the 7-day window cutoff from first use
    {
        $addFields: {
            seven_day_cutoff: {
                $dateAdd: { startDate: "$first_use", unit: "day", amount: 7 }
            }
        }
    },
    // Filter timestamps to find any usage AFTER first_use AND within 7 days
    {
        $addFields: {
            return_visits_within_7d: {
                $size: {
                    $filter: {
                        input: "$all_timestamps",
                        as: "ts",
                        cond: {
                            $and: [
                                // Must be strictly after first_use (not the same event)
                                { $gt: ["$$ts", "$first_use"] },
                                // Must be within 7 days of first use
                                { $lte: ["$$ts", "$seven_day_cutoff"] }
                            ]
                        }
                    }
                }
            }
        }
    },
    // Flag retained users
    {
        $addFields: {
            retained: { $cond: [{ $gt: ["$return_visits_within_7d", 0] }, 1, 0] }
        }
    },
    // Aggregate retention per event_type
    {
        $group: {
            _id: "$_id.feature",
            total_users: { $sum: 1 },
            retained_users: { $sum: "$retained" }
        }
    },
    {
        $addFields: {
            retention_rate_7day_pct: {
                $round: [
                    { $multiply: [{ $divide: ["$retained_users", "$total_users"] }, 100] },
                    2
                ]
            }
        }
    },
    { $sort: { retention_rate_7day_pct: -1 } }
]).forEach(printjson);


// ============================================================
// Q3: Funnel Analysis
// Build an onboarding funnel:
//   signup -> first_login -> workspace_created -> first_project -> invited_teammate
// Calculate drop-off rates at each stage and median time between steps.
// ============================================================

/*
  Approach:
  - Use the onboarding_events collection
  - Group by customer_id, find earliest timestamp per step
  - Count unique users at each stage for funnel drop-off
  - Pivot steps per customer and calculate time gaps (in hours)
  - Use $median across all customers for median time between steps
*/

print("\n=== Q3: Onboarding Funnel Analysis ===");

// Part A: Step counts (funnel)
print("\n--- Funnel Step Counts ---");
db.onboarding_events.aggregate([
    {
        $addFields: {
            norm_customer_id: {
                $ifNull: ["$customer_id", { $ifNull: ["$customerId", "$customerID"] }]
            },
            norm_step: { $toLower: { $ifNull: ["$step", "$event_step"] } },
            parsed_timestamp: {
                $cond: {
                    if: { $eq: [{ $type: "$timestamp" }, "date"] },
                    then: "$timestamp",
                    else: { $toDate: { $ifNull: ["$timestamp", new Date("2024-01-01")] } }
                }
            }
        }
    },
    // Count unique users at each step
    {
        $group: {
            _id: "$norm_step",
            unique_users: { $addToSet: "$norm_customer_id" },
            earliest: { $min: "$parsed_timestamp" },
            latest: { $max: "$parsed_timestamp" }
        }
    },
    {
        $addFields: { user_count: { $size: "$unique_users" } }
    },
    { $project: { unique_users: 0 } },
    { $sort: { user_count: -1 } }
]).forEach(printjson);


// Part B: Median time between steps
print("\n--- Median Time Between Onboarding Steps ---");
db.onboarding_events.aggregate([
    {
        $addFields: {
            norm_customer_id: {
                $ifNull: ["$customer_id", { $ifNull: ["$customerId", "$customerID"] }]
            },
            norm_step: { $toLower: { $ifNull: ["$step", "$event_step"] } },
            parsed_timestamp: {
                $cond: {
                    if: { $eq: [{ $type: "$timestamp" }, "date"] },
                    then: "$timestamp",
                    else: { $toDate: { $ifNull: ["$timestamp", new Date("2024-01-01")] } }
                }
            }
        }
    },
    // Get earliest timestamp per customer per step
    {
        $group: {
            _id: { customer_id: "$norm_customer_id", step: "$norm_step" },
            step_time: { $min: "$parsed_timestamp" }
        }
    },
    // Pivot: collect all steps per customer
    {
        $group: {
            _id: "$_id.customer_id",
            steps: {
                $push: { step: "$_id.step", time: "$step_time" }
            }
        }
    },
    // Extract each step's timestamp
    {
        $addFields: {
            signup_time: {
                $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "signup"] } }
            },
            first_login_time: {
                $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "first_login"] } }
            },
            workspace_time: {
                $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "workspace_created"] } }
            },
            project_time: {
                $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "first_project"] } }
            },
            invite_time: {
                $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "invited_teammate"] } }
            }
        }
    },
    // Calculate time gaps in hours between consecutive steps
    {
        $addFields: {
            signup_to_login_hrs: {
                $cond: {
                    if: { $and: [{ $gt: [{ $size: "$signup_time" }, 0] }, { $gt: [{ $size: "$first_login_time" }, 0] }] },
                    then: { $divide: [{ $subtract: [{ $arrayElemAt: ["$first_login_time.time", 0] }, { $arrayElemAt: ["$signup_time.time", 0] }] }, 3600000] },
                    else: null
                }
            },
            login_to_workspace_hrs: {
                $cond: {
                    if: { $and: [{ $gt: [{ $size: "$first_login_time" }, 0] }, { $gt: [{ $size: "$workspace_time" }, 0] }] },
                    then: { $divide: [{ $subtract: [{ $arrayElemAt: ["$workspace_time.time", 0] }, { $arrayElemAt: ["$first_login_time.time", 0] }] }, 3600000] },
                    else: null
                }
            },
            workspace_to_project_hrs: {
                $cond: {
                    if: { $and: [{ $gt: [{ $size: "$workspace_time" }, 0] }, { $gt: [{ $size: "$project_time" }, 0] }] },
                    then: { $divide: [{ $subtract: [{ $arrayElemAt: ["$project_time.time", 0] }, { $arrayElemAt: ["$workspace_time.time", 0] }] }, 3600000] },
                    else: null
                }
            },
            project_to_invite_hrs: {
                $cond: {
                    if: { $and: [{ $gt: [{ $size: "$project_time" }, 0] }, { $gt: [{ $size: "$invite_time" }, 0] }] },
                    then: { $divide: [{ $subtract: [{ $arrayElemAt: ["$invite_time.time", 0] }, { $arrayElemAt: ["$project_time.time", 0] }] }, 3600000] },
                    else: null
                }
            }
        }
    },
    // Get medians across all customers
    {
        $group: {
            _id: null,
            median_signup_to_login_hrs: { $median: { input: "$signup_to_login_hrs", method: "approximate" } },
            median_login_to_workspace_hrs: { $median: { input: "$login_to_workspace_hrs", method: "approximate" } },
            median_workspace_to_project_hrs: { $median: { input: "$workspace_to_project_hrs", method: "approximate" } },
            median_project_to_invite_hrs: { $median: { input: "$project_to_invite_hrs", method: "approximate" } },
            total_customers: { $sum: 1 }
        }
    },
    { $project: { _id: 0 } }
]).forEach(printjson);


// ============================================================
// Q4: Cross-Reference
// Identify the top 20 most engaged users who are on the FREE
// tier. These are potential upsell targets.
// ============================================================

/*
  ENGAGEMENT SCORE METHODOLOGY (0-100 scale):

  We compute a composite engagement score from 4 behavioral dimensions:

  1. Activity Frequency (30% weight):
     - Total number of events
     - Normalized: total_events / 500 (capped at 1)

  2. Session Depth (25% weight):
     - Average session duration (seconds)
     - Normalized: avg_duration / 7200 (capped at 1, assuming 2hr max)

  3. Feature Breadth (25% weight):
     - Number of distinct event_types used
     - Normalized: distinct_types / 14 (capped at 1, 14 known types)

  4. Recency (20% weight):
     - Inverse of days since last activity
     - Normalized: 1 - (days_since_last / 90), capped at [0,1]

  JUSTIFICATION:
  - Multi-dimensional: Captures both quantity and quality of usage
  - Balanced: No single dimension dominates the score
  - Recency-weighted: Prioritizes currently active users over lapsed ones
  - Business rationale: Free-tier users with high engagement scores are
    power users hitting plan limits, making them prime upsell candidates

  CROSS-REFERENCE WITH SQL:
  - The customer_tiers lookup collection (created by nimbus_tier_sync.py)
    provides the plan_tier for each customer_id
  - We filter to plan_tier = "free" BEFORE ranking, so the output
    contains only free-tier users
*/

print("\n=== Q4: Top 20 Most Engaged FREE-Tier Users (Upsell Targets) ===");

db.user_activity_logs.aggregate([
    // Stage 1: Normalize fields
    {
        $addFields: {
            parsed_timestamp: {
                $cond: {
                    if: { $eq: [{ $type: "$timestamp" }, "date"] },
                    then: "$timestamp",
                    else: { $toDate: { $ifNull: ["$timestamp", new Date("2024-01-01")] } }
                }
            },
            norm_customer_id: {
                $toInt: {
                    $ifNull: [
                        "$customer_id",
                        { $ifNull: ["$customerId", { $ifNull: ["$customerID", 0] }] }
                    ]
                }
            },
            session_dur: { $ifNull: ["$session_duration_sec", 0] }
        }
    },
    // Stage 2: Group by customer to compute engagement metrics
    {
        $group: {
            _id: "$norm_customer_id",
            total_events: { $sum: 1 },
            distinct_event_types: { $addToSet: "$event_type" },
            avg_session_duration: { $avg: "$session_dur" },
            max_session_duration: { $max: "$session_dur" },
            last_activity: { $max: "$parsed_timestamp" },
            first_activity: { $min: "$parsed_timestamp" },
            total_sessions: {
                $sum: { $cond: [{ $gt: ["$session_dur", 0] }, 1, 0] }
            }
        }
    },
    // Stage 3: Lookup tier from customer_tiers collection
    {
        $lookup: {
            from: "customer_tiers",
            localField: "_id",
            foreignField: "customer_id",
            as: "tier_info"
        }
    },
    {
        $addFields: {
            plan_tier: {
                $ifNull: [
                    { $arrayElemAt: ["$tier_info.plan_tier", 0] },
                    "unknown"
                ]
            }
        }
    },
    // Stage 4: Filter to FREE tier only
    {
        $match: { plan_tier: "free" }
    },
    // Stage 5: Calculate engagement dimensions
    {
        $addFields: {
            event_type_count: { $size: "$distinct_event_types" },
            days_since_last_activity: {
                $divide: [
                    { $subtract: [new Date(), "$last_activity"] },
                    86400000
                ]
            }
        }
    },
    // Stage 6: Compute composite engagement score (0-100)
    {
        $addFields: {
            engagement_score: {
                $round: [{
                    $add: [
                        // Frequency (30%): events / 500, capped at 1
                        { $multiply: [{ $min: [{ $divide: ["$total_events", 500] }, 1] }, 30] },
                        // Depth (25%): avg session / 7200 sec (2hr), capped
                        { $multiply: [{ $min: [{ $divide: [{ $ifNull: ["$avg_session_duration", 0] }, 7200] }, 1] }, 25] },
                        // Breadth (25%): distinct event types / 14, capped
                        { $multiply: [{ $min: [{ $divide: ["$event_type_count", 14] }, 1] }, 25] },
                        // Recency (20%): inverse of days since last (max 90)
                        { $multiply: [{ $max: [{ $subtract: [1, { $divide: [{ $min: ["$days_since_last_activity", 90] }, 90] }] }, 0] }, 20] }
                    ]
                }, 2]
            }
        }
    },
    // Stage 7: Sort and limit to top 20
    { $sort: { engagement_score: -1 } },
    { $limit: 20 },
    // Stage 8: Clean output
    {
        $project: {
            _id: 0,
            customer_id: "$_id",
            plan_tier: 1,
            engagement_score: 1,
            total_events: 1,
            total_sessions: 1,
            event_type_count: 1,
            avg_session_duration_sec: { $round: ["$avg_session_duration", 1] },
            days_since_last_activity: { $round: ["$days_since_last_activity", 0] }
        }
    }
]).forEach(printjson);

print("\n=== All MongoDB Queries Complete ===");
