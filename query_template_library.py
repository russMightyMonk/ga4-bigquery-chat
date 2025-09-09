QUERY_TEMPLATE_LIBRARY = {
        "analyze_user_activity_status": {
        "description": "Shows breakdown of active vs inactive users. Good for questions like 'how many active users?', 'active vs inactive user breakdown', or 'user activity analysis'.",
        "template": """
-- Analyzes users by their activity status (active/inactive)
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN is_active_user IS TRUE THEN 'Active_Users'
        WHEN is_active_user IS FALSE THEN 'Inactive_Users'
        ELSE 'Unknown_Status'
    END AS activity_status,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional filters as specified by user
GROUP BY
    activity_status
ORDER BY
    user_count DESC
"""
    },

    "analyze_user_lifetime_value": {
        "description": "Analyzes user lifetime value (LTV) metrics and revenue. Good for questions like 'user LTV analysis', 'lifetime value by currency', 'revenue per user', or 'LTV distribution'.",
        "template": """
-- Analyzes user lifetime value including revenue and currency breakdown
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    user_ltv.currency AS revenue_currency,
    COUNT(DISTINCT user_pseudo_id) AS users_with_ltv,
    ROUND(AVG(user_ltv.revenue), 2) AS avg_ltv_revenue,
    ROUND(SUM(user_ltv.revenue), 2) AS total_ltv_revenue,
    MIN(user_ltv.revenue) AS min_ltv_revenue,
    MAX(user_ltv.revenue) AS max_ltv_revenue
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND user_ltv.revenue IS NOT NULL
    -- LLM: Include additional WHERE conditions based on user requirements
GROUP BY
    revenue_currency
ORDER BY
    total_ltv_revenue DESC
"""
    },

    "analyze_user_acquisition_cohorts": {
        "description": "Analyzes users by their first touch timestamp (acquisition date). Good for questions like 'users by acquisition date', 'new user cohorts', 'when did users first visit?', or 'user acquisition timeline'.",
        "template": """
-- Analyzes users by their acquisition date (first touch)
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS acquisition_date,
    COUNT(DISTINCT user_pseudo_id) AS new_users_acquired,
    COUNT(DISTINCT CASE WHEN is_active_user IS TRUE THEN user_pseudo_id END) AS active_users_acquired
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND user_first_touch_timestamp IS NOT NULL
    -- LLM: Apply user-specified filters
GROUP BY
    acquisition_date
ORDER BY
    acquisition_date DESC
"""
    },

    "extract_specific_user_property": {
        "description": "Extracts and analyzes a specific user property by key. Good for questions like 'show user property [property_name]', 'users by [property_name]', 'user property analysis for [property_name]', or 'custom user attribute breakdown'.",
        "template": """
-- Extracts and analyzes a specific user property
-- LLM: Replace {property_key}, {start_date} and {end_date} with user-specified values
SELECT
    COALESCE(
        (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = '{property_key}'),
        CAST((SELECT value.int_value FROM UNNEST(user_properties) WHERE key = '{property_key}') AS STRING),
        CAST((SELECT value.float_value FROM UNNEST(user_properties) WHERE key = '{property_key}') AS STRING),
        CAST((SELECT value.double_value FROM UNNEST(user_properties) WHERE key = '{property_key}') AS STRING),
        '(not_set)'
    ) AS property_value,
    COUNT(DISTINCT user_pseudo_id) AS user_count,
    COUNT(DISTINCT CASE WHEN is_active_user IS TRUE THEN user_pseudo_id END) AS active_user_count
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional filtering conditions
GROUP BY
    property_value
ORDER BY
    user_count DESC
"""
    },

    "analyze_user_property_timestamps": {
        "description": "Analyzes when user properties were last updated. Good for questions like 'when was user property [property_name] last set?', 'user property update timeline', or 'property freshness analysis'.",
        "template": """
-- Analyzes when specific user properties were last set/updated
-- LLM: Replace {property_key}, {start_date} and {end_date} with user-specified values
SELECT
    DATE(TIMESTAMP_MICROS((SELECT value.set_timestamp_micros FROM UNNEST(user_properties) WHERE key = '{property_key}'))) AS property_set_date,
    COUNT(DISTINCT user_pseudo_id) AS users_with_property,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = '{property_key}') IS NOT NULL
             OR (SELECT value.int_value FROM UNNEST(user_properties) WHERE key = '{property_key}') IS NOT NULL
             OR (SELECT value.float_value FROM UNNEST(user_properties) WHERE key = '{property_key}') IS NOT NULL
             OR (SELECT value.double_value FROM UNNEST(user_properties) WHERE key = '{property_key}') IS NOT NULL
             THEN user_pseudo_id
        END
    ) AS users_with_property_value
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND EXISTS (SELECT 1 FROM UNNEST(user_properties) WHERE key = '{property_key}')
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    property_set_date
ORDER BY
    property_set_date DESC
"""
    },

    "compare_user_ids_vs_pseudo_ids": {
        "description": "Compares users with custom user_id vs those with only pseudo_id. Good for questions like 'identified vs anonymous users', 'user identification rate', 'logged in vs guest users', or 'user authentication analysis'.",
        "template": """
-- Compares users with custom user_id vs anonymous users (pseudo_id only)
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN user_id IS NOT NULL THEN 'Identified_Users'
        ELSE 'Anonymous_Users'
    END AS user_identification_status,
    COUNT(DISTINCT user_pseudo_id) AS user_count,
    COUNT(DISTINCT CASE WHEN is_active_user IS TRUE THEN user_pseudo_id END) AS active_user_count,
    ROUND(AVG(user_ltv.revenue), 2) AS avg_ltv_revenue
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply additional filters as needed
GROUP BY
    user_identification_status
ORDER BY
    user_count DESC
"""
    },

    "list_available_user_properties": {
        "description": "Lists all available user property keys in the data. Good for questions like 'what user properties are available?', 'list user property keys', 'show available user attributes', or 'user property discovery'.",
        "template": """
-- Lists all available user property keys with usage statistics
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    user_prop.key AS property_key,
    COUNT(DISTINCT user_pseudo_id) AS users_with_property,
    COUNT(DISTINCT 
        CASE WHEN user_prop.value.string_value IS NOT NULL 
             OR user_prop.value.int_value IS NOT NULL 
             OR user_prop.value.float_value IS NOT NULL 
             OR user_prop.value.double_value IS NOT NULL 
             THEN user_pseudo_id 
        END
    ) AS users_with_values,
    CASE 
        WHEN COUNTIF(user_prop.value.string_value IS NOT NULL) > 0 THEN 'string'
        WHEN COUNTIF(user_prop.value.int_value IS NOT NULL) > 0 THEN 'integer'
        WHEN COUNTIF(user_prop.value.float_value IS NOT NULL) > 0 THEN 'float'
        WHEN COUNTIF(user_prop.value.double_value IS NOT NULL) > 0 THEN 'double'
        ELSE 'unknown'
    END AS primary_data_type
FROM
    `{project_id}.{dataset_id}.events_*`,
    UNNEST(user_properties) AS user_prop
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add filtering conditions if user specifies
GROUP BY
    property_key
ORDER BY
    users_with_property DESC
"""
    },
    "classify_visitor_type": {
        "description": "Categorizes users as first-time or repeat visitors based on their session history. Good for questions like 'show me new vs returning visitors', 'breakdown of visitor types', or 'how many new visitors do we have?'.",
        "template": """
-- Categorizes users based on whether they are first-time or repeat visitors
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE
        WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE event_name = 'session_start' AND key = 'ga_session_number') = 1 
            THEN 'first_time_visitor'
        WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE event_name = 'session_start' AND key = 'ga_session_number') > 1 
            THEN 'repeat_visitor'
        ELSE NULL 
    END AS visitor_category,
    COUNT(DISTINCT user_pseudo_id) AS total_users
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional filters here if user specifies (e.g., country, device, traffic source)
GROUP BY
    visitor_category
HAVING
    visitor_category IS NOT NULL
ORDER BY
    total_users DESC
"""
    },

    "analyze_session_distribution": {
        "description": "Shows how sessions are distributed across users (session depth analysis). Good for questions like 'how many sessions do users typically have?', 'session count distribution', or 'user engagement by session frequency'.",
        "template": """
-- Analyzes the distribution of session counts across users
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH user_session_data AS (
    SELECT
        user_pseudo_id,
        MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number')) 
            OVER (PARTITION BY user_pseudo_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_user_sessions
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Add more WHERE conditions if user specifies filters
)
SELECT
    total_user_sessions AS session_depth,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM
    user_session_data
GROUP BY
    total_user_sessions
ORDER BY
    total_user_sessions ASC
"""
    },

    "calculate_total_users": {
        "description": "Counts the total number of unique users. Good for basic questions like 'how many users?', 'total user count', or 'user volume'.",
        "template": """
-- Calculates the total count of unique users
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT user_pseudo_id) AS unique_users
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional WHERE clauses based on user filters (device, location, etc.)
"""
    },

    "measure_engaged_users": {
        "description": "Counts users who showed engagement during their visit. Good for questions like 'how many engaged users?', 'active user count', or 'user engagement metrics'.",
        "template": """
-- Measures users who demonstrated engagement through time spent or interaction
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') > 0  
                  OR (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN user_pseudo_id 
             ELSE NULL 
        END
    ) AS engaged_user_count
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add filters if user specifies additional criteria
"""
    },

    "identify_first_time_users": {
        "description": "Counts users who are visiting for the first time. Good for questions like 'how many new users?', 'first-time visitor count', or 'user acquisition metrics'.",
        "template": """
-- Identifies and counts users on their first visit
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
             ELSE NULL 
        END
    ) AS first_time_users
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add WHERE conditions for additional user specifications
"""
    },

    "calculate_new_user_percentage": {
        "description": "Calculates the percentage of users who are first-time visitors. Good for questions like 'what percent are new users?', 'new user ratio', or 'acquisition vs retention ratio'.",
        "template": """
-- Calculates the percentage of first-time users relative to total users
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
                 THEN user_pseudo_id 
                 ELSE NULL 
            END
        ) / COUNT(DISTINCT user_pseudo_id) * 100, 2
    ) AS new_user_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include additional filters as specified by user
"""
    },

    "count_initial_sessions": {
        "description": "Counts sessions that are a user's first session. Good for questions like 'how many new sessions?', 'first session count', or 'session acquisition metrics'.",
        "template": """
-- Counts sessions where users are visiting for the first time
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
             ELSE NULL 
        END
    ) AS initial_sessions
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add user-specified filters here
"""
    },

    "calculate_new_session_rate": {
        "description": "Calculates the percentage of sessions from first-time users. Good for questions like 'what percentage are new sessions?', 'first session ratio', or 'session acquisition rate'.",
        "template": """
-- Calculates the percentage of sessions that are from first-time users
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
                 THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                 ELSE NULL 
            END
        ) / COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))) * 100, 2
    ) AS new_session_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply additional filters based on user requirements
"""
    },

    "calculate_sessions_per_user": {
        "description": "Calculates the average number of sessions per user. Good for questions like 'average sessions per user', 'session frequency', or 'user session behavior'.",
        "template": """
-- Calculates the average number of sessions per individual user
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    ROUND(
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))) 
        / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include user-specified filtering conditions
"""
    },

    "calculate_events_per_user": {
        "description": "Calculates the average number of specific events per user. Good for questions like 'average page views per user', 'events per user for [event_name]', or 'user interaction frequency'.",
        "template": """
-- Calculates the average count of a specific event type per user
-- LLM: Replace {event_name}, {start_date} and {end_date} with user-specified values
SELECT
    ROUND(
        COUNTIF(event_name = '{event_name}') / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_events_per_user
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional WHERE conditions as needed
"""
    },

    "calculate_engaged_sessions_per_user": {
        "description": "Calculates the average number of engaged sessions per user. Good for questions like 'engaged sessions per user', 'quality sessions per user', or 'user engagement depth'.",
        "template": """
-- Calculates the average number of engaged sessions per individual user
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                 THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                 ELSE NULL 
            END
        ) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_engaged_sessions_per_user
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply user-specified filters here
"""
    },

    "analyze_campaign_sessions": {
        "description": "Analyzes sessions by marketing campaign attribution. Good for questions like 'which campaigns drive the most sessions?', 'campaign performance by sessions', or 'session breakdown by campaign'.",
        "template": """
-- Analyzes session distribution across marketing campaigns
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH campaign_data AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_identifier,
        ARRAY_AGG(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') 
            IGNORE NULLS ORDER BY event_timestamp
        )[SAFE_OFFSET(0)] AS marketing_campaign
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Add additional filters if user specifies criteria
    GROUP BY
        user_pseudo_id,
        session_identifier
)
SELECT
    COALESCE(marketing_campaign, '(unassigned)') AS campaign_name,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_identifier AS STRING))) AS session_count
FROM
    campaign_data
GROUP BY
    campaign_name
ORDER BY
    session_count DESC
"""
    },

    "analyze_medium_sessions": {
        "description": "Analyzes sessions by traffic medium (organic, paid, referral, etc.). Good for questions like 'sessions by traffic medium', 'which mediums perform best?', or 'medium breakdown for sessions'.",
        "template": """
-- Analyzes session distribution across traffic mediums
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH medium_data AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_identifier,
        ARRAY_AGG(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') 
            IGNORE NULLS ORDER BY event_timestamp
        )[SAFE_OFFSET(0)] AS traffic_medium
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Include user-specified filtering conditions
    GROUP BY
        user_pseudo_id,
        session_identifier
)
SELECT
    COALESCE(traffic_medium, '(undefined)') AS medium_type,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_identifier AS STRING))) AS session_count
FROM
    medium_data
GROUP BY
    medium_type
ORDER BY
    session_count DESC
"""
    },

    "analyze_source_sessions": {
        "description": "Analyzes sessions by traffic source (google, facebook, direct, etc.). Good for questions like 'top traffic sources by sessions', 'which sources drive sessions?', or 'session source analysis'.",
        "template": """
-- Analyzes session distribution across traffic sources
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH source_data AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_identifier,
        ARRAY_AGG(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') 
            IGNORE NULLS ORDER BY event_timestamp
        )[SAFE_OFFSET(0)] AS traffic_source
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Apply additional WHERE conditions as specified
    GROUP BY
        user_pseudo_id,
        session_identifier
)
SELECT
    COALESCE(traffic_source, '(direct_access)') AS source_name,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_identifier AS STRING))) AS session_count
FROM
    source_data
GROUP BY
    source_name
ORDER BY
    session_count DESC
"""
    },

    "analyze_source_medium_users": {
        "description": "Analyzes user acquisition by source/medium combination using built-in traffic_source fields. Good for questions like 'user acquisition by source/medium', 'which source/medium brings users?', or 'top user acquisition channels'.",
        "template": """
-- Analyzes user distribution across source/medium combinations using traffic_source data
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CONCAT(
        COALESCE(traffic_source.source, '(direct_access)'), 
        ' / ', 
        COALESCE(traffic_source.medium, '(undefined)')
    ) AS acquisition_channel,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add user-specified filters here
GROUP BY
    acquisition_channel
ORDER BY
    user_count DESC
"""
    },

    "analyze_source_medium_sessions": {
        "description": "Analyzes sessions by source/medium combination from event parameters. Good for questions like 'sessions by source/medium', 'traffic channel performance', or 'session attribution analysis'.",
        "template": """
-- Analyzes session distribution across source/medium combinations
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH attribution_data AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_identifier,
        ARRAY_AGG(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') 
            IGNORE NULLS ORDER BY event_timestamp
        )[SAFE_OFFSET(0)] AS traffic_source,
        ARRAY_AGG(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') 
            IGNORE NULLS ORDER BY event_timestamp
        )[SAFE_OFFSET(0)] AS traffic_medium
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Include additional filtering as needed
    GROUP BY
        user_pseudo_id,
        session_identifier
)
SELECT
    CONCAT(
        COALESCE(traffic_source, '(direct_access)'), 
        ' / ', 
        COALESCE(traffic_medium, '(undefined)')
    ) AS source_medium_combination,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_identifier AS STRING))) AS session_count
FROM
    attribution_data
GROUP BY
    source_medium_combination
ORDER BY
    session_count DESC
"""
    },

    "classify_user_channels": {
        "description": "Classifies users by default channel grouping based on first-touch attribution. Good for questions like 'users by channel', 'acquisition channel performance', or 'user channel classification'.",
        "template": """
-- Classifies users into default channel groups based on traffic source attribution
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE
        WHEN (traffic_source.source IS NULL OR traffic_source.source = '(direct)') 
             AND (traffic_source.medium IS NULL OR traffic_source.medium IN ('(not set)', '(none)')) 
             THEN 'Direct_Access'
        WHEN traffic_source.name LIKE '%cross-network%' 
             THEN 'Cross_Network'
        WHEN (REGEXP_CONTAINS(traffic_source.source, 'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
              OR REGEXP_CONTAINS(traffic_source.name, r'^(.*(([^a-df-z]|^)shop|shopping).*)$'))
             AND REGEXP_CONTAINS(traffic_source.medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Shopping'
        WHEN REGEXP_CONTAINS(traffic_source.source, 'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex') 
             AND REGEXP_CONTAINS(traffic_source.medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Search'
        WHEN REGEXP_CONTAINS(traffic_source.source, 'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
             AND REGEXP_CONTAINS(traffic_source.medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Social'
        WHEN REGEXP_CONTAINS(traffic_source.source, 'dailymotion|disneyplus|netflix|youtube|vimeo|twitch') 
             AND REGEXP_CONTAINS(traffic_source.medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Video'
        WHEN traffic_source.medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') 
             THEN 'Display_Advertising'
        WHEN REGEXP_CONTAINS(traffic_source.medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Other'
        WHEN REGEXP_CONTAINS(traffic_source.source, 'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart') 
             OR REGEXP_CONTAINS(traffic_source.name, r'^(.*(([^a-df-z]|^)shop|shopping).*)$') 
             THEN 'Organic_Shopping'
        WHEN REGEXP_CONTAINS(traffic_source.source, 'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp') 
             OR traffic_source.medium IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media') 
             THEN 'Organic_Social'
        WHEN REGEXP_CONTAINS(traffic_source.source, 'dailymotion|disneyplus|netflix|youtube|vimeo|twitch') 
             OR REGEXP_CONTAINS(traffic_source.medium, r'^(.*video.*)$') 
             THEN 'Organic_Video'
        WHEN REGEXP_CONTAINS(traffic_source.source, 'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex') 
             OR traffic_source.medium = 'organic' 
             THEN 'Organic_Search'
        WHEN traffic_source.medium IN ('referral', 'app', 'link') 
             THEN 'Referral_Traffic'
        WHEN REGEXP_CONTAINS(traffic_source.source, 'email|e-mail|e_mail|e mail') 
             OR REGEXP_CONTAINS(traffic_source.medium, 'email|e-mail|e_mail|e mail') 
             THEN 'Email_Marketing'
        WHEN traffic_source.medium = 'affiliate' 
             THEN 'Affiliate_Marketing'
        WHEN traffic_source.medium = 'audio' 
             THEN 'Audio_Advertising'
        WHEN traffic_source.source = 'sms' OR traffic_source.medium = 'sms' 
             THEN 'SMS_Marketing'
        WHEN traffic_source.medium LIKE '%push' 
             OR REGEXP_CONTAINS(traffic_source.medium, 'mobile|notification') 
             OR traffic_source.source = 'firebase' 
             THEN 'Push_Notifications'
        ELSE 'Unclassified'
    END AS user_channel_group,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional WHERE clauses based on user requirements
GROUP BY
    user_channel_group
ORDER BY
    user_count DESC
"""
    },

    "classify_session_channels": {
        "description": "Classifies sessions by default channel grouping based on session attribution. Good for questions like 'sessions by channel', 'channel performance by sessions', or 'session channel classification'.",
        "template": """
-- Classifies sessions into default channel groups based on attribution data
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH session_attribution AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_identifier,
        ARRAY_AGG(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') 
            IGNORE NULLS ORDER BY event_timestamp
        )[SAFE_OFFSET(0)] AS traffic_source,
        ARRAY_AGG(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') 
            IGNORE NULLS ORDER BY event_timestamp
        )[SAFE_OFFSET(0)] AS traffic_medium,
        ARRAY_AGG(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') 
            IGNORE NULLS ORDER BY event_timestamp
        )[SAFE_OFFSET(0)] AS marketing_campaign
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Apply user-specified filters
    GROUP BY
        user_pseudo_id,
        session_identifier
)
SELECT
    CASE
        WHEN (traffic_source IS NULL OR traffic_source = '(direct)') 
             AND (traffic_medium IS NULL OR traffic_medium IN ('(not set)', '(none)')) 
             THEN 'Direct_Access'
        WHEN marketing_campaign LIKE '%cross-network%' 
             THEN 'Cross_Network'
        WHEN (REGEXP_CONTAINS(traffic_source, 'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
              OR REGEXP_CONTAINS(marketing_campaign, r'^(.*(([^a-df-z]|^)shop|shopping).*)$'))
             AND REGEXP_CONTAINS(traffic_medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Shopping'
        WHEN REGEXP_CONTAINS(traffic_source, 'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex') 
             AND REGEXP_CONTAINS(traffic_medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Search'
        WHEN REGEXP_CONTAINS(traffic_source, 'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
             AND REGEXP_CONTAINS(traffic_medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Social'
        WHEN REGEXP_CONTAINS(traffic_source, 'dailymotion|disneyplus|netflix|youtube|vimeo|twitch') 
             AND REGEXP_CONTAINS(traffic_medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Video'
        WHEN traffic_medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') 
             THEN 'Display_Advertising'
        WHEN REGEXP_CONTAINS(traffic_medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') 
             THEN 'Paid_Other'
        WHEN REGEXP_CONTAINS(traffic_source, 'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart') 
             OR REGEXP_CONTAINS(marketing_campaign, r'^(.*(([^a-df-z]|^)shop|shopping).*)$') 
             THEN 'Organic_Shopping'
        WHEN REGEXP_CONTAINS(traffic_source, 'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp') 
             OR traffic_medium IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media') 
             THEN 'Organic_Social'
        WHEN REGEXP_CONTAINS(traffic_source, 'dailymotion|disneyplus|netflix|youtube|vimeo|twitch') 
             OR REGEXP_CONTAINS(traffic_medium, r'^(.*video.*)$') 
             THEN 'Organic_Video'
        WHEN REGEXP_CONTAINS(traffic_source, 'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex') 
             OR traffic_medium = 'organic' 
             THEN 'Organic_Search'
        WHEN traffic_medium IN ('referral', 'app', 'link') 
             THEN 'Referral_Traffic'
        WHEN REGEXP_CONTAINS(traffic_source, 'email|e-mail|e_mail|e mail') 
             OR REGEXP_CONTAINS(traffic_medium, 'email|e-mail|e_mail|e mail') 
             THEN 'Email_Marketing'
        WHEN traffic_medium = 'affiliate' 
             THEN 'Affiliate_Marketing'
        WHEN traffic_medium = 'audio' 
             THEN 'Audio_Advertising'
        WHEN traffic_source = 'sms' OR traffic_medium = 'sms' 
             THEN 'SMS_Marketing'
        WHEN traffic_medium LIKE '%push' 
             OR REGEXP_CONTAINS(traffic_medium, 'mobile|notification') 
             OR traffic_source = 'firebase' 
             THEN 'Push_Notifications'
        ELSE 'Unclassified'
    END AS session_channel_group,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_identifier AS STRING))) AS session_count
FROM
    session_attribution
GROUP BY
    session_channel_group
ORDER BY
    session_count DESC
"""
    },

    "analyze_user_referrers": {
        "description": "Analyzes user acquisition by referring page/website for first-time users. Good for questions like 'top referring sites for users', 'user referrer analysis', or 'which sites send us users?'.",
        "template": """
-- Analyzes user acquisition based on the referring page from their first session
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH referrer_prep AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_identifier,
        MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer')) AS referring_page
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Add additional filters as specified by user
    GROUP BY
        user_pseudo_id,
        session_identifier
),
first_session_referrer AS (
    SELECT
        user_pseudo_id,
        session_identifier,
        referring_page,
        RANK() OVER (PARTITION BY user_pseudo_id ORDER BY session_identifier) AS session_rank
    FROM
        referrer_prep
    QUALIFY session_rank = 1
)
SELECT
    COALESCE(referring_page, '(direct_entry)') AS user_referrer_source,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM
    first_session_referrer
GROUP BY
    user_referrer_source
ORDER BY
    user_count DESC
"""
    },

    "analyze_session_referrers": {
        "description": "Analyzes sessions by their referring page/website. Good for questions like 'sessions by referrer', 'which sites refer traffic?', or 'referral traffic analysis by sessions'.",
        "template": """
-- Analyzes sessions based on their referring page or website
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH session_referrer_data AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_identifier,
        MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer')) AS referring_page
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Include user-specified WHERE conditions
    GROUP BY
        user_pseudo_id,
        session_identifier
)
SELECT
    COALESCE(referring_page, '(direct_entry)') AS session_referrer_source,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_identifier AS STRING))) AS session_count
FROM
    session_referrer_data
GROUP BY
    session_referrer_source
ORDER BY
    session_count DESC
"""
    },
        "count_total_sessions": {
        "description": "Counts the total number of unique sessions. Good for basic questions like 'how many sessions?', 'total session count', or 'session volume'.",
        "template": """
-- Counts the total number of unique sessions in the specified date range
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional WHERE clauses based on user filters (device, location, etc.)
"""
    },

    "count_engaged_sessions": {
        "description": "Counts sessions where users showed meaningful engagement. Good for questions like 'how many engaged sessions?', 'quality sessions count', or 'engaged session volume'.",
        "template": """
-- Counts sessions that meet engagement criteria (user interaction beyond simple page load)
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
             ELSE NULL 
        END
    ) AS engaged_session_count
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include user-specified filtering conditions
"""
    },

    "calculate_engagement_rate": {
        "description": "Calculates the percentage of sessions that were engaged. Good for questions like 'what is the engagement rate?', 'percentage of engaged sessions', or 'session quality metrics'.",
        "template": """
-- Calculates the ratio of engaged sessions to total sessions as a percentage
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                     ELSE NULL 
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply additional filters as specified by user
"""
    },

    "calculate_average_engagement_time": {
        "description": "Calculates the average engagement time per engaged session in seconds. Good for questions like 'average engagement time', 'how long do users engage?', or 'session engagement duration'.",
        "template": """
-- Calculates the average engagement time for sessions that showed engagement
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH session_engagement_data AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_identifier,
        MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged')) AS is_engaged,
        SUM((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec')) / 1000 AS engagement_seconds
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Add user-specified filters here
    GROUP BY
        user_pseudo_id,
        session_identifier
)
SELECT
    ROUND(
        SAFE_DIVIDE(
            SUM(engagement_seconds),
            COUNT(DISTINCT 
                CASE WHEN is_engaged = '1' 
                     THEN CONCAT(user_pseudo_id, CAST(session_identifier AS STRING))
                     ELSE NULL 
                END
            )
        ), 2
    ) AS avg_engagement_time_seconds
FROM
    session_engagement_data
"""
    },

    "count_bounce_sessions": {
        "description": "Counts sessions that bounced (no engagement). Good for questions like 'how many bounces?', 'bounce session count', or 'non-engaged sessions'.",
        "template": """
-- Counts sessions that did not meet engagement criteria (bounced sessions)
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) - COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
             ELSE NULL 
        END
    ) AS bounce_session_count
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include additional WHERE conditions as needed
"""
    },

    "calculate_bounce_rate": {
        "description": "Calculates the percentage of sessions that bounced (showed no engagement). Good for questions like 'what is the bounce rate?', 'percentage of bounced sessions', or 'session abandonment rate'.",
        "template": """
-- Calculates the percentage of sessions that did not show engagement
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            )) - COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                     ELSE NULL 
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS bounce_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply user-specified filtering conditions
"""
    },

    "calculate_events_per_session": {
        "description": "Calculates the average number of specific events per session. Good for questions like 'average page views per session', 'events per session for [event_name]', or 'session interaction depth'.",
        "template": """
-- Calculates the average count of a specific event type per session
-- LLM: Replace {event_name}, {start_date} and {end_date} with user-specified values
SELECT
    ROUND(
        SAFE_DIVIDE(
            COUNTIF(event_name = '{event_name}'),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ), 2
    ) AS avg_events_per_session
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional WHERE conditions as specified
"""
    },

    "calculate_average_session_duration": {
        "description": "Calculates the average session duration in seconds. Good for questions like 'average session duration', 'how long are sessions?', or 'session length analysis'.",
        "template": """
-- Calculates the average duration of sessions based on first and last event timestamps
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH session_duration_data AS (
    SELECT
        CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        ) AS session_identifier,
        (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000 AS duration_seconds
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Include user-specified filters
    GROUP BY
        session_identifier
)
SELECT
    ROUND(
        SUM(duration_seconds) / COUNT(DISTINCT session_identifier), 2
    ) AS avg_session_duration_seconds
FROM
    session_duration_data
"""
    },

    "calculate_pages_per_session": {
        "description": "Calculates the average number of page views per session. Good for questions like 'pages per session', 'average page views per session', or 'session page depth'.",
        "template": """
-- Calculates the average number of page views per session
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH session_pageview_data AS (
    SELECT
        CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        ) AS session_identifier,
        COUNTIF(event_name = 'page_view') AS page_views
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Add additional WHERE clauses based on user requirements
    GROUP BY
        session_identifier
)
SELECT
    ROUND(
        SUM(page_views) / COUNT(DISTINCT session_identifier), 2
    ) AS avg_pages_per_session
FROM
    session_pageview_data
"""
    },
        "analyze_device_categories": {
        "description": "Analyzes users/sessions by device category (mobile, desktop, tablet). Good for questions like 'users by device type', 'mobile vs desktop usage', 'device category breakdown', or 'platform distribution'.",
        "template": """
-- Analyzes user and session distribution across device categories
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(device.category, 'Unknown_Device') AS device_type,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional filters as specified by user
GROUP BY
    device_type
ORDER BY
    unique_users DESC
"""
    },

    "analyze_mobile_devices": {
        "description": "Analyzes mobile device brands, models, and marketing names. Good for questions like 'top mobile devices', 'iPhone vs Android usage', 'mobile device breakdown', or 'device model analysis'.",
        "template": """
-- Analyzes mobile device characteristics including brand, model, and marketing name
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(device.mobile_brand_name, 'Unknown_Brand') AS mobile_brand,
    COALESCE(device.mobile_model_name, 'Unknown_Model') AS mobile_model,
    COALESCE(device.mobile_marketing_name, 'Unknown_Marketing_Name') AS marketing_name,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND device.category = 'mobile'
    -- LLM: Include additional WHERE conditions based on user requirements
GROUP BY
    mobile_brand, mobile_model, marketing_name
ORDER BY
    unique_users DESC
"""
    },

    "analyze_operating_systems": {
        "description": "Analyzes operating systems and their versions. Good for questions like 'users by operating system', 'iOS vs Android', 'OS version distribution', or 'operating system breakdown'.",
        "template": """
-- Analyzes operating system distribution and version usage
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(device.operating_system, 'Unknown_OS') AS operating_system,
    COALESCE(device.operating_system_version, 'Unknown_Version') AS os_version,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    ROUND(AVG(device.time_zone_offset_seconds / 3600), 1) AS avg_timezone_offset_hours
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply user-specified filtering conditions
GROUP BY
    operating_system, os_version
ORDER BY
    unique_users DESC
"""
    },

    "analyze_browser_usage": {
        "description": "Analyzes web browser usage and versions. Good for questions like 'users by browser', 'Chrome vs Safari usage', 'browser version distribution', or 'web browser breakdown'.",
        "template": """
-- Analyzes web browser usage including versions for web traffic
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(device.browser, device.web_info.browser, 'Unknown_Browser') AS browser_name,
    COALESCE(device.browser_version, device.web_info.browser_version, 'Unknown_Version') AS browser_version,
    COALESCE(device.web_info.hostname, 'Unknown_Hostname') AS website_hostname,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND platform = 'WEB'
    -- LLM: Add additional WHERE clauses as needed
GROUP BY
    browser_name, browser_version, website_hostname
ORDER BY
    unique_users DESC
"""
    },

    "analyze_app_information": {
        "description": "Analyzes mobile app information including versions, install sources, and stores. Good for questions like 'app version usage', 'install source breakdown', 'app store analysis', or 'app version distribution'.",
        "template": """
-- Analyzes mobile app characteristics including version, install source, and store
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(app_info.id, 'Unknown_App_ID') AS app_bundle_id,
    COALESCE(app_info.version, 'Unknown_Version') AS app_version,
    COALESCE(app_info.install_store, 'Unknown_Store') AS installation_store,
    COALESCE(app_info.install_source, 'Unknown_Source') AS installation_source,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND platform = 'APP'
    AND app_info.id IS NOT NULL
    -- LLM: Include user-specified filtering conditions
GROUP BY
    app_bundle_id, app_version, installation_store, installation_source
ORDER BY
    unique_users DESC
"""
    },

    "analyze_device_languages": {
        "description": "Analyzes device language settings and geographic distribution. Good for questions like 'users by language', 'device language breakdown', 'language preferences', or 'localization analysis'.",
        "template": """
-- Analyzes device language settings and timezone distribution
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(device.language, 'Unknown_Language') AS device_language,
    ROUND(device.time_zone_offset_seconds / 3600, 1) AS timezone_offset_hours,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(DISTINCT device.category) AS device_categories_used
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply additional filters as specified
GROUP BY
    device_language, timezone_offset_hours
ORDER BY
    unique_users DESC
"""
    },

    "analyze_platform_streams": {
        "description": "Analyzes platform distribution and data streams. Good for questions like 'web vs app usage', 'platform breakdown', 'data stream analysis', or 'cross-platform user behavior'.",
        "template": """
-- Analyzes platform distribution (web vs app) and associated data streams
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(platform, 'Unknown_Platform') AS platform_type,
    COALESCE(CAST(stream_id AS STRING), 'Unknown_Stream') AS data_stream_id,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_name) AS unique_event_types
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add user-specified WHERE conditions
GROUP BY
    platform_type, data_stream_id
ORDER BY
    unique_users DESC
"""
    },

    "analyze_ad_tracking_settings": {
        "description": "Analyzes advertising tracking preferences and advertising IDs. Good for questions like 'ad tracking opt-out rates', 'advertising ID usage', 'privacy settings analysis', or 'tracking consent breakdown'.",
        "template": """
-- Analyzes advertising tracking settings and ID availability
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN device.is_limited_ad_tracking IS TRUE THEN 'Ad_Tracking_Limited'
        WHEN device.is_limited_ad_tracking IS FALSE THEN 'Ad_Tracking_Allowed'
        ELSE 'Ad_Tracking_Unknown'
    END AS ad_tracking_status,
    CASE 
        WHEN device.advertising_id IS NOT NULL THEN 'Has_Advertising_ID'
        WHEN device.vendor_id IS NOT NULL THEN 'Has_Vendor_ID_Only'
        ELSE 'No_Advertising_Identifiers'
    END AS identifier_availability,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include additional filtering as needed
GROUP BY
    ad_tracking_status, identifier_availability
ORDER BY
    unique_users DESC
"""
    },
        "analyze_global_reach": {
        "description": "Analyzes user distribution across continents and subcontinents. Good for questions like 'global user distribution', 'users by continent', 'worldwide reach analysis', or 'international audience breakdown'.",
        "template": """
-- Analyzes user and session distribution across global regions (continents and subcontinents)
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(geo.continent, 'Unknown_Continent') AS continent_name,
    COALESCE(geo.sub_continent, 'Unknown_Subcontinent') AS subcontinent_name,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    ROUND(COUNT(DISTINCT user_pseudo_id) * 100.0 / SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2) AS user_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional filters as specified by user
GROUP BY
    continent_name, subcontinent_name
ORDER BY
    unique_users DESC
"""
    },

    "analyze_country_performance": {
        "description": "Analyzes user behavior and engagement by country. Good for questions like 'top countries by users', 'country performance analysis', 'international market breakdown', or 'users by country'.",
        "template": """
-- Analyzes user distribution and engagement metrics by country
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(geo.country, 'Unknown_Country') AS country_name,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    country_name
ORDER BY
    unique_users DESC
"""
    },

    "analyze_regional_markets": {
        "description": "Analyzes user distribution by regions/states within countries. Good for questions like 'users by state', 'regional breakdown', 'top regions by users', or 'state-level analysis'.",
        "template": """
-- Analyzes user distribution across regions/states within countries
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(geo.country, 'Unknown_Country') AS country_name,
    COALESCE(geo.region, 'Unknown_Region') AS region_state,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply additional filters as needed
GROUP BY
    country_name, region_state
ORDER BY
    unique_users DESC
"""
    },

    "analyze_city_markets": {
        "description": "Analyzes user distribution and behavior by city. Good for questions like 'top cities by users', 'city-level analysis', 'urban market performance', or 'users by city'.",
        "template": """
-- Analyzes user distribution and engagement at the city level
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(geo.country, 'Unknown_Country') AS country_name,
    COALESCE(geo.region, 'Unknown_Region') AS region_state,
    COALESCE(geo.city, 'Unknown_City') AS city_name,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include additional filtering conditions
GROUP BY
    country_name, region_state, city_name
ORDER BY
    unique_users DESC
"""
    },

    "analyze_metro_areas": {
        "description": "Analyzes user distribution by metropolitan areas. Good for questions like 'users by metro area', 'metropolitan market analysis', 'DMA breakdown', or 'metro area performance'.",
        "template": """
-- Analyzes user distribution across metropolitan/DMA areas
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(geo.country, 'Unknown_Country') AS country_name,
    COALESCE(geo.metro, 'Unknown_Metro') AS metro_area,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_name) AS unique_event_types,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2) AS avg_events_per_user
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND geo.metro IS NOT NULL
    -- LLM: Add user-specified WHERE clauses
GROUP BY
    country_name, metro_area
ORDER BY
    unique_users DESC
"""
    },

    "analyze_specific_country": {
        "description": "Deep dive analysis of a specific country including regions and cities. Good for questions like 'analyze users in [country]', 'breakdown of [country] traffic', '[country] market analysis', or 'regional distribution within [country]'.",
        "template": """
-- Provides detailed geographic breakdown for a specific country
-- LLM: Replace {country_name}, {start_date} and {end_date} with user-specified values
SELECT
    geo.country AS country_name,
    COALESCE(geo.region, 'Unknown_Region') AS region_state,
    COALESCE(geo.city, 'Unknown_City') AS city_name,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
                 THEN user_pseudo_id 
            END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS new_user_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND geo.country = '{country_name}'
    -- LLM: Apply additional filters as specified
GROUP BY
    country_name, region_state, city_name
ORDER BY
    unique_users DESC
"""
    },

    "compare_geographic_segments": {
        "description": "Compares user behavior across different geographic segments or regions. Good for questions like 'compare [region1] vs [region2]', 'geographic performance comparison', 'regional A/B analysis', or 'market comparison'.",
        "template": """
-- Compares user metrics across different geographic segments
-- LLM: Replace {start_date} and {end_date} with user-specified date range, modify WHERE clause for specific regions
SELECT
    CASE 
        WHEN geo.continent IN ('North America', 'South America') THEN 'Americas'
        WHEN geo.continent = 'Europe' THEN 'Europe'
        WHEN geo.continent = 'Asia' THEN 'Asia'
        WHEN geo.continent IN ('Africa', 'Oceania') THEN 'Other_Regions'
        ELSE 'Unknown_Region'
    END AS geographic_segment,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Modify grouping logic or WHERE clause based on user's comparison needs
GROUP BY
    geographic_segment
ORDER BY
    unique_users DESC
"""
    },
        "analyze_acquisition_campaigns": {
        "description": "Analyzes user acquisition by marketing campaign name. Good for questions like 'top performing campaigns', 'campaign acquisition analysis', 'which campaigns bring the most users?', or 'marketing campaign effectiveness'.",
        "template": """
-- Analyzes user acquisition performance by marketing campaign name
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(traffic_source.name, '(campaign_not_set)') AS campaign_name,
    COUNT(DISTINCT user_pseudo_id) AS acquired_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users_from_campaign,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
                 THEN user_pseudo_id 
            END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS new_user_percentage,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional filters as specified by user
GROUP BY
    campaign_name
ORDER BY
    acquired_users DESC
"""
    },
    "analyze_acquisition_mediums": {
        "description": "Analyzes user acquisition by traffic medium (organic, paid, email, social, etc.). Good for questions like 'users by traffic medium', 'organic vs paid performance', 'medium effectiveness analysis', or 'acquisition channel breakdown'.",
        "template": """
-- Analyzes user acquisition performance across different traffic mediums
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(traffic_source.medium, '(medium_not_set)') AS traffic_medium,
    COUNT(DISTINCT user_pseudo_id) AS acquired_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user,
    COUNTIF(event_name = 'page_view') AS page_views,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_page_views_per_user,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS user_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    traffic_medium
ORDER BY
    acquired_users DESC
"""
    },
    "analyze_acquisition_sources": {
        "description": "Analyzes user acquisition by traffic source (google, facebook, direct, etc.). Good for questions like 'top traffic sources', 'which sources bring users?', 'source performance analysis', or 'referral source breakdown'.",
        "template": """
-- Analyzes user acquisition performance by traffic source
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(traffic_source.source, '(source_not_set)') AS traffic_source_name,
    COUNT(DISTINCT user_pseudo_id) AS acquired_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS first_time_users,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply additional filters as needed
GROUP BY
    traffic_source_name
ORDER BY
    acquired_users DESC
"""
    },
    "analyze_source_medium_combinations": {
        "description": "Analyzes user acquisition by source/medium combinations. Good for questions like 'google organic vs google paid', 'source/medium performance', 'detailed attribution analysis', or 'channel combination effectiveness'.",
        "template": """
-- Analyzes user acquisition across source/medium combinations for detailed attribution
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(traffic_source.source, '(source_not_set)') AS traffic_source_name,
    COALESCE(traffic_source.medium, '(medium_not_set)') AS traffic_medium,
    CONCAT(
        COALESCE(traffic_source.source, '(source_not_set)'), 
        ' / ', 
        COALESCE(traffic_source.medium, '(medium_not_set)')
    ) AS source_medium_combination,
    COUNT(DISTINCT user_pseudo_id) AS acquired_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_events_per_user,
    COUNT(DISTINCT event_name) AS unique_event_types
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include additional filtering conditions
GROUP BY
    traffic_source_name, traffic_medium, source_medium_combination
ORDER BY
    acquired_users DESC
"""
    },
    "analyze_campaign_source_performance": {
        "description": "Analyzes specific campaigns across different sources. Good for questions like 'campaign performance by source', 'which sources work best for [campaign]?', 'campaign attribution analysis', or 'cross-source campaign effectiveness'.",
        "template": """
-- Analyzes how specific campaigns perform across different traffic sources
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(traffic_source.name, '(campaign_not_set)') AS campaign_name,
    COALESCE(traffic_source.source, '(source_not_set)') AS traffic_source_name,
    COALESCE(traffic_source.medium, '(medium_not_set)') AS traffic_medium,
    COUNT(DISTINCT user_pseudo_id) AS acquired_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users_acquired,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND traffic_source.name IS NOT NULL
    -- LLM: Add user-specified WHERE clauses
GROUP BY
    campaign_name, traffic_source_name, traffic_medium
ORDER BY
    acquired_users DESC
"""
    },
    "compare_paid_vs_organic": {
        "description": "Compares paid vs organic traffic performance. Good for questions like 'paid vs organic performance', 'organic vs paid users', 'acquisition cost effectiveness', or 'paid vs free traffic analysis'.",
        "template": """
-- Compares user acquisition and engagement between paid and organic traffic
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN REGEXP_CONTAINS(COALESCE(traffic_source.medium, ''), r'(.*cp.*|ppc|paid.*|cpm|cpc|cpa|retargeting)') THEN 'Paid_Traffic'
        WHEN COALESCE(traffic_source.medium, '') IN ('organic', 'referral', 'social', 'email') THEN 'Organic_Traffic'
        WHEN COALESCE(traffic_source.source, '') = '(direct)' OR traffic_source.medium IS NULL THEN 'Direct_Traffic'
        ELSE 'Other_Traffic'
    END AS traffic_category,
    COUNT(DISTINCT user_pseudo_id) AS acquired_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply user-specified filtering conditions
GROUP BY
    traffic_category
ORDER BY
    acquired_users DESC
"""
    },
    "analyze_specific_campaign": {
        "description": "Deep dive analysis of a specific marketing campaign. Good for questions like 'analyze [campaign_name] performance', 'how did [campaign] perform?', 'campaign deep dive for [campaign]', or 'detailed [campaign] metrics'.",
        "template": """
-- Provides detailed analysis of a specific marketing campaign's performance
-- LLM: Replace {campaign_name}, {start_date} and {end_date} with user-specified values
SELECT
    traffic_source.name AS campaign_name,
    COALESCE(traffic_source.source, '(source_not_set)') AS traffic_source_name,
    COALESCE(traffic_source.medium, '(medium_not_set)') AS traffic_medium,
    COUNT(DISTINCT user_pseudo_id) AS total_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )), 2
    ) AS avg_page_views_per_session,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND traffic_source.name = '{campaign_name}'
    -- LLM: Include additional filters as specified
GROUP BY
    campaign_name, traffic_source_name, traffic_medium
ORDER BY
    total_users DESC
"""
    },
    "identify_top_acquisition_channels": {
        "description": "Identifies the most effective user acquisition channels combining source, medium, and campaign data. Good for questions like 'best acquisition channels', 'top performing acquisition sources', 'most effective marketing channels', or 'channel ROI analysis'.",
        "template": """
-- Identifies top-performing acquisition channels with comprehensive metrics
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN traffic_source.name IS NOT NULL THEN CONCAT('Campaign: ', traffic_source.name)
        ELSE CONCAT(
            COALESCE(traffic_source.source, '(direct)'), 
            ' / ', 
            COALESCE(traffic_source.medium, '(none)')
        )
    END AS acquisition_channel,
    COALESCE(traffic_source.source, '(direct)') AS source_detail,
    COALESCE(traffic_source.medium, '(none)') AS medium_detail,
    COALESCE(traffic_source.name, '(no_campaign)') AS campaign_detail,
    COUNT(DISTINCT user_pseudo_id) AS total_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
                 THEN user_pseudo_id 
            END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS new_user_rate_percentage,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS channel_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional WHERE conditions based on user requirements
GROUP BY
    acquisition_channel, source_detail, medium_detail, campaign_detail
ORDER BY
    total_users DESC
"""
    },
        "analyze_utm_campaign_performance": {
        "description": "Analyzes UTM campaign performance including campaign ID and name tracking. Good for questions like 'UTM campaign performance', 'which UTM campaigns work best?', 'campaign tracking analysis', or 'manual campaign attribution'.",
        "template": """
-- Analyzes performance of manually tracked UTM campaigns
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(collected_traffic_source.manual_campaign_id, '(campaign_id_not_set)') AS utm_campaign_id,
    COALESCE(collected_traffic_source.manual_campaign_name, '(campaign_name_not_set)') AS utm_campaign_name,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (collected_traffic_source.manual_campaign_id IS NOT NULL 
         OR collected_traffic_source.manual_campaign_name IS NOT NULL)
    -- LLM: Add additional filters as specified by user
GROUP BY
    utm_campaign_id, utm_campaign_name
ORDER BY
    unique_users DESC
"""
    },

    "analyze_utm_source_medium": {
        "description": "Analyzes UTM source and medium combinations for detailed attribution. Good for questions like 'UTM source/medium performance', 'manual tracking attribution', 'which utm_source/utm_medium combinations work?', or 'detailed UTM analysis'.",
        "template": """
-- Analyzes performance across UTM source and medium combinations
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(collected_traffic_source.manual_source, '(utm_source_not_set)') AS utm_source,
    COALESCE(collected_traffic_source.manual_medium, '(utm_medium_not_set)') AS utm_medium,
    CONCAT(
        COALESCE(collected_traffic_source.manual_source, '(utm_source_not_set)'), 
        ' / ', 
        COALESCE(collected_traffic_source.manual_medium, '(utm_medium_not_set)')
    ) AS utm_source_medium,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT collected_traffic_source.manual_campaign_name) AS campaign_count,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )), 2
    ) AS avg_page_views_per_session
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (collected_traffic_source.manual_source IS NOT NULL 
         OR collected_traffic_source.manual_medium IS NOT NULL)
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    utm_source, utm_medium, utm_source_medium
ORDER BY
    unique_users DESC
"""
    },

    "analyze_utm_keywords_terms": {
        "description": "Analyzes UTM term/keyword performance for search campaigns. Good for questions like 'UTM keyword performance', 'which utm_terms work best?', 'search term analysis', or 'keyword attribution tracking'.",
        "template": """
-- Analyzes UTM term/keyword performance for search and keyword-based campaigns
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(collected_traffic_source.manual_term, '(utm_term_not_set)') AS utm_keyword_term,
    COALESCE(collected_traffic_source.manual_source, '(utm_source_not_set)') AS utm_source,
    COALESCE(collected_traffic_source.manual_medium, '(utm_medium_not_set)') AS utm_medium,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND collected_traffic_source.manual_term IS NOT NULL
    -- LLM: Apply additional filters as needed
GROUP BY
    utm_keyword_term, utm_source, utm_medium
ORDER BY
    unique_users DESC
"""
    },

    "analyze_utm_content_creative": {
        "description": "Analyzes UTM content and creative performance for A/B testing and creative optimization. Good for questions like 'UTM content performance', 'which utm_content works best?', 'creative A/B test results', or 'ad creative analysis'.",
        "template": """
-- Analyzes UTM content performance for creative testing and optimization
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(collected_traffic_source.manual_content, '(utm_content_not_set)') AS utm_content,
    COALESCE(collected_traffic_source.manual_campaign_name, '(campaign_not_set)') AS utm_campaign_name,
    COALESCE(collected_traffic_source.manual_creative_format, '(creative_format_not_set)') AS creative_format,
    COALESCE(collected_traffic_source.manual_marketing_tactic, '(marketing_tactic_not_set)') AS marketing_tactic,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_events_per_user,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (collected_traffic_source.manual_content IS NOT NULL 
         OR collected_traffic_source.manual_creative_format IS NOT NULL)
    -- LLM: Include additional filtering conditions
GROUP BY
    utm_content, utm_campaign_name, creative_format, marketing_tactic
ORDER BY
    unique_users DESC
"""
    },

    "analyze_google_click_ids": {
        "description": "Analyzes Google click IDs (gclid, dclid, srsltid) for Google Ads attribution. Good for questions like 'Google Ads click performance', 'gclid attribution analysis', 'Google campaign tracking', or 'paid Google traffic analysis'.",
        "template": """
-- Analyzes Google click identifiers for detailed Google Ads attribution
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN collected_traffic_source.gclid IS NOT NULL THEN 'Google_Ads_Search_Shopping'
        WHEN collected_traffic_source.dclid IS NOT NULL THEN 'Display_Video_360_CM360'
        WHEN collected_traffic_source.srsltid IS NOT NULL THEN 'Google_Merchant_Center'
        ELSE 'Other_Google_Traffic'
    END AS google_click_type,
    CASE 
        WHEN collected_traffic_source.gclid IS NOT NULL THEN 'Has_GCLID'
        WHEN collected_traffic_source.dclid IS NOT NULL THEN 'Has_DCLID'  
        WHEN collected_traffic_source.srsltid IS NOT NULL THEN 'Has_SRSLTID'
        ELSE 'No_Click_ID'
    END AS click_id_status,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (collected_traffic_source.gclid IS NOT NULL 
         OR collected_traffic_source.dclid IS NOT NULL 
         OR collected_traffic_source.srsltid IS NOT NULL)
    -- LLM: Add user-specified WHERE clauses
GROUP BY
    google_click_type, click_id_status
ORDER BY
    unique_users DESC
"""
    },

    "analyze_campaign_platform_tactics": {
        "description": "Analyzes campaign performance by platform and marketing tactics. Good for questions like 'performance by source platform', 'marketing tactic effectiveness', 'platform-specific campaign analysis', or 'tactic and format performance'.",
        "template": """
-- Analyzes campaign performance across platforms and marketing tactics
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(collected_traffic_source.manual_source_platform, '(source_platform_not_set)') AS source_platform,
    COALESCE(collected_traffic_source.manual_marketing_tactic, '(marketing_tactic_not_set)') AS marketing_tactic,
    COALESCE(collected_traffic_source.manual_creative_format, '(creative_format_not_set)') AS creative_format,
    COALESCE(collected_traffic_source.manual_campaign_name, '(campaign_not_set)') AS utm_campaign_name,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (collected_traffic_source.manual_source_platform IS NOT NULL 
         OR collected_traffic_source.manual_marketing_tactic IS NOT NULL
         OR collected_traffic_source.manual_creative_format IS NOT NULL)
    -- LLM: Apply additional filters as specified
GROUP BY
    source_platform, marketing_tactic, creative_format, utm_campaign_name
ORDER BY
    unique_users DESC
"""
    },

    "compare_utm_vs_auto_attribution": {
        "description": "Compares manual UTM tracking vs automatic attribution data. Good for questions like 'UTM vs auto attribution comparison', 'manual vs automatic tracking', 'attribution data quality analysis', or 'tracking implementation audit'.",
        "template": """
-- Compares manual UTM attribution with automatic traffic source attribution
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN collected_traffic_source.manual_source IS NOT NULL AND traffic_source.source IS NOT NULL THEN 'Both_Manual_and_Auto'
        WHEN collected_traffic_source.manual_source IS NOT NULL AND traffic_source.source IS NULL THEN 'Manual_UTM_Only'
        WHEN collected_traffic_source.manual_source IS NULL AND traffic_source.source IS NOT NULL THEN 'Auto_Attribution_Only'
        ELSE 'No_Attribution_Data'
    END AS attribution_data_status,
    COALESCE(collected_traffic_source.manual_source, traffic_source.source, '(no_source)') AS primary_source,
    COALESCE(collected_traffic_source.manual_medium, traffic_source.medium, '(no_medium)') AS primary_medium,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS user_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include user-specified filtering conditions
GROUP BY
    attribution_data_status, primary_source, primary_medium
ORDER BY
    unique_users DESC
"""
    },

    "analyze_specific_utm_campaign": {
        "description": "Deep dive analysis of a specific UTM campaign with all UTM parameters. Good for questions like 'analyze UTM campaign [campaign_name]', 'detailed UTM tracking for [campaign]', 'complete UTM parameter breakdown for [campaign]', or 'UTM campaign deep dive'.",
        "template": """
-- Provides comprehensive analysis of a specific UTM campaign with all parameters
-- LLM: Replace {utm_campaign_name}, {start_date} and {end_date} with user-specified values
SELECT
    collected_traffic_source.manual_campaign_name AS utm_campaign,
    COALESCE(collected_traffic_source.manual_campaign_id, '(no_campaign_id)') AS utm_campaign_id,
    COALESCE(collected_traffic_source.manual_source, '(no_utm_source)') AS utm_source,
    COALESCE(collected_traffic_source.manual_medium, '(no_utm_medium)') AS utm_medium,
    COALESCE(collected_traffic_source.manual_term, '(no_utm_term)') AS utm_term,
    COALESCE(collected_traffic_source.manual_content, '(no_utm_content)') AS utm_content,
    COALESCE(collected_traffic_source.manual_source_platform, '(no_source_platform)') AS source_platform,
    COALESCE(collected_traffic_source.manual_creative_format, '(no_creative_format)') AS creative_format,
    COALESCE(collected_traffic_source.manual_marketing_tactic, '(no_marketing_tactic)') AS marketing_tactic,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )), 2
    ) AS avg_page_views_per_session,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND collected_traffic_source.manual_campaign_name = '{utm_campaign_name}'
    -- LLM: Add additional WHERE clauses as needed
GROUP BY
    utm_campaign, utm_campaign_id, utm_source, utm_medium, utm_term, 
    utm_content, source_platform, creative_format, marketing_tactic
ORDER BY
    unique_users DESC
"""
    },
        "analyze_last_click_campaign_attribution": {
        "description": "Analyzes session performance by last-click manual campaign attribution. Good for questions like 'last-click campaign performance', 'which campaigns get credit for conversions?', 'session attribution by campaign', or 'last-touch campaign analysis'.",
        "template": """
-- Analyzes session performance using last-click manual campaign attribution
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(session_traffic_source_last_click.manual_campaign.campaign_id, '(campaign_id_not_set)') AS last_click_campaign_id,
    COALESCE(session_traffic_source_last_click.manual_campaign.campaign_name, '(campaign_name_not_set)') AS last_click_campaign_name,
    COALESCE(session_traffic_source_last_click.manual_campaign.source, '(source_not_set)') AS last_click_source,
    COALESCE(session_traffic_source_last_click.manual_campaign.medium, '(medium_not_set)') AS last_click_medium,
    COUNT(DISTINCT user_pseudo_id) AS attributed_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS attributed_sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 100.0 / SUM(COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        ))) OVER (), 2
    ) AS session_attribution_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (session_traffic_source_last_click.manual_campaign.campaign_id IS NOT NULL 
         OR session_traffic_source_last_click.manual_campaign.campaign_name IS NOT NULL)
    -- LLM: Add additional filters as specified by user
GROUP BY
    last_click_campaign_id, last_click_campaign_name, last_click_source, last_click_medium
ORDER BY
    attributed_sessions DESC
"""
    },

    "analyze_last_click_creative_performance": {
        "description": "Analyzes creative performance using last-click attribution including terms, content, and formats. Good for questions like 'last-click creative performance', 'which creatives get conversion credit?', 'creative attribution analysis', or 'last-touch creative optimization'.",
        "template": """
-- Analyzes creative performance using last-click attribution for optimization
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(session_traffic_source_last_click.manual_campaign.campaign_name, '(campaign_not_set)') AS last_click_campaign,
    COALESCE(session_traffic_source_last_click.manual_campaign.term, '(term_not_set)') AS last_click_keyword_term,
    COALESCE(session_traffic_source_last_click.manual_campaign.content, '(content_not_set)') AS last_click_content,
    COALESCE(session_traffic_source_last_click.manual_campaign.creative_format, '(creative_format_not_set)') AS creative_format,
    COALESCE(session_traffic_source_last_click.manual_campaign.marketing_tactic, '(marketing_tactic_not_set)') AS marketing_tactic,
    COUNT(DISTINCT user_pseudo_id) AS attributed_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS attributed_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNT(*) AS total_events,
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_events_per_attributed_user,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (session_traffic_source_last_click.manual_campaign.term IS NOT NULL 
         OR session_traffic_source_last_click.manual_campaign.content IS NOT NULL
         OR session_traffic_source_last_click.manual_campaign.creative_format IS NOT NULL)
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    last_click_campaign, last_click_keyword_term, last_click_content, 
    creative_format, marketing_tactic
ORDER BY
    attributed_sessions DESC
"""
    },

    "analyze_google_ads_last_click_performance": {
        "description": "Analyzes Google Ads campaign performance using last-click attribution. Good for questions like 'Google Ads last-click performance', 'which Google Ads campaigns get conversion credit?', 'Google Ads attribution analysis', or 'last-touch Google Ads optimization'.",
        "template": """
-- Analyzes Google Ads campaign performance using last-click attribution
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(session_traffic_source_last_click.google_ads_campaign.customer_id, '(customer_id_not_set)') AS google_ads_customer_id,
    COALESCE(session_traffic_source_last_click.google_ads_campaign.account_name, '(account_name_not_set)') AS google_ads_account,
    COALESCE(session_traffic_source_last_click.google_ads_campaign.campaign_id, '(campaign_id_not_set)') AS google_ads_campaign_id,
    COALESCE(session_traffic_source_last_click.google_ads_campaign.campaign_name, '(campaign_name_not_set)') AS google_ads_campaign_name,
    COUNT(DISTINCT user_pseudo_id) AS attributed_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS attributed_sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS attributed_new_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )), 2
    ) AS avg_page_views_per_session,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (session_traffic_source_last_click.google_ads_campaign.campaign_id IS NOT NULL 
         OR session_traffic_source_last_click.google_ads_campaign.campaign_name IS NOT NULL)
    -- LLM: Apply additional filters as needed
GROUP BY
    google_ads_customer_id, google_ads_account, google_ads_campaign_id, google_ads_campaign_name
ORDER BY
    attributed_sessions DESC
"""
    },

    "analyze_google_ads_adgroup_performance": {
        "description": "Analyzes Google Ads ad group performance using last-click attribution. Good for questions like 'Google Ads ad group performance', 'which ad groups get conversion credit?', 'ad group last-click analysis', or 'Google Ads ad group optimization'.",
        "template": """
-- Analyzes Google Ads ad group performance using last-click attribution
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(session_traffic_source_last_click.google_ads_campaign.account_name, '(account_not_set)') AS google_ads_account,
    COALESCE(session_traffic_source_last_click.google_ads_campaign.campaign_name, '(campaign_not_set)') AS google_ads_campaign,
    COALESCE(session_traffic_source_last_click.google_ads_campaign.ad_group_id, '(adgroup_id_not_set)') AS ad_group_id,
    COALESCE(session_traffic_source_last_click.google_ads_campaign.ad_group_name, '(adgroup_name_not_set)') AS ad_group_name,
    COUNT(DISTINCT user_pseudo_id) AS attributed_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS attributed_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(*) AS total_events,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_attributed_user,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 100.0 / SUM(COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        ))) OVER (), 2
    ) AS session_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (session_traffic_source_last_click.google_ads_campaign.ad_group_id IS NOT NULL 
         OR session_traffic_source_last_click.google_ads_campaign.ad_group_name IS NOT NULL)
    -- LLM: Include additional filtering conditions
GROUP BY
    google_ads_account, google_ads_campaign, ad_group_id, ad_group_name
ORDER BY
    attributed_sessions DESC
"""
    },

        "compare_manual_vs_google_ads_attribution": {
        "description": "Compares manual campaign attribution vs Google Ads attribution for the same sessions. Good for questions like 'manual vs Google Ads attribution comparison', 'attribution data quality', 'tracking overlap analysis', or 'attribution method comparison'.",
        "template": """
-- Compares manual UTM attribution vs Google Ads attribution using last-click data
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN session_traffic_source_last_click.manual_campaign.campaign_name IS NOT NULL 
             AND session_traffic_source_last_click.google_ads_campaign.campaign_name IS NOT NULL 
             THEN 'Both_Manual_and_Google_Ads'
        WHEN session_traffic_source_last_click.manual_campaign.campaign_name IS NOT NULL 
             AND session_traffic_source_last_click.google_ads_campaign.campaign_name IS NULL 
             THEN 'Manual_Attribution_Only'
        WHEN session_traffic_source_last_click.manual_campaign.campaign_name IS NULL 
             AND session_traffic_source_last_click.google_ads_campaign.campaign_name IS NOT NULL 
             THEN 'Google_Ads_Attribution_Only'
        ELSE 'No_Last_Click_Attribution'
    END AS attribution_data_coverage,
    COALESCE(session_traffic_source_last_click.manual_campaign.campaign_name, 
             session_traffic_source_last_click.google_ads_campaign.campaign_name, 
             '(no_campaign_attribution)') AS primary_campaign_name,
    COALESCE(session_traffic_source_last_click.manual_campaign.source, '(no_manual_source)') AS manual_source,
    COALESCE(session_traffic_source_last_click.google_ads_campaign.account_name, '(no_google_ads_account)') AS google_ads_account,
    COUNT(DISTINCT user_pseudo_id) AS attributed_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS attributed_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNT(*) AS total_events,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 100.0 / SUM(COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        ))) OVER (), 2
    ) AS session_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add user-specified WHERE clauses
GROUP BY
    attribution_data_coverage, primary_campaign_name, manual_source, google_ads_account
ORDER BY
    attributed_sessions DESC
"""
    },

    "analyze_last_click_platform_performance": {
        "description": "Analyzes campaign performance by source platform using last-click attribution. Good for questions like 'platform performance by last-click', 'which platforms get conversion credit?', 'platform attribution analysis', or 'cross-platform last-click performance'.",
        "template": """
-- Analyzes source platform performance using last-click attribution
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(session_traffic_source_last_click.manual_campaign.source_platform, '(source_platform_not_set)') AS last_click_source_platform,
    COALESCE(session_traffic_source_last_click.manual_campaign.marketing_tactic, '(marketing_tactic_not_set)') AS marketing_tactic,
    COALESCE(session_traffic_source_last_click.manual_campaign.creative_format, '(creative_format_not_set)') AS creative_format,
    COUNT(DISTINCT user_pseudo_id) AS attributed_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS attributed_sessions,
    COUNT(DISTINCT session_traffic_source_last_click.manual_campaign.campaign_name) AS unique_campaigns_attributed,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS attributed_new_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_attributed_user,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND session_traffic_source_last_click.manual_campaign.source_platform IS NOT NULL
    -- LLM: Apply additional filters as specified
GROUP BY
    last_click_source_platform, marketing_tactic, creative_format
ORDER BY
    attributed_sessions DESC
"""
    },

    "analyze_specific_last_click_campaign": {
        "description": "Deep dive analysis of a specific campaign using last-click attribution. Good for questions like 'last-click analysis for [campaign_name]', 'how did [campaign] perform in last-click attribution?', 'campaign last-touch deep dive', or 'detailed last-click campaign metrics'.",
        "template": """
-- Provides comprehensive last-click attribution analysis for a specific campaign
-- LLM: Replace {campaign_name}, {start_date} and {end_date} with user-specified values
SELECT
    COALESCE(session_traffic_source_last_click.manual_campaign.campaign_name, 
             session_traffic_source_last_click.google_ads_campaign.campaign_name) AS campaign_name,
    COALESCE(session_traffic_source_last_click.manual_campaign.campaign_id, 
             session_traffic_source_last_click.google_ads_campaign.campaign_id, 
             '(no_campaign_id)') AS campaign_id,
    COALESCE(session_traffic_source_last_click.manual_campaign.source, 'Google_Ads') AS traffic_source,
    COALESCE(session_traffic_source_last_click.manual_campaign.medium, 'cpc') AS traffic_medium,
    COALESCE(session_traffic_source_last_click.manual_campaign.term, '(no_keywords)') AS keywords_terms,
    COALESCE(session_traffic_source_last_click.manual_campaign.content, 
             session_traffic_source_last_click.google_ads_campaign.ad_group_name, 
             '(no_content_adgroup)') AS content_or_adgroup,
    COUNT(DISTINCT user_pseudo_id) AS attributed_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS attributed_new_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS attributed_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNT(*) AS total_events,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )), 2
    ) AS avg_page_views_per_session,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND (session_traffic_source_last_click.manual_campaign.campaign_name = '{campaign_name}' 
         OR session_traffic_source_last_click.google_ads_campaign.campaign_name = '{campaign_name}')
    -- LLM: Include additional WHERE conditions as needed
GROUP BY
    campaign_name, campaign_id, traffic_source, traffic_medium, 
    keywords_terms, content_or_adgroup
ORDER BY
    attributed_sessions DESC
"""
    },
        "analyze_performance_by_time_period": {
        "description": "Analyzes performance trends over different time periods (daily, weekly, monthly). Good for questions like 'daily performance trends', 'weekly user growth', 'monthly analysis', or 'performance over time'.",
        "template": """
-- Analyzes performance metrics across different time periods for trend analysis
-- LLM: Replace {time_granularity}, {start_date} and {end_date} with user-specified values
-- LLM: {time_granularity} should be 'daily', 'weekly', or 'monthly'
SELECT
    CASE 
        WHEN '{time_granularity}' = 'daily' THEN PARSE_DATE('%Y%m%d', event_date)
        WHEN '{time_granularity}' = 'weekly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(SUNDAY))
        WHEN '{time_granularity}' = 'monthly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)
        ELSE PARSE_DATE('%Y%m%d', event_date)
    END AS time_period,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional filters as specified by user
GROUP BY
    time_period
ORDER BY
    time_period
"""
    },

    "analyze_day_of_week_patterns": {
        "description": "Analyzes user behavior patterns by day of the week. Good for questions like 'which days perform best?', 'day of week analysis', 'weekday vs weekend performance', or 'daily usage patterns'.",
        "template": """
-- Analyzes user behavior and engagement patterns across days of the week
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', event_date)) AS day_of_week_name,
    FORMAT_DATE('%w', PARSE_DATE('%Y%m%d', event_date)) AS day_of_week_number,
    CASE 
        WHEN FORMAT_DATE('%w', PARSE_DATE('%Y%m%d', event_date)) IN ('0', '6') THEN 'Weekend'
        ELSE 'Weekday'
    END AS weekday_weekend,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    day_of_week_name, day_of_week_number, weekday_weekend
ORDER BY
    day_of_week_number
"""
    },

    "analyze_hourly_usage_patterns": {
        "description": "Analyzes user activity patterns by hour of day. Good for questions like 'what time are users most active?', 'hourly usage patterns', 'peak activity hours', or 'when should we post content?'.",
        "template": """
-- Analyzes user activity and engagement patterns across hours of the day
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp)) AS hour_of_day,
    CASE 
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp)) BETWEEN 6 AND 11 THEN 'Morning (6-11)'
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp)) BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp)) BETWEEN 18 AND 22 THEN 'Evening (18-22)'
        ELSE 'Night (23-5)'
    END AS time_period_category,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2
    ) AS event_share_percentage,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply additional filters as needed
GROUP BY
    hour_of_day, time_period_category
ORDER BY
    hour_of_day
"""
    },

    "compare_time_periods": {
        "description": "Compares performance between two specific time periods. Good for questions like 'this month vs last month', 'compare periods', 'period over period analysis', or 'year over year comparison'.",
        "template": """
-- Compares key metrics between two time periods for performance analysis
-- LLM: Replace {period1_start}, {period1_end}, {period2_start}, {period2_end} with user-specified date ranges
-- LLM: Also replace {period1_name} and {period2_name} with descriptive names like 'This Month', 'Last Month'
WITH period_data AS (
    SELECT
        CASE 
            WHEN _table_suffix BETWEEN '{period1_start}' AND '{period1_end}' THEN '{period1_name}'
            WHEN _table_suffix BETWEEN '{period2_start}' AND '{period2_end}' THEN '{period2_name}'
        END AS time_period,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS session_engaged,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS session_number,
        event_name
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        (_table_suffix BETWEEN '{period1_start}' AND '{period1_end}' 
         OR _table_suffix BETWEEN '{period2_start}' AND '{period2_end}')
        -- LLM: Add additional WHERE conditions as specified
)
SELECT
    time_period,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN session_number = 1 THEN user_pseudo_id END
    ) AS new_users,
    COUNT(DISTINCT 
        CASE WHEN session_engaged = '1' 
             THEN CONCAT(user_pseudo_id, CAST(session_id AS STRING)) 
        END
    ) AS engaged_sessions,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN session_engaged = '1' 
                     THEN CONCAT(user_pseudo_id, CAST(session_id AS STRING)) 
                END
            ),
            COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING)))
        ) * 100, 2
    ) AS engagement_rate_percentage,
    ROUND(
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) * 1.0 / 
        COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user
FROM
    period_data
WHERE
    time_period IS NOT NULL
GROUP BY
    time_period
ORDER BY
    time_period
"""
    },

    "analyze_user_acquisition_timing": {
        "description": "Analyzes when users were first acquired over time. Good for questions like 'user acquisition trends', 'when did users first visit?', 'acquisition cohort analysis', or 'first-touch timing analysis'.",
        "template": """
-- Analyzes user acquisition timing based on first touch timestamps
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS first_touch_date,
    FORMAT_DATE('%A', DATE(TIMESTAMP_MICROS(user_first_touch_timestamp))) AS first_touch_day_of_week,
    DATE_TRUNC(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)), WEEK(SUNDAY)) AS first_touch_week,
    DATE_TRUNC(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)), MONTH) AS first_touch_month,
    COUNT(DISTINCT user_pseudo_id) AS users_acquired,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions_from_cohort,
    COUNTIF(event_name = 'page_view') AS page_views_from_cohort,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions_from_cohort,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_acquired_user
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND user_first_touch_timestamp IS NOT NULL
    -- LLM: Include additional filtering conditions
GROUP BY
    first_touch_date, first_touch_day_of_week, first_touch_week, first_touch_month
ORDER BY
    first_touch_date DESC
"""
    },

       "analyze_seasonal_trends": {
        "description": "Analyzes performance across months to identify seasonal patterns. Good for questions like 'seasonal trends', 'monthly patterns', 'which months perform best?', or 'seasonal business analysis'.",
        "template": """
-- Analyzes seasonal trends and monthly patterns in user behavior
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    FORMAT_DATE('%B', PARSE_DATE('%Y%m%d', event_date)) AS month_name,
    FORMAT_DATE('%m', PARSE_DATE('%Y%m%d', event_date)) AS month_number,
    FORMAT_DATE('%Y', PARSE_DATE('%Y%m%d', event_date)) AS year,
    CASE 
        WHEN FORMAT_DATE('%m', PARSE_DATE('%Y%m%d', event_date)) IN ('12', '01', '02') THEN 'Winter'
        WHEN FORMAT_DATE('%m', PARSE_DATE('%Y%m%d', event_date)) IN ('03', '04', '05') THEN 'Spring'
        WHEN FORMAT_DATE('%m', PARSE_DATE('%Y%m%d', event_date)) IN ('06', '07', '08') THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS user_share_percentage,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply additional filters as specified
GROUP BY
    month_name, month_number, year, season
ORDER BY
    year, month_number
"""
    },

    "analyze_event_timing_delays": {
        "description": "Analyzes timing differences between event logging and server processing. Good for questions like 'data collection delays', 'event processing timing', 'data quality timing analysis', or 'real-time vs batch data'.",
        "template": """
-- Analyzes timing differences between client event logging and server processing
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    CASE 
        WHEN event_server_timestamp_offset IS NULL THEN 'No_Offset_Data'
        WHEN ABS(event_server_timestamp_offset) < 1000000 THEN 'Near_Realtime (< 1sec)'
        WHEN ABS(event_server_timestamp_offset) < 60000000 THEN 'Short_Delay (1-60sec)'
        WHEN ABS(event_server_timestamp_offset) < 3600000000 THEN 'Medium_Delay (1-60min)'
        ELSE 'Long_Delay (> 1hour)'
    END AS processing_delay_category,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    AVG(ABS(event_server_timestamp_offset) / 1000000) AS avg_delay_seconds,
    MIN(ABS(event_server_timestamp_offset) / 1000000) AS min_delay_seconds,
    MAX(ABS(event_server_timestamp_offset) / 1000000) AS max_delay_seconds,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY PARSE_DATE('%Y%m%d', event_date)), 2
    ) AS daily_event_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include additional WHERE conditions as needed
GROUP BY
    event_date, processing_delay_category
ORDER BY
    event_date DESC, event_count DESC
"""
    },
        "analyze_top_page_performance": {
        "description": "Analyzes top-performing pages by views, users, and engagement. Good for questions like 'which pages are most popular?', 'top page performance', 'best performing content', or 'page popularity analysis'.",
        "template": """
-- Analyzes top-performing pages with comprehensive engagement metrics
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' AND event_name = 'page_view') AS page_url,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title' AND event_name = 'page_view') AS page_title,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS unique_sessions,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_page_views_per_user,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions_with_page,
    ROUND(
        COUNTIF(event_name = 'page_view') * 100.0 / SUM(COUNTIF(event_name = 'page_view')) OVER (), 2
    ) AS page_view_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'page_view'
    -- LLM: Add additional filters as specified by user
GROUP BY
    page_url, page_title
HAVING
    page_url IS NOT NULL
ORDER BY
    page_views DESC
LIMIT 50
"""
    },

    "analyze_landing_page_performance": {
        "description": "Analyzes landing page effectiveness and conversion rates. Good for questions like 'best landing pages', 'landing page performance', 'which entry points work best?', or 'landing page optimization'.",
        "template": """
-- Analyzes landing page performance including user acquisition and engagement
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' AND event_name = 'page_view') AS landing_page_url,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title' AND event_name = 'page_view') AS landing_page_title,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS total_entrances,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
             THEN user_pseudo_id 
        END
    ) AS unique_users_entering,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
             AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions_from_landing,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
             AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users_from_landing,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
                     AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            )
        ) * 100, 2
    ) AS landing_page_engagement_rate_percentage,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
                 THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
            END
        ) * 100.0 / SUM(COUNT(DISTINCT 
            CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
                 THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
            END
        )) OVER (), 2
    ) AS entrance_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'page_view'
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1
    -- LLM: Include additional filtering conditions
GROUP BY
    landing_page_url, landing_page_title
HAVING
    landing_page_url IS NOT NULL
ORDER BY
    total_entrances DESC
"""
    },

    "analyze_exit_page_patterns": {
        "description": "Analyzes where users commonly exit the site to identify potential issues. Good for questions like 'where do users exit?', 'exit page analysis', 'content optimization opportunities', or 'user journey drop-offs'.",
        "template": """
-- Analyzes exit patterns to identify potential user experience issues
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH session_pages AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_url,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
        event_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') 
            ORDER BY event_timestamp DESC
        ) AS page_rank_desc
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name = 'page_view'
        -- LLM: Apply additional filters as needed
)
SELECT
    page_url AS exit_page_url,
    page_title AS exit_page_title,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) AS sessions_exiting_from_page,
    COUNT(DISTINCT user_pseudo_id) AS users_exiting_from_page,
    ROUND(
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) * 100.0 / 
        SUM(COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING)))) OVER (), 2
    ) AS exit_share_percentage
FROM
    session_pages
WHERE
    page_rank_desc = 1  -- Last page in session (exit page)
    AND page_url IS NOT NULL
GROUP BY
    exit_page_url, exit_page_title
ORDER BY
    sessions_exiting_from_page DESC
"""
    },

    "analyze_content_section_performance": {
        "description": "Analyzes performance by website sections or page categories. Good for questions like 'how do different site sections perform?', 'content category analysis', 'section performance comparison', or 'content strategy insights'.",
        "template": """
-- Analyzes performance across different website sections or content categories
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'/blog/|/articles/|/news/') THEN 'Blog_Content'
        WHEN REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'/product/|/products/|/shop/|/store/') THEN 'Product_Pages'
        WHEN REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'/about/|/contact/|/team/') THEN 'Company_Info'
        WHEN REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'/support/|/help/|/faq/|/docs/') THEN 'Support_Help'
        WHEN REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'^[^/]*//[^/]+/?$') THEN 'Homepage'
        ELSE 'Other_Pages'
    END AS content_section,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS unique_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS entrances_to_section,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions_in_section,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_page_views_per_user,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS section_engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'page_view'
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    content_section
ORDER BY
    page_views DESC
"""
    },

    "analyze_user_page_journey": {
        "description": "Analyzes common user page navigation patterns and paths. Good for questions like 'common user paths', 'page flow analysis', 'user journey patterns', or 'navigation behavior'.",
        "template": """
-- Analyzes common page-to-page navigation patterns in user journeys
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH page_sequences AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS current_page,
        LAG((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')) OVER (
            PARTITION BY user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') 
            ORDER BY event_timestamp
        ) AS previous_page,
        event_timestamp
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name = 'page_view'
        -- LLM: Apply additional filters as specified
)
SELECT
    COALESCE(previous_page, '(session_start)') AS from_page,
    current_page AS to_page,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) AS sessions_with_transition,
    COUNT(*) AS total_transitions,
    COUNT(DISTINCT user_pseudo_id) AS unique_users_making_transition,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2
    ) AS transition_share_percentage
FROM
    page_sequences
WHERE
    current_page IS NOT NULL
GROUP BY
    from_page, to_page
HAVING
    total_transitions >= 10  -- Filter for meaningful patterns
ORDER BY
    total_transitions DESC
LIMIT 50
"""
    },
    "analyze_hostname_performance": {
        "description": "Analyzes performance across different hostnames/domains. Good for questions like 'subdomain performance', 'domain comparison', 'hostname analysis', or 'multi-domain site performance'.",
        "template": """
-- Analyzes performance across different hostnames and domains
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(device.web_info.hostname, '(hostname_not_set)') AS hostname,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS unique_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS entrances,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )), 2
    ) AS avg_pages_per_session,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage,
    ROUND(
        COUNTIF(event_name = 'page_view') * 100.0 / SUM(COUNTIF(event_name = 'page_view')) OVER (), 2
    ) AS page_view_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND platform = 'WEB'
    -- LLM: Include additional filtering conditions
GROUP BY
    hostname
ORDER BY
    page_views DESC
"""
    },

    "analyze_specific_page_performance": {
        "description": "Deep dive analysis of a specific page's performance. Good for questions like 'how does [page_url] perform?', 'analyze specific page', 'page performance deep dive', or 'detailed page metrics'.",
        "template": """
-- Provides comprehensive performance analysis for a specific page
-- LLM: Replace {page_url}, {start_date} and {end_date} with user-specified values
SELECT
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' AND event_name = 'page_view') AS page_url,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title' AND event_name = 'page_view') AS page_title,
    COUNTIF(event_name = 'page_view') AS total_page_views,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS unique_sessions_with_page,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS entrances_to_page,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users_viewing_page,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions_with_page,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_page_views_per_user,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
                     AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            )
        ) * 100, 2
    ) AS landing_page_engagement_rate_percentage,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) * 100.0 / COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS overall_engagement_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'page_view'
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%{page_url}%'
    -- LLM: Add additional WHERE clauses as needed
GROUP BY
    page_url, page_title
HAVING
    page_url IS NOT NULL
ORDER BY
    total_page_views DESC
"""
    },
        "analyze_event_performance": {
        "description": "Analyzes performance of different event types. Good for questions like 'which events are most common?', 'event performance analysis', 'user interaction patterns', or 'event tracking overview'.",
        "template": """
-- Analyzes performance and frequency of different event types
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    event_name,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_pseudo_id) AS unique_users_triggering_event,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS sessions_with_event,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2) AS avg_events_per_user,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS event_share_percentage,
    ROUND(AVG(event_value_in_usd), 2) AS avg_event_value_usd,
    SUM(event_value_in_usd) AS total_event_value_usd,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users_triggering_event
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional filters as specified by user
GROUP BY
    event_name
ORDER BY
    total_events DESC
"""
    },

    "analyze_specific_event_details": {
        "description": "Deep dive analysis of a specific event type with parameter breakdown. Good for questions like 'analyze [event_name] event', 'detailed [event_name] tracking', 'event parameter analysis', or 'custom event performance'.",
        "template": """
-- Provides detailed analysis of a specific event including parameter breakdown
-- LLM: Replace {event_name}, {start_date} and {end_date} with user-specified values
WITH event_params_expanded AS (
    SELECT
        event_name,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        event_timestamp,
        event_value_in_usd,
        param.key AS parameter_key,
        param.value.string_value AS param_string_value,
        param.value.int_value AS param_int_value,
        param.value.float_value AS param_float_value,
        param.value.double_value AS param_double_value
    FROM
        `{project_id}.{dataset_id}.events_*`,
        UNNEST(event_params) AS param
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name = '{event_name}'
        -- LLM: Add additional WHERE conditions as needed
)
SELECT
    event_name,
    parameter_key,
    COUNT(*) AS event_occurrences,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) AS unique_sessions,
    COUNT(DISTINCT param_string_value) AS unique_string_values,
    COUNT(DISTINCT param_int_value) AS unique_int_values,
    ROUND(AVG(param_int_value), 2) AS avg_int_value,
    ROUND(AVG(param_float_value), 2) AS avg_float_value,
    ROUND(AVG(param_double_value), 2) AS avg_double_value,
    COUNT(DISTINCT 
        CASE WHEN param_string_value IS NOT NULL THEN param_string_value END
    ) AS non_null_string_values,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS parameter_usage_percentage
FROM
    event_params_expanded
GROUP BY
    event_name, parameter_key
ORDER BY
    event_occurrences DESC
"""
    },

    "analyze_event_conversion_funnel": {
        "description": "Analyzes event sequence patterns to identify conversion funnels. Good for questions like 'event conversion funnel', 'user journey through events', 'conversion path analysis', or 'event sequence patterns'.",
        "template": """
-- Analyzes event sequences to understand user conversion patterns
-- LLM: Replace {start_date} and {end_date} with user-specified date range
-- LLM: Modify the funnel_events list based on user's conversion funnel
WITH user_events AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        event_name,
        event_timestamp,
        event_value_in_usd
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name IN ('page_view', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase')  -- LLM: Adjust these events based on user's funnel
        -- LLM: Include additional filtering conditions
),
funnel_analysis AS (
    SELECT
        user_pseudo_id,
        session_id,
        MAX(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) AS step_1_page_view,
        MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) AS step_2_view_item,
        MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS step_3_add_to_cart,
        MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS step_4_begin_checkout,
        MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS step_5_purchase,
        SUM(event_value_in_usd) AS total_session_value
    FROM
        user_events
    GROUP BY
        user_pseudo_id, session_id
)
SELECT
    'Step 1: Page Views' AS funnel_step,
    SUM(step_1_page_view) AS users_completing_step,
    ROUND(SUM(step_1_page_view) * 100.0 / COUNT(*), 2) AS completion_rate_percentage,
    NULL AS conversion_rate_from_previous_step
UNION ALL
SELECT
    'Step 2: View Item' AS funnel_step,
    SUM(step_2_view_item) AS users_completing_step,
    ROUND(SUM(step_2_view_item) * 100.0 / COUNT(*), 2) AS completion_rate_percentage,
    ROUND(SUM(step_2_view_item) * 100.0 / SUM(step_1_page_view), 2) AS conversion_rate_from_previous_step
UNION ALL
SELECT
    'Step 3: Add to Cart' AS funnel_step,
    SUM(step_3_add_to_cart) AS users_completing_step,
    ROUND(SUM(step_3_add_to_cart) * 100.0 / COUNT(*), 2) AS completion_rate_percentage,
    ROUND(SUM(step_3_add_to_cart) * 100.0 / SUM(step_2_view_item), 2) AS conversion_rate_from_previous_step
UNION ALL
SELECT
    'Step 4: Begin Checkout' AS funnel_step,
    SUM(step_4_begin_checkout) AS users_completing_step,
    ROUND(SUM(step_4_begin_checkout) * 100.0 / COUNT(*), 2) AS completion_rate_percentage,
    ROUND(SUM(step_4_begin_checkout) * 100.0 / SUM(step_3_add_to_cart), 2) AS conversion_rate_from_previous_step
UNION ALL
SELECT
    'Step 5: Purchase' AS funnel_step,
    SUM(step_5_purchase) AS users_completing_step,
    ROUND(SUM(step_5_purchase) * 100.0 / COUNT(*), 2) AS completion_rate_percentage,
    ROUND(SUM(step_5_purchase) * 100.0 / SUM(step_4_begin_checkout), 2) AS conversion_rate_from_previous_step
FROM
    funnel_analysis
ORDER BY
    funnel_step
"""
    },

    "analyze_event_timing_patterns": {
        "description": "Analyzes when events occur throughout the day and week. Good for questions like 'when do users interact most?', 'event timing patterns', 'optimal engagement times', or 'user activity patterns by event'.",
        "template": """
-- Analyzes temporal patterns of event occurrences for optimization insights
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    event_name,
    EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp)) AS hour_of_day,
    FORMAT_DATE('%A', DATE(TIMESTAMP_MICROS(event_timestamp))) AS day_of_week,
    CASE 
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp)) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp)) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp)) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS time_period,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    ROUND(AVG(event_value_in_usd), 2) AS avg_event_value,
    SUM(event_value_in_usd) AS total_event_value,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY event_name), 2) AS time_share_percentage_for_event
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name IN ('page_view', 'purchase', 'add_to_cart', 'view_item')  -- LLM: Adjust events based on user interest
    -- LLM: Apply additional filters as needed
GROUP BY
    event_name, hour_of_day, day_of_week, time_period
ORDER BY
    event_name, event_count DESC
"""
    },

    "analyze_event_parameter_values": {
        "description": "Analyzes specific parameter values for events. Good for questions like 'most common [parameter] values', 'parameter value analysis', 'event parameter breakdown', or 'custom parameter insights'.",
        "template": """
-- Analyzes the distribution and performance of specific event parameter values
-- LLM: Replace {event_name}, {parameter_key}, {start_date} and {end_date} with user-specified values
SELECT
    event_name,
    '{parameter_key}' AS parameter_name,
    COALESCE(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = '{parameter_key}'),
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = '{parameter_key}') AS STRING),
        CAST((SELECT value.float_value FROM UNNEST(event_params) WHERE key = '{parameter_key}') AS STRING),
        CAST((SELECT value.double_value FROM UNNEST(event_params) WHERE key = '{parameter_key}') AS STRING),
        '(parameter_not_set)'
    ) AS parameter_value,
    COUNT(*) AS event_occurrences,
    COUNT(DISTINCT user_pseudo_id) AS unique_users_with_value,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS sessions_with_value,
    ROUND(AVG(event_value_in_usd), 2) AS avg_event_value_usd,
    SUM(event_value_in_usd) AS total_value_usd,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS parameter_value_share_percentage,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users_with_parameter_value
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = '{event_name}'
    AND EXISTS (SELECT 1 FROM UNNEST(event_params) WHERE key = '{parameter_key}')
    -- LLM: Include additional WHERE conditions as specified
GROUP BY
    event_name, parameter_value
ORDER BY
    event_occurrences DESC
"""
    },

    "analyze_high_value_events": {
        "description": "Analyzes events with monetary value to identify revenue drivers. Good for questions like 'highest value events', 'revenue-generating events', 'event value analysis', or 'monetization insights'.",
        "template": """
-- Analyzes events that generate monetary value for business insights
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    event_name,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_pseudo_id) AS unique_users_generating_value,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS sessions_generating_value,
    ROUND(SUM(event_value_in_usd), 2) AS total_event_value_usd,
    ROUND(AVG(event_value_in_usd), 2) AS avg_event_value_usd,
    ROUND(MIN(event_value_in_usd), 2) AS min_event_value_usd,
    ROUND(MAX(event_value_in_usd), 2) AS max_event_value_usd,
    ROUND(SUM(event_value_in_usd) * 100.0 / SUM(SUM(event_value_in_usd)) OVER (), 2) AS value_share_percentage,
    ROUND(SUM(event_value_in_usd) / COUNT(DISTINCT user_pseudo_id), 2) AS avg_value_per_user,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users_generating_value
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_value_in_usd IS NOT NULL
    AND event_value_in_usd > 0
    -- LLM: Include additional filtering conditions as specified
GROUP BY
    event_name
ORDER BY
    total_event_value_usd DESC
"""
    },

    "analyze_custom_event_tracking": {
        "description": "Analyzes custom events and their implementation quality. Good for questions like 'custom event performance', 'event tracking audit', 'custom event analysis', or 'implementation quality check'.",
        "template": """
-- Analyzes custom events to assess tracking implementation and performance
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH event_classification AS (
    SELECT
        event_name,
        CASE 
            WHEN event_name IN ('page_view', 'session_start', 'first_visit', 'user_engagement') THEN 'Auto_Tracked'
            WHEN event_name IN ('purchase', 'add_to_cart', 'view_item', 'begin_checkout', 'add_payment_info', 'add_shipping_info') THEN 'Enhanced_Ecommerce'
            WHEN event_name IN ('login', 'sign_up', 'generate_lead', 'view_search_results', 'select_content') THEN 'Recommended_Events'
            ELSE 'Custom_Events'
        END AS event_category,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        event_timestamp,
        event_value_in_usd,
        ARRAY_LENGTH(event_params) AS parameter_count
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Apply additional filters as needed
)
SELECT
    event_category,
    event_name,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) AS unique_sessions,
    ROUND(AVG(parameter_count), 1) AS avg_parameters_per_event,
    COUNT(DISTINCT 
        CASE WHEN event_value_in_usd IS NOT NULL AND event_value_in_usd > 0 
             THEN CONCAT(user_pseudo_id, CAST(session_id AS STRING), event_name) 
        END
    ) AS events_with_value,
    ROUND(SUM(event_value_in_usd), 2) AS total_event_value_usd,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS event_frequency_percentage,
    DATE_DIFF(
        DATE(MAX(TIMESTAMP_MICROS(event_timestamp))), 
        DATE(MIN(TIMESTAMP_MICROS(event_timestamp))), 
        DAY
    ) AS days_active
FROM
    event_classification
GROUP BY
    event_category, event_name
ORDER BY
    event_category, total_events DESC
"""
    },

    "analyze_event_data_quality": {
        "description": "Analyzes event data quality including timing issues and missing parameters. Good for questions like 'event data quality', 'tracking implementation issues', 'data collection health', or 'event timing analysis'.",
        "template": """
-- Analyzes event data quality to identify potential tracking or collection issues
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    event_name,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    
    -- Timing quality checks
    COUNT(CASE WHEN event_server_timestamp_offset IS NULL THEN 1 END) AS events_missing_server_offset,
    COUNT(CASE WHEN ABS(event_server_timestamp_offset) > 3600000000 THEN 1 END) AS events_with_long_delay,
    ROUND(AVG(ABS(event_server_timestamp_offset) / 1000000), 2) AS avg_collection_delay_seconds,
    
    -- Parameter quality checks
    COUNT(CASE WHEN ARRAY_LENGTH(event_params) = 0 THEN 1 END) AS events_without_parameters,
    ROUND(AVG(ARRAY_LENGTH(event_params)), 1) AS avg_parameters_per_event,
    
    -- Value quality checks
    COUNT(CASE WHEN event_value_in_usd IS NOT NULL THEN 1 END) AS events_with_value,
    COUNT(CASE WHEN event_value_in_usd IS NOT NULL AND event_value_in_usd <= 0 THEN 1 END) AS events_with_zero_or_negative_value,
    
    -- Bundle and batch quality
    COUNT(DISTINCT event_bundle_sequence_id) AS unique_bundles,
    ROUND(AVG(batch_event_index), 1) AS avg_batch_position,
    
    -- Date consistency
    COUNT(CASE 
        WHEN DATE(TIMESTAMP_MICROS(event_timestamp)) != PARSE_DATE('%Y%m%d', event_date) 
        THEN 1 
    END) AS events_with_date_mismatch,
    
    -- Overall data quality score (0-100)
    ROUND(
        100 * (1 - (
            COUNT(CASE WHEN event_server_timestamp_offset IS NULL THEN 1 END) +
            COUNT(CASE WHEN ARRAY_LENGTH(event_params) = 0 THEN 1 END) +
            COUNT(CASE WHEN DATE(TIMESTAMP_MICROS(event_timestamp)) != PARSE_DATE('%Y%m%d', event_date) THEN 1 END)
        ) * 1.0 / COUNT(*)), 2
    ) AS data_quality_score
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include additional WHERE conditions as specified
GROUP BY
    event_name
ORDER BY
    total_events DESC
"""
    },
        "analyze_ecommerce_performance_overview": {
        "description": "Analyzes overall ecommerce performance including revenue, transactions, and key metrics. Good for questions like 'ecommerce performance summary', 'revenue analysis', 'transaction overview', or 'sales metrics'.",
        "template": """
-- Provides comprehensive ecommerce performance overview with key business metrics
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT ecommerce.transaction_id) AS total_transactions,
    COUNT(DISTINCT user_pseudo_id) AS unique_purchasing_users,
    SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue_usd,
    SUM(ecommerce.total_item_quantity) AS total_items_sold,
    SUM(ecommerce.unique_items) AS total_unique_items_sold,
    SUM(ecommerce.shipping_value_in_usd) AS total_shipping_revenue_usd,
    SUM(ecommerce.tax_value_in_usd) AS total_tax_collected_usd,
    ROUND(AVG(ecommerce.purchase_revenue_in_usd), 2) AS avg_order_value_usd,
    ROUND(AVG(ecommerce.total_item_quantity), 2) AS avg_items_per_transaction,
    ROUND(AVG(ecommerce.unique_items), 2) AS avg_unique_items_per_transaction,
    ROUND(SUM(ecommerce.purchase_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2) AS revenue_per_user,
    ROUND(COUNT(DISTINCT ecommerce.transaction_id) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2) AS transactions_per_user,
    ROUND(
        (SUM(ecommerce.purchase_revenue_in_usd) - SUM(ecommerce.shipping_value_in_usd) - SUM(ecommerce.tax_value_in_usd)) * 100.0 / 
        SUM(ecommerce.purchase_revenue_in_usd), 2
    ) AS product_revenue_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND ecommerce.transaction_id IS NOT NULL
    -- LLM: Add additional filters as specified by user
"""
    },

    "analyze_transaction_size_distribution": {
        "description": "Analyzes transaction size patterns and order value distribution. Good for questions like 'order value distribution', 'transaction size analysis', 'average order value by segment', or 'purchase behavior patterns'.",
        "template": """
-- Analyzes distribution of transaction sizes and order values for business insights
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN ecommerce.purchase_revenue_in_usd < 25 THEN 'Under $25'
        WHEN ecommerce.purchase_revenue_in_usd < 50 THEN '$25 - $49'
        WHEN ecommerce.purchase_revenue_in_usd < 100 THEN '$50 - $99'
        WHEN ecommerce.purchase_revenue_in_usd < 200 THEN '$100 - $199'
        WHEN ecommerce.purchase_revenue_in_usd < 500 THEN '$200 - $499'
        ELSE '$500+'
    END AS order_value_range,
    CASE 
        WHEN ecommerce.total_item_quantity = 1 THEN '1 Item'
        WHEN ecommerce.total_item_quantity BETWEEN 2 AND 3 THEN '2-3 Items'
        WHEN ecommerce.total_item_quantity BETWEEN 4 AND 5 THEN '4-5 Items'
        WHEN ecommerce.total_item_quantity BETWEEN 6 AND 10 THEN '6-10 Items'
        ELSE '10+ Items'
    END AS quantity_range,
    COUNT(DISTINCT ecommerce.transaction_id) AS transaction_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_buyers,
    SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue_usd,
    SUM(ecommerce.total_item_quantity) AS total_items,
    ROUND(AVG(ecommerce.purchase_revenue_in_usd), 2) AS avg_order_value_usd,
    ROUND(AVG(ecommerce.total_item_quantity), 2) AS avg_items_per_order,
    ROUND(
        COUNT(DISTINCT ecommerce.transaction_id) * 100.0 / 
        SUM(COUNT(DISTINCT ecommerce.transaction_id)) OVER (), 2
    ) AS transaction_share_percentage,
    ROUND(
        SUM(ecommerce.purchase_revenue_in_usd) * 100.0 / 
        SUM(SUM(ecommerce.purchase_revenue_in_usd)) OVER (), 2
    ) AS revenue_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND ecommerce.transaction_id IS NOT NULL
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    order_value_range, quantity_range
ORDER BY
    AVG(ecommerce.purchase_revenue_in_usd) DESC
"""
    },

    "analyze_repeat_vs_new_buyers": {
        "description": "Analyzes purchasing behavior of new vs returning customers. Good for questions like 'new vs repeat buyer analysis', 'customer loyalty metrics', 'buyer segmentation', or 'customer lifetime value patterns'.",
        "template": """
-- Analyzes purchasing patterns between new and returning customers
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH user_purchase_history AS (
    SELECT
        user_pseudo_id,
        COUNT(DISTINCT ecommerce.transaction_id) AS total_transactions,
        MIN(PARSE_DATE('%Y%m%d', event_date)) AS first_purchase_date,
        MAX(PARSE_DATE('%Y%m%d', event_date)) AS last_purchase_date,
        SUM(ecommerce.purchase_revenue_in_usd) AS total_user_revenue,
        AVG(ecommerce.purchase_revenue_in_usd) AS avg_order_value,
        SUM(ecommerce.total_item_quantity) AS total_items_purchased
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name = 'purchase'
        AND ecommerce.transaction_id IS NOT NULL
        -- LLM: Apply additional filters as needed
    GROUP BY
        user_pseudo_id
)
SELECT
    CASE 
        WHEN total_transactions = 1 THEN 'New_Buyer (1 transaction)'
        WHEN total_transactions BETWEEN 2 AND 3 THEN 'Occasional_Buyer (2-3 transactions)'
        WHEN total_transactions BETWEEN 4 AND 10 THEN 'Regular_Buyer (4-10 transactions)'
        ELSE 'Loyal_Buyer (10+ transactions)'
    END AS buyer_segment,
    COUNT(DISTINCT user_pseudo_id) AS unique_buyers,
    SUM(total_transactions) AS total_transactions_from_segment,
    SUM(total_user_revenue) AS total_revenue_from_segment,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value_usd,
    ROUND(AVG(total_user_revenue), 2) AS avg_customer_lifetime_value,
    ROUND(AVG(total_items_purchased), 2) AS avg_items_per_customer,
    ROUND(AVG(total_transactions), 2) AS avg_transactions_per_customer,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / 
        SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS buyer_share_percentage,
    ROUND(
        SUM(total_user_revenue) * 100.0 / 
        SUM(SUM(total_user_revenue)) OVER (), 2
    ) AS revenue_contribution_percentage
FROM
    user_purchase_history
GROUP BY
    buyer_segment
ORDER BY
    total_revenue_from_segment DESC
"""
    },

    "analyze_revenue_components": {
        "description": "Breaks down revenue into product, shipping, and tax components. Good for questions like 'revenue breakdown', 'shipping vs product revenue', 'tax analysis', or 'revenue component analysis'.",
        "template": """
-- Analyzes the composition of total revenue including product, shipping, and tax
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT ecommerce.transaction_id) AS total_transactions,
    SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue_usd,
    SUM(ecommerce.purchase_revenue_in_usd - COALESCE(ecommerce.shipping_value_in_usd, 0) - COALESCE(ecommerce.tax_value_in_usd, 0)) AS product_revenue_usd,
    SUM(ecommerce.shipping_value_in_usd) AS shipping_revenue_usd,
    SUM(ecommerce.tax_value_in_usd) AS tax_collected_usd,
    ROUND(AVG(ecommerce.purchase_revenue_in_usd), 2) AS avg_total_order_value,
    ROUND(AVG(ecommerce.purchase_revenue_in_usd - COALESCE(ecommerce.shipping_value_in_usd, 0) - COALESCE(ecommerce.tax_value_in_usd, 0)), 2) AS avg_product_value,
    ROUND(AVG(ecommerce.shipping_value_in_usd), 2) AS avg_shipping_per_order,
    ROUND(AVG(ecommerce.tax_value_in_usd), 2) AS avg_tax_per_order,
    ROUND(
        SUM(ecommerce.purchase_revenue_in_usd - COALESCE(ecommerce.shipping_value_in_usd, 0) - COALESCE(ecommerce.tax_value_in_usd, 0)) * 100.0 / 
        SUM(ecommerce.purchase_revenue_in_usd), 2
    ) AS product_revenue_percentage,
    ROUND(
        SUM(ecommerce.shipping_value_in_usd) * 100.0 / 
        SUM(ecommerce.purchase_revenue_in_usd), 2
    ) AS shipping_revenue_percentage,
    ROUND(
        SUM(ecommerce.tax_value_in_usd) * 100.0 / 
        SUM(ecommerce.purchase_revenue_in_usd), 2
    ) AS tax_percentage,
    COUNT(CASE WHEN ecommerce.shipping_value_in_usd > 0 THEN 1 END) AS transactions_with_shipping,
    COUNT(CASE WHEN ecommerce.tax_value_in_usd > 0 THEN 1 END) AS transactions_with_tax,
    ROUND(
        COUNT(CASE WHEN ecommerce.shipping_value_in_usd > 0 THEN 1 END) * 100.0 / 
        COUNT(DISTINCT ecommerce.transaction_id), 2
    ) AS shipping_charge_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND ecommerce.transaction_id IS NOT NULL
    -- LLM: Include additional filtering conditions
"""
    },

    "analyze_refund_patterns": {
        "description": "Analyzes refund patterns and return rates. Good for questions like 'refund analysis', 'return rates', 'refund patterns', or 'customer satisfaction metrics'.",
        "template": """
-- Analyzes refund patterns to understand return behavior and rates
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH purchases AS (
    SELECT
        ecommerce.transaction_id,
        user_pseudo_id,
        ecommerce.purchase_revenue_in_usd,
        ecommerce.total_item_quantity,
        PARSE_DATE('%Y%m%d', event_date) AS purchase_date
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name = 'purchase'
        AND ecommerce.transaction_id IS NOT NULL
),
refunds AS (
    SELECT
        ecommerce.transaction_id,
        user_pseudo_id,
        ecommerce.refund_value_in_usd,
        PARSE_DATE('%Y%m%d', event_date) AS refund_date
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name = 'refund'
        AND ecommerce.transaction_id IS NOT NULL
        AND ecommerce.refund_value_in_usd > 0
)
SELECT
    -- Purchase metrics
    COUNT(DISTINCT p.transaction_id) AS total_transactions,
    SUM(p.purchase_revenue_in_usd) AS total_purchase_revenue_usd,
    COUNT(DISTINCT p.user_pseudo_id) AS unique_buyers,
    
    -- Refund metrics  
    COUNT(DISTINCT r.transaction_id) AS transactions_with_refunds,
    COUNT(DISTINCT r.user_pseudo_id) AS users_requesting_refunds,
    SUM(r.refund_value_in_usd) AS total_refund_amount_usd,
    
    -- Refund rates
    ROUND(
        COUNT(DISTINCT r.transaction_id) * 100.0 / 
        COUNT(DISTINCT p.transaction_id), 2
    ) AS transaction_refund_rate_percentage,
    ROUND(
        COUNT(DISTINCT r.user_pseudo_id) * 100.0 / 
        COUNT(DISTINCT p.user_pseudo_id), 2
    ) AS user_refund_rate_percentage,
    ROUND(
        SUM(r.refund_value_in_usd) * 100.0 / 
        SUM(p.purchase_revenue_in_usd), 2
    ) AS revenue_refund_rate_percentage,
    
    -- Average values
    ROUND(AVG(r.refund_value_in_usd), 2) AS avg_refund_amount_usd,
    ROUND(AVG(p.purchase_revenue_in_usd), 2) AS avg_purchase_amount_usd,
    
    -- Timing analysis
    ROUND(AVG(DATE_DIFF(r.refund_date, p.purchase_date, DAY)), 1) AS avg_days_to_refund
FROM
    purchases p
    LEFT JOIN refunds r ON p.transaction_id = r.transaction_id
-- LLM: Add additional WHERE conditions as needed
"""
    },

        "analyze_high_value_transactions": {
        "description": "Analyzes high-value transactions and VIP customer behavior. Good for questions like 'high-value orders analysis', 'VIP customer behavior', 'premium transactions', or 'big spender patterns'.",
        "template": """
-- Analyzes high-value transactions to understand premium customer behavior
-- LLM: Replace {start_date} and {end_date} with user-specified date range
-- LLM: Adjust the high_value_threshold based on business context (default $500)
WITH transaction_analysis AS (
    SELECT
        ecommerce.transaction_id,
        user_pseudo_id,
        ecommerce.purchase_revenue_in_usd,
        ecommerce.total_item_quantity,
        ecommerce.unique_items,
        ecommerce.shipping_value_in_usd,
        ecommerce.tax_value_in_usd,
        CASE 
            WHEN ecommerce.purchase_revenue_in_usd >= 500 THEN 'High_Value ($500+)'
            WHEN ecommerce.purchase_revenue_in_usd >= 200 THEN 'Medium_High_Value ($200-499)'
            WHEN ecommerce.purchase_revenue_in_usd >= 100 THEN 'Medium_Value ($100-199)'
            ELSE 'Standard_Value (Under $100)'
        END AS value_tier
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name = 'purchase'
        AND ecommerce.transaction_id IS NOT NULL
        -- LLM: Apply additional filters as specified
)
SELECT
    value_tier,
    COUNT(DISTINCT transaction_id) AS transaction_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_customers,
    SUM(purchase_revenue_in_usd) AS total_revenue_usd,
    ROUND(AVG(purchase_revenue_in_usd), 2) AS avg_order_value_usd,
    ROUND(AVG(total_item_quantity), 2) AS avg_items_per_transaction,
    ROUND(AVG(unique_items), 2) AS avg_unique_items_per_transaction,
    ROUND(AVG(shipping_value_in_usd), 2) AS avg_shipping_per_transaction,
    ROUND(
        COUNT(DISTINCT transaction_id) * 100.0 / 
        SUM(COUNT(DISTINCT transaction_id)) OVER (), 2
    ) AS transaction_share_percentage,
    ROUND(
        SUM(purchase_revenue_in_usd) * 100.0 / 
        SUM(SUM(purchase_revenue_in_usd)) OVER (), 2
    ) AS revenue_contribution_percentage,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / 
        SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS customer_share_percentage,
    ROUND(
        SUM(purchase_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS revenue_per_customer_in_tier
FROM
    transaction_analysis
GROUP BY
    value_tier
ORDER BY
    AVG(purchase_revenue_in_usd) DESC
"""
    },

    "analyze_ecommerce_trends_over_time": {
        "description": "Analyzes ecommerce performance trends over time periods. Good for questions like 'sales trends over time', 'revenue growth analysis', 'seasonal ecommerce patterns', or 'performance by time period'.",
        "template": """
-- Analyzes ecommerce performance trends across different time periods
-- LLM: Replace {time_granularity}, {start_date} and {end_date} with user-specified values
-- LLM: {time_granularity} should be 'daily', 'weekly', or 'monthly'
SELECT
    CASE 
        WHEN '{time_granularity}' = 'daily' THEN PARSE_DATE('%Y%m%d', event_date)
        WHEN '{time_granularity}' = 'weekly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(SUNDAY))
        WHEN '{time_granularity}' = 'monthly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)
        ELSE PARSE_DATE('%Y%m%d', event_date)
    END AS time_period,
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions,
    COUNT(DISTINCT user_pseudo_id) AS unique_buyers,
    SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue_usd,
    SUM(ecommerce.total_item_quantity) AS items_sold,
    SUM(ecommerce.unique_items) AS unique_items_sold,
    ROUND(AVG(ecommerce.purchase_revenue_in_usd), 2) AS avg_order_value_usd,
    ROUND(AVG(ecommerce.total_item_quantity), 2) AS avg_items_per_order,
    ROUND(
        SUM(ecommerce.purchase_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS revenue_per_buyer,
    LAG(SUM(ecommerce.purchase_revenue_in_usd)) OVER (ORDER BY 
        CASE 
            WHEN '{time_granularity}' = 'daily' THEN PARSE_DATE('%Y%m%d', event_date)
            WHEN '{time_granularity}' = 'weekly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(SUNDAY))
            WHEN '{time_granularity}' = 'monthly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)
            ELSE PARSE_DATE('%Y%m%d', event_date)
        END
    ) AS previous_period_revenue,
    ROUND(
        (SUM(ecommerce.purchase_revenue_in_usd) - LAG(SUM(ecommerce.purchase_revenue_in_usd)) OVER (ORDER BY 
            CASE 
                WHEN '{time_granularity}' = 'daily' THEN PARSE_DATE('%Y%m%d', event_date)
                WHEN '{time_granularity}' = 'weekly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(SUNDAY))
                WHEN '{time_granularity}' = 'monthly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)
                ELSE PARSE_DATE('%Y%m%d', event_date)
            END
        )) * 100.0 / NULLIF(LAG(SUM(ecommerce.purchase_revenue_in_usd)) OVER (ORDER BY 
            CASE 
                WHEN '{time_granularity}' = 'daily' THEN PARSE_DATE('%Y%m%d', event_date)
                WHEN '{time_granularity}' = 'weekly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(SUNDAY))
                WHEN '{time_granularity}' = 'monthly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)
                ELSE PARSE_DATE('%Y%m%d', event_date)
            END
        ), 0), 2
    ) AS revenue_growth_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND ecommerce.transaction_id IS NOT NULL
    -- LLM: Include additional WHERE conditions as needed
GROUP BY
    time_period
ORDER BY
    time_period
"""
    },

    "analyze_transaction_completeness": {
        "description": "Analyzes data quality and completeness of ecommerce transactions. Good for questions like 'transaction data quality', 'missing ecommerce data', 'data completeness audit', or 'ecommerce tracking health'.",
        "template": """
-- Analyzes the completeness and quality of ecommerce transaction data
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COUNT(DISTINCT ecommerce.transaction_id) AS total_transactions,
    
    -- Data completeness checks
    COUNT(CASE WHEN ecommerce.purchase_revenue_in_usd IS NOT NULL THEN 1 END) AS transactions_with_revenue_usd,
    COUNT(CASE WHEN ecommerce.purchase_revenue IS NOT NULL THEN 1 END) AS transactions_with_revenue_local,
    COUNT(CASE WHEN ecommerce.total_item_quantity IS NOT NULL THEN 1 END) AS transactions_with_item_quantity,
    COUNT(CASE WHEN ecommerce.unique_items IS NOT NULL THEN 1 END) AS transactions_with_unique_items,
    COUNT(CASE WHEN ecommerce.shipping_value_in_usd IS NOT NULL AND ecommerce.shipping_value_in_usd > 0 THEN 1 END) AS transactions_with_shipping,
    COUNT(CASE WHEN ecommerce.tax_value_in_usd IS NOT NULL AND ecommerce.tax_value_in_usd > 0 THEN 1 END) AS transactions_with_tax,
    
    -- Data quality percentages
    ROUND(
        COUNT(CASE WHEN ecommerce.purchase_revenue_in_usd IS NOT NULL THEN 1 END) * 100.0 / 
        COUNT(DISTINCT ecommerce.transaction_id), 2
    ) AS revenue_completeness_percentage,
    ROUND(
        COUNT(CASE WHEN ecommerce.total_item_quantity IS NOT NULL THEN 1 END) * 100.0 / 
        COUNT(DISTINCT ecommerce.transaction_id), 2
    ) AS quantity_completeness_percentage,
    
    -- Value ranges for validation
    MIN(ecommerce.purchase_revenue_in_usd) AS min_transaction_value,
    MAX(ecommerce.purchase_revenue_in_usd) AS max_transaction_value,
    ROUND(AVG(ecommerce.purchase_revenue_in_usd), 2) AS avg_transaction_value,
    
    -- Suspicious data detection
    COUNT(CASE WHEN ecommerce.purchase_revenue_in_usd <= 0 THEN 1 END) AS transactions_with_zero_or_negative_revenue,
    COUNT(CASE WHEN ecommerce.total_item_quantity <= 0 THEN 1 END) AS transactions_with_zero_or_negative_quantity,
    COUNT(CASE WHEN ecommerce.purchase_revenue_in_usd > 10000 THEN 1 END) AS transactions_over_10k_usd,
    
    -- Overall data quality score
    ROUND(
        (COUNT(CASE WHEN ecommerce.purchase_revenue_in_usd IS NOT NULL 
                    AND ecommerce.total_item_quantity IS NOT NULL 
                    AND ecommerce.purchase_revenue_in_usd > 0 
                    AND ecommerce.total_item_quantity > 0 
               THEN 1 END) * 100.0) / COUNT(DISTINCT ecommerce.transaction_id), 2
    ) AS overall_data_quality_score
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    -- LLM: Apply additional filters as specified
"""
    },
        "analyze_top_product_performance": {
        "description": "Analyzes top-performing products by revenue, quantity, and user engagement. Good for questions like 'best selling products', 'top product performance', 'product revenue analysis', or 'bestseller insights'.",
        "template": """
-- Analyzes top-performing products across key business metrics
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    items.item_id,
    items.item_name,
    items.item_brand,
    items.item_category,
    SUM(items.quantity) AS total_quantity_sold,
    SUM(items.item_revenue_in_usd) AS total_item_revenue_usd,
    COUNT(DISTINCT user_pseudo_id) AS unique_buyers,
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions_containing_item,
    ROUND(AVG(items.price_in_usd), 2) AS avg_item_price_usd,
    ROUND(AVG(items.quantity), 2) AS avg_quantity_per_transaction,
    ROUND(
        SUM(items.item_revenue_in_usd) / SUM(items.quantity), 2
    ) AS revenue_per_unit_sold,
    ROUND(
        SUM(items.item_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS revenue_per_buyer,
    ROUND(
        SUM(items.quantity) * 100.0 / 
        SUM(SUM(items.quantity)) OVER (), 2
    ) AS quantity_share_percentage,
    ROUND(
        SUM(items.item_revenue_in_usd) * 100.0 / 
        SUM(SUM(items.item_revenue_in_usd)) OVER (), 2
    ) AS revenue_share_percentage,
    COUNT(DISTINCT 
        CASE WHEN items.coupon IS NOT NULL THEN ecommerce.transaction_id END
    ) AS transactions_with_coupon,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN items.coupon IS NOT NULL THEN ecommerce.transaction_id END
        ) * 100.0 / COUNT(DISTINCT ecommerce.transaction_id), 2
    ) AS coupon_usage_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`,
    UNNEST(items) AS items
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND items.item_id IS NOT NULL
    -- LLM: Add additional filters as specified by user
GROUP BY
    items.item_id, items.item_name, items.item_brand, items.item_category
ORDER BY
    total_item_revenue_usd DESC
LIMIT 50
"""
    },

    "analyze_product_category_performance": {
        "description": "Analyzes performance across product categories and subcategories. Good for questions like 'category performance', 'which product categories sell best?', 'category revenue breakdown', or 'product line analysis'.",
        "template": """
-- Analyzes product performance across categories and subcategories
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(items.item_category, '(category_not_set)') AS primary_category,
    COALESCE(items.item_category2, '(subcategory_not_set)') AS secondary_category,
    COALESCE(items.item_category3, '(subcategory3_not_set)') AS tertiary_category,
    COUNT(DISTINCT items.item_id) AS unique_products_in_category,
    SUM(items.quantity) AS total_units_sold,
    SUM(items.item_revenue_in_usd) AS total_category_revenue_usd,
    COUNT(DISTINCT user_pseudo_id) AS unique_buyers,
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions_with_category,
    ROUND(AVG(items.price_in_usd), 2) AS avg_item_price_in_category,
    ROUND(AVG(items.quantity), 2) AS avg_quantity_per_transaction,
    ROUND(
        SUM(items.item_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS revenue_per_buyer,
    ROUND(
        SUM(items.item_revenue_in_usd) / SUM(items.quantity), 2
    ) AS revenue_per_unit,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / 
        SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS buyer_share_percentage,
    ROUND(
        SUM(items.item_revenue_in_usd) * 100.0 / 
        SUM(SUM(items.item_revenue_in_usd)) OVER (), 2
    ) AS revenue_share_percentage,
    ROUND(
        SUM(items.quantity) * 100.0 / 
        SUM(SUM(items.quantity)) OVER (), 2
    ) AS unit_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`,
    UNNEST(items) AS items
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND items.item_id IS NOT NULL
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    primary_category, secondary_category, tertiary_category
ORDER BY
    total_category_revenue_usd DESC
"""
    },

    "analyze_brand_performance": {
        "description": "Analyzes performance by product brands. Good for questions like 'brand performance analysis', 'which brands sell best?', 'brand revenue comparison', or 'brand portfolio analysis'.",
        "template": """
-- Analyzes product performance across different brands
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(items.item_brand, '(brand_not_set)') AS brand_name,
    COUNT(DISTINCT items.item_id) AS unique_products_per_brand,
    COUNT(DISTINCT items.item_category) AS categories_per_brand,
    SUM(items.quantity) AS total_units_sold,
    SUM(items.item_revenue_in_usd) AS total_brand_revenue_usd,
    COUNT(DISTINCT user_pseudo_id) AS unique_brand_buyers,
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions_with_brand,
    ROUND(AVG(items.price_in_usd), 2) AS avg_brand_price_usd,
    ROUND(
        SUM(items.item_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS revenue_per_brand_buyer,
    ROUND(
        SUM(items.item_revenue_in_usd) / SUM(items.quantity), 2
    ) AS avg_revenue_per_unit,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / 
        SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS brand_buyer_share_percentage,
    ROUND(
        SUM(items.item_revenue_in_usd) * 100.0 / 
        SUM(SUM(items.item_revenue_in_usd)) OVER (), 2
    ) AS brand_revenue_share_percentage,
    MIN(items.price_in_usd) AS lowest_brand_price,
    MAX(items.price_in_usd) AS highest_brand_price,
    COUNT(DISTINCT 
        CASE WHEN items.coupon IS NOT NULL THEN ecommerce.transaction_id END
    ) AS transactions_with_brand_coupons,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN items.coupon IS NOT NULL THEN ecommerce.transaction_id END
        ) * 100.0 / COUNT(DISTINCT ecommerce.transaction_id), 2
    ) AS brand_coupon_usage_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`,
    UNNEST(items) AS items
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND items.item_id IS NOT NULL
    -- LLM: Apply additional filters as needed
GROUP BY
    brand_name
ORDER BY
    total_brand_revenue_usd DESC
"""
    },

    "analyze_product_pricing_performance": {
        "description": "Analyzes how product pricing affects sales performance. Good for questions like 'price point analysis', 'pricing strategy effectiveness', 'price vs volume analysis', or 'optimal pricing insights'.",
        "template": """
-- Analyzes product performance across different price points
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN items.price_in_usd < 10 THEN 'Under $10'
        WHEN items.price_in_usd < 25 THEN '$10 - $24'
        WHEN items.price_in_usd < 50 THEN '$25 - $49'
        WHEN items.price_in_usd < 100 THEN '$50 - $99'
        WHEN items.price_in_usd < 200 THEN '$100 - $199'
        ELSE '$200+'
    END AS price_range,
    COUNT(DISTINCT items.item_id) AS unique_products_in_range,
    SUM(items.quantity) AS total_units_sold,
    SUM(items.item_revenue_in_usd) AS total_revenue_usd,
    COUNT(DISTINCT user_pseudo_id) AS unique_buyers,
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions_in_range,
    ROUND(AVG(items.price_in_usd), 2) AS avg_price_in_range,
    ROUND(AVG(items.quantity), 2) AS avg_quantity_per_transaction,
    ROUND(
        SUM(items.item_revenue_in_usd) / SUM(items.quantity), 2
    ) AS revenue_per_unit,
    ROUND(
        SUM(items.item_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS revenue_per_buyer,
    ROUND(
        SUM(items.quantity) * 100.0 / 
        SUM(SUM(items.quantity)) OVER (), 2
    ) AS unit_volume_share_percentage,
    ROUND(
        SUM(items.item_revenue_in_usd) * 100.0 / 
        SUM(SUM(items.item_revenue_in_usd)) OVER (), 2
    ) AS revenue_share_percentage,
    COUNT(DISTINCT 
        CASE WHEN items.coupon IS NOT NULL THEN ecommerce.transaction_id END
    ) AS transactions_with_coupons,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN items.coupon IS NOT NULL THEN ecommerce.transaction_id END
        ) * 100.0 / COUNT(DISTINCT ecommerce.transaction_id), 2
    ) AS coupon_usage_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`,
    UNNEST(items) AS items
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND items.item_id IS NOT NULL
    AND items.price_in_usd IS NOT NULL
    -- LLM: Include additional filtering conditions
GROUP BY
    price_range
ORDER BY
    AVG(items.price_in_usd)
"""
    },

    "analyze_product_promotion_effectiveness": {
        "description": "Analyzes the effectiveness of product promotions and coupons. Good for questions like 'promotion performance', 'coupon effectiveness', 'promotional campaign analysis', or 'discount impact on sales'.",
        "template": """
-- Analyzes the effectiveness of promotions and coupons on product sales
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(items.coupon, '(no_coupon)') AS coupon_code,
    COALESCE(items.promotion_name, '(no_promotion)') AS promotion_name,
    COALESCE(items.promotion_id, '(no_promotion_id)') AS promotion_id,
    COUNT(DISTINCT items.item_id) AS unique_products_promoted,
    COUNT(DISTINCT user_pseudo_id) AS unique_buyers_using_promotion,
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions_with_promotion,
    SUM(items.quantity) AS total_promoted_units_sold,
    SUM(items.item_revenue_in_usd) AS total_promoted_revenue_usd,
    ROUND(AVG(items.price_in_usd), 2) AS avg_promoted_item_price,
    ROUND(AVG(items.quantity), 2) AS avg_promoted_quantity_per_transaction,
    ROUND(
        SUM(items.item_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS revenue_per_promotion_user,
    ROUND(
        SUM(items.item_revenue_in_usd) / SUM(items.quantity), 2
    ) AS revenue_per_promoted_unit,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / 
        SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS user_share_percentage,
    ROUND(
        SUM(items.item_revenue_in_usd) * 100.0 / 
        SUM(SUM(items.item_revenue_in_usd)) OVER (), 2
    ) AS revenue_share_percentage,
    COUNT(DISTINCT items.item_brand) AS brands_in_promotion,
    COUNT(DISTINCT items.item_category) AS categories_in_promotion
FROM
    `{project_id}.{dataset_id}.events_*`,
    UNNEST(items) AS items
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND items.item_id IS NOT NULL
    -- LLM: Apply additional filters as specified
GROUP BY
    coupon_code, promotion_name, promotion_id
ORDER BY
    total_promoted_revenue_usd DESC
"""
    },

    "analyze_product_refund_patterns": {
        "description": "Analyzes product return and refund patterns to identify quality issues. Good for questions like 'which products get returned most?', 'product refund analysis', 'return rate by product', or 'product quality insights'.",
        "template": """
-- Analyzes product return patterns to identify potential quality or satisfaction issues
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH purchases AS (
    SELECT
        items.item_id,
        items.item_name,
        items.item_brand,
        items.item_category,
        SUM(items.quantity) AS total_purchased_quantity,
        SUM(items.item_revenue_in_usd) AS total_purchase_revenue_usd,
        COUNT(DISTINCT user_pseudo_id) AS buyers_of_item
    FROM
        `{project_id}.{dataset_id}.events_*`,
        UNNEST(items) AS items
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name = 'purchase'
        AND items.item_id IS NOT NULL
    GROUP BY
        items.item_id, items.item_name, items.item_brand, items.item_category
),
refunds AS (
    SELECT
        items.item_id,
        SUM(items.quantity) AS total_refunded_quantity,
        SUM(items.item_refund_in_usd) AS total_refund_amount_usd,
        COUNT(DISTINCT user_pseudo_id) AS users_returning_item
    FROM
        `{project_id}.{dataset_id}.events_*`,
        UNNEST(items) AS items
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        AND event_name = 'refund'
        AND items.item_id IS NOT NULL
        AND items.item_refund_in_usd > 0
    GROUP BY
        items.item_id
)
SELECT
    p.item_id,
    p.item_name,
    p.item_brand,
    p.item_category,
    p.total_purchased_quantity,
    p.total_purchase_revenue_usd,
    p.buyers_of_item,
    COALESCE(r.total_refunded_quantity, 0) AS total_refunded_quantity,
    COALESCE(r.total_refund_amount_usd, 0) AS total_refund_amount_usd,
    COALESCE(r.users_returning_item, 0) AS users_returning_item,
    ROUND(
        COALESCE(r.total_refunded_quantity, 0) * 100.0 / p.total_purchased_quantity, 2
    ) AS unit_return_rate_percentage,
    ROUND(
        COALESCE(r.total_refund_amount_usd, 0) * 100.0 / p.total_purchase_revenue_usd, 2
    ) AS revenue_return_rate_percentage,
    ROUND(
        COALESCE(r.users_returning_item, 0) * 100.0 / p.buyers_of_item, 2
    ) AS buyer_return_rate_percentage,
    CASE 
        WHEN COALESCE(r.total_refunded_quantity, 0) * 100.0 / p.total_purchased_quantity > 10 THEN 'High_Return_Risk'
        WHEN COALESCE(r.total_refunded_quantity, 0) * 100.0 / p.total_purchased_quantity > 5 THEN 'Medium_Return_Risk'
        ELSE 'Low_Return_Risk'
    END AS return_risk_category
FROM
    purchases p
    LEFT JOIN refunds r ON p.item_id = r.item_id
WHERE
    p.total_purchased_quantity > 5  -- Filter for products with meaningful volume
    -- LLM: Include additional WHERE conditions as needed
ORDER BY
    unit_return_rate_percentage DESC
"""
    },

    "analyze_product_list_performance": {
        "description": "Analyzes how products perform in different lists and positions. Good for questions like 'product list effectiveness', 'merchandising performance', 'list position analysis', or 'product placement optimization'.",
        "template": """
-- Analyzes product performance across different lists and positions for merchandising insights
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(items.item_list_name, '(no_list)') AS product_list_name,
    COALESCE(items.item_list_id, '(no_list_id)') AS product_list_id,
    CASE 
        WHEN items.item_list_index <= 3 THEN 'Top 3 Positions'
        WHEN items.item_list_index <= 10 THEN 'Positions 4-10'
        WHEN items.item_list_index <= 20 THEN 'Positions 11-20'
        ELSE 'Position 20+'
    END AS list_position_group,
    COUNT(DISTINCT items.item_id) AS unique_products_in_list,
    SUM(items.quantity) AS total_units_from_list,
    SUM(items.item_revenue_in_usd) AS total_revenue_from_list_usd,
    COUNT(DISTINCT user_pseudo_id) AS unique_buyers_from_list,
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions_from_list,
    ROUND(AVG(items.item_list_index), 1) AS avg_list_position,
    ROUND(AVG(items.price_in_usd), 2) AS avg_item_price_in_list,
    ROUND(
        SUM(items.item_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS revenue_per_list_buyer,
    ROUND(
        SUM(items.item_revenue_in_usd) / SUM(items.quantity), 2
    ) AS revenue_per_unit_from_list,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / 
        SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS buyer_share_percentage,
    ROUND(
        SUM(items.item_revenue_in_usd) * 100.0 / 
        SUM(SUM(items.item_revenue_in_usd)) OVER (), 2
    ) AS revenue_share_percentage,
    COUNT(DISTINCT items.item_brand) AS brands_in_list,
    COUNT(DISTINCT items.item_category) AS categories_in_list
FROM
    `{project_id}.{dataset_id}.events_*`,
    UNNEST(items) AS items
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND items.item_id IS NOT NULL
    AND (items.item_list_name IS NOT NULL OR items.item_list_id IS NOT NULL)
    -- LLM: Apply additional filters as needed
GROUP BY
    product_list_name, product_list_id, list_position_group
ORDER BY
    total_revenue_from_list_usd DESC
"""
    },

    "analyze_specific_product_performance": {
        "description": "Deep dive analysis of a specific product's performance. Good for questions like 'analyze product [item_name]', 'detailed product performance', 'specific item analysis', or 'product deep dive'.",
        "template": """
-- Provides comprehensive analysis of a specific product's performance
-- LLM: Replace {item_id_or_name}, {start_date} and {end_date} with user-specified values
SELECT
    items.item_id,
    items.item_name,
    items.item_brand,
    items.item_category,
    items.item_category2,
    items.item_category3,
    items.item_variant,
    SUM(items.quantity) AS total_quantity_sold,
    SUM(items.item_revenue_in_usd) AS total_item_revenue_usd,
    COUNT(DISTINCT user_pseudo_id) AS unique_buyers,
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions_containing_item,
    ROUND(AVG(items.price_in_usd), 2) AS avg_selling_price_usd,
    ROUND(MIN(items.price_in_usd), 2) AS min_selling_price_usd,
    ROUND(MAX(items.price_in_usd), 2) AS max_selling_price_usd,
    ROUND(AVG(items.quantity), 2) AS avg_quantity_per_transaction,
    ROUND(
        SUM(items.item_revenue_in_usd) / COUNT(DISTINCT user_pseudo_id), 2
    ) AS revenue_per_buyer,
    ROUND(
        SUM(items.item_revenue_in_usd) / SUM(items.quantity), 2
    ) AS revenue_per_unit,
    COUNT(DISTINCT 
        CASE WHEN items.coupon IS NOT NULL THEN ecommerce.transaction_id END
    ) AS transactions_with_coupon,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN items.coupon IS NOT NULL THEN ecommerce.transaction_id END
        ) * 100.0 / COUNT(DISTINCT ecommerce.transaction_id), 2
    ) AS coupon_usage_rate_percentage,
    COUNT(DISTINCT items.item_list_name) AS different_lists_appeared_in,
    STRING_AGG(DISTINCT items.item_list_name, ', ' ORDER BY items.item_list_name) AS lists_appeared_in,
    COUNT(DISTINCT items.coupon) AS different_coupons_used,
    STRING_AGG(DISTINCT items.coupon, ', ' ORDER BY items.coupon) AS coupons_used,
    COUNT(DISTINCT items.affiliation) AS different_affiliations,
    COUNT(DISTINCT items.location_id) AS different_locations
FROM
    `{project_id}.{dataset_id}.events_*`,
    UNNEST(items) AS items
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    AND event_name = 'purchase'
    AND (items.item_id = '{item_id_or_name}' OR items.item_name = '{item_id_or_name}')
    -- LLM: Add additional WHERE clauses as needed
GROUP BY
    items.item_id, items.item_name, items.item_brand, items.item_category,
    items.item_category2, items.item_category3, items.item_variant
ORDER BY
    total_item_revenue_usd DESC
"""
    },
        "analyze_privacy_consent_distribution": {
        "description": "Analyzes user privacy consent patterns and storage preferences. Good for questions like 'privacy consent rates', 'analytics vs ads storage consent', 'consent distribution analysis', or 'privacy compliance overview'.",
        "template": """
-- Analyzes distribution of privacy consent settings across users
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN privacy_info.analytics_storage = 'Yes' THEN 'Analytics_Consent_Granted'
        WHEN privacy_info.analytics_storage = 'No' THEN 'Analytics_Consent_Denied'
        ELSE 'Analytics_Consent_Unknown'
    END AS analytics_storage_consent,
    CASE 
        WHEN privacy_info.ads_storage = 'Yes' THEN 'Ads_Consent_Granted'
        WHEN privacy_info.ads_storage = 'No' THEN 'Ads_Consent_Denied'
        ELSE 'Ads_Consent_Unknown'
    END AS ads_storage_consent,
    CASE 
        WHEN privacy_info.uses_transient_token = 'Yes' THEN 'Uses_Transient_Token'
        WHEN privacy_info.uses_transient_token = 'No' THEN 'No_Transient_Token'
        ELSE 'Transient_Token_Unknown'
    END AS transient_token_usage,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNT(*) AS total_events,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 
             THEN user_pseudo_id 
        END
    ) AS new_users,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / 
        SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS user_share_percentage,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2
    ) AS event_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Add additional filters as specified by user
GROUP BY
    analytics_storage_consent, ads_storage_consent, transient_token_usage
ORDER BY
    unique_users DESC
"""
    },

    "analyze_consent_impact_on_behavior": {
        "description": "Analyzes how privacy consent choices affect user behavior and engagement. Good for questions like 'do consent choices affect engagement?', 'behavior by consent status', 'privacy impact on user activity', or 'consent vs engagement analysis'.",
        "template": """
-- Analyzes user behavior patterns based on privacy consent choices
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    CASE 
        WHEN privacy_info.analytics_storage = 'Yes' AND privacy_info.ads_storage = 'Yes' THEN 'Full_Consent'
        WHEN privacy_info.analytics_storage = 'Yes' AND privacy_info.ads_storage = 'No' THEN 'Analytics_Only'
        WHEN privacy_info.analytics_storage = 'No' AND privacy_info.ads_storage = 'Yes' THEN 'Ads_Only'
        WHEN privacy_info.analytics_storage = 'No' AND privacy_info.ads_storage = 'No' THEN 'No_Consent'
        ELSE 'Mixed_or_Unknown'
    END AS consent_category,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CONCAT(
        user_pseudo_id, 
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT 
        CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
             THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
        END
    ) AS engaged_sessions,
    COUNT(DISTINCT 
        CASE WHEN event_name = 'purchase' THEN user_pseudo_id END
    ) AS users_making_purchases,
    ROUND(
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) * 1.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS avg_sessions_per_user,
    ROUND(
        COUNTIF(event_name = 'page_view') * 1.0 / COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )), 2
    ) AS avg_page_views_per_session,
    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT 
                CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = '1' 
                     THEN CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))
                END
            ),
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
            ))
        ) * 100, 2
    ) AS engagement_rate_percentage,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN event_name = 'purchase' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS purchase_conversion_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    consent_category
ORDER BY
    unique_users DESC
"""
    },

    "analyze_consent_by_geography": {
        "description": "Analyzes privacy consent patterns by geographic location. Good for questions like 'consent rates by country', 'geographic privacy patterns', 'regional consent compliance', or 'GDPR impact analysis'.",
        "template": """
-- Analyzes privacy consent patterns across different geographic regions
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(geo.country, 'Unknown_Country') AS country,
    COALESCE(geo.continent, 'Unknown_Continent') AS continent,
    COUNT(DISTINCT user_pseudo_id) AS total_users,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.analytics_storage = 'Yes' THEN user_pseudo_id END
    ) AS users_consenting_to_analytics,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.ads_storage = 'Yes' THEN user_pseudo_id END
    ) AS users_consenting_to_ads,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.uses_transient_token = 'Yes' THEN user_pseudo_id END
    ) AS users_using_transient_tokens,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.analytics_storage = 'Yes' AND privacy_info.ads_storage = 'Yes' 
             THEN user_pseudo_id END
    ) AS users_with_full_consent,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.analytics_storage = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS analytics_consent_rate_percentage,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.ads_storage = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS ads_consent_rate_percentage,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.analytics_storage = 'Yes' AND privacy_info.ads_storage = 'Yes' 
                 THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS full_consent_rate_percentage,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.uses_transient_token = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS transient_token_usage_rate_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Apply additional filters as needed
GROUP BY
    country, continent
HAVING
    total_users >= 10  -- Filter for meaningful sample sizes
ORDER BY
    total_users DESC
"""
    },

    "analyze_consent_trends_over_time": {
        "description": "Analyzes how privacy consent patterns change over time. Good for questions like 'consent trends over time', 'privacy adoption rates', 'consent pattern changes', or 'privacy policy impact analysis'.",
        "template": """
-- Analyzes privacy consent trends across different time periods
-- LLM: Replace {time_granularity}, {start_date} and {end_date} with user-specified values
-- LLM: {time_granularity} should be 'daily', 'weekly', or 'monthly'
SELECT
    CASE 
        WHEN '{time_granularity}' = 'daily' THEN PARSE_DATE('%Y%m%d', event_date)
        WHEN '{time_granularity}' = 'weekly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(SUNDAY))
        WHEN '{time_granularity}' = 'monthly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)
        ELSE PARSE_DATE('%Y%m%d', event_date)
    END AS time_period,
    COUNT(DISTINCT user_pseudo_id) AS total_users,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.analytics_storage = 'Yes' THEN user_pseudo_id END
    ) AS users_consenting_to_analytics,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.ads_storage = 'Yes' THEN user_pseudo_id END
    ) AS users_consenting_to_ads,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.analytics_storage = 'Yes' AND privacy_info.ads_storage = 'Yes' 
             THEN user_pseudo_id END
    ) AS users_with_full_consent,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.analytics_storage = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS analytics_consent_rate_percentage,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.ads_storage = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS ads_consent_rate_percentage,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.analytics_storage = 'Yes' AND privacy_info.ads_storage = 'Yes' 
                 THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS full_consent_rate_percentage,
    LAG(ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.analytics_storage = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    )) OVER (ORDER BY 
        CASE 
            WHEN '{time_granularity}' = 'daily' THEN PARSE_DATE('%Y%m%d', event_date)
            WHEN '{time_granularity}' = 'weekly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(SUNDAY))
            WHEN '{time_granularity}' = 'monthly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)
            ELSE PARSE_DATE('%Y%m%d', event_date)
        END
    ) AS previous_analytics_consent_rate,
    ROUND(
        (COUNT(DISTINCT 
            CASE WHEN privacy_info.analytics_storage = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id)) - 
        LAG(COUNT(DISTINCT 
            CASE WHEN privacy_info.analytics_storage = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id)) OVER (ORDER BY 
            CASE 
                WHEN '{time_granularity}' = 'daily' THEN PARSE_DATE('%Y%m%d', event_date)
                WHEN '{time_granularity}' = 'weekly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(SUNDAY))
                WHEN '{time_granularity}' = 'monthly' THEN DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)
                ELSE PARSE_DATE('%Y%m%d', event_date)
            END
        ), 2
    ) AS analytics_consent_rate_change
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include additional filtering conditions
GROUP BY
    time_period
ORDER BY
    time_period
"""
    },

    "analyze_privacy_compliance_summary": {
        "description": "Provides a comprehensive privacy compliance overview for business reporting. Good for questions like 'privacy compliance report', 'consent management summary', 'privacy overview for executives', or 'consent compliance dashboard'.",
        "template": """
-- Provides comprehensive privacy and consent compliance overview
-- LLM: Replace {start_date} and {end_date} with user-specified date range
WITH privacy_summary AS (
    SELECT
        privacy_info.analytics_storage,
        privacy_info.ads_storage,
        privacy_info.uses_transient_token,
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNT(DISTINCT CONCAT(
            user_pseudo_id, 
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        )) AS sessions,
        COUNT(*) AS events
    FROM
        `{project_id}.{dataset_id}.events_*`
    WHERE
        _table_suffix BETWEEN '{start_date}' AND '{end_date}'
        -- LLM: Apply additional filters as specified
    GROUP BY
        analytics_storage, ads_storage, uses_transient_token
)
SELECT
    -- Overall compliance metrics
    SUM(users) AS total_users_tracked,
    SUM(sessions) AS total_sessions_tracked,
    SUM(events) AS total_events_tracked,
    
    -- Analytics storage consent
    SUM(CASE WHEN analytics_storage = 'Yes' THEN users ELSE 0 END) AS users_consenting_to_analytics,
    SUM(CASE WHEN analytics_storage = 'No' THEN users ELSE 0 END) AS users_declining_analytics,
    SUM(CASE WHEN analytics_storage IS NULL THEN users ELSE 0 END) AS users_unknown_analytics_consent,
    
    -- Ads storage consent
    SUM(CASE WHEN ads_storage = 'Yes' THEN users ELSE 0 END) AS users_consenting_to_ads,
    SUM(CASE WHEN ads_storage = 'No' THEN users ELSE 0 END) AS users_declining_ads,
    SUM(CASE WHEN ads_storage IS NULL THEN users ELSE 0 END) AS users_unknown_ads_consent,
    
    -- Transient token usage
    SUM(CASE WHEN uses_transient_token = 'Yes' THEN users ELSE 0 END) AS users_with_transient_tokens,
    SUM(CASE WHEN uses_transient_token = 'No' THEN users ELSE 0 END) AS users_without_transient_tokens,
    
    -- Combined consent patterns
    SUM(CASE WHEN analytics_storage = 'Yes' AND ads_storage = 'Yes' THEN users ELSE 0 END) AS users_full_consent,
    SUM(CASE WHEN analytics_storage = 'No' AND ads_storage = 'No' THEN users ELSE 0 END) AS users_no_consent,
    SUM(CASE WHEN analytics_storage = 'Yes' AND ads_storage = 'No' THEN users ELSE 0 END) AS users_analytics_only,
    SUM(CASE WHEN analytics_storage = 'No' AND ads_storage = 'Yes' THEN users ELSE 0 END) AS users_ads_only,
    
    -- Consent rate calculations
    ROUND(
        SUM(CASE WHEN analytics_storage = 'Yes' THEN users ELSE 0 END) * 100.0 / SUM(users), 2
    ) AS analytics_consent_rate_percentage,
    ROUND(
        SUM(CASE WHEN ads_storage = 'Yes' THEN users ELSE 0 END) * 100.0 / SUM(users), 2
    ) AS ads_consent_rate_percentage,
    ROUND(
        SUM(CASE WHEN analytics_storage = 'Yes' AND ads_storage = 'Yes' THEN users ELSE 0 END) * 100.0 / SUM(users), 2
    ) AS full_consent_rate_percentage,
    ROUND(
        SUM(CASE WHEN uses_transient_token = 'Yes' THEN users ELSE 0 END) * 100.0 / SUM(users), 2
    ) AS transient_token_usage_rate_percentage,
    
    -- Data quality indicators
    ROUND(
        SUM(CASE WHEN analytics_storage IS NOT NULL THEN users ELSE 0 END) * 100.0 / SUM(users), 2
    ) AS analytics_consent_data_completeness_percentage,
    ROUND(
        SUM(CASE WHEN ads_storage IS NOT NULL THEN users ELSE 0 END) * 100.0 / SUM(users), 2
    ) AS ads_consent_data_completeness_percentage
FROM
    privacy_summary
"""
    },

    "analyze_consent_by_traffic_source": {
        "description": "Analyzes privacy consent patterns by traffic source and acquisition channel. Good for questions like 'consent rates by traffic source', 'acquisition channel privacy patterns', 'how consent varies by source', or 'privacy by marketing channel'.",
        "template": """
-- Analyzes privacy consent patterns across different traffic sources and channels
-- LLM: Replace {start_date} and {end_date} with user-specified date range
SELECT
    COALESCE(traffic_source.source, '(direct)') AS traffic_source,
    COALESCE(traffic_source.medium, '(none)') AS traffic_medium,
    CONCAT(
        COALESCE(traffic_source.source, '(direct)'), 
        ' / ', 
        COALESCE(traffic_source.medium, '(none)')
    ) AS source_medium,
    COUNT(DISTINCT user_pseudo_id) AS total_users,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.analytics_storage = 'Yes' THEN user_pseudo_id END
    ) AS users_consenting_to_analytics,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.ads_storage = 'Yes' THEN user_pseudo_id END
    ) AS users_consenting_to_ads,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.analytics_storage = 'Yes' AND privacy_info.ads_storage = 'Yes' 
             THEN user_pseudo_id END
    ) AS users_with_full_consent,
    COUNT(DISTINCT 
        CASE WHEN privacy_info.analytics_storage = 'No' AND privacy_info.ads_storage = 'No' 
             THEN user_pseudo_id END
    ) AS users_with_no_consent,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.analytics_storage = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS analytics_consent_rate_percentage,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.ads_storage = 'Yes' THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS ads_consent_rate_percentage,
    ROUND(
        COUNT(DISTINCT 
            CASE WHEN privacy_info.analytics_storage = 'Yes' AND privacy_info.ads_storage = 'Yes' 
                 THEN user_pseudo_id END
        ) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2
    ) AS full_consent_rate_percentage,
    ROUND(
        COUNT(DISTINCT user_pseudo_id) * 100.0 / 
        SUM(COUNT(DISTINCT user_pseudo_id)) OVER (), 2
    ) AS traffic_share_percentage
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
    -- LLM: Include user-specified WHERE conditions
GROUP BY
    traffic_source, traffic_medium, source_medium
HAVING
    total_users >= 10  -- Filter for meaningful sample sizes
ORDER BY
    total_users DESC
"""
    }
}