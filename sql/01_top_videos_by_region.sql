-- Top 10 trending videos per region for the most recent day
WITH latest AS (
    SELECT MAX(trending_date) AS d FROM gold_daily_video_trending
),
ranked AS (
    SELECT v.*,
           ROW_NUMBER() OVER (PARTITION BY region ORDER BY best_rank, latest_view_count DESC) AS rn
    FROM gold_daily_video_trending v, latest
    WHERE v.trending_date = latest.d
)
SELECT region, rn AS position, title, channel_title, category_name,
       latest_view_count, view_gain, engagement_rate, hours_to_trend
FROM ranked
WHERE rn <= 10
ORDER BY region, rn;
