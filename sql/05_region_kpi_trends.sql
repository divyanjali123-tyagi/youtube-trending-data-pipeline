-- Daily KPI trend per region: how fast do videos trend and how engaged are viewers?
SELECT trending_date, region, unique_videos, median_views,
       ROUND(avg_engagement_rate * 100, 3) AS avg_engagement_pct,
       avg_hours_to_trend,
       ROUND(avg_duration_seconds / 60, 1) AS avg_duration_minutes,
       ROUND(hd_share * 100, 1)            AS hd_pct
FROM gold_region_daily_kpis
WHERE trending_date >= date_format(current_date - INTERVAL '30' DAY, '%Y-%m-%d')
ORDER BY trending_date DESC, region;
