-- Which categories dominate trending in each region (last 7 days)
SELECT region,
       category_name,
       SUM(video_count)                         AS videos,
       ROUND(AVG(share_of_videos) * 100, 2)     AS avg_share_pct,
       ROUND(AVG(avg_engagement_rate) * 100, 3) AS avg_engagement_pct
FROM gold_region_category_daily
WHERE trending_date >= date_format(current_date - INTERVAL '7' DAY, '%Y-%m-%d')
GROUP BY region, category_name
ORDER BY region, videos DESC;
