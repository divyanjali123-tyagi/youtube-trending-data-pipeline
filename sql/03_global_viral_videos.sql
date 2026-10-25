-- Videos trending in the most regions at once (global virality)
SELECT trending_date, title, channel_title, category_name,
       regions_count, array_join(regions, ', ') AS regions, max_views
FROM gold_cross_region_daily
WHERE regions_count >= 3
  AND trending_date >= date_format(current_date - INTERVAL '7' DAY, '%Y-%m-%d')
ORDER BY regions_count DESC, max_views DESC
LIMIT 50;
