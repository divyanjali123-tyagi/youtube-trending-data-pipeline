-- Top 5 channels per region by trending views on the most recent day
SELECT c.region, c.leaderboard_rank, c.channel_title, c.videos_trending, c.total_views, c.best_rank
FROM gold_channel_daily_leaderboard c
WHERE c.trending_date = (SELECT MAX(trending_date) FROM gold_channel_daily_leaderboard)
  AND c.leaderboard_rank <= 5
ORDER BY c.region, c.leaderboard_rank;
