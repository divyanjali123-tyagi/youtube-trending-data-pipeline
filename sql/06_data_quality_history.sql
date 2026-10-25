-- Data quality check history: failures and warnings over the last 14 days
SELECT trending_date, ingest_hour, suite, check_name, severity, value, operator, threshold, checked_at
FROM dq_results
WHERE passed = false
  AND trending_date >= date_format(current_date - INTERVAL '14' DAY, '%Y-%m-%d')
ORDER BY checked_at DESC;
