SELECT region, datasource, MAX(date) as latest_date
FROM public.silver_trips
WHERE region IN (SELECT region
                 FROM public.silver_trips
                 GROUP BY region
                 ORDER BY COUNT(*) DESC
                 LIMIT 2)
GROUP BY region, datasource
ORDER BY latest_date DESC
LIMIT 2
