WITH WeeklyTripData AS (
    SELECT
        DATE_TRUNC('week', date) AS week,
        COUNT(*) AS trip_count
    FROM public.bronze_trips
    WHERE 
        -- Include condition based on region or bounding box
        -- For region: UPPER(region) = UPPER('your_region') as the example below
		-- UPPER(region) = UPPER('prague')
        -- For bounding box: origin_latitude BETWEEN min_lat AND max_lat AND origin_longitude BETWEEN min_lon AND max_lon
		-- As the example below
	 	-- origin_latitude BETWEEN 10.07299025213017 AND 14.55782768207913 AND origin_longitude BETWEEN 45.06910822622201 AND 53.50847122244616
    GROUP BY week
)
SELECT
    week,
    ROUND(AVG(trip_count) OVER (),2) AS weekly_avg_trips
FROM WeeklyTripData;
