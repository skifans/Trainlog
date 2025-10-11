{base_filter}
{time_categories}

SELECT 
    jsonb_build_array(
        LEAST(origin_station, destination_station), 
        GREATEST(origin_station, destination_station)
    )::text AS route,
    SUM(is_past) AS "pastTrips",
    SUM(is_planned_future) AS "plannedFutureTrips",
    SUM(is_future) AS "futureTrips",
    SUM(is_past + is_planned_future + is_future) AS "count",
    SUM(trip_length * is_past) AS "pastKm",
    SUM(trip_length * is_planned_future) AS "plannedFutureKm",
    SUM(trip_length * is_future) AS "futureKm"
FROM time_categories
GROUP BY LEAST(origin_station, destination_station), GREATEST(origin_station, destination_station)
ORDER BY "count" DESC
LIMIT 10;