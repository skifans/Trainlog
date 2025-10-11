{base_filter}
{time_categories}

, stations AS (
    SELECT 
        origin_station AS station, 
        is_past, 
        is_planned_future, 
        is_future,
        trip_length
    FROM time_categories
    UNION ALL
    SELECT 
        destination_station AS station, 
        is_past, 
        is_planned_future, 
        is_future,
        trip_length
    FROM time_categories
)
SELECT 
    station,
    SUM(is_past) AS "pastTrips",
    SUM(is_planned_future) AS "plannedFutureTrips",
    SUM(is_past + is_planned_future + is_future) AS "count",
    SUM(trip_length * is_past) AS "pastKm",
    SUM(trip_length * is_planned_future) AS "plannedFutureKm"
FROM stations
GROUP BY station
ORDER BY count DESC
LIMIT 10;