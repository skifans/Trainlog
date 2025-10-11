{base_filter}
{time_categories}
{split_operators}

SELECT 
    operator,
    SUM(is_past) AS "pastTrips",
    SUM(is_planned_future) AS "plannedFutureTrips",
    SUM(is_past + is_planned_future) AS "totalTrips",
    SUM(trip_length * is_past) AS "pastKm",
    SUM(trip_length * is_planned_future) AS "plannedFutureKm",
    SUM(trip_length * (is_past + is_planned_future)) AS "totalKm"
FROM split_operators
GROUP BY operator
ORDER BY "totalTrips" DESC;