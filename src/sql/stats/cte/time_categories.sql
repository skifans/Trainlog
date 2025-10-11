-- Categorize trips by time (past, plannedFuture, future)
SELECT *,
CASE
  WHEN is_project = false
    AND filtered_datetime IS NOT NULL
    AND NOW() > filtered_datetime
  THEN 1 ELSE 0
END AS is_past,
CASE
  WHEN is_project = false
    AND filtered_datetime IS NOT NULL
    AND NOW() <= filtered_datetime
  THEN 1 ELSE 0
END AS is_planned_future,
CASE
  WHEN is_project = false AND filtered_datetime IS NULL
  THEN 1 ELSE 0
END AS is_future,
CASE
  WHEN :user_id IS NULL THEN
    CASE
      WHEN COALESCE(
        EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
        manual_trip_duration,
        estimated_trip_duration,
        0
      ) < 0 
      OR COALESCE(
        EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
        manual_trip_duration,
        estimated_trip_duration,
        0
      ) > (10 * 24 * 60 * 60) -- 10 days in seconds
      THEN 0
      ELSE COALESCE(
        EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
        manual_trip_duration,
        estimated_trip_duration,
        0
      )
    END
  ELSE
    COALESCE(
      EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
      manual_trip_duration,
      estimated_trip_duration,
      0
    )
END AS trip_duration
FROM base_filter