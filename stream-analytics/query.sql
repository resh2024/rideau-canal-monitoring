SELECT
    System.Timestamp AS windowEnd,
    location,
    
    AVG(ice_thickness_cm) AS avgIceThickness,
    MIN(ice_thickness_cm) AS minIceThickness,
    MAX(ice_thickness_cm) AS maxIceThickness,

    AVG(surface_temp_c) AS avgSurfaceTemp,
    MIN(surface_temp_c) AS minSurfaceTemp,
    MAX(surface_temp_c) AS maxSurfaceTemp,

    MAX(snow_accumulation_cm) AS maxSnowAccumulation,
    AVG(external_temp_c) AS avgExternalTemp,

    COUNT(*) AS readingCount,

    CASE 
        WHEN AVG(ice_thickness_cm) >= 30 THEN 'Safe'
        WHEN AVG(ice_thickness_cm) >= 20 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetyStatus
INTO
    SensorAggregations
FROM
    rideaucanalhub TIMESTAMP BY timestamp
GROUP BY
    TUMBLINGWINDOW(minute, 5),
    location;