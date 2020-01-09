-- preview the final etl data
SELECT *
FROM smart_hub_data_catalog.etl_output_parquet
LIMIT 10;


-- total cost in $'s for each device, at location 'b6a8d42425fde548'
-- from high to low, on December 21, 2019
SELECT device,
       concat('$', cast(cast(sum(cost) / 100 AS decimal(10, 2)) AS varchar)) AS total_cost
FROM smart_hub_data_catalog.etl_tmp_output_parquet
WHERE loc_id = 'b6a8d42425fde548'
    AND date (cast(ts AS timestamp)) = date '2019-12-21'
GROUP BY device
ORDER BY total_cost DESC;


-- count of smart hub residential locations in Oregon and California,
-- grouped by zip code, sorted by count
SELECT DISTINCT postcode, upper(state), count(postcode) AS smart_hub_count
FROM smart_hub_data_catalog.smart_hub_locations_parquet
WHERE state IN ('or', 'ca')
    AND length(cast(postcode AS varchar)) >= 5
GROUP BY state, postcode
ORDER BY smart_hub_count DESC, postcode;


-- electrical usage for the clothes washer
-- over a 30-minute period, on December 21, 2019
SELECT ts, device, location, type, cost
FROM smart_hub_data_catalog.etl_tmp_output_parquet
WHERE loc_id = 'b6a8d42425fde548'
    AND device = 'Clothes Washer'
    AND cast(ts AS timestamp)
        BETWEEN timestamp '2019-12-21 08:45:00'
            AND timestamp '2019-12-21 09:15:00'
ORDER BY ts;
