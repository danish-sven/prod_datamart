/*
Name: vehicle_master_daily_snapshot
Description: Vehicle transformation for clean ops usage
Authors: Jon Dabbs
  | Date       | Version | Description 
  |------------|---------|-------------
  | 2023-02-16 | 1.1     | Updated to reflect discussed changes to create vehicle master table
  | 2023-03-10 | 1.1     | Inclusion of Archived Bikes
  | 2023-03-14 | 1.1     | Update to customer uptime definition & historical operator count logic
  | 2023-03-22 | 1.1     | Exclusion of GetirNoMaintenance from productfamily, uptime, islive, VM
  | 2023-03-27 | 1.1     | Added vehicle assignment fields for Robert
  | 2023-07-14 | 1.2     | Added vehicle_assignment_id, latest_completed_wo and has_opened_wo - RY
  | 2023-07-18 | 1.2     | Added Benzina to Moped in Vehicle SKU - JD
  | 2023-07-18 | 1.2     | Updated customer uptime logic to prevent unneccessary filtering in Looker - JD
  | 2023-08-25 | 1.3     | Removed work order related metrics to reduce complexity & added previous b2x & customer - JD
  | 2023-09-19 | 1.4     | Updated sku logic to include all new vehicle types - JD
*/

WITH historic_vehicle_assignment AS (
  WITH vehicle_date_array as (
    SELECT vehicle_id, date 
        FROM(
            SELECT vehicle_id,
                MIN(CAST(created_at as date)) AS start_date,
                MAX(current_date()) AS end_date
            FROM `central-dev-f7c3.fleetio.vehicle_assignment_history` v
             GROUP BY 1
   )CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(start_date,end_date)) date)

, historicaL_operators as (

    SELECT vehicle_id
     , B.MY_DATE AS date
     , contact_id
     , contact_id != LAG(contact_id) OVER (ORDER BY contact_id, B.MY_DATE) as lag_contact_id
    FROM(
        SELECT vehicle_id
        , contact_id
        , CAST(created_at AS DATE) start_date
        , CASE WHEN v.CURRENT = TRUE THEN CURRENT_DATE()
            ELSE DATE_SUB(CAST(ended_at AS DATE), INTERVAL 1 DAY) END AS end_date
        FROM 
             `central-dev-f7c3.fleetio.vehicle_assignment_history` v
) 
    CROSS JOIN `central-dev-f7c3.date_dimensions.calendar` B
    WHERE DATE(B.MY_DATE) BETWEEN start_date AND end_date)

, filled_dates as (

    SELECT a.vehicle_id
     , a.date
     , b.contact_id
     , b.lag_contact_id
    FROM vehicle_date_array a
    LEFT JOIN historical_operators b on a.vehicle_id = b.vehicle_id and a.date = b.date)

    SELECT vehicle_id
     , date
     , SUM(historical_operator_count) as historical_operator_count
     , COUNT(date) as date_count 
    FROM(
        SELECT * EXCEPT(lag_contact_id)
        , 1 + COUNTIF(lag_contact_id) OVER (PARTITION BY vehicle_id ORDER BY date) AS historical_operator_count
        FROM filled_dates)
    GROUP BY 1,2
    HAVING COUNT(date) = 1)

, raw_vehicle AS (
SELECT
    CAST(V.date AS DATE) as date_snapshot
  , v.name as zid
  , v.id as vehicle_id
  , v.group_name as fleetio_group
  , v.group_id as fleetio_group_id
-- Location Data
  , COALESCE(l.group_ancestry, 'Unknown') AS fleetio_group_ancestry
  , COALESCE(l.site_owner, 'Unknown') AS site_owner
  , COALESCE(l.site, 'Unknown') AS site
  , COALESCE(l.city, 'Unknown') AS city
  , COALESCE(l.city_short, 'Unknown') AS city_short
  , COALESCE(l.state, 'Unknown') AS state
  , COALESCE(l.state_short, 'Unknown') AS state_short
  , COALESCE(l.supply_region, 'Unknown') AS supply_region
  , COALESCE(l.supply_region_short, 'Unknown') AS supply_region_short
  , l.is_launched AS city_is_launched
  , COALESCE(l.country, 'Unknown') AS country
  , COALESCE(l.country_short, 'Unknown') AS country_short
  , COALESCE(l.region, 'Unknown') AS region
  , COALESCE(l.region_short, 'Unknown') AS region_short
  , COALESCE(l.mega_region, 'Unknown') AS mega_region
  , COALESCE(l.mega_region_short, 'Unknown') AS mega_region_short
  , l.maintenance_type AS site_maintenance_type
  , l.timezone
  , CASE
      WHEN l.maintenance_type = 'Onsite' THEN CONCAT(l.country_short, ' - ', l.site_owner, ' - ', l.site)
      WHEN l.maintenance_type = 'Remote' THEN CONCAT(l.country_short, ' - ', l.site_owner, ' - Remote')
      ELSE CONCAT(l.country_short, ' - Other Sites')
      END AS location
  , v.original_location
  , CASE
      WHEN LEFT(v.contact_full_name, 5) = '[B2B]' THEN 'B2B'
      WHEN v.contact_full_name IS NOT NULL THEN 'B2C'
      ELSE 'Unassigned' 
      END AS b2x
  , CASE 
        WHEN LEFT(CAST(LAST_VALUE(contact_full_name IGNORE NULLS) 
        OVER (PARTITION BY name ORDER BY v.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS STRING), 5) = '[B2B]' THEN 'B2B'
        WHEN LAST_VALUE(contact_full_name IGNORE NULLS) 
        OVER (PARTITION BY name ORDER BY v.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) IS NOT NULL THEN 'B2C'
        ELSE 'Unassigned'
        END as b2x_previous
  , CASE
        WHEN LEFT(v.contact_full_name, 5) = '[B2B]' THEN b.Segment 
        WHEN v.contact_full_name IS NOT NULL AND v.historically_current = TRUE  THEN 'B2C'
        ELSE NULL END AS b2x_segment
  , CASE
      WHEN LEFT(v.contact_full_name, 5) = '[B2B]' THEN SPLIT(v.contact_full_name, ' | ')[SAFE_OFFSET(2)]
      WHEN v.contact_full_name IS NULL THEN NULL
      ELSE v.contact_full_name
      END AS customer
  , CASE 
        WHEN LEFT(CAST(LAST_VALUE(contact_full_name IGNORE NULLS) 
        OVER (PARTITION BY name ORDER BY v.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS STRING), 5) = '[B2B]' 
        THEN SPLIT(LAST_VALUE(contact_full_name IGNORE NULLS) 
        OVER (PARTITION BY name ORDER BY v.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), ' | ')[SAFE_OFFSET(2)]
        WHEN LAST_VALUE(contact_full_name IGNORE NULLS) 
        OVER (PARTITION BY name ORDER BY v.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) IS NULL THEN NULL
        ELSE LAST_VALUE(contact_full_name IGNORE NULLS) 
        OVER (PARTITION BY name ORDER BY v.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        END as customer_previous
  , CASE
      WHEN v.type_name in ('B2B|Rent', 'B2B|RTO', 'B2B|Buffer-Paid') 
      OR (v.contact_full_name LIKE '%[B2B]%' 
      AND (v.type_name <> 'B2B|Buffer-Unpaid' AND (v.type_name LIKE '%1-rent%' OR v.type_name LIKE '%2-rto%' OR v.type_name LIKE '%Buffer%')))
      THEN 'Rental'

      WHEN ((v.contact_full_name IN ('[B2B] | US | Getir', '[B2B] | UK | Getir', 
      '[B2B] | AU | MilkRun', '[B2B] | UK | Zapp', '[B2B] | US | Doordash') 
      AND (v.type_name LIKE '%3-buy%' 
      OR v.type_name LIKE '%2-own%'))) 
      OR v.type_name IN ('B2B|Buy-Maintenance', 'B2B|Maintenance')
      THEN 'Maintenance Contracts'

      WHEN (((v.contact_full_name IN ('[B2B] | US | Cornucopia', '[B2B] | UK | Upway') 
      AND (v.type_name LIKE '%3-buy%' OR v.type_name LIKE '%2-own%')))
      OR v.type_name='B2B|Buy')
      AND historical_operator_count > 1
      AND v.contact_full_name <> '[B2B] | UK | GetirNoMaintenance'
      AND current_date <= DATE_ADD(CAST(v.vehicle_assignment_created_at as date), INTERVAL 1 YEAR)
      THEN 'Used Outright Sale'

      WHEN (((v.contact_full_name IN ('[B2B] | US | Cornucopia', '[B2B] | UK | Upway') 
      AND (v.type_name LIKE '%3-buy%' OR v.type_name LIKE '%2-own%')))
      OR v.type_name='B2B|Buy')
      AND (historical_operator_count = 1 OR historical_operator_count IS NULL)
      AND v.contact_full_name <> '[B2B] | UK | GetirNoMaintenance'
      AND current_date <= DATE_ADD(CAST(v.vehicle_assignment_created_at as date), INTERVAL 1 YEAR)
      THEN 'New Outright Sale'

      WHEN (((v.contact_full_name IN ('[B2B] | US | Cornucopia', '[B2B] | UK | Upway') 
      AND (v.type_name LIKE '%3-buy%' OR v.type_name LIKE '%2-own%')))
      OR v.type_name='B2B|Buy')
      AND current_date > DATE_ADD(CAST(v.vehicle_assignment_created_at as date), INTERVAL 1 YEAR)
      THEN 'Expired Outright Sale'

      WHEN v.type_name = 'B2B|Buffer-Unpaid' THEN 'Unpaid Buffer'
      
      WHEN v.type_name = 'Default' THEN 'Not Allocated'

      ELSE 'B2C'

      END AS product_family
  , CASE
      WHEN v.type_name = 'B2B|Rent' THEN 'Rent'
      WHEN v.type_name = 'B2B|RTO'  THEN 'RTO'
      WHEN v.type_name = 'B2B|Buffer-Paid'  THEN 'Buffer'
      WHEN v.type_name = 'B2B|Buy'  THEN 'Sale'
      WHEN v.type_name = 'B2B|Buy-Maintenance'  THEN 'Sale & Maintenance Contract'
      WHEN v.type_name = 'B2B|Maintenance'  THEN 'Maintenance Only'
      ELSE 'Not Allocated' 
      END AS product
  , CASE
      WHEN v.make in ('Vmoto', 'Benzina','Super Soco') THEN 'Moped'
      WHEN v.make in ('EAV', 'Fulpra', 'Urban Arrow','Mubea','Iceni','Citkar','Vok','Radkutsche') THEN 'Cargo Bike'
      ELSE 'Bike' 
      END AS vehicle_sku
  , v.make
  , v.model
  , CASE
      WHEN v.make in ('Vmoto','Benzina','Super Soco','EAV', 'Fulpra', 'Urban Arrow','Mubea','Iceni','Citkar','Vok', 'Zoomo', 'ZERO','Radkutsche')
      THEN CONCAT(v.make, " ", v.model)
      ELSE 'Third Party Bike'
      END AS make_model
  , CONCAT(v.make, " ", v.model) AS make_model_detail
  , CASE 
      WHEN l.site = 'Investigation Group' 
      THEN TRUE ELSE FALSE 
      END AS is_in_investigation
  , CASE 
      WHEN v.vehicle_status_name IN ('S0. Staging', 'S2. On Inventory', 'S3. Assembled', 'S4. Quality Assured') 
      THEN 'Boxed Bikes'
      
      WHEN v.vehicle_status_name IN ('S5. Assembly Maintenance','S6. Freight Issue','S8. In Transfer', 'S11. Maintenance'
      , 'S22. Maintenance Customer', 'S24. Awaiting Pickup','S24. Awaiting Pick-up', 'S31. Needs Maintenance - B2B', 
      'S33. Long Term Maintenance','S23. External Maintenance', 'S39. Asset Quarantine')
      THEN 'In Maintenance'
      
      WHEN v.vehicle_status_name in ('S9. Fleet Available', 'S13. Reserved', 'S15. Sales (New)', 
      'S17. For Delivery', 'S32. Fleet Available - B2B')
      THEN 'Available Fleet'
      
      WHEN  v.vehicle_status_name in ('S10. Active', 'S12. Internal Use', 'S30. Works Well - B2B')
      THEN 'Active Fleet'

      WHEN v.vehicle_status_name in ('S34. Awaiting Decommission', 'S40. Overdue Investigation')
      THEN 'Pending Write Off'
      
      WHEN  v.vehicle_status_name in ('S7. DOA', 'S19. RMA', 'S19. Warranty Inquiry', 'S20. Part Redistribution', 
      'S21. Decommissioned')
      THEN 'Write Off Vehicles'
      
      WHEN v.vehicle_status_name = 'S14. In Recovery' 
      THEN 'In Recovery'
      
      WHEN v.vehicle_status_name = 'S1. In Transit'
      THEN 'Incoming Fleet'
      
      WHEN v.vehicle_status_name = 'S16. Sales (Used)'
      THEN 'Sales Allocation'

      WHEN v.vehicle_status_name = 'S18. Asset Sold'
      THEN 'Sold Vehicles'

      ELSE NULL END AS fleet_group
  , CASE 
      WHEN l.site = 'Investigation Group' 
      THEN 'Bikes in Investigation' 

      WHEN v.vehicle_status_name IN ('S0. Staging', 'S2. On Inventory', 'S3. Assembled', 'S4. Quality Assured','S5. Assembly Maintenance'
      ,'S6. Freight Issue','S8. In Transfer', 'S11. Maintenance' , 'S22. Maintenance Customer', 'S24. Awaiting Pickup',
      'S24. Awaiting Pick-up', 'S31. Needs Maintenance - B2B', 'S33. Long Term Maintenance','S23. External Maintenance', 
      'S39. Asset Quarantine', 'S1. In Transit','S9. Fleet Available', 'S13. Reserved', 'S15. Sales (New)', 'S16. Sales (Used)', 
      'S17. For Delivery', 'S32. Fleet Available - B2B', 'S16. Sales (Used)', 'S34. Awaiting Decommission')
      THEN 'Stock (Inventory)'
      
      WHEN  v.vehicle_status_name in ('S10. Active', 'S12. Internal Use', 'S30. Works Well - B2B')
      THEN 'Leased Asset'

      ELSE NULL END AS fleet_group_finance 
      -- for rob yang to confirm: null comprises: S14. In Recovery; S18. Asset Sold; S7. DOA'; S19. RMA; S19. Warranty Inquiry; 
      -- S20. Part Redistribution S21. Decommissioned
  , v.vehicle_status_name as fleetio_status
  , v.type_name as fleetio_type
  , v.archived_at as archived_at_utc
  , CASE 
        WHEN v.archived_at IS NOT NULL 
        THEN TRUE ELSE FALSE END AS is_archived
  , NULLIF(v.vin, '') as vin
  , NULLIF(v.supplier_serial_number,'') as frame_serial_number
  , NULLIF(v.iot_device_type, '') AS iot_device_type
  , NULLIF(v.iot_serial_number, '') AS iot_serial_number
  , v.historically_current as current_assignment
  , v.vehicle_assignment_created_at as vehicle_assignment_created_at_utc
  , v.vehicle_assignment_started_at as vehicle_assignment_started_at_utc
  , v.vehicle_assignment_ended_at as vehicle_assignment_ended_at_utc
  , v.vehicle_created_at as vehicle_created_at_utc
  , CASE
        WHEN v.historically_current = TRUE THEN v.contact_full_name
        WHEN v.historically_current = FALSE THEN 'Unassigned'
        ELSE NULL END as fleetio_operator
  , v.contact_id
  , IFNULL(historical_operator_count,0) AS historical_operator_count
  , assembly_date
  , CASE
     WHEN assembly_date IS NULL OR assembly_date > CURRENT_DATE() THEN NULL
     ELSE DATE_DIFF(CURRENT_DATE(), SAFE_CAST(assembly_date AS DATE),DAY)
     END AS vehicle_age_days
  , v.write_off_date
  , v.write_off_type
  , v.vehicle_assignment_id 
  , v.meter_unit
FROM
  `central-dev-f7c3.fleetio.vehicle_daily_snapshot` v
LEFT JOIN 
    historic_vehicle_assignment va
    ON v.id = va.vehicle_id and v.date = va.date
LEFT JOIN 
  `central-dev-f7c3.sheets.locations` l 
    ON v.group_id = l.group_id
LEFT JOIN 
    `central-dev-f7c3.sheets.b2b_customer_information_scheduled` b
        ON LOWER(SPLIT(v.contact_full_name, ' | ')[SAFE_OFFSET(2)]) = LOWER(b.Customer) AND l.country_short = b.Country
WHERE 1=1
  AND v.name <> 'Z123456'
  AND LOWER(v.name) NOT LIKE '%test%'
  AND lower(v.name) NOT LIKE '%shell%'
)

SELECT a.*
-- Boolean Fields using raw_vehicle data
     , CASE
        WHEN fleet_group IN ('Active Fleet')
        AND fleetio_type NOT IN ('B2B|Buffer-Paid', 'B2B|Buffer-Unpaid')
        AND customer <> 'GetirNoMaintenance'
        AND is_archived = FALSE
        AND current_assignment = TRUE
        AND left(zid, 3) <> 'ACJ'
        THEN TRUE ELSE FALSE
        END AS customer_uptime_numerator
     , CASE 
        WHEN fleet_group in ('Active Fleet','In Recovery','In Maintenance')
        AND fleetio_type NOT IN ('B2B|Buffer-Paid', 'B2B|Buffer-Unpaid')
        AND customer <> 'GetirNoMaintenance'
        AND is_archived = FALSE
        AND current_assignment = TRUE
        AND left(zid, 3) <> 'ACJ'
        THEN TRUE ELSE FALSE
        END AS customer_uptime_denominator
     , CASE
        WHEN fleetio_status IN ('S8. In Transfer','S9. Fleet Available','S10. Active', 'S12. Internal Use', 'S13. Reserved','S15. Sales (New)', 'S16. Sales (Used)',
         'S17. For Delivery','S24. Awaiting Pick-up','S30. Works Well - B2B','S32. Fleet Available - B2B')
        AND customer <> 'GetirNoMaintenance'
        AND left(zid, 3) <> 'ACJ'
        THEN TRUE ELSE FALSE
        END AS true_uptime_numerator
     , CASE 
        WHEN fleetio_status IN ('S8. In Transfer','S9. Fleet Available','S10. Active','S11. Maintenance','S12. Internal Use', 'S13. Reserved','S14. In Recovery',
        'S15. Sales (New)', 'S16. Sales (Used)', 'S17. For Delivery','S24. Awaiting Pick-up','S30. Works Well - B2B','S31. Needs Maintenance - B2B','S32. Fleet Available - B2B')
        AND customer <> 'GetirNoMaintenance'
        AND left(zid, 3) <> 'ACJ'
        THEN TRUE ELSE FALSE
        END AS true_uptime_denominator
     , CASE 
        WHEN fleet_group = 'Boxed Bikes'
        OR fleet_group = 'Available Fleet' 
        AND historical_operator_count IS NULL
        THEN TRUE ELSE FALSE
        END AS is_new_vehicle
     , CASE
        WHEN current_assignment = TRUE
        AND fleet_group NOT IN ('Write Off Vehicles','Sold Vehicles')
        AND customer <> 'GetirNoMaintenance'
        THEN TRUE
        ELSE FALSE END AS is_live
     , CASE
        WHEN fleet_group NOT IN ('Write Off Vehicles','Sold Vehicles')
        THEN TRUE
        ELSE FALSE END AS is_current_fleet
     , CASE
        WHEN b2x = 'B2B'
        AND current_assignment = TRUE
        AND product_family IN ('Rental', 'Maintenance Contracts', 'Used Outright Sale', 'New Outright Sale')
        AND fleet_group NOT IN ('Write Off Vehicles', 'Pending Write Off')
        THEN TRUE 
        WHEN current_assignment = TRUE
        AND  b2x <> 'B2B' THEN NULL
        ELSE FALSE
        END AS is_vehicles_moved_b2b -- to be sense checked vs VM

FROM raw_vehicle a
